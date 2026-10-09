//! Receives an uploaded file on the async side of the daemon.
//!
//! An upload arrives as a body stream on the HTTP runtime, and it can be
//! larger than memory, so it is written to disk as it arrives rather than
//! collected. Three things have to hold whatever the client does:
//!
//! - **The declared length is the length.** The client states it before
//!   sending anything, and admission decisions (size limits, free space) are
//!   made on that number. A body that runs past it is refused at the first
//!   extra byte instead of being allowed to fill the disk.
//! - **Nothing half-written is ever mistaken for an upload.** Bytes go to
//!   `<name>.part`; only a complete, synced file is renamed to its real name.
//! - **An abandoned upload cleans up after itself.** A client that
//!   disconnects, or a handler that bails out, simply drops the writer, and
//!   the `.part` file goes with it.
//!
//! The digest is computed while the bytes stream past, so knowing what was
//! uploaded never costs a second read of a multi-gigabyte file.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::blobstore::PutInfo;

/// Write buffer between the body stream and the file. HTTP bodies arrive in
/// small frames, and each tokio file write is a trip to the blocking pool.
const WRITE_BUFFER_BYTES: usize = 256 * 1024;

/// Streams an upload of a known length into `path`, atomically.
pub struct AsyncStagingWriter {
    path: PathBuf,
    part: PathBuf,
    file: Option<BufWriter<tokio::fs::File>>,
    expected: u64,
    written: u64,
    hasher: Sha256,
    committed: bool,
}

impl AsyncStagingWriter {
    /// Opens `<path>.part` for an upload that will be exactly `expected_len`
    /// bytes, creating the directory if needed.
    pub async fn create(path: impl Into<PathBuf>, expected_len: u64) -> Result<Self> {
        let path = path.into();
        let parent = path
            .parent()
            .context("a staging path needs a parent directory")?
            .to_path_buf();
        tokio::fs::create_dir_all(&parent)
            .await
            .with_context(|| format!("creating {}", parent.display()))?;

        let part = part_path(&path)?;
        let file = tokio::fs::File::create(&part)
            .await
            .with_context(|| format!("creating {}", part.display()))?;
        Ok(Self {
            path,
            part,
            file: Some(BufWriter::with_capacity(WRITE_BUFFER_BYTES, file)),
            expected: expected_len,
            written: 0,
            hasher: Sha256::new(),
            committed: false,
        })
    }

    /// Appends the next piece of the body.
    ///
    /// Refuses a piece that would take the upload past its declared length,
    /// before writing any of it.
    pub async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        let after = self.written + bytes.len() as u64;
        if after > self.expected {
            bail!(
                "the upload is longer than the {} bytes it declared",
                self.expected
            );
        }
        let file = self
            .file
            .as_mut()
            .context("the staging file is already closed")?;
        file.write_all(bytes)
            .await
            .with_context(|| format!("writing {}", self.part.display()))?;
        self.hasher.update(bytes);
        self.written = after;
        Ok(())
    }

    /// Bytes received so far.
    pub fn written(&self) -> u64 {
        self.written
    }

    /// Checks the upload is complete, syncs it and gives it its real name.
    pub async fn commit(mut self) -> Result<PutInfo> {
        if self.written != self.expected {
            bail!(
                "the upload ended after {} of its {} bytes",
                self.written,
                self.expected
            );
        }
        let mut file = self
            .file
            .take()
            .context("the staging file is already closed")?;
        file.flush()
            .await
            .with_context(|| format!("flushing {}", self.part.display()))?;
        let file = file.into_inner();
        file.sync_all()
            .await
            .with_context(|| format!("syncing {}", self.part.display()))?;
        drop(file);

        tokio::fs::rename(&self.part, &self.path)
            .await
            .with_context(|| format!("committing {}", self.path.display()))?;
        // From here the upload exists under its real name; the `.part` is gone
        // and must not be "cleaned up" by drop.
        self.committed = true;

        // The rename has to survive a crash too, or the job that reads the
        // upload could find its input missing after a reboot.
        let dir = self.path.parent().map(Path::to_path_buf);
        if let Some(dir) = dir {
            tokio::task::spawn_blocking(move || {
                std::fs::File::open(&dir).and_then(|d| d.sync_all())
            })
            .await
            .context("syncing the staging directory")?
            .context("syncing the staging directory")?;
        }

        Ok(PutInfo {
            bytes: self.written,
            sha256: hex::encode(std::mem::take(&mut self.hasher).finalize()),
        })
    }
}

impl Drop for AsyncStagingWriter {
    fn drop(&mut self) {
        if !self.committed {
            // Synchronous on purpose: drop cannot await, and unlinking a name
            // is a single cheap call. An open handle still being closed in the
            // background does not stop the name from going.
            let _ = std::fs::remove_file(&self.part);
        }
    }
}

/// `<path>.part`, kept in the same directory so the commit is a rename.
fn part_path(path: &Path) -> Result<PathBuf> {
    let name = path
        .file_name()
        .context("a staging path needs a file name")?;
    let mut part = name.to_os_string();
    part.push(".part");
    Ok(path.with_file_name(part))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blobstore::sha256_hex;
    use tempfile::TempDir;

    fn target(dir: &TempDir) -> PathBuf {
        dir.path().join("imports").join("01J").join("upload.mp4")
    }

    #[tokio::test]
    async fn a_complete_upload_is_committed_under_its_name() {
        let dir = TempDir::new().unwrap();
        let path = target(&dir);

        let mut writer = AsyncStagingWriter::create(&path, 11).await.unwrap();
        writer.write(b"hello ").await.unwrap();
        writer.write(b"world").await.unwrap();
        assert_eq!(writer.written(), 11);
        let info = writer.commit().await.unwrap();

        assert_eq!(info.bytes, 11);
        assert_eq!(info.sha256, sha256_hex(b"hello world"));
        assert_eq!(std::fs::read(&path).unwrap(), b"hello world");
        assert!(!part_path(&path).unwrap().exists());
    }

    #[tokio::test]
    async fn bytes_past_the_declared_length_are_refused() {
        let dir = TempDir::new().unwrap();
        let path = target(&dir);

        let mut writer = AsyncStagingWriter::create(&path, 4).await.unwrap();
        writer.write(b"abc").await.unwrap();
        let err = writer.write(b"de").await.unwrap_err();

        assert!(err.to_string().contains("longer than"), "{err}");
        assert_eq!(writer.written(), 3, "none of the refused piece is kept");
    }

    #[tokio::test]
    async fn a_short_upload_is_not_committed() {
        let dir = TempDir::new().unwrap();
        let path = target(&dir);

        let mut writer = AsyncStagingWriter::create(&path, 10).await.unwrap();
        writer.write(b"abc").await.unwrap();
        let err = writer.commit().await.unwrap_err();

        assert!(err.to_string().contains("3 of its 10"), "{err}");
        assert!(!path.exists(), "an incomplete upload must never get its name");
        assert!(
            !part_path(&path).unwrap().exists(),
            "and its partial file must be gone"
        );
    }

    #[tokio::test]
    async fn an_abandoned_upload_removes_its_partial_file() {
        // What a client disconnect looks like from here: the handler's body
        // stream errors and the writer is dropped mid-upload.
        let dir = TempDir::new().unwrap();
        let path = target(&dir);

        let mut writer = AsyncStagingWriter::create(&path, 1_000).await.unwrap();
        writer.write(&[7u8; 500]).await.unwrap();
        assert!(part_path(&path).unwrap().exists());
        drop(writer);

        assert!(!part_path(&path).unwrap().exists());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn an_empty_upload_of_declared_length_zero_commits() {
        let dir = TempDir::new().unwrap();
        let path = target(&dir);
        let info = AsyncStagingWriter::create(&path, 0)
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        assert_eq!(info.bytes, 0);
        assert!(path.exists());
    }

    #[test]
    fn the_partial_file_sits_beside_the_upload() {
        let path = Path::new("/data/imports/01J/upload.mp4");
        assert_eq!(
            part_path(path).unwrap(),
            Path::new("/data/imports/01J/upload.mp4.part")
        );
    }
}

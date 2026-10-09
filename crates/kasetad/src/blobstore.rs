//! Blob storage adapters.
//!
//! Everything Kaseta persists goes through [`BlobStore`], addressed by a
//! storage-agnostic [`BlobKey`]. The local adapter writes under a root
//! directory using the key verbatim as a relative path, so the on-disk tree and
//! an S3/R2 bucket have byte-identical layouts and syncing between them is a
//! copy, not a translation.
//!
//! Writes are atomic: a blob appears at its key complete or not at all. A
//! recording interrupted by a crash therefore never leaves a torn chunk that
//! would later be read as valid audio.
//!
//! Media objects can be gigabytes, so anything that may be large moves as a
//! stream: [`BlobStore::open`] reads without loading, [`PendingBlob`] and
//! [`BlobStore::put_file`] commit a file written beside its key, and
//! [`BlobStore::copy_file`] brings in a file from elsewhere while hashing it.
//! `get` and `put` remain for small objects such as manifests.

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use kaseta_contracts::BlobKey;
use sha2::{Digest, Sha256};

/// Content addressing for integrity checks. Chunks are verified on read and
/// before upload, so a bit-flip on disk surfaces as an error rather than as
/// corrupt audio in a transcript.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Size of the buffer large objects are streamed through.
///
/// Big enough that hashing and copying are not dominated by system calls,
/// small enough that it is irrelevant next to anything else in memory.
const STREAM_BUFFER_BYTES: usize = 256 * 1024;

/// Hashes everything `reader` yields, without holding more than one buffer.
///
/// Returns the byte count with the digest, because every caller that needs one
/// needs the other: an upload declares both, and a copy is checked on both.
pub fn sha256_reader(mut reader: impl Read) -> Result<(u64, String)> {
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; STREAM_BUFFER_BYTES];
    let mut total = 0u64;
    loop {
        let n = match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e).context("reading while hashing"),
        };
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    Ok((total, hex::encode(hasher.finalize())))
}

/// What a streamed write committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutInfo {
    pub bytes: u64,
    pub sha256: String,
}

pub trait BlobStore: Send + Sync {
    fn put(&self, key: &BlobKey, bytes: &[u8]) -> Result<()>;
    fn get(&self, key: &BlobKey) -> Result<Vec<u8>>;
    fn exists(&self, key: &BlobKey) -> Result<bool>;
    fn delete(&self, key: &BlobKey) -> Result<()>;
    /// Byte size without reading the object.
    fn size(&self, key: &BlobKey) -> Result<u64>;

    /// Writes only if the key is currently absent, atomically.
    ///
    /// Returns whether this call created the object. Implementations must make
    /// the absence check and the write a single indivisible step; a
    /// check-then-write is not sufficient, because two writers would both
    /// observe absence and the later one would silently replace the earlier.
    fn put_if_absent(&self, key: &BlobKey, bytes: &[u8]) -> Result<bool>;

    /// Reads a byte range, without loading the whole object.
    ///
    /// Media playback seeks, and a long recording's export is hundreds of
    /// megabytes; serving a seek by reading the entire object would put that
    /// much in memory per request.
    fn get_range(&self, key: &BlobKey, offset: u64, len: u64) -> Result<Vec<u8>>;

    /// Every key beneath `prefix`, in lexicographic order.
    ///
    /// Ordering matters: the key layout is designed so that sorting yields
    /// chronological order for recordings and capture order for chunks.
    fn list_prefix(&self, prefix: &str) -> Result<Vec<BlobKey>>;

    /// Writes only if absent, or verifies the existing object matches.
    ///
    /// Chunk writes are retried after crashes and re-uploaded on resume, so this
    /// makes those paths idempotent. A key that already holds *different* bytes
    /// is a bug — two recordings colliding, or a corrupted store — and is
    /// reported rather than silently overwritten.
    fn put_idempotent(&self, key: &BlobKey, bytes: &[u8]) -> Result<PutOutcome> {
        if self.put_if_absent(key, bytes)? {
            return Ok(PutOutcome::Written);
        }
        // The key was taken. Comparing only now, rather than before writing,
        // keeps the decision atomic: nothing between the check and the write
        // can change the outcome.
        let existing = self.get(key)?;
        if existing == bytes {
            Ok(PutOutcome::AlreadyPresent)
        } else {
            bail!(
                "blob {key} already exists with different content \
                 (existing sha256 {}, incoming {})",
                sha256_hex(&existing),
                sha256_hex(bytes)
            )
        }
    }

    /// Reads and verifies against an expected digest.
    fn get_verified(&self, key: &BlobKey, expected_sha256: &str) -> Result<Vec<u8>> {
        let bytes = self.get(key)?;
        let actual = sha256_hex(&bytes);
        if actual != expected_sha256 {
            bail!("blob {key} failed integrity check: expected {expected_sha256}, got {actual}");
        }
        Ok(bytes)
    }

    /// Reads an object as a stream.
    ///
    /// `get` loads the whole object, which is right for a manifest and wrong for
    /// an hour of video; anything that may be large is read through this.
    fn open(&self, key: &BlobKey) -> Result<Box<dyn Read + Send>>;

    /// The object as a local file, for callers that need to seek: range
    /// requests and multipart uploads read parts from arbitrary offsets.
    ///
    /// Only a store backed by the local filesystem can offer this, so the
    /// default refuses rather than pretending.
    fn open_file(&self, key: &BlobKey) -> Result<File> {
        bail!("blob {key} is not held in a local file by this store")
    }

    /// The directory objects live under, when the store is local.
    fn local_root(&self) -> Option<&Path> {
        None
    }

    /// A fresh, unique path beside `key`'s location, on the same filesystem.
    ///
    /// Output that is too large to assemble in memory is written here and then
    /// committed with [`BlobStore::put_file`]. Being a sibling is what makes the
    /// commit a rename rather than a copy. The name ends in `.tmp`, so a listing
    /// never mistakes an unfinished file for an object.
    fn temp_path_for(&self, key: &BlobKey) -> Result<PathBuf>;

    /// Moves a freshly written file into place as `key`, atomically.
    ///
    /// The file is flushed to disk before its name is published, so a crash
    /// leaves either the previous object or the whole new one. `tmp` is
    /// consumed. Meant for files this process just wrote, normally at
    /// [`BlobStore::temp_path_for`]; a file that must be kept is brought in with
    /// [`BlobStore::copy_file`] instead.
    fn put_file(&self, key: &BlobKey, tmp: &Path) -> Result<PutInfo>;

    /// Copies `src` into the store as `key`, verifying it on the way.
    ///
    /// The digest is computed while the bytes are copied and compared with
    /// `expected_sha256`, so the copy is proven equal to what the caller knows
    /// the file to be without reading either side twice. A destination that
    /// already holds those bytes is left alone, which makes a retried copy
    /// free. `src` is never modified.
    fn copy_file(&self, src: &Path, key: &BlobKey, expected_sha256: &str) -> Result<PutInfo>;
}

/// An object being written beside its key, committed in one step.
///
/// Encoders write exports through this rather than into a `Vec`: the bytes go
/// to disk as they are produced, and the object appears at its key only when
/// [`PendingBlob::commit`] succeeds. Dropped without a commit, for example
/// because encoding failed halfway, it removes its file, so a failure leaves
/// neither a torn object nor a stray temporary.
pub struct PendingBlob<'a> {
    store: &'a dyn BlobStore,
    key: BlobKey,
    path: PathBuf,
    file: Option<BufWriter<File>>,
}

impl<'a> PendingBlob<'a> {
    pub fn create(store: &'a dyn BlobStore, key: &BlobKey) -> Result<Self> {
        let path = store.temp_path_for(key)?;
        let file = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        Ok(Self {
            store,
            key: key.clone(),
            path,
            file: Some(BufWriter::with_capacity(STREAM_BUFFER_BYTES, file)),
        })
    }

    /// The writer for the object's bytes. Seekable, so a format whose header
    /// is only known at the end can patch it.
    pub fn writer(&mut self) -> &mut BufWriter<File> {
        self.file
            .as_mut()
            .expect("a pending blob holds its file until it is committed")
    }

    /// Flushes, syncs and moves the file into place.
    pub fn commit(mut self) -> Result<PutInfo> {
        let writer = self
            .file
            .take()
            .expect("a pending blob holds its file until it is committed");
        let file = writer
            .into_inner()
            .map_err(|e| anyhow::anyhow!("flushing {}: {}", self.path.display(), e.error()))?;
        file.sync_all()
            .with_context(|| format!("syncing {}", self.path.display()))?;
        drop(file);
        self.store.put_file(&self.key, &self.path)
    }
}

impl Drop for PendingBlob<'_> {
    fn drop(&mut self) {
        // After a commit the file has been renamed away and this finds nothing,
        // which is fine: the removal only matters when the commit never came.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Makes a rename or link inside `dir` durable.
///
/// Syncing a file persists its contents, not the directory entry that names
/// it. Without this, a crash shortly after a commit can lose the new name and
/// with it the object, even though its bytes reached the disk.
fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("syncing directory {}", dir.display()))
}

/// Whether a rename failed only because source and destination are on
/// different filesystems, which a copy can still satisfy.
fn crosses_devices(e: &std::io::Error) -> bool {
    #[cfg(target_os = "linux")]
    {
        e.raw_os_error() == Some(libc::EXDEV)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = e;
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Written,
    AlreadyPresent,
}

/// Stores blobs beneath a root directory on the local filesystem.
pub struct LocalFsStore {
    root: PathBuf,
}

impl LocalFsStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)
            .with_context(|| format!("creating blob root {}", root.display()))?;
        // Resolve symlinks and `..` once, so `resolve` can assert containment
        // against a real path.
        let root = root
            .canonicalize()
            .with_context(|| format!("canonicalising blob root {}", root.display()))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Writes `bytes` to a temporary sibling of `path` and returns its location.
    ///
    /// The name is unique per call, not per content. Naming it by digest alone
    /// would let two writers staging *identical* bytes share one temp file, so
    /// whichever finished first would delete the file the other was about to
    /// commit.
    ///
    /// The file is synced before it is returned. A rename publishes a name, not
    /// the data behind it, so committing an unsynced file can survive a crash
    /// as a correctly named object full of zeroes.
    fn staged(&self, path: &Path, bytes: &[u8]) -> Result<PathBuf> {
        let parent = path
            .parent()
            .context("blob path unexpectedly has no parent")?;
        let tmp = parent.join(format!(
            ".{}.{}.{}.tmp",
            std::process::id(),
            next_temp_id(),
            &sha256_hex(bytes)[..16]
        ));
        let mut file = File::create(&tmp).with_context(|| format!("writing {}", tmp.display()))?;
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .with_context(|| format!("writing {}", tmp.display()))
            .inspect_err(|_| {
                let _ = std::fs::remove_file(&tmp);
            })?;
        Ok(tmp)
    }

    /// Creates the directory a key's object lives in and returns it.
    fn parent_of(&self, path: &Path) -> Result<PathBuf> {
        let parent = path
            .parent()
            .context("blob path unexpectedly has no parent")?;
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
        Ok(parent.to_path_buf())
    }

    /// Copies `src` to a fresh temporary beside `path`, hashing as it goes,
    /// and syncs it. The caller decides whether the result may be committed.
    fn copy_to_temp(&self, src: &Path, path: &Path) -> Result<(PathBuf, PutInfo)> {
        let parent = path
            .parent()
            .context("blob path unexpectedly has no parent")?;
        let tmp = parent.join(format!(".{}.{}.tmp", std::process::id(), next_temp_id()));

        let copied = (|| -> Result<PutInfo> {
            let mut input =
                File::open(src).with_context(|| format!("opening {}", src.display()))?;
            let mut output =
                File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
            let mut hasher = Sha256::new();
            let mut buf = vec![0u8; STREAM_BUFFER_BYTES];
            let mut total = 0u64;
            loop {
                let n = match input.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e).with_context(|| format!("reading {}", src.display())),
                };
                hasher.update(&buf[..n]);
                output
                    .write_all(&buf[..n])
                    .with_context(|| format!("writing {}", tmp.display()))?;
                total += n as u64;
            }
            output
                .sync_all()
                .with_context(|| format!("syncing {}", tmp.display()))?;
            Ok(PutInfo {
                bytes: total,
                sha256: hex::encode(hasher.finalize()),
            })
        })();

        match copied {
            Ok(info) => Ok((tmp, info)),
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                Err(e)
            }
        }
    }

    /// Maps a key to an absolute path, refusing anything that would escape the
    /// root.
    ///
    /// `BlobKey` already rejects traversal segments on construction; this is a
    /// second, independent check so a future key source cannot turn into a path
    /// traversal.
    fn resolve(&self, key: &BlobKey) -> Result<PathBuf> {
        let path = self.root.join(key.as_str());
        let cleaned: PathBuf = path
            .components()
            .filter(|c| !matches!(c, std::path::Component::CurDir))
            .collect();
        if !cleaned.starts_with(&self.root) {
            bail!("blob key {key} resolves outside the store root");
        }
        if cleaned
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            bail!("blob key {key} contains a traversal component");
        }
        Ok(cleaned)
    }
}

impl BlobStore for LocalFsStore {
    fn put(&self, key: &BlobKey, bytes: &[u8]) -> Result<()> {
        let path = self.resolve(key)?;
        let parent = path
            .parent()
            .context("blob path unexpectedly has no parent")?;
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;

        let tmp = self.staged(&path, bytes)?;
        // `rename` within a directory is atomic, so a reader sees either no blob
        // or a whole one. This variant deliberately replaces an existing blob.
        if let Err(e) = std::fs::rename(&tmp, &path) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e).with_context(|| format!("committing {}", path.display()));
        }
        sync_dir(parent)
    }

    fn put_if_absent(&self, key: &BlobKey, bytes: &[u8]) -> Result<bool> {
        let path = self.resolve(key)?;
        let parent = path
            .parent()
            .context("blob path unexpectedly has no parent")?;
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;

        let tmp = self.staged(&path, bytes)?;

        // `hard_link` fails with AlreadyExists rather than replacing, which is
        // the atomic create-only commit `rename` cannot express. The link and
        // the temp file share an inode, so the temp is then just an extra name
        // to drop.
        let created = match std::fs::hard_link(&tmp, &path) {
            Ok(()) => true,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(e).with_context(|| format!("committing {}", path.display()));
            }
        };
        let _ = std::fs::remove_file(&tmp);
        if created {
            sync_dir(parent)?;
        }
        Ok(created)
    }

    fn get(&self, key: &BlobKey) -> Result<Vec<u8>> {
        let path = self.resolve(key)?;
        std::fs::read(&path).with_context(|| format!("reading blob {key}"))
    }

    fn exists(&self, key: &BlobKey) -> Result<bool> {
        Ok(self.resolve(key)?.is_file())
    }

    fn delete(&self, key: &BlobKey) -> Result<()> {
        let path = self.resolve(key)?;
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            // Deleting an absent blob is success: retention sweeps and failed
            // uploads both re-run.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("deleting blob {key}")),
        }
    }

    fn size(&self, key: &BlobKey) -> Result<u64> {
        let path = self.resolve(key)?;
        Ok(std::fs::metadata(&path)
            .with_context(|| format!("stat blob {key}"))?
            .len())
    }

    fn get_range(&self, key: &BlobKey, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let path = self.resolve(key)?;
        let mut file =
            std::fs::File::open(&path).with_context(|| format!("opening blob {key}"))?;
        file.seek(SeekFrom::Start(offset))
            .with_context(|| format!("seeking blob {key}"))?;

        let mut buf = vec![0u8; len as usize];
        let mut filled = 0usize;
        // A short read is normal at end of file; the caller asked for at most
        // `len`, not exactly `len`.
        while filled < buf.len() {
            match file.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e).with_context(|| format!("reading blob {key}")),
            }
        }
        buf.truncate(filled);
        Ok(buf)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<BlobKey>> {
        let root = self.root.join(prefix);
        let mut keys = Vec::new();
        collect_keys(&root, &self.root, &mut keys)?;
        keys.sort();
        Ok(keys)
    }

    fn open(&self, key: &BlobKey) -> Result<Box<dyn Read + Send>> {
        Ok(Box::new(self.open_file(key)?))
    }

    fn open_file(&self, key: &BlobKey) -> Result<File> {
        let path = self.resolve(key)?;
        File::open(&path).with_context(|| format!("opening blob {key}"))
    }

    fn local_root(&self) -> Option<&Path> {
        Some(&self.root)
    }

    fn temp_path_for(&self, key: &BlobKey) -> Result<PathBuf> {
        let path = self.resolve(key)?;
        let parent = self.parent_of(&path)?;
        Ok(parent.join(format!(".{}.{}.tmp", std::process::id(), next_temp_id())))
    }

    fn put_file(&self, key: &BlobKey, tmp: &Path) -> Result<PutInfo> {
        let path = self.resolve(key)?;
        let parent = self.parent_of(&path)?;

        // Synced before the rename for the same reason `staged` syncs: the name
        // must never become visible ahead of the bytes.
        let file = File::open(tmp).with_context(|| format!("opening {}", tmp.display()))?;
        file.sync_all()
            .with_context(|| format!("syncing {}", tmp.display()))?;
        let (bytes, sha256) =
            sha256_reader(&file).with_context(|| format!("hashing {}", tmp.display()))?;
        drop(file);

        match std::fs::rename(tmp, &path) {
            Ok(()) => {}
            // A file written somewhere else cannot be renamed across
            // filesystems; copying it is slower but just as atomic.
            Err(e) if crosses_devices(&e) => {
                let (copy, info) = self.copy_to_temp(tmp, &path)?;
                if info.sha256 != sha256 {
                    let _ = std::fs::remove_file(&copy);
                    bail!("{} changed while it was being stored as {key}", tmp.display());
                }
                if let Err(e) = std::fs::rename(&copy, &path) {
                    let _ = std::fs::remove_file(&copy);
                    return Err(e).with_context(|| format!("committing {}", path.display()));
                }
                let _ = std::fs::remove_file(tmp);
            }
            Err(e) => return Err(e).with_context(|| format!("committing {}", path.display())),
        }
        sync_dir(&parent)?;
        Ok(PutInfo { bytes, sha256 })
    }

    fn copy_file(&self, src: &Path, key: &BlobKey, expected_sha256: &str) -> Result<PutInfo> {
        let path = self.resolve(key)?;
        let parent = self.parent_of(&path)?;

        // A retry after a crash finds the earlier copy already in place. The
        // size check is free and rules out most mismatches before the hash
        // has to read anything.
        if let (Ok(existing), Ok(source)) = (std::fs::metadata(&path), std::fs::metadata(src)) {
            if existing.is_file() && existing.len() == source.len() {
                let file =
                    File::open(&path).with_context(|| format!("opening blob {key}"))?;
                let (bytes, sha256) = sha256_reader(file)?;
                if sha256 == expected_sha256 {
                    return Ok(PutInfo { bytes, sha256 });
                }
            }
        }

        let (tmp, info) = self.copy_to_temp(src, &path)?;
        if info.sha256 != expected_sha256 {
            let _ = std::fs::remove_file(&tmp);
            bail!(
                "{} does not match its expected digest (expected {expected_sha256}, read {})",
                src.display(),
                info.sha256
            );
        }
        if let Err(e) = std::fs::rename(&tmp, &path) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e).with_context(|| format!("committing {}", path.display()));
        }
        sync_dir(&parent)?;
        Ok(info)
    }
}

/// A per-process counter that makes every temporary name unique.
///
/// Unique per call, not per content: two writers staging identical bytes must
/// not share a file, or whichever finished first would delete the one the
/// other was about to commit.
fn next_temp_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Walks `dir`, appending every file as a key relative to `root`.
fn collect_keys(dir: &Path, root: &Path, out: &mut Vec<BlobKey>) -> Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        // An absent prefix is an empty listing, matching object-store semantics.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("listing {}", dir.display())),
    };

    for entry in entries {
        let path = entry.with_context(|| format!("reading {}", dir.display()))?.path();
        if path.is_dir() {
            collect_keys(&path, root, out)?;
            continue;
        }
        let Ok(relative) = path.strip_prefix(root) else {
            continue;
        };
        let Some(as_str) = relative.to_str() else {
            continue;
        };
        // Staged temporaries are not objects; they are never visible as keys.
        if as_str.ends_with(".tmp") {
            continue;
        }
        if let Ok(key) = BlobKey::new(as_str) {
            out.push(key);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn store() -> (TempDir, LocalFsStore) {
        let dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        (dir, store)
    }

    fn key(s: &str) -> BlobKey {
        BlobKey::new(s).unwrap()
    }

    #[test]
    fn round_trips_a_blob_through_nested_prefixes() {
        let (_dir, store) = store();
        let k = key("recordings/2026/07/25/x/tracks/a_local-mic_01/000000.flac");

        assert!(!store.exists(&k).unwrap());
        store.put(&k, b"audio bytes").unwrap();

        assert!(store.exists(&k).unwrap());
        assert_eq!(store.get(&k).unwrap(), b"audio bytes");
        assert_eq!(store.size(&k).unwrap(), 11);
    }

    #[test]
    fn writes_leave_no_temporary_files_behind() {
        let (dir, store) = store();
        store.put(&key("a/b/c.flac"), b"x").unwrap();

        // Match on the file name only: the enclosing temp directory is itself
        // named `.tmpXXXX`, so matching the full path always "finds" one.
        let leftovers: Vec<_> = walk(dir.path())
            .into_iter()
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(".tmp"))
            })
            .collect();
        assert!(leftovers.is_empty(), "found temp files: {leftovers:?}");
    }

    #[test]
    fn re_putting_identical_bytes_is_a_no_op() {
        let (_dir, store) = store();
        let k = key("a/b.flac");

        assert_eq!(store.put_idempotent(&k, b"same").unwrap(), PutOutcome::Written);
        assert_eq!(
            store.put_idempotent(&k, b"same").unwrap(),
            PutOutcome::AlreadyPresent
        );
    }

    #[test]
    fn re_putting_different_bytes_is_refused_rather_than_overwriting() {
        let (_dir, store) = store();
        let k = key("a/b.flac");
        store.put(&k, b"original").unwrap();

        let err = store.put_idempotent(&k, b"different").unwrap_err();
        assert!(err.to_string().contains("different content"));
        assert_eq!(
            store.get(&k).unwrap(),
            b"original",
            "the existing blob must survive a rejected write"
        );
    }

    #[test]
    fn detects_corruption_on_verified_read() {
        let (_dir, store) = store();
        let k = key("a/b.flac");
        store.put(&k, b"good").unwrap();
        let digest = sha256_hex(b"good");

        assert_eq!(store.get_verified(&k, &digest).unwrap(), b"good");

        // Simulate a bit-flip underneath the store.
        store.put(&k, b"bad!").unwrap();
        let err = store.get_verified(&k, &digest).unwrap_err();
        assert!(err.to_string().contains("integrity check"));
    }

    #[test]
    fn put_if_absent_reports_whether_it_created_the_object() {
        let (_dir, store) = store();
        let k = key("a/b.flac");

        assert!(store.put_if_absent(&k, b"first").unwrap());
        assert!(
            !store.put_if_absent(&k, b"second").unwrap(),
            "a taken key must not be reported as created"
        );
        assert_eq!(
            store.get(&k).unwrap(),
            b"first",
            "create-only must never replace existing bytes"
        );
    }

    #[test]
    fn a_failed_create_leaves_no_temporary_behind() {
        let (dir, store) = store();
        let k = key("a/b.flac");
        store.put_if_absent(&k, b"first").unwrap();
        store.put_if_absent(&k, b"second").unwrap();

        let leftovers: Vec<_> = walk(dir.path())
            .into_iter()
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(".tmp"))
            })
            .collect();
        assert!(leftovers.is_empty(), "found temp files: {leftovers:?}");
    }

    #[test]
    fn every_staged_temporary_is_unique_even_for_identical_bytes() {
        // Two writers staging the same bytes must not share a temp file: one
        // would delete the file the other was about to commit.
        let (_dir, store) = store();
        let path = store.resolve(&key("a/b.flac")).unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        let one = store.staged(&path, b"same").unwrap();
        let two = store.staged(&path, b"same").unwrap();
        let other = store.staged(&path, b"different").unwrap();

        assert_ne!(one, two, "identical content must still stage separately");
        assert_ne!(one, other);
        assert_eq!(std::fs::read(&one).unwrap(), b"same");
        assert_eq!(std::fs::read(&two).unwrap(), b"same");
    }

    #[test]
    fn deleting_an_absent_blob_succeeds() {
        let (_dir, store) = store();
        assert!(store.delete(&key("never/written.flac")).is_ok());
    }

    #[test]
    fn keys_cannot_escape_the_store_root() {
        let (dir, store) = store();
        // BlobKey rejects traversal at construction, so reach past it to prove
        // the store has its own independent guard.
        let escaping: BlobKey = serde_json::from_str(r#""a/b""#).unwrap();
        assert!(store.resolve(&escaping).unwrap().starts_with(dir.path()));

        assert!(BlobKey::new("../../etc/passwd").is_err());
        assert!(BlobKey::new("a/../../etc/passwd").is_err());
    }

    fn temporaries(dir: &Path) -> Vec<PathBuf> {
        walk(dir)
            .into_iter()
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(".tmp"))
            })
            .collect()
    }

    #[test]
    fn hashing_a_stream_matches_hashing_the_bytes() {
        let bytes: Vec<u8> = (0..(STREAM_BUFFER_BYTES * 2 + 17)).map(|i| i as u8).collect();
        let (len, digest) = sha256_reader(&bytes[..]).unwrap();
        assert_eq!(len, bytes.len() as u64);
        assert_eq!(digest, sha256_hex(&bytes));
        assert_eq!(sha256_reader(&b""[..]).unwrap().1, sha256_hex(b""));
    }

    #[test]
    fn an_object_can_be_read_as_a_stream_and_as_a_seekable_file() {
        use std::io::{Seek, SeekFrom};
        let (dir, store) = store();
        let k = key("a/b.bin");
        store.put(&k, b"0123456789").unwrap();

        let mut streamed = Vec::new();
        store.open(&k).unwrap().read_to_end(&mut streamed).unwrap();
        assert_eq!(streamed, b"0123456789");

        let mut file = store.open_file(&k).unwrap();
        file.seek(SeekFrom::Start(6)).unwrap();
        let mut tail = String::new();
        file.read_to_string(&mut tail).unwrap();
        assert_eq!(tail, "6789");

        assert_eq!(
            store.local_root().unwrap(),
            dir.path().canonicalize().unwrap()
        );
        assert!(store.open(&key("a/missing.bin")).is_err());
    }

    #[test]
    fn a_temporary_sits_beside_its_key_and_is_never_listed() {
        let (_dir, store) = store();
        let k = key("recordings/x/exports/mixed.flac");
        let one = store.temp_path_for(&k).unwrap();
        let two = store.temp_path_for(&k).unwrap();

        assert_ne!(one, two, "every caller gets its own file");
        assert_eq!(
            one.parent(),
            store.resolve(&k).unwrap().parent(),
            "a sibling, so committing it is a rename on one filesystem"
        );
        std::fs::write(&one, b"half written").unwrap();
        assert!(
            store.list_prefix("recordings/x").unwrap().is_empty(),
            "an unfinished file must not look like an object"
        );
    }

    #[test]
    fn put_file_moves_a_written_file_into_place() {
        let (dir, store) = store();
        let k = key("a/export.flac");
        let tmp = store.temp_path_for(&k).unwrap();
        std::fs::write(&tmp, b"encoded audio").unwrap();

        let info = store.put_file(&k, &tmp).unwrap();

        assert_eq!(
            info,
            PutInfo {
                bytes: 13,
                sha256: sha256_hex(b"encoded audio")
            }
        );
        assert_eq!(store.get(&k).unwrap(), b"encoded audio");
        assert!(!tmp.exists(), "the temporary is consumed");
        assert!(temporaries(dir.path()).is_empty());
    }

    #[test]
    fn a_failed_put_file_publishes_nothing() {
        let (_dir, store) = store();
        let k = key("a/export.flac");
        let missing = store.temp_path_for(&k).unwrap();

        assert!(store.put_file(&k, &missing).is_err());
        assert!(!store.exists(&k).unwrap(), "no object may appear on failure");
    }

    #[test]
    fn put_file_replaces_an_earlier_object_whole() {
        let (_dir, store) = store();
        let k = key("a/export.flac");
        store.put(&k, b"old export").unwrap();
        let tmp = store.temp_path_for(&k).unwrap();
        std::fs::write(&tmp, b"new").unwrap();

        store.put_file(&k, &tmp).unwrap();
        assert_eq!(store.get(&k).unwrap(), b"new");
    }

    #[test]
    fn copy_file_keeps_the_source_and_verifies_the_copy() {
        let (dir, store) = store();
        let outside = TempDir::new().unwrap();
        let src = outside.path().join("upload.mp4");
        let content: Vec<u8> = (0..STREAM_BUFFER_BYTES + 5).map(|i| (i * 7) as u8).collect();
        std::fs::write(&src, &content).unwrap();
        let digest = sha256_hex(&content);
        let k = key("recordings/x/source/original.mp4");

        let info = store.copy_file(&src, &k, &digest).unwrap();

        assert_eq!(info.bytes, content.len() as u64);
        assert_eq!(info.sha256, digest);
        assert_eq!(store.get(&k).unwrap(), content);
        assert_eq!(
            std::fs::read(&src).unwrap(),
            content,
            "the staged upload must survive the copy"
        );
        assert!(temporaries(dir.path()).is_empty());
    }

    #[test]
    fn copying_again_onto_an_identical_object_changes_nothing() {
        let (_dir, store) = store();
        let outside = TempDir::new().unwrap();
        let src = outside.path().join("upload.mp4");
        std::fs::write(&src, b"video").unwrap();
        let digest = sha256_hex(b"video");
        let k = key("a/original.mp4");

        store.copy_file(&src, &k, &digest).unwrap();
        let path = store.resolve(&k).unwrap();
        let first = std::fs::metadata(&path).unwrap();

        let again = store.copy_file(&src, &k, &digest).unwrap();
        let second = std::fs::metadata(&path).unwrap();

        assert_eq!(again.sha256, digest);
        assert_eq!(
            std::os::unix::fs::MetadataExt::ino(&first),
            std::os::unix::fs::MetadataExt::ino(&second),
            "a retried copy must not rewrite an object that is already right"
        );
    }

    #[test]
    fn a_copy_that_does_not_match_its_digest_is_refused() {
        let (dir, store) = store();
        let outside = TempDir::new().unwrap();
        let src = outside.path().join("upload.mp4");
        std::fs::write(&src, b"not what was uploaded").unwrap();
        let k = key("a/original.mp4");

        let err = store
            .copy_file(&src, &k, &sha256_hex(b"what was uploaded"))
            .unwrap_err();

        assert!(err.to_string().contains("expected digest"), "{err}");
        assert!(!store.exists(&k).unwrap());
        assert!(temporaries(dir.path()).is_empty());
    }

    #[test]
    fn a_wrong_object_in_the_way_is_replaced_by_the_verified_copy() {
        let (_dir, store) = store();
        let outside = TempDir::new().unwrap();
        let src = outside.path().join("upload.mp4");
        std::fs::write(&src, b"right").unwrap();
        let k = key("a/original.mp4");
        store.put(&k, b"wrong").unwrap();

        store.copy_file(&src, &k, &sha256_hex(b"right")).unwrap();
        assert_eq!(store.get(&k).unwrap(), b"right");
    }

    #[test]
    fn a_pending_blob_appears_only_when_committed() {
        let (dir, store) = store();
        let k = key("a/mixed.flac");

        let mut pending = PendingBlob::create(&store, &k).unwrap();
        pending.writer().write_all(b"frames").unwrap();
        assert!(!store.exists(&k).unwrap(), "nothing is visible before commit");
        let info = pending.commit().unwrap();

        assert_eq!(info.bytes, 6);
        assert_eq!(store.get(&k).unwrap(), b"frames");
        assert!(temporaries(dir.path()).is_empty());
    }

    #[test]
    fn an_abandoned_pending_blob_leaves_nothing_behind() {
        let (dir, store) = store();
        let k = key("a/mixed.flac");
        store.put(&k, b"previous").unwrap();

        let mut pending = PendingBlob::create(&store, &k).unwrap();
        pending.writer().write_all(b"half an encod").unwrap();
        drop(pending);

        assert_eq!(
            store.get(&k).unwrap(),
            b"previous",
            "a failed rewrite must not disturb the object it was replacing"
        );
        assert!(temporaries(dir.path()).is_empty());
    }

    fn walk(dir: &Path) -> Vec<PathBuf> {
        let mut out = vec![];
        let Ok(entries) = std::fs::read_dir(dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk(&path));
            } else {
                out.push(path);
            }
        }
        out
    }
}

//! Making a recording out of a file someone already has.
//!
//! An upload lands in staging (`imports/{ULID}/`), outside every recording,
//! with an intent describing what was asked for. The `ImportMedia` stage then
//! probes and decodes it inside a sandbox and writes the very objects capture
//! writes: a header, a track, chunks with sidecars, a manifest, exports.
//! Everything downstream (transcription, backup, retention, the library)
//! reads those objects and needs no idea that a file was involved.
//!
//! - [`media`] decides what is in the file and decodes its audio.
//! - [`sandbox`] runs the tools that do that, contained.
//! - [`job`] is the stage itself.
//! - [`sweep`] clears staging nothing will come back for.
//! - [`upload`] receives the file in the first place.
//! - [`cli`] sends one from a terminal, through the same route.

pub mod cli;
#[cfg(test)]
pub mod fixtures;
pub mod intent;
pub mod job;
pub mod media;
pub mod sandbox;
pub mod sweep;
pub mod upload;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use kaseta_contracts::ImportStaging;
use ulid::Ulid;

use crate::blobstore::BlobStore;
use sandbox::Toolchain;

/// Disk an import's decode is budgeted per second of audio: chunks, the
/// merged and mixed exports and the transcription audio together, rounded up
/// generously. Exact sizes depend on how well the audio compresses, and
/// running out halfway costs more than refusing early.
pub const DECODE_BYTES_PER_SECOND: u64 = 512 * 1024;

/// Free space that must remain beyond the decode's own budget, so finishing an
/// import never leaves the disk full for everything else on the machine.
pub const DECODE_HEADROOM_BYTES: u64 = 256 * 1024 * 1024;

/// Whether this daemon can import, decided once at startup, and the one slot
/// an upload occupies while it arrives.
///
/// Importing needs ffmpeg and ffprobe, and bubblewrap able to run them. Any of
/// those missing makes importing unavailable, with a reason naming what to
/// install, rather than an import that fails after the upload.
pub struct ImportRuntime {
    tools: std::result::Result<Toolchain, String>,
    uploads: Arc<upload::UploadGate>,
    /// Reads free space on the filesystem holding a path. A field so the
    /// upload's admission can be tested without filling a disk.
    free_space: fn(&Path) -> Result<u64>,
}

impl ImportRuntime {
    /// Looks for the tools and tries the sandbox once.
    pub fn detect() -> Self {
        let tools = Toolchain::detect();
        match &tools {
            Ok(t) => tracing::info!(ffmpeg = %t.version(), "importing is available, sandboxed"),
            Err(reason) => tracing::warn!(%reason, "importing is unavailable"),
        }
        Self::from_tools(tools)
    }

    fn from_tools(tools: std::result::Result<Toolchain, String>) -> Self {
        Self {
            tools,
            uploads: Arc::default(),
            free_space,
        }
    }

    /// A runtime that cannot import, for exercising that path.
    #[cfg(test)]
    pub fn unavailable(reason: &str) -> Self {
        Self::from_tools(Err(reason.to_string()))
    }

    /// A runtime with tools already found.
    #[cfg(test)]
    pub fn with(tools: Toolchain) -> Self {
        Self::from_tools(Ok(tools))
    }

    /// The same runtime, reading free space from `free_space` instead.
    #[cfg(test)]
    pub fn with_free_space(mut self, free_space: fn(&Path) -> Result<u64>) -> Self {
        self.free_space = free_space;
        self
    }

    /// The tools, or why there are none.
    pub fn toolchain(&self) -> std::result::Result<&Toolchain, &str> {
        self.tools.as_ref().map_err(String::as_str)
    }

    /// The upload slot, or `None` while another upload holds it.
    pub fn begin_upload(&self) -> Option<upload::UploadPermit> {
        self.uploads.try_enter()
    }

    /// Bytes free on the filesystem holding `path`.
    pub fn free_space(&self, path: &Path) -> Result<u64> {
        (self.free_space)(path)
    }
}

/// The command that installs `package` on this machine's distribution.
pub fn install_command(package: &str) -> String {
    let os_release = std::fs::read_to_string("/etc/os-release").unwrap_or_default();
    install_command_for(&os_release, package)
}

/// The install command for the distribution `/etc/os-release` describes.
///
/// Matched on `ID` and `ID_LIKE`, so a derivative (Manjaro, Mint, Rocky) gets
/// its parent's package manager.
fn install_command_for(os_release: &str, package: &str) -> String {
    let ids: Vec<String> = os_release
        .lines()
        .filter_map(|line| {
            let (key, value) = line.split_once('=')?;
            matches!(key.trim(), "ID" | "ID_LIKE")
                .then(|| value.trim().trim_matches('"').to_ascii_lowercase())
        })
        .flat_map(|v| v.split_whitespace().map(String::from).collect::<Vec<_>>())
        .collect();
    let is = |name: &str| ids.iter().any(|id| id == name);

    if is("arch") {
        format!("sudo pacman -S --needed {package}")
    } else if is("debian") || is("ubuntu") {
        format!("sudo apt install {package}")
    } else if is("fedora") || is("rhel") {
        format!("sudo dnf install {package}")
    } else {
        format!("install {package} with your package manager")
    }
}

/// Bytes free on the filesystem holding `path`, for an unprivileged user.
pub fn free_space(path: &Path) -> Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .with_context(|| format!("{} is not a usable path", path.display()))?;
    let mut stats: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c_path` is a valid NUL-terminated string and `stats` a valid,
    // writable `statvfs` for the duration of the call.
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stats) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("reading free space at {}", path.display()));
    }
    Ok((stats.f_bavail as u64).saturating_mul(stats.f_frsize as u64))
}

/// Where an import's staging lives on disk.
///
/// Imports are decoded by tools that read files, so staging only exists on a
/// store backed by the local filesystem.
pub fn staging_dir(store: &dyn BlobStore, id: Ulid) -> Result<PathBuf> {
    let root = store
        .local_root()
        .context("imports need a store on the local filesystem")?;
    Ok(root.join(ImportStaging::new(id).root().as_str()))
}

/// Removes an import's staging directory and everything in it, including a
/// half-written upload. Absent is fine.
pub fn remove_staging(store: &dyn BlobStore, id: Ulid) -> Result<()> {
    let dir = staging_dir(store, id)?;
    match std::fs::remove_dir_all(&dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing {}", dir.display())),
    }
}

/// Whether an import's upload is complete and still in staging, so decoding
/// it again is possible.
pub fn upload_present(store: &dyn BlobStore, id: Ulid) -> Result<bool> {
    let Some(intent) = intent::Intent::read(store, id)? else {
        return Ok(false);
    };
    if intent.state != intent::IntentState::Uploaded {
        return Ok(false);
    }
    store.exists(&intent.upload_key()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_install_command_fits_the_distribution() {
        let cases = [
            ("ID=arch\n", "sudo pacman -S --needed ffmpeg"),
            ("ID=manjaro\nID_LIKE=arch\n", "sudo pacman -S --needed ffmpeg"),
            ("ID=ubuntu\nID_LIKE=debian\n", "sudo apt install ffmpeg"),
            ("ID=linuxmint\nID_LIKE=\"ubuntu debian\"\n", "sudo apt install ffmpeg"),
            ("ID=\"rocky\"\nID_LIKE=\"rhel centos fedora\"\n", "sudo dnf install ffmpeg"),
            ("ID=fedora\n", "sudo dnf install ffmpeg"),
            ("ID=gentoo\n", "install ffmpeg with your package manager"),
            ("", "install ffmpeg with your package manager"),
        ];
        for (os_release, expected) in cases {
            assert_eq!(install_command_for(os_release, "ffmpeg"), expected, "{os_release:?}");
        }
    }

    #[test]
    fn free_space_is_read_from_the_filesystem() {
        let dir = tempfile::TempDir::new().unwrap();
        assert!(free_space(dir.path()).unwrap() > 0);
        assert!(free_space(&dir.path().join("absent")).is_err());
    }

    #[test]
    fn staging_is_beside_recordings_not_inside_them() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let id = Ulid::new();

        let staging = staging_dir(&store, id).unwrap();
        assert_eq!(staging, store.root().join(format!("imports/{id}")));

        std::fs::create_dir_all(staging.join("deep")).unwrap();
        std::fs::write(staging.join("upload.mp4.part"), b"half").unwrap();
        remove_staging(&store, id).unwrap();
        assert!(!staging.exists());
        remove_staging(&store, id).unwrap();
    }

    /// A runtime that cannot import says why, in words that name the fix.
    #[test]
    fn an_unavailable_runtime_gives_its_reason() {
        let runtime = ImportRuntime::unavailable("Importing needs bubblewrap: sudo pacman -S --needed bubblewrap");
        assert_eq!(
            runtime.toolchain().unwrap_err(),
            "Importing needs bubblewrap: sudo pacman -S --needed bubblewrap"
        );
    }
}

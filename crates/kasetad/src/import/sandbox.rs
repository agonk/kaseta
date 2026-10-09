//! Running ffprobe and ffmpeg on a file nobody here produced.
//!
//! Demuxers and decoders are large C codebases that parse hostile input for a
//! living, and an imported file is input from anywhere. Every run therefore
//! happens inside bubblewrap: no network, no view of the home directory or
//! the data directory, only `/usr` and the one staging directory holding the
//! upload, both read-only. A decoder bug that turned into code execution would
//! find nothing to read and nowhere to send it.
//!
//! The sandbox is not optional. Without bubblewrap, importing is unavailable
//! and says why, rather than quietly running the decoder with the daemon's
//! full access.

use std::ffi::{OsStr, OsString};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};

/// How much of a tool's error output is kept: enough for its last complaint,
/// bounded however much it prints.
const STDERR_TAIL_BYTES: usize = 8 * 1024;

/// Read size for a tool's output.
const READ_BYTES: usize = 64 * 1024;

/// How long the startup check may take to run each tool once.
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);

/// The two tools an import runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tool {
    Ffmpeg,
    Ffprobe,
}

/// How one of the host's top-level library and binary directories is
/// reproduced inside the sandbox.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Mirror {
    /// A symlink on the host, such as `/lib -> usr/lib` on a merged-`/usr`
    /// system, recreated as the same symlink.
    Symlink(PathBuf),
    /// A real directory, bound read-only.
    Directory,
}

/// Where the host keeps what the tools load at run time, outside `/usr`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostLayout {
    entries: Vec<(PathBuf, Mirror)>,
}

impl HostLayout {
    /// The directories that may hold the dynamic loader and libraries.
    const MIRRORED: [&'static str; 4] = ["/lib", "/lib64", "/bin", "/sbin"];

    /// Reads the layout of the running system.
    pub fn of_host() -> Self {
        Self::under(Path::new("/"))
    }

    /// Reads the layout of a system rooted at `root`, naming every entry as
    /// it appears from inside that system. Lets the argument shape be tested
    /// against a constructed tree rather than whatever this machine has.
    ///
    /// Each directory is reproduced as what it is: a symlink is recreated, a
    /// directory is bound, and an absent one is skipped. Assuming the
    /// merged-`/usr` symlinks everywhere would hide a real `/lib64` holding
    /// the loader on a system that has one, and every tool would fail to
    /// start.
    pub fn under(root: &Path) -> Self {
        let mut entries = Vec::new();
        for name in Self::MIRRORED {
            let on_host = root.join(name.trim_start_matches('/'));
            let Ok(meta) = std::fs::symlink_metadata(&on_host) else {
                continue;
            };
            if meta.file_type().is_symlink() {
                if let Ok(target) = std::fs::read_link(&on_host) {
                    entries.push((PathBuf::from(name), Mirror::Symlink(target)));
                }
            } else if meta.is_dir() {
                entries.push((PathBuf::from(name), Mirror::Directory));
            }
        }
        Self { entries }
    }
}

/// bubblewrap, ffmpeg and ffprobe, found and shown to work together.
#[derive(Clone, Debug)]
pub struct Toolchain {
    bwrap: PathBuf,
    ffmpeg: PathBuf,
    ffprobe: PathBuf,
    layout: HostLayout,
    /// ffmpeg's own version line, for the log and `doctor`.
    version: String,
    /// ffprobe's, for `doctor`: the two are separate binaries, and a
    /// mismatched pair is worth being able to see.
    probe_version: String,
}

/// How a tool run ended.
#[derive(Debug)]
pub struct Finished {
    pub status: ExitStatus,
    /// The end of what the tool wrote to stderr.
    pub stderr_tail: String,
    /// Killed for exceeding its time budget.
    pub timed_out: bool,
}

impl Toolchain {
    /// Finds the tools and proves the sandbox can run them.
    ///
    /// The reason it cannot is phrased for the person who has to fix it,
    /// including the command that installs what is missing.
    pub fn detect() -> std::result::Result<Self, String> {
        let ffmpeg = find_tool("ffmpeg")?;
        let ffprobe = find_tool("ffprobe")?;
        let bwrap = find_in_path("bwrap").ok_or_else(|| {
            format!(
                "Importing needs bubblewrap: {}",
                super::install_command("bubblewrap")
            )
        })?;

        let mut tools = Self {
            bwrap,
            ffmpeg,
            ffprobe,
            layout: HostLayout::of_host(),
            version: String::new(),
            probe_version: String::new(),
        };

        let mut versions = Vec::new();
        for tool in [Tool::Ffmpeg, Tool::Ffprobe] {
            let version = tools.version_of(tool).map_err(|e| {
                format!(
                    "Importing needs a working sandbox, and bubblewrap could not run {}: {e:#}",
                    tools.program(tool).display()
                )
            })?;
            versions.push(version);
        }
        tools.probe_version = versions.pop().unwrap_or_default();
        tools.version = versions.pop().unwrap_or_default();
        Ok(tools)
    }

    /// The tools at fixed locations, without the startup check. For tests
    /// that need a toolchain shaped a particular way.
    #[cfg(test)]
    pub fn at(bwrap: PathBuf, ffmpeg: PathBuf, ffprobe: PathBuf, layout: HostLayout) -> Self {
        Self {
            bwrap,
            ffmpeg,
            ffprobe,
            layout,
            version: String::new(),
            probe_version: String::new(),
        }
    }

    /// ffmpeg's version line.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// ffprobe's version line.
    pub fn probe_version(&self) -> &str {
        &self.probe_version
    }

    fn program(&self, tool: Tool) -> &Path {
        match tool {
            Tool::Ffmpeg => &self.ffmpeg,
            Tool::Ffprobe => &self.ffprobe,
        }
    }

    /// Runs `tool -version` in the sandbox and returns its first line.
    fn version_of(&self, tool: Tool) -> Result<String> {
        let mut out = Vec::new();
        let finished = self.run(None, tool, &["-version".into()], PROBE_TIMEOUT, |bytes| {
            if out.len() < 64 * 1024 {
                out.extend_from_slice(bytes);
            }
            Ok(true)
        })?;
        anyhow::ensure!(
            finished.status.success() && !finished.timed_out,
            "it exited with {} ({})",
            finished.status,
            finished.stderr_tail.trim()
        );
        let first = String::from_utf8_lossy(&out)
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();
        anyhow::ensure!(first.contains("version"), "it printed no version ({first:?})");
        Ok(first)
    }

    /// The full command line for running `tool` with `args` in the sandbox.
    ///
    /// `staging` is bound read-only at the same absolute path it has outside,
    /// so a `file:` URL naming the upload is valid inside as well, and the
    /// tool starts in it. Bound after the `/tmp` tmpfs, so a staging
    /// directory under `/tmp` is still visible.
    pub fn argv(&self, staging: Option<&Path>, tool: Tool, args: &[OsString]) -> Vec<OsString> {
        let mut argv: Vec<OsString> = vec![self.bwrap.clone().into()];
        let mut push = |parts: &[&OsStr]| argv.extend(parts.iter().map(|p| p.to_os_string()));

        push(&[
            "--unshare-all".as_ref(),
            "--die-with-parent".as_ref(),
            "--new-session".as_ref(),
            "--ro-bind".as_ref(),
            "/usr".as_ref(),
            "/usr".as_ref(),
        ]);
        for (path, mirror) in &self.layout.entries {
            match mirror {
                Mirror::Symlink(target) => {
                    push(&["--symlink".as_ref(), target.as_os_str(), path.as_os_str()])
                }
                Mirror::Directory => {
                    push(&["--ro-bind".as_ref(), path.as_os_str(), path.as_os_str()])
                }
            }
        }
        push(&[
            "--ro-bind-try".as_ref(),
            "/etc/ld.so.cache".as_ref(),
            "/etc/ld.so.cache".as_ref(),
            // Debian and Ubuntu reach some libraries through here.
            "--ro-bind-try".as_ref(),
            "/etc/alternatives".as_ref(),
            "/etc/alternatives".as_ref(),
            "--proc".as_ref(),
            "/proc".as_ref(),
            "--dev".as_ref(),
            "/dev".as_ref(),
            "--tmpfs".as_ref(),
            "/tmp".as_ref(),
        ]);
        match staging {
            Some(dir) => push(&[
                "--ro-bind".as_ref(),
                dir.as_os_str(),
                dir.as_os_str(),
                "--chdir".as_ref(),
                dir.as_os_str(),
            ]),
            None => push(&["--chdir".as_ref(), "/".as_ref()]),
        }
        push(&["--".as_ref(), self.program(tool).as_os_str()]);
        argv.extend(args.iter().cloned());
        argv
    }

    /// Runs `tool` in the sandbox, handing its stdout to `on_output` as it
    /// arrives, and returns how it ended.
    ///
    /// `on_output` returns whether to keep going; `false` or an error stops
    /// the tool at once. The whole process group is killed on timeout or on a
    /// stop: bubblewrap leads the group, and its `--die-with-parent` and PID
    /// namespace take the tool down with it, so nothing outlives the run.
    ///
    /// The environment is emptied apart from `PATH` and `LANG`: nothing the
    /// daemon was started with is the tool's business.
    pub fn run(
        &self,
        staging: Option<&Path>,
        tool: Tool,
        args: &[OsString],
        timeout: Duration,
        mut on_output: impl FnMut(&[u8]) -> Result<bool>,
    ) -> Result<Finished> {
        use std::os::unix::process::CommandExt;

        let argv = self.argv(staging, tool, args);
        let mut command = Command::new(&argv[0]);
        command
            .args(&argv[1..])
            .env_clear()
            .current_dir(staging.unwrap_or(Path::new("/")))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        for name in ["PATH", "LANG"] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }

        let mut child = command
            .spawn()
            .with_context(|| format!("starting {}", self.bwrap.display()))?;
        let group = child.id() as i32;
        let mut stdout = child.stdout.take().context("the tool has no stdout")?;
        let mut stderr = child.stderr.take().context("the tool has no stderr")?;

        let tail = std::thread::spawn(move || {
            let mut kept: Vec<u8> = Vec::new();
            let mut buf = [0u8; 4096];
            while let Ok(n) = stderr.read(&mut buf) {
                if n == 0 {
                    break;
                }
                kept.extend_from_slice(&buf[..n]);
                if kept.len() > STDERR_TAIL_BYTES {
                    kept.drain(..kept.len() - STDERR_TAIL_BYTES);
                }
            }
            String::from_utf8_lossy(&kept).into_owned()
        });

        // Set once the child has been reaped. The watchdog kills only while
        // it is not, so a group id the system has since handed to someone
        // else is never signalled.
        let reaped = Arc::new(Mutex::new(false));
        let timed_out = Arc::new(AtomicBool::new(false));
        let (done, until_done) = mpsc::channel::<()>();
        let watchdog = {
            let reaped = Arc::clone(&reaped);
            let timed_out = Arc::clone(&timed_out);
            std::thread::spawn(move || {
                if let Err(mpsc::RecvTimeoutError::Timeout) = until_done.recv_timeout(timeout) {
                    if let Ok(reaped) = reaped.lock() {
                        if !*reaped {
                            timed_out.store(true, Ordering::SeqCst);
                            kill_group(group);
                        }
                    }
                }
            })
        };

        let mut stop: Option<Result<()>> = None;
        let mut buf = vec![0u8; READ_BYTES];
        loop {
            match stdout.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => match on_output(&buf[..n]) {
                    Ok(true) => {}
                    Ok(false) => {
                        stop = Some(Ok(()));
                        break;
                    }
                    Err(e) => {
                        stop = Some(Err(e));
                        break;
                    }
                },
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    stop = Some(Err(anyhow::Error::from(e).context("reading the tool's output")));
                    break;
                }
            }
        }
        if stop.is_some() {
            kill_group(group);
        }
        drop(stdout);

        let status = child.wait().context("waiting for the tool");
        if let Ok(mut reaped) = reaped.lock() {
            *reaped = true;
        }
        let _ = done.send(());
        let _ = watchdog.join();
        let stderr_tail = tail.join().unwrap_or_default();
        let status = status?;

        if let Some(Err(e)) = stop {
            return Err(e);
        }
        Ok(Finished {
            status,
            stderr_tail,
            timed_out: timed_out.load(Ordering::SeqCst),
        })
    }
}

/// Kills every process in a group.
fn kill_group(group: i32) {
    // SAFETY: `kill` takes no pointers; a negative pid addresses the group
    // the child was started as the leader of.
    unsafe {
        libc::kill(-group, libc::SIGKILL);
    }
}

/// Finds ffmpeg or ffprobe, as an absolute path under `/usr`.
///
/// Only `/usr` is visible inside the sandbox, so a tool installed anywhere
/// else could not run there. Symlinks are resolved first, so a link in
/// `/usr/bin` to a build in someone's home is refused rather than failing
/// mysteriously at the first import.
fn find_tool(name: &str) -> std::result::Result<PathBuf, String> {
    let found = find_in_path(name)
        .ok_or_else(|| format!("Importing needs ffmpeg: {}", super::install_command("ffmpeg")))?;
    let resolved = std::fs::canonicalize(&found).unwrap_or(found);
    if !resolved.starts_with("/usr") {
        return Err(format!(
            "Importing needs {name} installed under /usr, where the sandbox can see it; \
             found {}",
            resolved.display()
        ));
    }
    Ok(resolved)
}

/// Where `name` is on `PATH`, if anywhere. For reporting what is installed;
/// whether it is usable is [`Toolchain::detect`]'s question.
pub fn locate(name: &str) -> Option<PathBuf> {
    find_in_path(name)
}

fn find_in_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(name))
        .find(|candidate| is_executable(candidate))
}

fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(argv: &[OsString]) -> Vec<String> {
        argv.iter().map(|a| a.to_string_lossy().into_owned()).collect()
    }

    fn toolchain(layout: HostLayout) -> Toolchain {
        Toolchain::at(
            "/usr/bin/bwrap".into(),
            "/usr/bin/ffmpeg".into(),
            "/usr/bin/ffprobe".into(),
            layout,
        )
    }

    /// Arch and current Debian: `/lib`, `/lib64`, `/bin` and `/sbin` are all
    /// symlinks into `/usr`.
    #[test]
    fn a_merged_usr_host_is_mirrored_with_symlinks() {
        let root = tempfile::TempDir::new().unwrap();
        for (name, target) in [("lib", "usr/lib"), ("lib64", "usr/lib"), ("bin", "usr/bin"), ("sbin", "usr/bin")] {
            std::os::unix::fs::symlink(target, root.path().join(name)).unwrap();
        }

        let argv = strings(&toolchain(HostLayout::under(root.path())).argv(
            Some(Path::new("/data/imports/X")),
            Tool::Ffprobe,
            &["-v".into(), "error".into()],
        ));

        assert_eq!(
            argv,
            [
                "/usr/bin/bwrap",
                "--unshare-all",
                "--die-with-parent",
                "--new-session",
                "--ro-bind", "/usr", "/usr",
                "--symlink", "usr/lib", "/lib",
                "--symlink", "usr/lib", "/lib64",
                "--symlink", "usr/bin", "/bin",
                "--symlink", "usr/bin", "/sbin",
                "--ro-bind-try", "/etc/ld.so.cache", "/etc/ld.so.cache",
                "--ro-bind-try", "/etc/alternatives", "/etc/alternatives",
                "--proc", "/proc",
                "--dev", "/dev",
                "--tmpfs", "/tmp",
                "--ro-bind", "/data/imports/X", "/data/imports/X",
                "--chdir", "/data/imports/X",
                "--", "/usr/bin/ffprobe",
                "-v", "error",
            ]
            .map(String::from)
        );
    }

    /// A host with a real `/lib64` holding the loader: binding it is the
    /// only way the tools start, and a symlink in its place would hide it.
    #[test]
    fn real_directories_are_bound_and_absent_ones_skipped() {
        let root = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(root.path().join("lib")).unwrap();
        std::fs::create_dir(root.path().join("lib64")).unwrap();
        std::os::unix::fs::symlink("usr/bin", root.path().join("bin")).unwrap();

        let argv = strings(&toolchain(HostLayout::under(root.path())).argv(
            None,
            Tool::Ffmpeg,
            &[],
        ));
        let joined = argv.join(" ");

        assert!(joined.contains("--ro-bind /lib /lib --ro-bind /lib64 /lib64 --symlink usr/bin /bin --ro-bind-try"), "{joined}");
        assert!(!joined.contains("/sbin"), "an absent directory is not mentioned: {joined}");
        assert!(joined.ends_with("--chdir / -- /usr/bin/ffmpeg"), "{joined}");
        assert!(!joined.contains("--share-net"), "the network stays unshared");
    }

    /// The sandbox is entered first and the tool named last, so nothing in
    /// the tool's own arguments can be read as an instruction to bubblewrap.
    #[test]
    fn the_tools_arguments_come_after_the_separator() {
        let argv = strings(&toolchain(HostLayout { entries: Vec::new() }).argv(
            None,
            Tool::Ffmpeg,
            &["--bind".into(), "/".into(), "/".into()],
        ));
        let separator = argv.iter().position(|a| a == "--").unwrap();
        assert_eq!(argv[separator + 1], "/usr/bin/ffmpeg");
        assert_eq!(argv[separator + 2..], ["--bind", "/", "/"]);
    }

    /// Inside, only the staging directory exists of everything the daemon can
    /// see: a file beside it, in the data directory, is not there.
    #[test]
    fn a_tool_sees_the_staging_directory_and_nothing_beside_it() {
        let Some(tools) = crate::import::fixtures::toolchain() else { return };
        let data = tempfile::TempDir::new().unwrap();
        let staging = data.path().join("imports/one");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(staging.join("inside.txt"), b"x").unwrap();
        std::fs::write(data.path().join("kaseta.db"), b"secret").unwrap();

        let probe = |path: &Path| {
            let args = ["-v", "error", "-show_format"]
                .map(OsString::from)
                .into_iter()
                .chain([path.as_os_str().to_os_string()])
                .collect::<Vec<_>>();
            tools
                .run(Some(&staging), Tool::Ffprobe, &args, Duration::from_secs(60), |_| Ok(true))
                .unwrap()
        };

        let outside = probe(&data.path().join("kaseta.db"));
        assert!(!outside.status.success());
        assert!(outside.stderr_tail.contains("No such file"), "{}", outside.stderr_tail);

        // The file inside is reachable (and refused only for not being media).
        let inside = probe(&staging.join("inside.txt"));
        assert!(!inside.stderr_tail.contains("No such file"), "{}", inside.stderr_tail);
    }

    #[test]
    fn this_hosts_layout_is_read() {
        // Whatever this machine is, every entry is one of the four, named
        // from the root.
        for (path, _) in HostLayout::of_host().entries {
            assert!(HostLayout::MIRRORED.contains(&path.to_str().unwrap()), "{path:?}");
        }
    }
}

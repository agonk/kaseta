//! Owns the one live recording, on behalf of everything else.
//!
//! A recording session owns OS threads, is consumed by `stop()`, and is
//! deliberately not `Sync`. Sharing it behind a mutex would mean holding a lock
//! across a blocking thread join, which would stall every status request for as
//! long as finalisation takes.
//!
//! Instead a single supervisor thread owns the session exclusively. Callers send
//! commands and receive replies; readers see a cheap [`DaemonStatus`] snapshot
//! that the supervisor republishes as things change. Nothing outside this module
//! touches a [`RecordingSession`].
//!
//! Only one recording exists at a time. Starting while one runs is refused
//! rather than silently queued or ignored.

// The daemon that drives this is the next piece; the supervisor is complete and
// test-covered ahead of it.
#![allow(dead_code)]

use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use anyhow::{anyhow, Context, Result};
use kaseta_contracts::manifest::{RecordingNotes, TrackRole};
use kaseta_contracts::{RecordingManifest, RecordingPrefix, TrackId};
use serde::Serialize;
use tokio::sync::{oneshot, watch};
use ulid::Ulid;

use crate::blobstore::BlobStore;
use crate::capture::devices::{self, DeviceKind};
use crate::db::Db;
use crate::capture::session::{RecordingSession, SessionOutcome, TrackSpec};

/// Conditions the caller can correct, as opposed to faults.
///
/// Distinguished so the API can answer "you asked for something that does not
/// apply right now" differently from "something broke".
#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    #[error("a recording is already running")]
    AlreadyRecording,
    #[error("no recording is running")]
    NotRecording,
}

/// How often the supervisor re-reads session health while idle at its channel.
///
/// Bounds how long a track failing mid-recording can go unreported.
const HEALTH_POLL: std::time::Duration = std::time::Duration::from_millis(250);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordingState {
    Idle,
    Recording,
    /// Capture has stopped and the recording is being sealed and exported. This
    /// is reported separately from `Idle` because it can take a noticeable time
    /// on a long meeting, and the interface should not claim to be finished.
    Finalizing,
}

/// A cheap, cloneable view of what the daemon is doing.
#[derive(Clone, Debug, Serialize)]
pub struct DaemonStatus {
    pub state: RecordingState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recording_id: Option<Ulid>,
    #[serde(with = "time::serde::rfc3339::option", default)]
    pub started_at: Option<time::OffsetDateTime>,
    /// Seconds elapsed since capture began.
    pub elapsed_s: u64,
    /// Tracks that stopped capturing before a stop was requested.
    pub degraded_tracks: Vec<String>,
    /// Tracks whose device went away and are being reattached. Distinct from
    /// degraded: audio is being lost right now, but the track is expected back.
    #[serde(default)]
    pub reconnecting: Vec<String>,
    /// Set when persistence failed; nothing further is being written.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

impl Default for DaemonStatus {
    fn default() -> Self {
        Self {
            state: RecordingState::Idle,
            recording_id: None,
            started_at: None,
            elapsed_s: 0,
            degraded_tracks: Vec::new(),
            reconnecting: Vec::new(),
            failure: None,
        }
    }
}

/// A recording whose capture has stopped and whose manifest is durable.
///
/// Returned before exports are produced. Once the manifest is written the
/// recording is safe and complete; merging and mixing decode and re-encode the
/// whole thing and can take minutes on a long meeting, which no caller should
/// wait on.
#[derive(Debug)]
pub struct StoppedRecording {
    pub manifest: RecordingManifest,
    pub prefix: RecordingPrefix,
    pub outcome: SessionOutcome,
}

enum Command {
    Start {
        notes: RecordingNotes,
        reply: oneshot::Sender<Result<Ulid>>,
    },
    Stop {
        reply: oneshot::Sender<Result<StoppedRecording>>,
    },
    Shutdown,
}

pub struct Supervisor {
    commands: Sender<Command>,
    status: watch::Receiver<DaemonStatus>,
    thread: Option<JoinHandle<()>>,
}

/// Recordings whose exports are still being produced.
///
/// Reported so the interface can say a recording is still being prepared rather
/// than appearing finished while its audio is not yet playable.
type ExportsInFlight = Arc<AtomicUsize>;

impl Supervisor {
    pub fn spawn(store: Arc<dyn BlobStore>, db: Arc<std::sync::Mutex<Db>>) -> Result<Self> {
        let (commands, rx) = mpsc::channel::<Command>();
        let (status_tx, status) = watch::channel(DaemonStatus::default());

        let thread = std::thread::Builder::new()
            .name("kaseta-supervisor".into())
            .spawn(move || run(rx, status_tx, store, db))
            .context("spawning supervisor thread")?;

        Ok(Self {
            commands,
            status,
            thread: Some(thread),
        })
    }

    pub fn status(&self) -> DaemonStatus {
        self.status.borrow().clone()
    }

    /// Begins a recording, returning its id.
    ///
    /// Fails if one is already running: a second concurrent recording would
    /// contend for the same devices and produce two half-recordings.
    pub async fn start(&self, notes: RecordingNotes) -> Result<Ulid> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Start { notes, reply })
            .map_err(|_| anyhow!("supervisor is not running"))?;
        response.await.map_err(|_| anyhow!("supervisor dropped the request"))?
    }

    /// Stops the active recording and waits for it to be sealed.
    pub async fn stop(&self) -> Result<StoppedRecording> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Stop { reply })
            .map_err(|_| anyhow!("supervisor is not running"))?;
        response.await.map_err(|_| anyhow!("supervisor dropped the request"))?
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        // A daemon shutting down mid-recording must seal what it has rather than
        // abandoning the capture threads.
        let _ = self.commands.send(Command::Shutdown);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The supervisor loop. Owns the session exclusively for its whole lifetime.
fn run(
    commands: mpsc::Receiver<Command>,
    status: watch::Sender<DaemonStatus>,
    store: Arc<dyn BlobStore>,
    db: Arc<std::sync::Mutex<Db>>,
) {
    let mut active: Option<Active> = None;
    let exporting: ExportsInFlight = Arc::new(AtomicUsize::new(0));

    loop {
        // Waiting with a timeout rather than blocking indefinitely is what lets
        // health be republished while no commands arrive.
        match commands.recv_timeout(HEALTH_POLL) {
            Ok(Command::Start { notes, reply }) => {
                let result = start_recording(&mut active, &store, notes);
                publish(&status, &active, &exporting);
                let _ = reply.send(result);
            }
            Ok(Command::Stop { reply }) => {
                let Some(session) = active.take() else {
                    let _ = reply.send(Err(SupervisorError::NotRecording.into()));
                    publish(&status, &active, &exporting);
                    continue;
                };

                // Publish before finalising: sealing and exporting a long
                // recording is not instant, and the interface should say so
                // rather than appear hung or already idle.
                let _ = status.send(DaemonStatus {
                    state: RecordingState::Finalizing,
                    recording_id: Some(session.id),
                    started_at: Some(session.started_at),
                    elapsed_s: session.elapsed_s(),
                    degraded_tracks: session.degraded_track_names(),
                    reconnecting: Vec::new(),
                    failure: session.session.failure(),
                });

                match seal(session, &*store) {
                    Ok(stopped) => {
                        // The manifest is durable, so the recording is safe.
                        // Exports are derived and slow; producing them on a
                        // separate thread keeps the supervisor able to accept
                        // another recording immediately.
                        spawn_exports(
                            Arc::clone(&store),
                            Arc::clone(&db),
                            stopped.manifest.clone(),
                            stopped.prefix.clone(),
                            Arc::clone(&exporting),
                        );
                        let _ = reply.send(Ok(stopped));
                    }
                    Err(e) => {
                        let _ = reply.send(Err(e));
                    }
                }
                publish(&status, &active, &exporting);
            }
            Ok(Command::Shutdown) => {
                if let Some(session) = active.take() {
                    tracing::warn!("shutting down while recording; sealing what was captured");
                    match seal(session, &*store) {
                        Ok(stopped) => {
                            // Exports run inline here: the process is exiting,
                            // so there is nowhere to defer them to.
                            let _ = crate::export::merge_recording(
                                &*store,
                                &stopped.manifest,
                                &stopped.prefix,
                            );
                            let _ = crate::export::mix_recording(
                                &*store,
                                &stopped.manifest,
                                &stopped.prefix,
                            );
                        }
                        Err(e) => {
                            tracing::error!(error = %format!("{e:#}"), "failed to seal recording")
                        }
                    }
                }
                return;
            }
            Err(RecvTimeoutError::Timeout) => publish(&status, &active, &exporting),
            // Every command sender is gone, so nothing can ask for a stop.
            Err(RecvTimeoutError::Disconnected) => {
                if let Some(session) = active.take() {
                    let _ = seal(session, &*store);
                }
                return;
            }
        }
    }
}

/// A running recording plus what the status snapshot needs.
struct Active {
    id: Ulid,
    started_at: time::OffsetDateTime,
    started_boottime_ns: u64,
    session: RecordingSession,
}

impl Active {
    fn elapsed_s(&self) -> u64 {
        crate::clock::boottime_ns().saturating_sub(self.started_boottime_ns) / 1_000_000_000
    }

    fn degraded_track_names(&self) -> Vec<String> {
        self.session
            .health()
            .degraded_tracks
            .iter()
            .map(|(id, _)| id.to_string())
            .collect()
    }
}

fn publish(
    status: &watch::Sender<DaemonStatus>,
    active: &Option<Active>,
    exporting: &ExportsInFlight,
) {
    let next = match active {
        None if exporting.load(Ordering::SeqCst) > 0 => DaemonStatus {
            state: RecordingState::Finalizing,
            ..DaemonStatus::default()
        },
        None => DaemonStatus::default(),
        Some(a) => {
            let health = a.session.health();
            DaemonStatus {
                state: RecordingState::Recording,
                recording_id: Some(a.id),
                started_at: Some(a.started_at),
                elapsed_s: a.elapsed_s(),
                degraded_tracks: health
                    .degraded_tracks
                    .iter()
                    .map(|(id, _)| id.to_string())
                    .collect(),
                reconnecting: health.reconnecting.iter().map(|id| id.to_string()).collect(),
                failure: health.failure,
            }
        }
    };
    // A send failure only means nobody is watching.
    let _ = status.send(next);
}

fn start_recording(
    active: &mut Option<Active>,
    store: &Arc<dyn BlobStore>,
    notes: RecordingNotes,
) -> Result<Ulid> {
    if active.is_some() {
        return Err(SupervisorError::AlreadyRecording.into());
    }

    let specs = default_meeting_tracks()?;
    let started_at = time::OffsetDateTime::now_utc();
    let started_boottime_ns = crate::clock::boottime_ns();

    let session = RecordingSession::start(specs, Arc::clone(store), started_at, notes)
        .context("starting capture")?;
    let id = session.recording_id();

    *active = Some(Active {
        id,
        started_at,
        started_boottime_ns,
        session,
    });
    Ok(id)
}

/// Produces a recording's exports off the supervisor thread.
fn spawn_exports(
    store: Arc<dyn BlobStore>,
    db: Arc<std::sync::Mutex<Db>>,
    manifest: RecordingManifest,
    prefix: RecordingPrefix,
    exporting: ExportsInFlight,
) {
    exporting.fetch_add(1, Ordering::SeqCst);
    let counter = Arc::clone(&exporting);
    let spawned = std::thread::Builder::new()
        .name("kaseta-export".into())
        .spawn(move || {
            // Exports are convenience artifacts; failing to produce them must
            // not lose a recording whose chunks and manifest are durable.
            if let Err(e) = crate::export::merge_recording(&*store, &manifest, &prefix) {
                tracing::error!(error = %format!("{e:#}"), "merging tracks failed");
            }
            if let Err(e) = crate::export::mix_recording(&*store, &manifest, &prefix) {
                tracing::error!(error = %format!("{e:#}"), "mixing recording failed");
            }

            // Queued only once the audio transcription needs actually exists.
            // Queueing earlier would have the scheduler fail on missing exports
            // and burn retries waiting for work that had not finished.
            match db.lock() {
                Ok(guard) => {
                    if let Err(e) =
                        crate::scheduler::enqueue_for_recording(&guard, manifest.recording_id)
                    {
                        tracing::error!(error = %format!("{e:#}"), "could not queue transcription");
                    }
                }
                Err(_) => tracing::error!("database lock poisoned; transcription not queued"),
            }

            counter.fetch_sub(1, Ordering::SeqCst);
        });

    if spawned.is_err() {
        exporting.fetch_sub(1, Ordering::SeqCst);
        tracing::error!("could not spawn the export thread; audio will need `kasetad export`");
    }
}

/// Stops capture and writes the manifest, making the recording durable.
fn seal(active: Active, store: &dyn BlobStore) -> Result<StoppedRecording> {
    let prefix = active.session.prefix().clone();
    let outcome = active.session.stop().context("stopping capture")?;

    let manifest_json =
        serde_json::to_vec_pretty(&outcome.manifest).context("serialising manifest")?;
    store
        .put(&prefix.manifest(), &manifest_json)
        .context("writing manifest")?;

    Ok(StoppedRecording {
        manifest: outcome.manifest.clone(),
        prefix,
        outcome,
    })
}

/// Builds the two tracks a meeting needs from the session's default devices.
fn default_meeting_tracks() -> Result<Vec<TrackSpec>> {
    let devices = devices::list_devices()
        .context("could not reach PipeWire — run `kasetad doctor` to diagnose")?;

    // Discovery orders defaults first, so the first of each kind is what the
    // user is actually speaking into and listening to.
    let mic = devices
        .iter()
        .find(|d| d.kind == DeviceKind::Microphone)
        .context("no microphone found — cannot record your own audio")?
        .clone();
    let playback = devices
        .iter()
        .find(|d| d.kind == DeviceKind::SinkMonitor)
        .context("no playback monitor found — cannot record the far end")?
        .clone();

    TrackSpec::meeting(mic, playback)
}

/// The role a track carries, for display.
pub fn role_label(role: TrackRole) -> &'static str {
    match role {
        TrackRole::LocalMic => "you",
        TrackRole::RemoteMix => "others",
        TrackRole::Application => "application",
        TrackRole::Visual => "video",
    }
}

/// Whether a track id names the combined mix rather than a captured track.
pub fn is_mix(track_id: &TrackId) -> bool {
    track_id.as_str() == "mixed"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_idle_daemon_reports_no_recording() {
        let status = DaemonStatus::default();
        assert_eq!(status.state, RecordingState::Idle);
        assert!(status.recording_id.is_none());
        assert_eq!(status.elapsed_s, 0);
    }

    #[test]
    fn status_serialises_without_absent_fields() {
        let json = serde_json::to_string(&DaemonStatus::default()).unwrap();
        assert!(json.contains("\"state\":\"idle\""));
        assert!(
            !json.contains("recording_id"),
            "absent fields must not appear as nulls: {json}"
        );
    }

    #[test]
    fn recording_status_carries_what_the_interface_needs() {
        let status = DaemonStatus {
            state: RecordingState::Recording,
            recording_id: Some(Ulid::nil()),
            started_at: Some(time::OffsetDateTime::UNIX_EPOCH),
            elapsed_s: 42,
            degraded_tracks: vec!["a_local-mic_01".into()],
            failure: None,
        };
        let json = serde_json::to_string(&status).unwrap();

        assert!(json.contains("\"state\":\"recording\""));
        assert!(json.contains("\"elapsed_s\":42"));
        assert!(json.contains("a_local-mic_01"));
    }

    #[test]
    fn finalizing_is_distinct_from_idle() {
        // A long recording takes real time to seal and export. Reporting idle
        // during that would make the interface look finished when it is not.
        let json = serde_json::to_string(&DaemonStatus {
            state: RecordingState::Finalizing,
            ..DaemonStatus::default()
        })
        .unwrap();
        assert!(json.contains("\"state\":\"finalizing\""));
    }

    #[test]
    fn roles_read_as_people_not_internals() {
        assert_eq!(role_label(TrackRole::LocalMic), "you");
        assert_eq!(role_label(TrackRole::RemoteMix), "others");
    }
}

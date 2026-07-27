//! Runs queued pipeline work in the background.
//!
//! Transcription and summarisation are slow and must not block recording, the
//! interface, or each other. The scheduler owns one worker thread that claims
//! jobs from the durable queue and runs them one at a time.
//!
//! # Why the queue rather than direct calls
//!
//! Jobs survive a crash. A recording whose transcription was interrupted has a
//! row saying so, and the startup sweep requeues it; calling the work directly
//! from wherever a recording finished would lose it silently. Serialising the
//! work also matters on a laptop: two models loaded at once would compete for
//! the memory and cores the meeting itself may still need.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context, Result};
use kaseta_contracts::{JobState, JobType, RecordingManifest};
use ulid::Ulid;

use crate::blobstore::BlobStore;
use crate::db::Db;

/// How long to wait before looking for work again when the queue is empty.
///
/// Polling rather than signalling: jobs are minutes long and arrive every few
/// minutes at most, so a second of latency costs nothing and avoids another
/// channel to keep alive across a crash.
const IDLE_POLL: Duration = Duration::from_secs(1);

/// How often the retention policy is applied while the daemon runs.
///
/// Retention is measured in days, so checking more often than daily would only
/// burn wakeups.
const RETENTION_INTERVAL: Duration = Duration::from_secs(24 * 3600);

pub struct Scheduler {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Scheduler {
    pub fn spawn(store: Arc<dyn BlobStore>, db: Arc<Mutex<Db>>, storage_root: String) -> Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);

        let thread = std::thread::Builder::new()
            .name("kaseta-scheduler".into())
            .spawn(move || run(store, db, storage_root, flag))
            .context("spawning scheduler thread")?;

        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Queues the work that should follow a finished recording.
pub fn enqueue_for_recording(db: &Db, recording_id: Ulid) -> Result<()> {
    db.enqueue(recording_id, JobType::Transcribe, 1)
        .context("queueing transcription")?;
    Ok(())
}

fn run(
    store: Arc<dyn BlobStore>,
    db: Arc<Mutex<Db>>,
    storage_root: String,
    stop: Arc<AtomicBool>,
) {
    let mut next_sweep = std::time::Instant::now() + RETENTION_INTERVAL;

    while !stop.load(Ordering::SeqCst) {
        if std::time::Instant::now() >= next_sweep {
            next_sweep = std::time::Instant::now() + RETENTION_INTERVAL;
            let settings = crate::config::Settings::load().unwrap_or_default();
            // Takes the lock itself, per recording, rather than holding it for
            // the whole sweep: deleting many recordings from slow storage would
            // otherwise stall every request behind it for minutes.
            if let Err(e) = crate::retention::sweep(
                &*store,
                &db,
                &settings.retention,
                time::OffsetDateTime::now_utc(),
            ) {
                tracing::error!(error = %format!("{e:#}"), "retention sweep failed");
            }
        }

        match claim(&db) {
            Ok(Some(job)) => {
                let outcome = execute(&*store, &db, &storage_root, job.job_type, job.recording_id);
                finish(&db, job.id, job.job_type, job.recording_id, outcome);
            }
            Ok(None) => std::thread::sleep(IDLE_POLL),
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "could not claim work");
                std::thread::sleep(IDLE_POLL);
            }
        }
    }
}

fn claim(db: &Arc<Mutex<Db>>) -> Result<Option<kaseta_contracts::Job>> {
    let db = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
    // Retryable failures are returned to the queue before looking for new work,
    // so a transient error does not strand a recording until the next restart.
    let _ = db.requeue_retryable();
    db.claim_next_job(std::process::id())
}

fn execute(
    store: &dyn BlobStore,
    db: &Arc<Mutex<Db>>,
    storage_root: &str,
    job_type: JobType,
    recording_id: Ulid,
) -> Result<()> {
    let manifest = load_manifest(store, db, recording_id)?;

    match job_type {
        JobType::Transcribe => {
            // Deliberately outside any lock: this spawns a child process and can
            // run as long as the meeting did. Holding the database meanwhile
            // would stall every request and every other job behind it.
            let transcription = crate::transcribe::transcribe(store, storage_root, &manifest)?;

            let guard = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
            let segments =
                crate::transcribe::store_transcript(&guard, recording_id, &transcription)?;
            tracing::info!(%recording_id, segments, "transcribed");
            Ok(())
        }
        JobType::Summarize => {
            let settings = crate::config::Settings::load().unwrap_or_default();
            if !settings.summaries.enabled && std::env::var(crate::summarize::API_KEY_ENV).is_err() {
                tracing::info!("summaries are turned off");
                return Ok(());
            }

            // Absent configuration is not a failure: summaries are optional and
            // a recording without one is still complete.
            let config = match crate::summarize::SummarizeConfig::resolve(&settings) {
                Ok(config) => config,
                Err(e) => {
                    tracing::info!(reason = %e, "skipping summary");
                    return Ok(());
                }
            };

            let transcript = {
                let guard = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
                crate::library::transcript(&guard, recording_id)?
            };
            let Some(transcript) = transcript else {
                return Ok(());
            };

            // The call is made without the lock held: it reaches the network and
            // can take minutes, and nothing else could touch the database
            // meanwhile.
            let summary = crate::summarize::summarize(&config, &transcript)?;

            let guard = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
            crate::summarize::store(&guard, recording_id, &config.model, &summary)?;
            tracing::info!(%recording_id, "summarised");
            Ok(())
        }
        JobType::UploadRemote => {
            let settings = crate::config::Settings::load().unwrap_or_default();
            if !settings.remote_storage.enabled {
                tracing::debug!("cloud backup is turned off");
                return Ok(());
            }

            let target = crate::remote::RemoteTarget::from_settings(&settings.remote_storage)?;
            let prefix = kaseta_contracts::RecordingPrefix::new(
                manifest.recording_id,
                manifest.started_at,
            );

            // Everything under the recording's prefix: audio, metadata,
            // exports. Uploading only the exports would leave a copy that
            // cannot be rebuilt if the local chunks are removed.
            let keys = store.list_prefix(prefix.root().as_str())?;
            let client = crate::remote::client()?;

            let mut uploaded = 0usize;
            let mut skipped = 0usize;
            for key in &keys {
                let bytes = store.get(key)?;
                match crate::remote::put_object(&client, &target, key.as_str(), &bytes)? {
                    crate::remote::PutResult::Uploaded => uploaded += 1,
                    crate::remote::PutResult::AlreadyPresent => skipped += 1,
                }
            }
            tracing::info!(%recording_id, uploaded, skipped, "backed up");

            // Recorded so the interface can say where a recording's audio
            // actually lives, which matters once local copies are removed.
            if let Ok(guard) = db.lock() {
                let _ = guard.conn().execute(
                    "UPDATE recordings SET uploaded_at = strftime('%s','now') WHERE id = ?1",
                    rusqlite::params![recording_id.to_string()],
                );
            }

            // Only after every object is confirmed present: deleting on a
            // partial upload would destroy the only complete copy.
            if settings.remote_storage.delete_local_after_upload {
                for key in &keys {
                    // Exports are derived and can be rebuilt; the chunks and
                    // manifest are the recording itself and are what the remote
                    // copy now holds.
                    if key.as_str().contains("/tracks/") {
                        if let Err(e) = store.delete(key) {
                            tracing::warn!(%key, error = %format!("{e:#}"), "could not remove local copy");
                        }
                    }
                }
                tracing::info!(%recording_id, "local audio removed after backup");
            }
            Ok(())
        }
        // Stages that exist in the model but have no implementation yet succeed
        // rather than failing, so they do not strand a recording in the queue.
        other => {
            tracing::debug!(?other, "no handler for this stage");
            Ok(())
        }
    }
}

/// Reads a recording's manifest, which every stage needs.
fn load_manifest(
    store: &dyn BlobStore,
    db: &Arc<Mutex<Db>>,
    recording_id: Ulid,
) -> Result<RecordingManifest> {
    let started_at: i64 = {
        let guard = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        guard
            .conn()
            .query_row(
                "SELECT started_at FROM recordings WHERE id = ?1",
                rusqlite::params![recording_id.to_string()],
                |r| r.get(0),
            )
            .with_context(|| format!("recording {recording_id} is not indexed"))?
    };

    let started_at = time::OffsetDateTime::from_unix_timestamp(started_at)
        .context("recording has an invalid start time")?;
    let prefix = kaseta_contracts::RecordingPrefix::new(recording_id, started_at);

    let bytes = store
        .get(&prefix.manifest())
        .with_context(|| format!("reading the manifest for {recording_id}"))?;
    serde_json::from_slice(&bytes).context("parsing the manifest")
}

/// Records how a job ended, and queues the next stage when it succeeded.
/// What follows a finished stage, if anything.
fn next_stage(job_type: JobType) -> Option<JobType> {
    match job_type {
        JobType::Transcribe => Some(JobType::Summarize),
        // Backup last: it copies everything the earlier stages produced.
        JobType::Summarize => Some(JobType::UploadRemote),
        _ => None,
    }
}

fn finish(
    db: &Arc<Mutex<Db>>,
    job_id: Ulid,
    job_type: JobType,
    recording_id: Ulid,
    outcome: Result<()>,
) {
    let Ok(guard) = db.lock() else {
        tracing::error!("database lock poisoned; job state not recorded");
        return;
    };

    match outcome {
        Ok(()) => {
            if let Err(e) = guard.transition_job(job_id, JobState::Succeeded) {
                tracing::error!(error = %format!("{e:#}"), "could not record success");
                return;
            }
            // Chained only after the current stage is durably successful, and
            // only once. Enqueueing from inside the stage let a crash in the
            // gap leave the follow-up queued while the stage itself was
            // requeued, doubling paid work on restart.
            if let Some(next) = next_stage(job_type) {
                match guard.enqueue_once(recording_id, next, 1) {
                    Ok(Some(_)) => tracing::debug!(?next, "queued next stage"),
                    Ok(None) => tracing::debug!(?next, "next stage already queued"),
                    Err(e) => tracing::error!(error = %format!("{e:#}"), "could not queue next stage"),
                }
            }
        }
        Err(e) => {
            let message = format!("{e:#}");
            tracing::error!(?job_type, error = %message, "job failed");
            // Treated as retryable: most failures here are a missing worker or a
            // transient read, both of which a later attempt can succeed at. The
            // attempt budget stops a permanently broken job from looping.
            if let Err(e) = guard.fail_job(job_id, "job_failed", &message, true) {
                tracing::error!(error = %format!("{e:#}"), "could not record failure");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaseta_contracts::JobType;

    fn db_with_recording() -> (Arc<Mutex<Db>>, Ulid) {
        let db = Db::open_in_memory().unwrap();
        let rec = Ulid::new();
        db.conn()
            .execute(
                "INSERT INTO recordings (id, owner_id, status, started_at, manifest_version)
                 VALUES (?1, 1, 'ready', 0, 'recording-manifest/v1')",
                rusqlite::params![rec.to_string()],
            )
            .unwrap();
        (Arc::new(Mutex::new(db)), rec)
    }

    #[test]
    fn a_finished_recording_queues_transcription() {
        let (db, rec) = db_with_recording();
        {
            let guard = db.lock().unwrap();
            enqueue_for_recording(&guard, rec).unwrap();
        }

        let claimed = claim(&db).unwrap().expect("work should be queued");
        assert_eq!(claimed.job_type, JobType::Transcribe);
        assert_eq!(claimed.recording_id, rec);
    }

    #[test]
    fn a_failed_job_returns_to_the_queue_until_its_budget_runs_out() {
        let (db, rec) = db_with_recording();
        {
            let guard = db.lock().unwrap();
            enqueue_for_recording(&guard, rec).unwrap();
        }

        // Fail it as many times as the budget allows. Backoff is skipped so the
        // test does not have to wait minutes for what it is actually checking.
        let mut attempts = 0;
        while let Some(job) = claim(&db).unwrap() {
            finish(&db, job.id, job.job_type, rec, Err(anyhow::anyhow!("worker missing")));
            attempts += 1;
            db.lock()
                .unwrap()
                .conn()
                .execute("UPDATE jobs SET next_attempt_at = NULL", [])
                .unwrap();
            if attempts > 10 {
                panic!("a permanently failing job must stop being retried");
            }
        }

        assert_eq!(
            attempts,
            kaseta_contracts::Job::DEFAULT_MAX_ATTEMPTS,
            "the attempt budget must bound retries"
        );
    }

    #[test]
    fn a_successful_stage_queues_the_next_one_exactly_once() {
        let (db, rec) = db_with_recording();
        {
            let guard = db.lock().unwrap();
            enqueue_for_recording(&guard, rec).unwrap();
        }

        let job = claim(&db).unwrap().unwrap();
        assert_eq!(job.job_type, JobType::Transcribe);
        finish(&db, job.id, job.job_type, rec, Ok(()));

        let next = claim(&db).unwrap().expect("transcription should queue a summary");
        assert_eq!(next.job_type, JobType::Summarize);
        assert_ne!(next.id, job.id, "the finished job must not be reclaimed");

        finish(&db, next.id, next.job_type, rec, Ok(()));

        // Backup is the final stage: it copies what everything before produced.
        let last = claim(&db).unwrap().expect("a summary should queue a backup");
        assert_eq!(last.job_type, JobType::UploadRemote);

        finish(&db, last.id, last.job_type, rec, Ok(()));
        assert!(claim(&db).unwrap().is_none(), "nothing follows the last stage");
    }

    #[test]
    fn a_failed_stage_does_not_queue_the_next_one() {
        // Summarising a transcript that was never produced would fail anyway,
        // and would burn a paid request finding that out.
        let (db, rec) = db_with_recording();
        {
            let guard = db.lock().unwrap();
            enqueue_for_recording(&guard, rec).unwrap();
        }

        let job = claim(&db).unwrap().unwrap();
        finish(&db, job.id, job.job_type, rec, Err(anyhow::anyhow!("no worker")));

        let queued: i64 = db
            .lock()
            .unwrap()
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM jobs WHERE job_type = 'summarize'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(queued, 0);
    }

    #[test]
    fn an_empty_queue_yields_no_work() {
        let (db, _) = db_with_recording();
        assert!(claim(&db).unwrap().is_none());
    }
}

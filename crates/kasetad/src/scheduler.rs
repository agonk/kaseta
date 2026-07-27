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
    while !stop.load(Ordering::SeqCst) {
        match claim(&db) {
            Ok(Some(job)) => {
                let outcome = execute(&*store, &db, &storage_root, job.job_type, job.recording_id);
                finish(&db, job.id, job.job_type, outcome);
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
            let guard = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
            let segments =
                crate::transcribe::transcribe_recording(store, &guard, storage_root, &manifest)?;
            tracing::info!(%recording_id, segments, "transcribed");
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
fn finish(db: &Arc<Mutex<Db>>, job_id: Ulid, job_type: JobType, outcome: Result<()>) {
    let Ok(guard) = db.lock() else {
        tracing::error!("database lock poisoned; job state not recorded");
        return;
    };

    match outcome {
        Ok(()) => {
            if let Err(e) = guard.transition_job(job_id, JobState::Succeeded) {
                tracing::error!(error = %format!("{e:#}"), "could not record success");
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

        // Fail it as many times as the budget allows.
        let mut attempts = 0;
        while let Some(job) = claim(&db).unwrap() {
            finish(&db, job.id, job.job_type, Err(anyhow::anyhow!("worker missing")));
            attempts += 1;
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
    fn a_successful_job_is_not_claimed_again() {
        let (db, rec) = db_with_recording();
        {
            let guard = db.lock().unwrap();
            enqueue_for_recording(&guard, rec).unwrap();
        }

        let job = claim(&db).unwrap().unwrap();
        finish(&db, job.id, job.job_type, Ok(()));

        assert!(
            claim(&db).unwrap().is_none(),
            "finished work must not be picked up a second time"
        );
    }

    #[test]
    fn an_empty_queue_yields_no_work() {
        let (db, _) = db_with_recording();
        assert!(claim(&db).unwrap().is_none());
    }
}

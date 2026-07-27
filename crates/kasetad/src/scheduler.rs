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

/// How often changed recordings are re-queued for backup.
///
/// The delay is the point, not a compromise. Renaming a recording marks it as
/// needing re-upload, and someone correcting a title three times in a row
/// should cost one upload rather than three. A minute is long enough to absorb
/// that and short enough that nobody waits on it.
const DIRTY_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

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
///
/// Transcription and backup are queued side by side rather than in sequence.
/// Backup used to hang off the end of the chain, which meant a recording whose
/// transcription failed was never copied anywhere — precisely the recording
/// most worth having a copy of. The transcript follows it up: writing one marks
/// the recording changed, and the sweep brings it round for a second pass.
///
/// Idempotent, so being called twice after a crash costs nothing.
pub fn enqueue_for_recording(db: &Db, recording_id: Ulid) -> Result<()> {
    db.enqueue_once(recording_id, JobType::Transcribe, 1)
        .context("queueing transcription")?;
    db.enqueue_once(recording_id, JobType::UploadRemote, 1)
        .context("queueing backup")?;
    Ok(())
}

fn run(
    store: Arc<dyn BlobStore>,
    db: Arc<Mutex<Db>>,
    storage_root: String,
    stop: Arc<AtomicBool>,
) {
    let mut next_sweep = std::time::Instant::now() + RETENTION_INTERVAL;
    let mut next_dirty_sweep = std::time::Instant::now() + DIRTY_SWEEP_INTERVAL;

    while !stop.load(Ordering::SeqCst) {
        if std::time::Instant::now() >= next_dirty_sweep {
            next_dirty_sweep = std::time::Instant::now() + DIRTY_SWEEP_INTERVAL;
            let settings = crate::config::Settings::load().unwrap_or_default();
            match db.lock() {
                Ok(guard) => {
                    if let Err(e) = crate::derived::requeue_dirty(&guard, &settings) {
                        tracing::warn!(error = %format!("{e:#}"), "could not queue changed backups");
                    }
                }
                Err(_) => tracing::error!("database lock poisoned"),
            }
        }

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

/// Why a stage can do nothing right now, phrased for a person to read.
///
/// Two callers need the same answer for opposite reasons. The scheduler skips
/// and logs: a recording that is simply complete without a summary must not
/// wear a permanent failure because the automatic chain reached a stage that
/// was switched off. The API refuses: someone who pressed a button asked for a
/// specific thing, and a job that reports success having done nothing is
/// indistinguishable from a button that does not work.
///
/// Deliberately advisory rather than authoritative. A transcript can be deleted
/// by retention between the button and the job, so the worker keeps its own
/// guards instead of trusting a check made earlier.
pub fn why_not_runnable(
    db: &Db,
    settings: &crate::config::Settings,
    recording_id: Ulid,
    job_type: JobType,
) -> Result<Option<String>> {
    Ok(match job_type {
        JobType::Summarize => {
            if !settings.summaries.enabled {
                Some("summaries are switched off in Settings".into())
            } else if crate::summarize::SummarizeConfig::resolve(settings).is_err() {
                Some("summaries need an API key, which is not set in Settings".into())
            } else if crate::library::transcript(db, recording_id)?.is_none() {
                Some("there is no transcript to summarise yet".into())
            } else {
                None
            }
        }
        JobType::UploadRemote => {
            if !settings.remote_storage.enabled {
                Some("cloud backup is switched off in Settings".into())
            } else {
                // The validator's own words. Summarising them here as a missing
                // bucket would misdirect anyone whose actual problem was a
                // plain-http endpoint, which it also rejects.
                crate::remote::RemoteTarget::from_settings(&settings.remote_storage)
                    .err()
                    .map(|e| format!("cloud backup is not usable: {e}"))
            }
        }
        JobType::Transcribe => {
            if !settings.transcription.enabled {
                Some("transcription is switched off in Settings".into())
            } else {
                None
            }
        }
        // Listed rather than defaulted: a stage added later becomes a
        // compilation error here, which is the only reliable way to notice that
        // it needs preconditions of its own.
        JobType::FinalizeRecording | JobType::MergeTranscript => None,
    })
}

fn execute(
    store: &dyn BlobStore,
    db: &Arc<Mutex<Db>>,
    storage_root: &str,
    job_type: JobType,
    recording_id: Ulid,
) -> Result<StageOutcome> {
    let manifest = load_manifest(store, db, recording_id)?;
    let settings = crate::config::Settings::load().unwrap_or_default();

    // Asked once, for every stage, before any work starts. A stage that cannot
    // run says so rather than running and quietly achieving nothing.
    {
        let guard = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        if let Some(reason) = why_not_runnable(&guard, &settings, recording_id, job_type)? {
            // One combination deserves more than a skip note. Someone who set a
            // key in the environment and never opened Settings would otherwise
            // just stop getting summaries with nothing said about why.
            if job_type == JobType::Summarize
                && !settings.summaries.enabled
                && std::env::var(crate::summarize::API_KEY_ENV)
                    .is_ok_and(|k| !k.trim().is_empty())
            {
                tracing::warn!(
                    "a summary key is set in the environment but summaries are switched off, \
                     so nothing was sent; switch them on in Settings to use it"
                );
            }
            return Ok(StageOutcome::Skipped(reason));
        }
    }

    match job_type {
        JobType::Transcribe => {
            // Deliberately outside any lock: this spawns a child process and can
            // run as long as the meeting did. Holding the database meanwhile
            // would stall every request and every other job behind it.
            let transcription = crate::transcribe::transcribe(store, storage_root, &manifest)?;

            let guard = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
            let prefix = kaseta_contracts::RecordingPrefix::new(
                manifest.recording_id,
                manifest.started_at,
            );
            let segments = crate::transcribe::store_transcript(
                store,
                &prefix,
                &guard,
                recording_id,
                &transcription,
            )?;
            tracing::info!(%recording_id, segments, "transcribed");
            Ok(StageOutcome::Done)
        }
        JobType::Summarize => {
            let config = crate::summarize::SummarizeConfig::resolve(&settings)?;

            let transcript = {
                let guard = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
                crate::library::transcript(&guard, recording_id)?
            };
            // Checked a moment ago, so its absence now means retention or a
            // delete landed in between. Still not a failure.
            let Some(transcript) = transcript else {
                return Ok(StageOutcome::Skipped(
                    "the transcript went away before it could be summarised".into(),
                ));
            };

            // The call is made without the lock held: it reaches the network and
            // can take minutes, and nothing else could touch the database
            // meanwhile.
            let summary = crate::summarize::summarize(&config, &transcript)?;

            let guard = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
            let prefix = kaseta_contracts::RecordingPrefix::new(
                manifest.recording_id,
                manifest.started_at,
            );
            crate::summarize::store(
                store,
                &prefix,
                &guard,
                recording_id,
                &config.model,
                &summary,
            )?;
            tracing::info!(%recording_id, "summarised");
            Ok(StageOutcome::Done)
        }
        JobType::UploadRemote => {
            let target = crate::remote::RemoteTarget::from_settings(&settings.remote_storage)?;
            let prefix = kaseta_contracts::RecordingPrefix::new(
                manifest.recording_id,
                manifest.started_at,
            );

            // Anything derived that has not reached the store yet is written
            // first, so this uploads a complete recording rather than one
            // missing whatever happened to fail earlier.
            let store_complete = {
                let guard = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
                match crate::derived::store_for(store, &guard, recording_id, &prefix) {
                    Ok(_) => {
                        // The store is current, whatever happens to the upload
                        // next. Leaving this set would have the startup sweep
                        // rewrite blobs it had already written.
                        let _ = crate::derived::clear_store_dirty(&guard, recording_id);
                        true
                    }
                    Err(e) => {
                        tracing::warn!(
                            %recording_id,
                            error = %format!("{e:#}"),
                            "some derived artefacts could not be stored before backup"
                        );
                        false
                    }
                }
            };

            // Everything under the recording's prefix: audio, metadata,
            // exports, transcript, summary, title. Uploading only the exports
            // would leave a copy that cannot be rebuilt if the local chunks are
            // removed.
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
                // Only when the store was complete before the upload started.
                // Clearing it regardless would call a backup finished while an
                // artefact that failed to be written locally is missing from
                // the bucket, and nothing would ever go back for it.
                if store_complete {
                    let _ = crate::derived::clear_remote_dirty(&guard, recording_id);
                    // Recorded only for a copy that is actually complete. The
                    // interface reads this as "there is a remote copy", and
                    // saying so of a recording whose transcript failed to be
                    // written would be the same overstatement this whole change
                    // set exists to remove.
                    let _ = guard.conn().execute(
                        "UPDATE recordings SET uploaded_at = strftime('%s','now') WHERE id = ?1",
                        rusqlite::params![recording_id.to_string()],
                    );
                } else {
                    tracing::warn!(
                        %recording_id,
                        "backed up, but something derived is missing; it stays queued for another pass"
                    );
                }
            }

            // Chunks are also what transcription reads: the model's input is
            // rebuilt from them, not from the exports. Backup no longer waits
            // for transcription, so an upload can now arrive first — and
            // removing the chunks then would leave the recording safely in the
            // bucket and permanently untranscribable here.
            //
            // Waiting for the transcript costs one more pass; the flag stays
            // set, and the next sweep deletes them once there is one.
            let transcribed = {
                let guard = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
                crate::library::transcript(&guard, recording_id)?.is_some()
            };

            // Only after every object is confirmed present: deleting on a
            // partial upload would destroy the only complete copy.
            if settings.remote_storage.delete_local_after_upload
                && !may_delete_local(&settings, transcribed)
            {
                tracing::info!(
                    %recording_id,
                    "keeping local audio until it has been transcribed"
                );
            } else if may_delete_local(&settings, transcribed) {
                for key in &keys {
                    // Chunks only. The mixed export stays, and it is what the
                    // player reads — there is no path that streams audio back
                    // out of the bucket, so removing it would leave a recording
                    // that is backed up and unplayable. That makes this reclaim
                    // roughly half of what a recording occupies rather than all
                    // of it, which the setting's wording should not overstate.
                    if key.as_str().contains("/tracks/") {
                        if let Err(e) = store.delete(key) {
                            tracing::warn!(%key, error = %format!("{e:#}"), "could not remove local copy");
                        }
                    }
                }
                tracing::info!(%recording_id, "local audio removed after backup");
            }
            Ok(StageOutcome::Done)
        }
        // Named rather than caught by a wildcard. These exist in the job model
        // and are performed elsewhere — sealing happens as capture ends, and
        // merging is folded into transcription — so reaching one here means
        // nothing is owed. A wildcard would have quietly reported success for a
        // stage added later and never implemented.
        JobType::FinalizeRecording | JobType::MergeTranscript => Ok(StageOutcome::Skipped(
            "this stage is handled elsewhere in the pipeline".into(),
        )),
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
/// Whether a backed-up recording's local chunks may now be removed.
///
/// The chunks are two things at once: the recording, and transcription's input.
/// Backup no longer waits for transcription, so "it is in the bucket" is no
/// longer sufficient — removing them before a transcript exists leaves the
/// recording safe remotely and permanently unprocessable here.
///
/// Deferring costs nothing. Writing a transcript marks the recording as
/// changed, which brings it round for another upload, and the chunks go then.
/// Waiting forever is not the same as waiting: with transcription switched off
/// there is no transcript coming, and holding the chunks for one would quietly
/// refuse a setting the person did ask for.
fn may_delete_local(settings: &crate::config::Settings, transcribed: bool) -> bool {
    settings.remote_storage.delete_local_after_upload
        && (transcribed || !settings.transcription.enabled)
}

/// What a stage did, as distinct from whether it failed.
///
/// A stage that was switched off has neither succeeded nor failed: it had
/// nothing to do. Collapsing that into success made two things go wrong at
/// once — the next stage was queued as though work had been done, and the
/// interface offered to run something it had no way of knowing was skipped.
#[derive(Debug)]
pub enum StageOutcome {
    Done,
    /// Nothing to do, and why — in words meant for a person.
    Skipped(String),
}

fn next_stage(job_type: JobType) -> Option<JobType> {
    match job_type {
        // Summarising reads the transcript, so it follows transcription.
        // Backup does not: a recording whose transcription failed is the one
        // most worth having a copy of, so it is queued at finalisation and
        // brought round again by the sweep whenever anything derived changes.
        JobType::Transcribe => Some(JobType::Summarize),
        JobType::Summarize
        | JobType::UploadRemote
        | JobType::FinalizeRecording
        | JobType::MergeTranscript => None,
    }
}

fn finish(
    db: &Arc<Mutex<Db>>,
    job_id: Ulid,
    job_type: JobType,
    recording_id: Ulid,
    outcome: Result<StageOutcome>,
) {
    let Ok(guard) = db.lock() else {
        tracing::error!("database lock poisoned; job state not recorded");
        return;
    };

    match outcome {
        Ok(stage) => {
            if let Err(e) = guard.transition_job(job_id, JobState::Succeeded) {
                tracing::error!(error = %format!("{e:#}"), "could not record success");
                return;
            }

            // A skip is recorded rather than left to look like work. Without
            // it the interface cannot tell "summarised" from "summaries are
            // off", and the finished row would refuse a later manual run.
            if let StageOutcome::Skipped(reason) = &stage {
                if let Err(e) = guard.mark_job_skipped(job_id, reason) {
                    tracing::warn!(error = %format!("{e:#}"), "could not record a skip");
                }
                tracing::info!(?job_type, %reason, "stage skipped");
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
            // One stage, not the whole set: finalisation now queues backup
            // alongside transcription, and failing both would count two
            // budgets as one.
            guard.enqueue(rec, JobType::Transcribe, 1).unwrap();
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
        finish(&db, job.id, job.job_type, rec, Ok(StageOutcome::Done));

        // Backup was queued alongside transcription, not behind it.
        let mut seen = Vec::new();
        while let Some(next) = claim(&db).unwrap() {
            assert_ne!(next.id, job.id, "the finished job must not be reclaimed");
            seen.push(next.job_type);
            finish(&db, next.id, next.job_type, rec, Ok(StageOutcome::Done));
        }

        assert!(seen.contains(&JobType::UploadRemote), "backup must be queued");
        assert!(
            seen.contains(&JobType::Summarize),
            "transcription should queue a summary"
        );
        assert_eq!(seen.len(), 2, "nothing else should have been queued");
    }

    /// Backup is queued at finalisation rather than at the end of the chain, so
    /// a recording whose transcription never succeeds is still copied — which
    /// is the case where a copy matters most.
    #[test]
    fn backup_is_queued_even_if_transcription_never_succeeds() {
        let (db, rec) = db_with_recording();
        {
            let guard = db.lock().unwrap();
            enqueue_for_recording(&guard, rec).unwrap();
        }

        let job = claim(&db).unwrap().unwrap();
        assert_eq!(job.job_type, JobType::Transcribe);
        finish(
            &db,
            job.id,
            job.job_type,
            rec,
            Err(anyhow::anyhow!("the model would not load")),
        );

        let next = claim(&db).unwrap().expect("backup must still be waiting");
        assert_eq!(next.job_type, JobType::UploadRemote);
    }

    /// A stage that had nothing to do must not pretend work happened. Chaining
    /// on a skip would queue a summary for a transcript that was never made.
    #[test]
    fn a_skipped_stage_does_not_queue_the_next_one() {
        let (db, rec) = db_with_recording();
        {
            let guard = db.lock().unwrap();
            guard.enqueue(rec, JobType::Transcribe, 1).unwrap();
        }

        let job = claim(&db).unwrap().unwrap();
        finish(
            &db,
            job.id,
            job.job_type,
            rec,
            Ok(StageOutcome::Skipped("transcription is switched off".into())),
        );

        assert!(
            claim(&db).unwrap().is_none(),
            "a skipped stage must not queue the one that depends on it"
        );
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

    /// Summaries configured every way that should stop them, checked one at a
    /// time so a passing case cannot mask a failing one.
    fn summaries_on_with_key() -> crate::config::Settings {
        let mut s = crate::config::Settings::default();
        s.summaries.enabled = true;
        s.summaries.api_key = Some("sk-or-test".into());
        s
    }

    #[test]
    fn summarising_is_refused_while_the_switch_is_off() {
        let (db, rec) = db_with_recording();
        let mut settings = summaries_on_with_key();
        settings.summaries.enabled = false;

        let guard = db.lock().unwrap();
        let reason = why_not_runnable(&guard, &settings, rec, JobType::Summarize).unwrap();
        assert!(reason.unwrap().contains("switched off"));
    }

    /// The switch governs whether transcript text leaves the machine, so an
    /// available key must not reopen it. Asserting the *reason* rather than
    /// merely that there is one is what makes this meaningful: the switch is
    /// tested before anything looks for a key, so reaching this answer with a
    /// key present proves the key was never consulted.
    #[test]
    fn a_key_does_not_reopen_a_closed_switch() {
        let (db, rec) = db_with_recording();
        let mut settings = summaries_on_with_key();
        settings.summaries.enabled = false;

        let guard = db.lock().unwrap();
        let reason = why_not_runnable(&guard, &settings, rec, JobType::Summarize)
            .unwrap()
            .expect("a switched-off summary must stay off");
        assert!(
            reason.contains("switched off"),
            "the switch must be the reason even with a key available, got: {reason}"
        );
    }

    /// Unconditional: a key in the environment would also satisfy the key
    /// check, so the transcript branch is reached either way.
    #[test]
    fn summarising_is_refused_before_there_is_a_transcript() {
        let (db, rec) = db_with_recording();
        let settings = summaries_on_with_key();

        let guard = db.lock().unwrap();
        let reason = why_not_runnable(&guard, &settings, rec, JobType::Summarize).unwrap();
        assert!(reason.unwrap().contains("no transcript"));
    }

    #[test]
    fn backup_is_refused_until_it_is_configured() {
        let (db, rec) = db_with_recording();
        let mut settings = crate::config::Settings::default();

        let guard = db.lock().unwrap();
        let off = why_not_runnable(&guard, &settings, rec, JobType::UploadRemote).unwrap();
        assert!(off.unwrap().contains("switched off"));

        // Switched on but with nothing to upload to is a different complaint,
        // and saying "switched off" there would send someone to the wrong knob.
        settings.remote_storage.enabled = true;
        let bare = why_not_runnable(&guard, &settings, rec, JobType::UploadRemote).unwrap();
        assert!(bare.unwrap().contains("missing"));
    }

    /// A fully configured backup can still be rejected for a reason that has
    /// nothing to do with missing fields. Summarising every rejection as an
    /// incomplete form would send this person to check a bucket name that was
    /// never the problem.
    #[test]
    fn a_backup_refusal_says_what_was_actually_wrong() {
        let (db, rec) = db_with_recording();
        let mut settings = crate::config::Settings::default();
        settings.remote_storage.enabled = true;
        settings.remote_storage.bucket = Some("meetings".into());
        settings.remote_storage.region = Some("auto".into());
        settings.remote_storage.access_key_id = Some("key".into());
        settings.remote_storage.secret_access_key = Some("secret".into());
        settings.remote_storage.endpoint = Some("http://example.com".into());

        let guard = db.lock().unwrap();
        let reason = why_not_runnable(&guard, &settings, rec, JobType::UploadRemote)
            .unwrap()
            .expect("a plain-http endpoint is not usable");
        assert!(reason.contains("https"), "got: {reason}");
    }

    /// Backup used to run only after transcription, so a transcript always
    /// existed by the time local audio was removed. It no longer does, and the
    /// chunks are transcription's input — deleting them first would leave a
    /// recording that is backed up and can never be processed here again.
    #[test]
    fn local_audio_is_kept_until_there_is_a_transcript() {
        let mut settings = crate::config::Settings::default();
        settings.remote_storage.delete_local_after_upload = true;

        assert!(
            !may_delete_local(&settings, false),
            "an untranscribed recording must keep the audio transcription needs"
        );
        assert!(may_delete_local(&settings, true));
    }

    #[test]
    fn local_audio_is_never_removed_unless_asked_for() {
        let settings = crate::config::Settings::default();
        assert!(!may_delete_local(&settings, true));
        assert!(!may_delete_local(&settings, false));
    }

    /// Transcription depends on nothing that can be configured wrongly, so it
    /// must never be refused — the guard exists to explain, not to gate.
    #[test]
    fn transcription_is_never_refused() {
        let (db, rec) = db_with_recording();
        let settings = crate::config::Settings::default();

        let guard = db.lock().unwrap();
        assert!(why_not_runnable(&guard, &settings, rec, JobType::Transcribe)
            .unwrap()
            .is_none());
    }
}

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
use crate::import::ImportRuntime;

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
    pub fn spawn(
        store: Arc<dyn BlobStore>,
        db: Arc<Mutex<Db>>,
        storage_root: String,
        imports: Arc<ImportRuntime>,
    ) -> Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);

        let thread = std::thread::Builder::new()
            .name("kaseta-scheduler".into())
            .spawn(move || run(store, db, storage_root, imports, flag))
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
    imports: Arc<ImportRuntime>,
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
                let outcome = execute(
                    &*store,
                    &db,
                    &storage_root,
                    &imports,
                    job.id,
                    job.job_type,
                    job.recording_id,
                );
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
    // An import that has not finished decoding has no manifest, no audio and
    // nothing derived from either, so every other stage waits for it. Asked
    // first because it is true whatever the settings say.
    if job_type != JobType::ImportMedia {
        if let Some(reason) = unfinished_import(db, recording_id)? {
            return Ok(Some(reason));
        }
    }

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
        JobType::PublishWebhook => {
            if !settings.webhook.enabled {
                Some("sending is switched off in Settings".into())
            } else if !settings.webhook.is_configured() {
                Some("sending needs a URL and a token, which are not set in Settings".into())
            } else if crate::derived::transcript_document(db, recording_id)?.is_none() {
                Some("there is no transcript to send yet".into())
            } else {
                None
            }
        }
        JobType::FinalizeRecording | JobType::MergeTranscript => None,
        // Whether the uploaded file is still there to decode is a question
        // about storage, which the stage answers for itself when it runs.
        JobType::ImportMedia => None,
    })
}

/// Why an imported recording is not ready for anything else yet, if it is not.
fn unfinished_import(db: &Db, recording_id: Ulid) -> Result<Option<String>> {
    use rusqlite::OptionalExtension;
    let status: Option<String> = db
        .conn()
        .query_row(
            "SELECT status FROM recordings WHERE id = ?1 AND origin = 'imported'",
            rusqlite::params![recording_id.to_string()],
            |r| r.get(0),
        )
        .optional()?;
    Ok(match status.as_deref() {
        None | Some("ready") => None,
        Some("failed") => {
            Some("the file could not be imported, so there is nothing to work on".into())
        }
        Some(_) => Some("the recording is still being imported".into()),
    })
}

fn execute(
    store: &dyn BlobStore,
    db: &Arc<Mutex<Db>>,
    storage_root: &str,
    imports: &ImportRuntime,
    job_id: Ulid,
    job_type: JobType,
    recording_id: Ulid,
) -> Result<StageOutcome> {
    let settings = crate::config::Settings::load().unwrap_or_default();

    // Decoding is what produces the manifest, so it is dispatched before one
    // is read. It also skips the generic precondition check below: whether it
    // can run depends on its upload still being in storage, and a missing
    // upload is a failure to show on the recording rather than a quiet skip.
    if job_type == JobType::ImportMedia {
        let limits = crate::import::job::Limits::from_settings(&settings.imports);
        return crate::import::job::run(store, db, imports, &limits, recording_id, job_id);
    }

    let manifest = load_manifest(store, db, recording_id)?;

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

            // Streamed from the files, never loaded: a kept original can be
            // gigabytes. Digests the manifest already holds are reused rather
            // than taken again from the largest objects the recording has.
            let crate::remote::Uploaded { uploaded, skipped } = crate::remote::upload_keys(
                store,
                &client,
                &target,
                &keys,
                &crate::remote::known_digests(&manifest),
            )?;
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
                remove_local_copies(store, db, recording_id, &keys);
                tracing::info!(%recording_id, "local audio removed after backup");
            }
            Ok(StageOutcome::Done)
        }
        // Named rather than caught by a wildcard. These exist in the job model
        // and are performed elsewhere — sealing happens as capture ends, and
        // merging is folded into transcription — so reaching one here means
        // nothing is owed. A wildcard would have quietly reported success for a
        // stage added later and never implemented.
        JobType::PublishWebhook => {
            let (transcript, summary, item) = {
                let guard = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
                (
                    crate::derived::transcript_document(&guard, recording_id)?,
                    crate::derived::summary_document(&guard, recording_id)?,
                    crate::library::get(&guard, recording_id)?,
                )
            };
            let Some(transcript) = transcript else {
                return Ok(StageOutcome::Skipped("there is no transcript to send yet".into()));
            };
            let Some(item) = item else {
                return Ok(StageOutcome::Skipped("the recording is gone".into()));
            };

            let source = item.source.as_ref();
            let status = crate::webhook::publish(
                &settings.webhook,
                &crate::webhook::Recording {
                    id: recording_id,
                    title: &item.title,
                    recorded_at: crate::webhook::recorded_at(
                        item.started_at,
                        source.and_then(|s| s.media_created_at),
                    ),
                    origin: item.origin,
                    original_filename: source.map(|s| s.filename.as_str()),
                    transcript: &transcript,
                    summary: summary.as_ref(),
                    duration_s: (item.duration_ms > 0)
                        .then(|| item.duration_ms as f64 / 1000.0),
                },
            )?;
            tracing::info!(%recording_id, status, "sent to the webhook");
            Ok(StageOutcome::Done)
        }
        JobType::FinalizeRecording | JobType::MergeTranscript => Ok(StageOutcome::Skipped(
            "this stage is handled elsewhere in the pipeline".into(),
        )),
        // Dispatched above, before any manifest exists.
        JobType::ImportMedia => Err(anyhow::anyhow!(
            "decoding an import is dispatched before the manifest is read"
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

/// Removes the local copies a confirmed backup makes redundant.
///
/// Chunks and a kept original. The mixed export stays, and it is what the
/// player reads: there is no path that streams audio back out of the bucket,
/// so removing it would leave a recording that is backed up and unplayable.
/// That makes this reclaim the chunks' share of a recording rather than all of
/// it, which the setting's wording should not overstate. An original is
/// different: nothing here needs it once the audio is decoded, and it is
/// usually the largest object a recording has.
///
/// The index stops calling the original local only once it is actually gone,
/// so the interface never offers to play a file that is not there and never
/// hides one that is.
fn remove_local_copies(
    store: &dyn BlobStore,
    db: &Arc<Mutex<Db>>,
    recording_id: Ulid,
    keys: &[kaseta_contracts::BlobKey],
) {
    let mut original_removed = false;
    for key in keys {
        let original = crate::library::is_original(key);
        if !(original || key.as_str().contains("/tracks/")) {
            continue;
        }
        match store.delete(key) {
            Ok(()) => original_removed |= original,
            Err(e) => {
                tracing::warn!(%key, error = %format!("{e:#}"), "could not remove local copy")
            }
        }
    }
    if original_removed {
        let result = db
            .lock()
            .map_err(|_| anyhow::anyhow!("database lock poisoned"))
            .and_then(|guard| crate::library::forget_local_original(&guard, recording_id));
        if let Err(e) = result {
            tracing::warn!(
                %recording_id,
                error = %format!("{e:#}"),
                "could not record the original as removed"
            );
        }
    }
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
    /// The stage recorded its own success, and queued what follows, in the
    /// same transaction as its result. Only decoding an import does this: its
    /// result and its success must not be separable by a crash, so there is
    /// nothing left for [`finish`] to record.
    Finalized,
}

/// A failure that retrying cannot fix.
///
/// Every stage error is retryable unless it says otherwise, which is right for
/// a missing worker or a transient read and wrong for a statement about the
/// input: a payload the receiver rejected, or a file with no audio in it, will
/// fail identically on every attempt, and the reason somebody needs to read is
/// buried until the attempt budget runs out.
#[derive(Debug)]
pub struct Permanent(pub String);

impl std::fmt::Display for Permanent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Permanent {}

/// Whether finishing this stage is the moment to send the recording.
///
/// Off unless sending is switched on AND has somewhere to go: queueing a job
/// that can only report "not configured" turns a setting nobody filled in into
/// a failed stage on every recording.
///
/// `Manual` queues nothing, which is the whole of what it means.
fn publishes_after(webhook: &crate::config::WebhookSettings, job_type: JobType) -> bool {
    use crate::config::SendWhen;
    if !webhook.is_configured() {
        return false;
    }
    match webhook.send_when {
        SendWhen::Transcript => job_type == JobType::Transcribe,
        SendWhen::Summary => job_type == JobType::Summarize,
        SendWhen::Manual => false,
    }
}

fn next_stage(job_type: JobType) -> Option<JobType> {
    match job_type {
        // Summarising reads the transcript, so it follows transcription.
        // Backup does not: a recording whose transcription failed is the one
        // most worth having a copy of, so it is queued at finalisation and
        // brought round again by the sweep whenever anything derived changes.
        JobType::Transcribe => Some(JobType::Summarize),
        // Decoding an import is followed by transcription and backup, but they
        // are queued by finalisation, in the transaction that indexes the
        // recording, rather than here.
        JobType::ImportMedia
        | JobType::Summarize
        | JobType::UploadRemote
        | JobType::PublishWebhook
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

    // Deleting a recording while it is being imported takes the job with it.
    // That is the deletion working, not a failure to report.
    if job_type == JobType::ImportMedia {
        match guard.job(job_id) {
            Ok(Some(_)) => {}
            Ok(None) => {
                tracing::info!(%recording_id, "the import's recording was deleted while it ran");
                return;
            }
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "could not read the import's job");
                return;
            }
        }
    }

    match outcome {
        Ok(StageOutcome::Finalized) => {
            tracing::debug!(?job_type, %recording_id, "stage recorded its own success");
        }
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
            // Sending a recording is not a chained stage — nothing depends on
            // it — but it does have a moment, and the moment is a setting.
            // Read here rather than baked into next_stage() so that
            // "after the transcript" and "after the summary" are one branch
            // instead of two shapes of the pipeline.
            let webhook = crate::config::Settings::load().unwrap_or_default().webhook;
            if publishes_after(&webhook, job_type) {
                // Keyed on the transcript's content, not on 1. `enqueue_once`
                // counts a succeeded row as a duplicate, so a fixed revision
                // would let a recording be published exactly once and never
                // again — and a re-transcription, which is the whole reason to
                // send a second time, would be the case it silently dropped.
                let revision = crate::webhook::transcript_revision(&guard, recording_id)
                    .ok()
                    .flatten();
                // Nothing to send yet is not a reason to stop: the
                // chaining below still has to run, and an early return here
                // would leave a transcript without its summary.
                match revision.map(|r| guard.enqueue_once(recording_id, JobType::PublishWebhook, r))
                {
                    None => tracing::debug!(%recording_id, "no transcript to send yet"),
                    Some(Ok(Some(_))) => tracing::debug!("queued the send"),
                    Some(Ok(None)) => tracing::debug!("send already queued"),
                    Some(Err(e)) => {
                        tracing::error!(error = %format!("{e:#}"), "could not queue the send")
                    }
                }
            }

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
            // Retryable unless the stage said otherwise: most failures here are
            // a missing worker or a transient read, both of which a later
            // attempt can succeed at, and the attempt budget stops a
            // permanently broken job from looping. But some failures are
            // statements about the request rather than the moment — a payload
            // the receiver rejected will be rejected identically every time — and
            // burying those behind an attempt budget hides the one thing the
            // person needs to read.
            let retryable = e.downcast_ref::<Permanent>().is_none();
            tracing::error!(?job_type, retryable, error = %message, "job failed");
            // An import that ends here fails its recording inside the same
            // call, so the job and the row can never disagree after a crash.
            if let Err(e) = guard.fail_job(job_id, "job_failed", &message, retryable) {
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

    /// And the skip has to be legible afterwards, or the interface shows a
    /// stage that looks done and refuses to run when the setting is corrected.
    #[test]
    fn a_skip_is_recorded_with_its_reason() {
        let (db, rec) = db_with_recording();
        {
            let guard = db.lock().unwrap();
            guard.enqueue(rec, JobType::Summarize, 1).unwrap();
        }

        let job = claim(&db).unwrap().unwrap();
        finish(
            &db,
            job.id,
            job.job_type,
            rec,
            Ok(StageOutcome::Skipped("summaries are switched off".into())),
        );

        let guard = db.lock().unwrap();
        let stages = crate::library::stages_for(&guard, rec).unwrap();
        let summarize = stages.iter().find(|s| s.stage == "summarize").unwrap();

        assert_eq!(summarize.state, "skipped");
        assert_eq!(summarize.error.as_deref(), Some("summaries are switched off"));
        assert!(
            summarize.retryable,
            "correcting the setting must leave something to act on"
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

    fn original_local(db: &Arc<Mutex<Db>>, rec: Ulid) -> i64 {
        db.lock()
            .unwrap()
            .conn()
            .query_row(
                "SELECT original_local FROM recordings WHERE id = ?1",
                rusqlite::params![rec.to_string()],
                |r| r.get(0),
            )
            .unwrap()
    }

    /// After a confirmed backup the chunks and a kept original go, and the
    /// index stops calling the original local in the same pass. The mixed
    /// export and the metadata stay: they are what the player and the library
    /// read, and nothing streams them back from the bucket.
    #[test]
    fn removing_local_copies_takes_chunks_and_the_original() {
        let (db, rec) = db_with_recording();
        db.lock()
            .unwrap()
            .conn()
            .execute(
                "UPDATE recordings SET origin = 'imported', original_local = 1 WHERE id = ?1",
                rusqlite::params![rec.to_string()],
            )
            .unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let prefix = kaseta_contracts::RecordingPrefix::new(
            rec,
            time::OffsetDateTime::UNIX_EPOCH,
        );
        let chunk = kaseta_contracts::BlobKey::new(format!(
            "{}/tracks/a_imported_01/000000.flac",
            prefix.root()
        ))
        .unwrap();
        let original = prefix.original("mp4").unwrap();
        let mixed = prefix.export("mixed.flac").unwrap();
        let manifest = prefix.manifest();
        for key in [&chunk, &original, &mixed, &manifest] {
            store.put(key, b"x").unwrap();
        }
        let keys = store.list_prefix(prefix.root().as_str()).unwrap();

        remove_local_copies(&store, &db, rec, &keys);

        assert!(!store.exists(&chunk).unwrap());
        assert!(!store.exists(&original).unwrap());
        assert!(store.exists(&mixed).unwrap(), "the player reads the mixed export");
        assert!(store.exists(&manifest).unwrap());
        assert_eq!(original_local(&db, rec), 0);
    }

    /// A captured recording has no original, and its index row is left as it
    /// was.
    #[test]
    fn removing_local_copies_of_a_capture_touches_only_its_chunks() {
        let (db, rec) = db_with_recording();
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let prefix = kaseta_contracts::RecordingPrefix::new(
            rec,
            time::OffsetDateTime::UNIX_EPOCH,
        );
        let chunk = kaseta_contracts::BlobKey::new(format!(
            "{}/tracks/a_local-mic_01/000000.flac",
            prefix.root()
        ))
        .unwrap();
        let mixed = prefix.export("mixed.flac").unwrap();
        store.put(&chunk, b"x").unwrap();
        store.put(&mixed, b"x").unwrap();
        let updated_before: i64 = db
            .lock()
            .unwrap()
            .conn()
            .query_row("SELECT updated_at FROM recordings", [], |r| r.get(0))
            .unwrap();

        let keys = store.list_prefix(prefix.root().as_str()).unwrap();
        remove_local_copies(&store, &db, rec, &keys);

        assert!(!store.exists(&chunk).unwrap());
        assert!(store.exists(&mixed).unwrap());
        let updated_after: i64 = db
            .lock()
            .unwrap()
            .conn()
            .query_row("SELECT updated_at FROM recordings", [], |r| r.get(0))
            .unwrap();
        assert_eq!(updated_before, updated_after, "nothing about the row changed");
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

#[cfg(test)]
mod import_tests {
    use super::*;
    use crate::db::{IndexFields, NewImport};
    use kaseta_contracts::Origin;

    fn running_import() -> (Arc<Mutex<Db>>, Ulid, kaseta_contracts::Job) {
        let db = Db::open_in_memory().unwrap();
        let rec = Ulid::new();
        db.create_import(&NewImport {
            recording_id: rec,
            started_at: time::macros::datetime!(2026-10-09 08:00:00 UTC),
            title: "Lecture".into(),
        })
        .unwrap();
        let db = Arc::new(Mutex::new(db));
        let job = claim(&db).unwrap().unwrap();
        assert_eq!(job.job_type, JobType::ImportMedia);
        (db, rec, job)
    }

    fn status_of(db: &Arc<Mutex<Db>>, rec: Ulid) -> String {
        db.lock()
            .unwrap()
            .conn()
            .query_row(
                "SELECT status FROM recordings WHERE id = ?1",
                rusqlite::params![rec.to_string()],
                |r| r.get(0),
            )
            .unwrap()
    }

    fn fields() -> IndexFields {
        IndexFields {
            title: None,
            started_at: 0,
            ended_at: None,
            manifest_version: kaseta_contracts::MANIFEST_VERSION.into(),
            clock_started_ns: 1,
            duration_ms: 1_000,
            has_mixed: true,
            tracks_json: "[]".into(),
            origin: Origin::Imported,
            source_json: None,
            original_local: false,
        }
    }

    /// A file with no audio in it will have none on the next attempt either,
    /// and the person needs to read why on the recording, not on a job.
    #[test]
    fn a_permanent_import_failure_fails_the_recording_at_once() {
        let (db, rec, job) = running_import();
        finish(
            &db,
            job.id,
            job.job_type,
            rec,
            Err(Permanent("This file has no audio track".into()).into()),
        );

        let guard = db.lock().unwrap();
        let after = guard.job(job.id).unwrap().unwrap();
        assert_eq!(after.state, JobState::FailedTerminal);
        assert_eq!(after.error_message.as_deref(), Some("This file has no audio track"));
        drop(guard);
        assert_eq!(status_of(&db, rec), "failed");
    }

    /// Retryable failures come back around, and the recording keeps reading
    /// "processing" while they do. Once the budget is spent it fails like any
    /// other terminal end.
    #[test]
    fn an_import_that_runs_out_of_attempts_fails_the_recording() {
        let (db, rec, mut job) = running_import();
        for attempt in 1..=kaseta_contracts::Job::DEFAULT_MAX_ATTEMPTS {
            finish(&db, job.id, job.job_type, rec, Err(anyhow::anyhow!("ffmpeg crashed")));
            if attempt < kaseta_contracts::Job::DEFAULT_MAX_ATTEMPTS {
                assert_eq!(status_of(&db, rec), "processing", "attempt {attempt}");
                db.lock()
                    .unwrap()
                    .conn()
                    .execute("UPDATE jobs SET next_attempt_at = NULL", [])
                    .unwrap();
                job = claim(&db).unwrap().expect("the import comes back around");
            }
        }

        assert_eq!(status_of(&db, rec), "failed");
        assert!(claim(&db).unwrap().is_none());
    }

    /// Finalisation already marked the job and queued what follows, inside
    /// its own transaction. Doing either again here would fail the
    /// transition, or queue the follow-ups twice.
    #[test]
    fn a_finalised_import_is_left_as_finalisation_left_it() {
        let (db, rec, job) = running_import();
        assert!(db
            .lock()
            .unwrap()
            .finalize_import_success(rec, job.id, &fields())
            .unwrap());

        finish(&db, job.id, job.job_type, rec, Ok(StageOutcome::Finalized));

        let guard = db.lock().unwrap();
        assert_eq!(guard.job(job.id).unwrap().unwrap().state, JobState::Succeeded);
        let queued: i64 = guard
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM jobs WHERE recording_id = ?1 AND state = 'queued'",
                rusqlite::params![rec.to_string()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(queued, 2, "transcription and backup, once each");
    }

    /// Deleting a recording during its import cascades the job away. The
    /// import's own ending must then be a quiet no-op, not an error about a
    /// job that cannot be found.
    #[test]
    fn an_import_whose_recording_was_deleted_finishes_quietly() {
        for outcome in [
            Ok(StageOutcome::Skipped("the recording was deleted".into())),
            Err(anyhow::anyhow!("decode interrupted")),
            Err(Permanent("This file has no audio track".into()).into()),
        ] {
            let (db, rec, job) = running_import();
            db.lock()
                .unwrap()
                .conn()
                .execute(
                    "DELETE FROM recordings WHERE id = ?1",
                    rusqlite::params![rec.to_string()],
                )
                .unwrap();

            finish(&db, job.id, job.job_type, rec, outcome);

            let guard = db.lock().unwrap();
            assert!(guard.job(job.id).unwrap().is_none());
            let rows: i64 = guard
                .conn()
                .query_row("SELECT COUNT(*) FROM recordings", [], |r| r.get(0))
                .unwrap();
            assert_eq!(rows, 0, "nothing may be resurrected");
        }
    }

    /// Every other stage waits for the decode: there is no manifest, no audio
    /// and nothing derived until it finishes.
    #[test]
    fn no_other_stage_runs_on_an_unfinished_import() {
        let (db, rec, _) = running_import();
        let mut settings = crate::config::Settings::default();
        settings.summaries.enabled = true;
        settings.summaries.api_key = Some("sk-or-test".into());

        let guard = db.lock().unwrap();
        for stage in [
            JobType::Transcribe,
            JobType::Summarize,
            JobType::UploadRemote,
            JobType::PublishWebhook,
        ] {
            let reason = why_not_runnable(&guard, &settings, rec, stage)
                .unwrap()
                .unwrap_or_else(|| panic!("{stage:?} must wait for the import"));
            assert!(reason.contains("still being imported"), "{stage:?}: {reason}");
        }
        assert!(why_not_runnable(&guard, &settings, rec, JobType::ImportMedia)
            .unwrap()
            .is_none());

        guard
            .conn()
            .execute(
                "UPDATE recordings SET status = 'failed' WHERE id = ?1",
                rusqlite::params![rec.to_string()],
            )
            .unwrap();
        let reason = why_not_runnable(&guard, &settings, rec, JobType::Transcribe)
            .unwrap()
            .unwrap();
        assert!(reason.contains("could not be imported"), "{reason}");
    }

    #[test]
    fn importing_chains_to_nothing_through_finish() {
        assert_eq!(next_stage(JobType::ImportMedia), None);
    }

    /// The decode stage is what produces the manifest, so it must be reached
    /// without reading one: a manifest-first dispatch would fail every import
    /// with "reading the manifest". With no upload staged, what it reaches is
    /// the decode's own verdict, and that verdict is final.
    #[test]
    fn an_import_is_dispatched_before_any_manifest_is_read() {
        let (db, rec, job) = running_import();
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let imports = ImportRuntime::unavailable("no tools here");

        let err =
            execute(&store, &db, "", &imports, job.id, JobType::ImportMedia, rec).unwrap_err();
        assert_eq!(
            err.downcast_ref::<Permanent>().map(|p| p.0.as_str()),
            Some(crate::import::job::UPLOAD_GONE),
            "{err:#}"
        );

        finish(&db, job.id, job.job_type, rec, Err(err));
        assert_eq!(status_of(&db, rec), "failed");
    }

    /// The whole way through the scheduler: a staged file is decoded, the
    /// stage records its own success, and what follows is queued once.
    #[test]
    fn a_staged_file_goes_through_the_scheduler_to_ready() {
        use crate::import::fixtures::{Fixture, Harness};
        let Some(h) = Harness::new() else { return };
        let Some(file) = h.fixture(Fixture::VideoFirstMp4) else { return };
        let (rec, job) = h.stage(&file, false);

        let outcome = execute(&h.store, &h.db, "", &h.runtime, job.id, job.job_type, rec);
        assert!(matches!(outcome, Ok(StageOutcome::Finalized)), "{outcome:?}");
        finish(&h.db, job.id, job.job_type, rec, outcome);

        assert_eq!(status_of(&h.db, rec), "ready");
        let mut queued = Vec::new();
        while let Some(next) = claim(&h.db).unwrap() {
            queued.push(next.job_type);
            h.db
                .lock()
                .unwrap()
                .transition_job(next.id, JobState::Succeeded)
                .unwrap();
        }
        assert_eq!(queued, [JobType::Transcribe, JobType::UploadRemote]);
    }

    /// A file with no sound fails the recording with the reason, at once.
    #[test]
    fn a_file_without_sound_fails_its_recording_through_the_scheduler() {
        use crate::import::fixtures::{Fixture, Harness};
        let Some(h) = Harness::new() else { return };
        let Some(file) = h.fixture(Fixture::NoAudioMp4) else { return };
        let (rec, job) = h.stage(&file, false);

        let outcome = execute(&h.store, &h.db, "", &h.runtime, job.id, job.job_type, rec);
        finish(&h.db, job.id, job.job_type, rec, outcome);

        assert_eq!(status_of(&h.db, rec), "failed");
        let after = h.db.lock().unwrap().job(job.id).unwrap().unwrap();
        assert_eq!(after.state, JobState::FailedTerminal);
        assert_eq!(after.error_message.as_deref(), Some(crate::import::job::NO_AUDIO));
    }
}

#[cfg(test)]
mod send_trigger_tests {
    use super::*;
    use crate::config::{SendWhen, WebhookSettings};

    fn configured(send_when: SendWhen) -> WebhookSettings {
        WebhookSettings {
            enabled: true,
            url: Some("https://example.test/api/agent/intake".into()),
            token: Some("k_live_x".into()),
            send_when,
        }
    }

    #[test]
    fn transcript_mode_sends_when_the_transcript_lands() {
        let s = configured(SendWhen::Transcript);
        assert!(publishes_after(&s, JobType::Transcribe));
        assert!(!publishes_after(&s, JobType::Summarize));
    }

    /// Summaries are off by default, so this mode is a deliberate choice to wait
    /// for one — and it must not also fire on the transcript, or the recording
    /// would be sent before the summary it was told to wait for.
    #[test]
    fn summary_mode_waits_for_the_summary_and_only_that() {
        let s = configured(SendWhen::Summary);
        assert!(publishes_after(&s, JobType::Summarize));
        assert!(!publishes_after(&s, JobType::Transcribe));
    }

    #[test]
    fn manual_mode_queues_nothing_at_all() {
        let s = configured(SendWhen::Manual);
        for job in [JobType::Transcribe, JobType::Summarize, JobType::UploadRemote] {
            assert!(!publishes_after(&s, job), "{job:?} must not queue a send");
        }
    }

    /// A switch turned on but never filled in would otherwise queue a job that
    /// can only report "not configured" — once per recording, for ever.
    #[test]
    fn nothing_is_queued_until_there_is_somewhere_to_send_to() {
        for s in [
            WebhookSettings { enabled: false, ..configured(SendWhen::Transcript) },
            WebhookSettings { token: None, ..configured(SendWhen::Transcript) },
            WebhookSettings { url: None, ..configured(SendWhen::Transcript) },
        ] {
            assert!(!publishes_after(&s, JobType::Transcribe));
        }
    }

    #[test]
    fn the_send_chains_to_nothing() {
        assert_eq!(next_stage(JobType::PublishWebhook), None);
    }

    /// A rejected payload is a statement about the request, so retrying it
    /// spends the attempt budget to learn the same thing five times and hides
    /// the reason until it runs out.
    #[test]
    fn a_permanent_failure_is_not_retryable() {
        let err: anyhow::Error = crate::webhook::Permanent("token expired".into()).into();
        assert!(err.downcast_ref::<Permanent>().is_some(), "one type, wherever it is named");

        let transient = anyhow::anyhow!("connection reset");
        assert!(transient.downcast_ref::<Permanent>().is_none());
    }
}

//! Embedded metadata store.
//!
//! SQLite is the right fit while one daemon owns the data: it is a single
//! writer, needs no server process, and costs almost nothing at rest. The schema
//! is deliberately written the way a multi-user one would be — surrogate keys,
//! an explicit `owner_id`, no reliance on `rowid` — so the eventual move to a
//! networked database is a migration rather than a redesign.
//!
//! Connections use WAL so the HTTP handlers can read while capture writes.

// The job queue is complete and test-covered ahead of the scheduler that
// drives it. Suppressed here rather than per-item so the surface stays visible.
#![allow(dead_code)]

use anyhow::{Context, Result};
use kaseta_contracts::{Job, JobState, JobType, Origin};
use rusqlite::{params, Connection, OptionalExtension};
use ulid::Ulid;

/// Bumped whenever the schema changes. Migrations run in order at startup.
const SCHEMA_VERSION: i64 = 8;

/// The single local user. Present so every query is already scoped by owner and
/// adding real accounts does not mean rewriting them.
pub const LOCAL_OWNER_ID: i64 = 1;

pub struct Db {
    conn: Connection,
}

impl Db {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating database directory {}", parent.display()))?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("opening database {}", path.display()))?;
        Self::configure(&conn)?;
        let db = Self { conn };
        db.migrate()?;
        Ok(db)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::configure(&conn)?;
        let db = Self { conn };
        db.migrate()?;
        Ok(db)
    }

    fn configure(conn: &Connection) -> Result<()> {
        // WAL lets readers proceed during capture writes. `synchronous=NORMAL`
        // is the standard pairing: durable across process crashes, which is the
        // failure this design actually guards against. Blob writes are what must
        // survive power loss, and those are fsynced independently.
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(())
    }

    fn migrate(&self) -> Result<()> {
        let current: i64 = self
            .conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap_or(0);

        if current >= SCHEMA_VERSION {
            return Ok(());
        }

        // All of it, or none of it, including the version stamp.
        //
        // Without this each statement commits on its own, so a migration that
        // failed halfway would leave the schema changed and the version behind
        // it — and the next startup would run the whole step again against a
        // database it had already half-modified. That is survivable for a
        // migration which only adds a column; migration 007 rebuilds a table,
        // and a failure between DROP and RENAME would lose the jobs table
        // outright. `user_version` lives in the database header and is written
        // transactionally like anything else, so it rolls back with the rest.
        let tx = self.conn.unchecked_transaction()?;

        if current < 1 {
            self.conn
                .execute_batch(include_str!("migrations/001_initial.sql"))
                .context("applying migration 001_initial")?;
        }
        if current < 2 {
            self.conn
                .execute_batch(include_str!("migrations/002_library.sql"))
                .context("applying migration 002_library")?;
        }
        if current < 3 {
            self.conn
                .execute_batch(include_str!("migrations/003_exports.sql"))
                .context("applying migration 003_exports")?;
        }
        if current < 4 {
            self.conn
                .execute_batch(include_str!("migrations/004_backoff.sql"))
                .context("applying migration 004_backoff")?;
        }
        if current < 5 {
            self.conn
                .execute_batch(include_str!("migrations/005_progress.sql"))
                .context("applying migration 005_progress")?;
        }
        if current < 6 {
            self.conn
                .execute_batch(include_str!("migrations/006_derived_backup.sql"))
                .context("applying migration 006_derived_backup")?;
        }
        if current < 7 {
            self.conn
                .execute_batch(include_str!("migrations/007_publish_webhook.sql"))
                .context("applying migration 007_publish_webhook")?;
        }
        if current < 8 {
            self.conn
                .execute_batch(include_str!("migrations/008_import.sql"))
                .context("applying migration 008_import")?;
        }

        self.conn
            .pragma_update(None, "user_version", SCHEMA_VERSION)?;
        tx.commit()?;
        Ok(())
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    // ---- jobs -------------------------------------------------------------

    pub fn enqueue(&self, recording_id: Ulid, job_type: JobType, revision: u32) -> Result<Ulid> {
        let id = Ulid::new();
        // `enqueue_seq` is allocated inside the insert so concurrent enqueues
        // cannot collide on it; the unique index would reject a duplicate.
        self.conn.execute(
            "INSERT INTO jobs
                 (id, enqueue_seq, recording_id, job_type, revision, state, attempt, max_attempts)
             VALUES
                 (?1, (SELECT COALESCE(MAX(enqueue_seq), 0) + 1 FROM jobs),
                  ?2, ?3, ?4, ?5, 0, ?6)",
            params![
                id.to_string(),
                recording_id.to_string(),
                job_type.as_str(),
                revision,
                JobState::Queued.as_str(),
                Job::DEFAULT_MAX_ATTEMPTS,
            ],
        )?;
        Ok(id)
    }

    /// Claims the oldest queued job and marks it running.
    ///
    /// The `state = 'queued'` predicate in the UPDATE is what makes the claim
    /// atomic: even though one daemon is expected, two schedulers racing cannot
    /// both win the same row.
    pub fn claim_next_job(&self, worker_pid: u32) -> Result<Option<Job>> {
        let tx = self.conn.unchecked_transaction()?;

        let candidate: Option<String> = tx
            .query_row(
                "SELECT id FROM jobs
                 WHERE state = ?1
                   AND (next_attempt_at IS NULL OR next_attempt_at <= strftime('%s','now'))
                 ORDER BY enqueue_seq ASC LIMIT 1",
                params![JobState::Queued.as_str()],
                |r| r.get(0),
            )
            .optional()?;

        let Some(id) = candidate else {
            return Ok(None);
        };

        let updated = tx.execute(
            "UPDATE jobs SET state = ?1, worker_pid = ?2, attempt = attempt + 1,
                    started_at = strftime('%s','now')
             WHERE id = ?3 AND state = ?4",
            params![
                JobState::Running.as_str(),
                worker_pid,
                id,
                JobState::Queued.as_str()
            ],
        )?;

        if updated == 0 {
            // Lost the race; the caller retries.
            return Ok(None);
        }

        let job = Self::load_job(&tx, &id)?;
        tx.commit()?;
        Ok(job)
    }

    /// Moves a job to a new state, refusing transitions the model disallows.
    pub fn transition_job(&self, id: Ulid, next: JobState) -> Result<Job> {
        let tx = self.conn.unchecked_transaction()?;
        let mut job = Self::load_job(&tx, &id.to_string())?
            .with_context(|| format!("job {id} not found"))?;

        job.transition(next)?;

        tx.execute(
            "UPDATE jobs SET state = ?1, worker_pid = ?2 WHERE id = ?3",
            params![next.as_str(), job.worker_pid, id.to_string()],
        )?;
        tx.commit()?;
        Ok(job)
    }

    /// Records a failed attempt and decides whether the job gets another.
    ///
    /// An import that this makes terminal also fails its recording, in the
    /// same transaction.
    pub fn fail_job(
        &self,
        id: Ulid,
        code: &str,
        message: &str,
        retryable: bool,
    ) -> Result<JobState> {
        let tx = self.conn.unchecked_transaction()?;
        let mut job = Self::load_job(&tx, &id.to_string())?
            .with_context(|| format!("job {id} not found"))?;

        // A retryable failure that has burned its budget becomes terminal, so a
        // permanently broken job cannot loop forever.
        let next = if retryable && !job.attempts_exhausted() {
            JobState::FailedRetryable
        } else {
            JobState::FailedTerminal
        };
        job.transition(next)?;

        // Back off so a permanently broken job does not consume its whole
        // budget in the time it takes to notice. Doubling from a minute gives
        // 1, 2, 4 — long enough to fix a missing worker, short enough that a
        // transient error still clears on its own.
        let delay_s = 60i64 * (1 << job.attempt.min(6));

        tx.execute(
            "UPDATE jobs SET state = ?1, worker_pid = NULL, error_code = ?2, error_message = ?3,
                    next_attempt_at = strftime('%s','now') + ?4
             WHERE id = ?5",
            params![next.as_str(), code, message, delay_s, id.to_string()],
        )?;

        // However an import ends for good, its recording ends with it, in the
        // same transaction that decided it was over: nothing else will ever
        // move the row out of "processing", and recovery only looks at jobs
        // still running, so a crash between two commits would strand it.
        if next == JobState::FailedTerminal && job.job_type == JobType::ImportMedia {
            Self::fail_import_row(&tx, &job.recording_id.to_string())?;
        }

        tx.commit()?;
        Ok(next)
    }

    /// Returns retryable failures to the queue.
    /// Returns retryable failures to the queue once their backoff has elapsed.
    pub fn requeue_retryable(&self) -> Result<usize> {
        Ok(self.conn.execute(
            "UPDATE jobs SET state = ?1, error_code = NULL, error_message = NULL
             WHERE state = ?2 AND attempt < max_attempts
               AND (next_attempt_at IS NULL OR next_attempt_at <= strftime('%s','now'))",
            params![JobState::Queued.as_str(), JobState::FailedRetryable.as_str()],
        )?)
    }

    /// Queues a job only if an equivalent one is not already pending.
    ///
    /// Stage chaining and crash recovery can otherwise both queue the same
    /// follow-up work, which for a paid API means paying twice.
    pub fn enqueue_once(
        &self,
        recording_id: Ulid,
        job_type: JobType,
        revision: u32,
    ) -> Result<Option<Ulid>> {
        let existing: Option<String> = self
            .conn
            .query_row(
                "SELECT id FROM jobs
                 WHERE recording_id = ?1 AND job_type = ?2 AND revision = ?3
                   AND state IN ('queued','running','failed_retryable','succeeded')
                 LIMIT 1",
                params![recording_id.to_string(), job_type.as_str(), revision],
                |r| r.get(0),
            )
            .optional()?;

        if existing.is_some() {
            return Ok(None);
        }
        self.enqueue(recording_id, job_type, revision).map(Some)
    }

    /// Records that a finished job had nothing to do, and why.
    ///
    /// Stored on the job rather than inferred later, because "no summary
    /// exists" has several possible causes and only the run itself knows which
    /// one applied. Reuses the error columns: a skip is not an error, but it is
    /// the same shape — a terminal outcome with a reason worth showing.
    pub fn mark_job_skipped(&self, id: Ulid, reason: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE jobs SET error_code = 'skipped', error_message = ?1 WHERE id = ?2",
            params![reason, id.to_string()],
        )?;
        Ok(())
    }

    /// Queues a job that has already run, so it can run again.
    ///
    /// `enqueue_once` treats a previous success as a reason never to repeat the
    /// work, which is right for anything derived from the audio: the audio has
    /// not changed. It is wrong for an upload, because what is worth uploading
    /// can change afterwards — a transcript arrives, or someone renames the
    /// recording — and the bucket then holds a copy that is quietly out of
    /// date.
    ///
    /// Only a previous *success* is cleared. A failure — retryable or not — is
    /// left exactly where it is, because the retry machinery owns it: deleting
    /// a `failed_retryable` row would discard its backoff and its attempt
    /// count, and a sweep that runs every minute would then retry a failing
    /// upload every minute, forever. Getting a terminally failed stage moving
    /// again is a deliberate act, and there is a button for it.
    ///
    /// Work already queued or running is left alone; it will pick up whatever
    /// is current when it runs.
    pub fn enqueue_again(
        &self,
        recording_id: Ulid,
        job_type: JobType,
        revision: u32,
    ) -> Result<Option<Ulid>> {
        let pending: Option<String> = self
            .conn
            .query_row(
                "SELECT id FROM jobs
                 WHERE recording_id = ?1 AND job_type = ?2 AND revision = ?3
                   AND state IN ('queued','running','failed_retryable','failed_terminal')
                 LIMIT 1",
                params![recording_id.to_string(), job_type.as_str(), revision],
                |r| r.get(0),
            )
            .optional()?;

        if pending.is_some() {
            return Ok(None);
        }

        self.conn.execute(
            "DELETE FROM jobs
             WHERE recording_id = ?1 AND job_type = ?2 AND revision = ?3
               AND state IN ('succeeded','canceled')",
            params![recording_id.to_string(), job_type.as_str(), revision],
        )?;
        self.enqueue(recording_id, job_type, revision).map(Some)
    }

    /// Recovers jobs orphaned by a daemon that died mid-flight.
    ///
    /// This replaces distributed leasing. Any job still marked `running` at
    /// startup has no live worker, because the daemon that spawned it is gone.
    /// Model inference is not resumable, so orphans restart from the beginning
    /// and their partial output is discarded by the worker's own atomic write.
    ///
    /// An import that has used up its attempts also fails its recording, in the
    /// same transaction. Nothing else would: the job that would have finalised
    /// it is the one being given up on, so the row would read "processing" for
    /// ever.
    pub fn recover_orphaned_jobs(&self) -> Result<Vec<Ulid>> {
        let tx = self.conn.unchecked_transaction()?;

        let ids: Vec<(String, String, String)> = {
            let mut stmt =
                tx.prepare("SELECT id, job_type, recording_id FROM jobs WHERE state = ?1")?;
            let rows = stmt.query_map(params![JobState::Running.as_str()], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })?;
            rows.collect::<Result<_, _>>()?
        };

        let mut recovered = Vec::new();
        for (id, job_type, recording_id) in &ids {
            // Attempts already incremented on claim, so a job that repeatedly
            // kills the daemon eventually exhausts its budget instead of
            // crash-looping.
            let exhausted: bool = tx.query_row(
                "SELECT attempt >= max_attempts FROM jobs WHERE id = ?1",
                params![id],
                |r| r.get(0),
            )?;

            let next = if exhausted {
                JobState::FailedTerminal
            } else {
                JobState::Queued
            };

            tx.execute(
                "UPDATE jobs SET state = ?1, worker_pid = NULL,
                        error_code = CASE WHEN ?1 = ?2 THEN 'orphaned' ELSE error_code END,
                        error_message = CASE WHEN ?1 = ?2
                            THEN 'daemon exited while this job was running' ELSE error_message END
                 WHERE id = ?3",
                params![next.as_str(), JobState::FailedTerminal.as_str(), id],
            )?;

            let import = job_type.as_str() == JobType::ImportMedia.as_str();
            if next == JobState::FailedTerminal && import {
                Self::fail_import_row(&tx, recording_id)?;
            }

            recovered.push(Ulid::from_string(id)?);
        }

        tx.commit()?;
        Ok(recovered)
    }

    /// One job, by id. `None` once it is gone, which for an import means its
    /// recording was deleted and the row cascaded with it.
    pub fn job(&self, id: Ulid) -> Result<Option<Job>> {
        Self::load_job(&self.conn, &id.to_string())
    }

    // ---- imports ----------------------------------------------------------

    /// Registers an uploaded file as a recording waiting to be decoded, and
    /// queues the stage that decodes it. Returns that job's id.
    ///
    /// One transaction, so there is never a recording with nothing coming to
    /// produce it, nor a decode with no recording to fill in.
    ///
    /// The row starts with no clock origin, no source and no local original,
    /// and with neither dirty flag set: nothing exists to store or back up
    /// until the file has been decoded, and finalisation sets both.
    pub fn create_import(&self, import: &NewImport) -> Result<Ulid> {
        // The prefix every object goes under is built from the start time to
        // the second. A fraction stored here would name a different moment
        // than the prefix does, and anything rebuilding the prefix from the
        // row would look in the wrong place.
        anyhow::ensure!(
            import.started_at.nanosecond() == 0,
            "an import's start time must be whole seconds"
        );

        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO recordings
                 (id, owner_id, status, title, started_at, manifest_version, clock_started_ns,
                  origin, source_json, original_local, derived_dirty, remote_dirty)
             VALUES (?1, ?2, 'processing', ?3, ?4, ?5, 0, ?6, NULL, 0, 0, 0)",
            params![
                import.recording_id.to_string(),
                LOCAL_OWNER_ID,
                import.title,
                import.started_at.unix_timestamp(),
                kaseta_contracts::MANIFEST_VERSION,
                Origin::Imported.as_str(),
            ],
        )?;
        let job = self.enqueue(import.recording_id, JobType::ImportMedia, 1)?;
        tx.commit()?;
        Ok(job)
    }

    /// Marks a decoded import ready, if it is still wanted, and queues what
    /// follows. Returns whether it did.
    ///
    /// A compare-and-set rather than an update: the recording must still
    /// exist, must not have been deleted, must be an import, and `job_id` must
    /// be its decode stage and still running. Any of those failing means the
    /// work was overtaken while it ran, most often by someone deleting the
    /// recording, and indexing it anyway would bring back something that was
    /// deleted. The caller then removes what it wrote.
    ///
    /// Indexing, the job's success and the follow-up stages commit together.
    /// Split, a crash between them would leave a ready recording whose
    /// transcription was never queued, or a job requeued to decode a file that
    /// had already been indexed.
    pub fn finalize_import_success(
        &self,
        recording_id: Ulid,
        job_id: Ulid,
        fields: &IndexFields,
    ) -> Result<bool> {
        let tx = self.conn.unchecked_transaction()?;

        // The start time is deliberately not updated: it was fixed when the
        // upload arrived, and it names the prefix everything was written to.
        let updated = tx.execute(
            "UPDATE recordings SET
                 status           = 'ready',
                 title            = COALESCE(?3, title),
                 ended_at         = ?4,
                 manifest_version = ?5,
                 clock_started_ns = ?6,
                 duration_ms      = ?7,
                 has_mixed        = ?8,
                 tracks_json      = ?9,
                 source_json      = ?10,
                 original_local   = ?11,
                 derived_dirty    = 1,
                 remote_dirty     = 1,
                 updated_at       = strftime('%s','now')
             WHERE id = ?1
               AND deleted_at IS NULL
               AND origin = 'imported'
               AND EXISTS (
                   SELECT 1 FROM jobs
                   WHERE id = ?2 AND recording_id = ?1
                     AND job_type = 'import_media' AND state = 'running'
               )",
            params![
                recording_id.to_string(),
                job_id.to_string(),
                fields.title,
                fields.ended_at,
                fields.manifest_version,
                fields.clock_started_ns,
                fields.duration_ms,
                fields.has_mixed as i64,
                fields.tracks_json,
                fields.source_json,
                fields.original_local as i64,
            ],
        )?;
        if updated == 0 {
            return Ok(false);
        }

        let mut job = Self::load_job(&tx, &job_id.to_string())?
            .with_context(|| format!("job {job_id} not found"))?;
        job.transition(JobState::Succeeded)?;
        tx.execute(
            "UPDATE jobs SET state = ?1, worker_pid = NULL WHERE id = ?2",
            params![JobState::Succeeded.as_str(), job_id.to_string()],
        )?;

        // The same follow-up a capture gets when it ends, side by side for the
        // same reason: a recording whose transcription fails is still copied.
        self.enqueue_once(recording_id, JobType::Transcribe, 1)?;
        self.enqueue_once(recording_id, JobType::UploadRemote, 1)?;

        tx.commit()?;
        Ok(true)
    }

    /// Marks an unfinished import's recording failed. Only an import still on
    /// its way: a ready recording is never demoted by a stale failure, and a
    /// deleted one stays as its deletion left it.
    fn fail_import_row(conn: &Connection, recording_id: &str) -> Result<()> {
        conn.execute(
            "UPDATE recordings SET status = 'failed', updated_at = strftime('%s','now')
             WHERE id = ?1 AND origin = 'imported' AND status = 'processing'
               AND deleted_at IS NULL",
            params![recording_id],
        )?;
        Ok(())
    }

    /// Gives a failed import another go: the recording back to `processing`
    /// and a fresh decode queued. Returns the new job's id, or `None` when
    /// there was nothing to retry.
    ///
    /// Only a `failed` import qualifies. One still processing already has a
    /// decode on its way, and a second would race it over the same objects; a
    /// ready one has nothing to redo. Whether the uploaded file is still there
    /// to decode is the caller's question, since it lives in storage.
    pub fn reset_import_for_retry(&self, recording_id: Ulid) -> Result<Option<Ulid>> {
        let tx = self.conn.unchecked_transaction()?;
        let reset = tx.execute(
            "UPDATE recordings SET status = 'processing', updated_at = strftime('%s','now')
             WHERE id = ?1 AND origin = 'imported' AND status = 'failed' AND deleted_at IS NULL",
            params![recording_id.to_string()],
        )?;
        if reset == 0 {
            return Ok(None);
        }

        // The failed attempt is history once a new one is queued; leaving it
        // would let the interface show the old reason beside a running decode.
        tx.execute(
            "DELETE FROM jobs WHERE recording_id = ?1 AND job_type = ?2
               AND state IN ('failed_terminal','failed_retryable','canceled')",
            params![recording_id.to_string(), JobType::ImportMedia.as_str()],
        )?;
        let Some(job) = self.enqueue_once(recording_id, JobType::ImportMedia, 1)? else {
            // Something is already pending, which a failed row should never
            // have. Leave everything as it was rather than half-reset.
            return Ok(None);
        };
        tx.commit()?;
        Ok(Some(job))
    }

    fn load_job(conn: &Connection, id: &str) -> Result<Option<Job>> {
        conn.query_row(
            "SELECT id, recording_id, job_type, revision, state, attempt, max_attempts,
                    worker_pid, error_code, error_message
             FROM jobs WHERE id = ?1",
            params![id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, u32>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, u32>(5)?,
                    r.get::<_, u32>(6)?,
                    r.get::<_, Option<u32>>(7)?,
                    r.get::<_, Option<String>>(8)?,
                    r.get::<_, Option<String>>(9)?,
                ))
            },
        )
        .optional()
        .context("loading job")?
        .map(|row| -> Result<Job> {
            Ok(Job {
                id: Ulid::from_string(&row.0).context("job id is not a valid ULID")?,
                recording_id: Ulid::from_string(&row.1)
                    .context("job recording_id is not a valid ULID")?,
                job_type: parse_job_type(&row.2)?,
                revision: row.3,
                state: parse_job_state(&row.4)?,
                attempt: row.5,
                max_attempts: row.6,
                worker_pid: row.7,
                error_code: row.8,
                error_message: row.9,
            })
        })
        .transpose()
    }
}

/// An uploaded file about to become a recording.
#[derive(Clone, Debug)]
pub struct NewImport {
    pub recording_id: Ulid,
    /// When the upload arrived, in whole UTC seconds. Fixed once, because it
    /// names the prefix every object of the recording is written under.
    pub started_at: time::OffsetDateTime,
    /// What the person called it, or the file's name without its extension.
    pub title: String,
}

/// Everything the index records about a recording that is derived from its
/// manifest and what exists under its prefix.
///
/// Computed in one place and written by two: indexing a captured or rebuilt
/// recording, and finalising an import. Two computations would drift, and an
/// import indexed by the reconciler would then look different from the same
/// import finalised by its job.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexFields {
    pub title: Option<String>,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub manifest_version: String,
    pub clock_started_ns: i64,
    pub duration_ms: i64,
    pub has_mixed: bool,
    pub tracks_json: String,
    pub origin: Origin,
    pub source_json: Option<String>,
    /// Whether the kept original is in local storage right now.
    pub original_local: bool,
}

/// Parsers reject unknown values rather than substituting a default.
///
/// A silent fallback would turn database corruption, or a row written by a newer
/// schema, into a job quietly misclassified as a different type or state — which
/// could resurrect finished work or discard live work.
fn parse_job_type(s: &str) -> Result<JobType> {
    Ok(match s {
        "finalize_recording" => JobType::FinalizeRecording,
        "transcribe" => JobType::Transcribe,
        "publish_webhook" => JobType::PublishWebhook,
        "merge_transcript" => JobType::MergeTranscript,
        "summarize" => JobType::Summarize,
        "upload_remote" => JobType::UploadRemote,
        "import_media" => JobType::ImportMedia,
        other => anyhow::bail!("unknown job_type in database: {other:?}"),
    })
}

fn parse_job_state(s: &str) -> Result<JobState> {
    Ok(match s {
        "queued" => JobState::Queued,
        "running" => JobState::Running,
        "succeeded" => JobState::Succeeded,
        "failed_retryable" => JobState::FailedRetryable,
        "failed_terminal" => JobState::FailedTerminal,
        "canceled" => JobState::Canceled,
        "superseded" => JobState::Superseded,
        other => anyhow::bail!("unknown job state in database: {other:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simulates the backoff having elapsed, so retry behaviour is testable
    /// without waiting minutes.
    fn elapse_backoff(db: &Db, id: Ulid) {
        db.conn
            .execute(
                "UPDATE jobs SET next_attempt_at = NULL WHERE id = ?1",
                params![id.to_string()],
            )
            .unwrap();
    }

    fn db_with_recording() -> (Db, Ulid) {
        let db = Db::open_in_memory().unwrap();
        let rec = Ulid::new();
        db.conn
            .execute(
                "INSERT INTO recordings (id, owner_id, status, started_at, manifest_version)
                 VALUES (?1, ?2, 'recording', 0, 'recording-manifest/v1')",
                params![rec.to_string(), LOCAL_OWNER_ID],
            )
            .unwrap();
        (db, rec)
    }

    /// The regression this whole change started from.
    ///
    /// `job_type` carries a CHECK listing the stages by name, and sending was
    /// added without extending it — so every attempt to queue one failed the
    /// constraint before it reached the network, and the stage had never run
    /// anywhere. Enumerated rather than spot-checked: the next stage added will
    /// fail here rather than in production.
    #[test]
    fn every_stage_can_actually_be_queued() {
        let (db, rec) = db_with_recording();
        for job in [
            JobType::FinalizeRecording,
            JobType::Transcribe,
            JobType::MergeTranscript,
            JobType::Summarize,
            JobType::UploadRemote,
            JobType::PublishWebhook,
            JobType::ImportMedia,
        ] {
            db.enqueue(rec, job, 1)
                .unwrap_or_else(|e| panic!("{job:?} cannot be queued: {e:#}"));
        }
    }

    #[test]
    fn claims_jobs_in_creation_order() {
        let (db, rec) = db_with_recording();
        let first = db.enqueue(rec, JobType::Transcribe, 1).unwrap();
        let second = db.enqueue(rec, JobType::Summarize, 1).unwrap();

        assert_eq!(db.claim_next_job(100).unwrap().unwrap().id, first);
        assert_eq!(db.claim_next_job(101).unwrap().unwrap().id, second);
        assert!(db.claim_next_job(102).unwrap().is_none());
    }

    #[test]
    fn ordering_holds_for_jobs_enqueued_within_the_same_millisecond() {
        // ULIDs minted in one millisecond share a timestamp and differ only in
        // random bits, so ordering by id claims jobs out of order. This burst is
        // fast enough to land in the same millisecond and would fail against an
        // id-ordered queue.
        let (db, rec) = db_with_recording();
        let enqueued: Vec<Ulid> = (0..50)
            .map(|_| db.enqueue(rec, JobType::Transcribe, 1).unwrap())
            .collect();

        let claimed: Vec<Ulid> = (0..50)
            .map(|i| db.claim_next_job(i).unwrap().unwrap().id)
            .collect();

        assert_eq!(
            claimed, enqueued,
            "jobs must be claimed in the order they were enqueued"
        );
    }

    #[test]
    fn enqueue_sequence_is_strictly_increasing() {
        let (db, rec) = db_with_recording();
        for _ in 0..10 {
            db.enqueue(rec, JobType::Transcribe, 1).unwrap();
        }

        let seqs: Vec<i64> = {
            let mut stmt = db
                .conn
                .prepare("SELECT enqueue_seq FROM jobs ORDER BY enqueue_seq")
                .unwrap();
            let rows = stmt.query_map([], |r| r.get(0)).unwrap();
            rows.collect::<Result<_, _>>().unwrap()
        };

        assert_eq!(seqs, (1..=10).collect::<Vec<i64>>());
    }

    #[test]
    fn claiming_records_the_worker_and_counts_the_attempt() {
        let (db, rec) = db_with_recording();
        db.enqueue(rec, JobType::Transcribe, 1).unwrap();

        let job = db.claim_next_job(4242).unwrap().unwrap();
        assert_eq!(job.state, JobState::Running);
        assert_eq!(job.worker_pid, Some(4242));
        assert_eq!(job.attempt, 1);
    }

    #[test]
    fn a_job_cannot_be_claimed_twice() {
        let (db, rec) = db_with_recording();
        db.enqueue(rec, JobType::Transcribe, 1).unwrap();

        assert!(db.claim_next_job(1).unwrap().is_some());
        assert!(
            db.claim_next_job(2).unwrap().is_none(),
            "a running job must not be handed to a second worker"
        );
    }

    #[test]
    fn a_daemon_crash_requeues_running_jobs() {
        let (db, rec) = db_with_recording();
        let id = db.enqueue(rec, JobType::Transcribe, 1).unwrap();
        db.claim_next_job(4242).unwrap().unwrap();

        // Daemon dies here. On restart:
        let recovered = db.recover_orphaned_jobs().unwrap();
        assert_eq!(recovered, vec![id]);

        let job = Db::load_job(&db.conn, &id.to_string()).unwrap().unwrap();
        assert_eq!(job.state, JobState::Queued);
        assert_eq!(job.worker_pid, None, "orphaned PID must be cleared");
    }

    #[test]
    fn a_job_that_repeatedly_kills_the_daemon_stops_being_retried() {
        let (db, rec) = db_with_recording();
        let id = db.enqueue(rec, JobType::Transcribe, 1).unwrap();

        for _ in 0..Job::DEFAULT_MAX_ATTEMPTS {
            db.claim_next_job(1).unwrap();
            db.recover_orphaned_jobs().unwrap();
        }

        let job = Db::load_job(&db.conn, &id.to_string()).unwrap().unwrap();
        assert_eq!(
            job.state,
            JobState::FailedTerminal,
            "crash-looping job must exhaust its budget, not retry forever"
        );
        assert_eq!(job.error_code.as_deref(), Some("orphaned"));
    }

    #[test]
    fn recovery_is_a_no_op_when_nothing_was_running() {
        let (db, rec) = db_with_recording();
        db.enqueue(rec, JobType::Transcribe, 1).unwrap();
        assert!(db.recover_orphaned_jobs().unwrap().is_empty());
    }

    #[test]
    fn retryable_failures_return_to_the_queue_until_the_budget_runs_out() {
        let (db, rec) = db_with_recording();
        let id = db.enqueue(rec, JobType::Transcribe, 1).unwrap();

        db.claim_next_job(1).unwrap();
        assert_eq!(
            db.fail_job(id, "net", "connection reset", true).unwrap(),
            JobState::FailedRetryable
        );

        // Backed off: retrying instantly would burn the whole budget in the
        // time it takes anyone to notice the failure.
        assert_eq!(
            db.requeue_retryable().unwrap(),
            0,
            "a just-failed job must wait before being retried"
        );

        elapse_backoff(&db, id);
        assert_eq!(db.requeue_retryable().unwrap(), 1);

        let job = Db::load_job(&db.conn, &id.to_string()).unwrap().unwrap();
        assert_eq!(job.state, JobState::Queued);
        assert_eq!(job.error_code, None, "requeue must clear the stale error");
    }

    #[test]
    fn a_failed_job_is_not_reclaimable_until_its_backoff_elapses() {
        let (db, rec) = db_with_recording();
        let id = db.enqueue(rec, JobType::Transcribe, 1).unwrap();

        db.claim_next_job(1).unwrap();
        db.fail_job(id, "net", "connection reset", true).unwrap();
        db.requeue_retryable().unwrap();

        assert!(
            db.claim_next_job(2).unwrap().is_none(),
            "a backed-off job must not be claimed immediately"
        );

        elapse_backoff(&db, id);
        db.requeue_retryable().unwrap();
        assert!(db.claim_next_job(3).unwrap().is_some());
    }

    /// Re-queueing after a change must not become a way around backoff. The
    /// sweep that calls this runs every minute; if it cleared a failing job's
    /// retry budget, a failing upload would be retried every minute forever.
    #[test]
    fn re_queueing_leaves_a_failing_job_to_its_backoff() {
        let (db, rec) = db_with_recording();
        let id = db.enqueue(rec, JobType::UploadRemote, 1).unwrap();

        db.claim_next_job(1).unwrap();
        db.fail_job(id, "net", "connection reset", true).unwrap();

        assert!(
            db.enqueue_again(rec, JobType::UploadRemote, 1).unwrap().is_none(),
            "a job waiting on backoff must not be replaced by a fresh one"
        );

        let job = Db::load_job(&db.conn, &id.to_string()).unwrap().unwrap();
        assert_eq!(job.attempt, 1, "its attempt count must survive");
    }

    /// A success, though, is exactly what should be repeatable: the recording
    /// changed after it was uploaded.
    #[test]
    fn re_queueing_runs_again_after_a_success() {
        let (db, rec) = db_with_recording();
        let id = db.enqueue(rec, JobType::UploadRemote, 1).unwrap();

        db.claim_next_job(1).unwrap();
        db.transition_job(id, JobState::Succeeded).unwrap();

        assert!(
            db.enqueue_once(rec, JobType::UploadRemote, 1).unwrap().is_none(),
            "the ordinary path still refuses to repeat finished work"
        );
        assert!(
            db.enqueue_again(rec, JobType::UploadRemote, 1).unwrap().is_some(),
            "a recording that changed after upload must be uploadable again"
        );
    }

    #[test]
    fn backoff_grows_with_each_attempt() {
        let (db, rec) = db_with_recording();
        let id = db.enqueue(rec, JobType::Transcribe, 1).unwrap();

        let mut delays = Vec::new();
        for _ in 0..2 {
            db.claim_next_job(1).unwrap();
            db.fail_job(id, "net", "again", true).unwrap();
            let at: i64 = db
                .conn
                .query_row(
                    "SELECT next_attempt_at - strftime('%s','now') FROM jobs WHERE id = ?1",
                    params![id.to_string()],
                    |r| r.get(0),
                )
                .unwrap();
            delays.push(at);
            elapse_backoff(&db, id);
            db.requeue_retryable().unwrap();
        }

        assert!(
            delays[1] > delays[0],
            "each failure should wait longer: {delays:?}"
        );
    }

    #[test]
    fn queueing_the_same_stage_twice_does_nothing() {
        // Stage chaining and crash recovery can both try to queue the same
        // follow-up, which for a paid API means paying twice.
        let (db, rec) = db_with_recording();
        assert!(db.enqueue_once(rec, JobType::Summarize, 1).unwrap().is_some());
        assert!(db.enqueue_once(rec, JobType::Summarize, 1).unwrap().is_none());

        let count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM jobs WHERE recording_id = ?1 AND job_type = 'summarize'",
                params![rec.to_string()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn a_non_retryable_failure_is_terminal_immediately() {
        let (db, rec) = db_with_recording();
        let id = db.enqueue(rec, JobType::Transcribe, 1).unwrap();
        db.claim_next_job(1).unwrap();

        assert_eq!(
            db.fail_job(id, "bad_input", "manifest malformed", false)
                .unwrap(),
            JobState::FailedTerminal
        );
        assert_eq!(db.requeue_retryable().unwrap(), 0);
    }

    #[test]
    fn illegal_transitions_are_rejected() {
        let (db, rec) = db_with_recording();
        let id = db.enqueue(rec, JobType::Transcribe, 1).unwrap();
        db.claim_next_job(1).unwrap();
        db.transition_job(id, JobState::Succeeded).unwrap();

        assert!(
            db.transition_job(id, JobState::Running).is_err(),
            "a finished job must not restart in place"
        );
    }

    #[test]
    fn deleting_a_recording_removes_its_jobs() {
        let (db, rec) = db_with_recording();
        db.enqueue(rec, JobType::Transcribe, 1).unwrap();

        db.conn
            .execute("DELETE FROM recordings WHERE id = ?1", params![rec.to_string()])
            .unwrap();

        let remaining: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 0, "foreign keys must cascade");
    }

    #[test]
    fn an_unknown_state_in_the_database_is_an_error_not_a_guess() {
        let (db, rec) = db_with_recording();
        let id = db.enqueue(rec, JobType::Transcribe, 1).unwrap();

        // Simulate a row written by a newer schema. The CHECK constraint is
        // bypassed deliberately to reach the parser.
        db.conn
            .execute("PRAGMA ignore_check_constraints = ON", [])
            .unwrap();
        db.conn
            .execute(
                "UPDATE jobs SET state = 'quarantined' WHERE id = ?1",
                params![id.to_string()],
            )
            .unwrap();

        let err = Db::load_job(&db.conn, &id.to_string()).unwrap_err();
        assert!(
            err.to_string().contains("unknown job state"),
            "expected a hard error, got: {err}"
        );
    }

    #[test]
    fn a_corrupt_job_id_is_an_error_not_a_nil_ulid() {
        // `recording_id` cannot be corrupted while the foreign key holds, so
        // the primary key is the reachable case.
        let (db, rec) = db_with_recording();
        let id = db.enqueue(rec, JobType::Transcribe, 1).unwrap();
        db.conn
            .execute(
                "UPDATE jobs SET id = 'not-a-ulid' WHERE id = ?1",
                params![id.to_string()],
            )
            .unwrap();

        let err = Db::load_job(&db.conn, "not-a-ulid").unwrap_err();
        assert!(
            err.to_string().contains("not a valid ULID"),
            "expected a parse error, got: {err}"
        );
    }

    #[test]
    fn every_migration_file_is_wired_into_migrate() {
        // Adding a migration file without registering it leaves the schema
        // silently behind, surfacing much later as a missing column.
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/migrations");
        let count = std::fs::read_dir(&dir)
            .expect("migrations directory")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "sql"))
            .count() as i64;

        assert_eq!(
            count, SCHEMA_VERSION,
            "{count} migration files exist but SCHEMA_VERSION is {SCHEMA_VERSION}"
        );
    }

    #[test]
    fn the_schema_has_every_column_the_library_queries() {
        let db = Db::open_in_memory().unwrap();
        let stmt = db.conn.prepare("SELECT * FROM recordings LIMIT 0").unwrap();
        let columns: Vec<String> = stmt
            .column_names()
            .into_iter()
            .map(str::to_string)
            .collect();

        for required in [
            "id", "title", "title_override", "status", "started_at", "ended_at",
            "duration_ms", "has_mixed", "tracks_json", "deleted_at", "purge_pending",
        ] {
            assert!(
                columns.contains(&required.to_string()),
                "recordings is missing {required}; have {columns:?}"
            );
        }
        drop(stmt);
    }

    #[test]
    fn an_existing_database_upgrades_rather_than_failing() {
        // The realistic case: a database created by an earlier version, opened
        // by a newer one. Only the missing migrations must run.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("kaseta.db");

        {
            let conn = Connection::open(&path).unwrap();
            Db::configure(&conn).unwrap();
            conn.execute_batch(include_str!("migrations/001_initial.sql"))
                .unwrap();
            conn.execute_batch(include_str!("migrations/002_library.sql"))
                .unwrap();
            conn.pragma_update(None, "user_version", 2i64).unwrap();
        }

        let db = Db::open(&path).expect("opening an older database must upgrade it");

        let version: i64 = db
            .conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        // The query that failed in the field must now succeed.
        db.conn
            .query_row(
                "SELECT id, title, title_override, status, started_at, ended_at,
                        duration_ms, has_mixed, tracks_json
                 FROM recordings LIMIT 1",
                [],
                |_| Ok(()),
            )
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(()),
                other => Err(other),
            })
            .expect("the library query must run against an upgraded database");
    }

    // ---- imports ----------------------------------------------------------

    fn new_import(id: Ulid) -> NewImport {
        NewImport {
            recording_id: id,
            started_at: time::macros::datetime!(2026-10-09 08:00:00 UTC),
            title: "Lecture 3".into(),
        }
    }

    /// An import's row as it stands, for asserting on.
    fn import_row(db: &Db, id: Ulid) -> (String, String, i64, i64, i64, Option<String>) {
        db.conn
            .query_row(
                "SELECT status, origin, original_local, derived_dirty, remote_dirty, source_json
                 FROM recordings WHERE id = ?1",
                params![id.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
            )
            .unwrap()
    }

    fn job_types_queued(db: &Db, id: Ulid) -> Vec<String> {
        let mut stmt = db
            .conn
            .prepare(
                "SELECT job_type FROM jobs WHERE recording_id = ?1 AND state = 'queued'
                 ORDER BY enqueue_seq",
            )
            .unwrap();
        let rows = stmt
            .query_map(params![id.to_string()], |r| r.get::<_, String>(0))
            .unwrap();
        rows.collect::<Result<_, _>>().unwrap()
    }

    fn index_fields() -> IndexFields {
        IndexFields {
            title: Some("Lecture 3".into()),
            started_at: time::macros::datetime!(2026-10-09 08:00:00 UTC).unix_timestamp(),
            ended_at: Some(time::macros::datetime!(2026-10-09 09:00:00 UTC).unix_timestamp()),
            manifest_version: kaseta_contracts::MANIFEST_VERSION.into(),
            clock_started_ns: 42_000_000_000,
            duration_ms: 3_600_000,
            has_mixed: true,
            tracks_json: r#"[{"track_id":"a_imported_01","role":"unattributed"}]"#.into(),
            origin: Origin::Imported,
            source_json: Some(r#"{"original_filename":"lecture.mp4"}"#.into()),
            original_local: true,
        }
    }

    /// Creates an import and claims its job, which is where finalisation
    /// finds it.
    fn running_import(db: &Db) -> (Ulid, Ulid) {
        let rec = Ulid::new();
        let job = db.create_import(&new_import(rec)).unwrap();
        let claimed = db.claim_next_job(1).unwrap().unwrap();
        assert_eq!(claimed.id, job);
        assert_eq!(claimed.job_type, JobType::ImportMedia);
        (rec, job)
    }

    #[test]
    fn an_import_starts_as_a_processing_row_with_its_job() {
        let db = Db::open_in_memory().unwrap();
        let rec = Ulid::new();
        let job_id = db.create_import(&new_import(rec)).unwrap();

        let (status, origin, original_local, derived_dirty, remote_dirty, source_json) =
            import_row(&db, rec);
        assert_eq!(status, "processing");
        assert_eq!(origin, "imported");
        assert_eq!(original_local, 0);
        assert_eq!(source_json, None, "filled only once the file is decoded");
        // Nothing may store or back up a recording that does not exist yet.
        assert_eq!((derived_dirty, remote_dirty), (0, 0));

        let (title, started_at, manifest_version, clock): (String, i64, String, i64) = db
            .conn
            .query_row(
                "SELECT title, started_at, manifest_version, clock_started_ns
                 FROM recordings WHERE id = ?1",
                params![rec.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(title, "Lecture 3");
        assert_eq!(started_at, new_import(rec).started_at.unix_timestamp());
        assert_eq!(manifest_version, kaseta_contracts::MANIFEST_VERSION);
        assert_eq!(clock, 0);

        let job = db.job(job_id).unwrap().unwrap();
        assert_eq!(job.job_type, JobType::ImportMedia);
        assert_eq!(job.state, JobState::Queued);
        assert_eq!(job.recording_id, rec);
        assert_eq!(job_types_queued(&db, rec), vec!["import_media"]);
    }

    /// The prefix is built from whole seconds, so a start time with a
    /// fraction would name a different place than the one stored.
    #[test]
    fn an_import_start_time_must_be_whole_seconds() {
        let db = Db::open_in_memory().unwrap();
        let mut import = new_import(Ulid::new());
        import.started_at += time::Duration::milliseconds(250);
        assert!(db.create_import(&import).is_err());
        let rows: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM recordings", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn finalising_an_import_indexes_it_and_queues_what_follows_exactly_once() {
        let db = Db::open_in_memory().unwrap();
        let (rec, job) = running_import(&db);

        assert!(db.finalize_import_success(rec, job, &index_fields()).unwrap());

        let (status, origin, original_local, derived_dirty, remote_dirty, source_json) =
            import_row(&db, rec);
        assert_eq!(status, "ready");
        assert_eq!(origin, "imported");
        assert_eq!(original_local, 1);
        assert_eq!((derived_dirty, remote_dirty), (1, 1));
        assert_eq!(source_json.as_deref(), index_fields().source_json.as_deref());

        let (duration, has_mixed, clock, tracks): (i64, i64, i64, String) = db
            .conn
            .query_row(
                "SELECT duration_ms, has_mixed, clock_started_ns, tracks_json
                 FROM recordings WHERE id = ?1",
                params![rec.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(duration, 3_600_000);
        assert_eq!(has_mixed, 1);
        assert_eq!(clock, 42_000_000_000);
        assert_eq!(tracks, index_fields().tracks_json);

        assert_eq!(db.job(job).unwrap().unwrap().state, JobState::Succeeded);
        assert_eq!(job_types_queued(&db, rec), vec!["transcribe", "upload_remote"]);

        // A second finalisation finds the job no longer running and changes
        // nothing: no second transcription, no second backup.
        assert!(!db.finalize_import_success(rec, job, &index_fields()).unwrap());
        assert_eq!(job_types_queued(&db, rec), vec!["transcribe", "upload_remote"]);
    }

    /// Deleted while it was decoding. Indexing it now would bring back a
    /// recording somebody deleted.
    #[test]
    fn finalising_refuses_a_deleted_import() {
        let db = Db::open_in_memory().unwrap();
        let (rec, job) = running_import(&db);
        db.conn
            .execute(
                "UPDATE recordings SET deleted_at = 1, purge_pending = 1 WHERE id = ?1",
                params![rec.to_string()],
            )
            .unwrap();

        assert!(!db.finalize_import_success(rec, job, &index_fields()).unwrap());
        assert_eq!(import_row(&db, rec).0, "processing");
        assert_eq!(db.job(job).unwrap().unwrap().state, JobState::Running);
        assert!(job_types_queued(&db, rec).is_empty());
    }

    #[test]
    fn finalising_refuses_a_row_that_is_gone() {
        let db = Db::open_in_memory().unwrap();
        let (rec, job) = running_import(&db);
        db.conn
            .execute("DELETE FROM recordings WHERE id = ?1", params![rec.to_string()])
            .unwrap();

        assert!(!db.finalize_import_success(rec, job, &index_fields()).unwrap());
        let rows: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM recordings", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 0, "finalising must not resurrect the row");
    }

    #[test]
    fn finalising_refuses_a_job_that_is_not_the_one_running() {
        let db = Db::open_in_memory().unwrap();
        let (rec, job) = running_import(&db);

        assert!(!db.finalize_import_success(rec, Ulid::new(), &index_fields()).unwrap());

        // The job of a different recording does not count either.
        let (other, other_job) = running_import(&db);
        assert!(!db.finalize_import_success(rec, other_job, &index_fields()).unwrap());
        assert!(!db.finalize_import_success(other, job, &index_fields()).unwrap());

        assert_eq!(import_row(&db, rec).0, "processing");
        assert!(job_types_queued(&db, rec).is_empty());
    }

    /// A job requeued by recovery, or failed by a terminal error, is no longer
    /// the attempt entitled to finalise.
    #[test]
    fn finalising_refuses_a_job_that_is_not_running() {
        let db = Db::open_in_memory().unwrap();
        let rec = Ulid::new();
        let job = db.create_import(&new_import(rec)).unwrap();
        assert!(!db.finalize_import_success(rec, job, &index_fields()).unwrap(), "queued");

        db.claim_next_job(1).unwrap();
        db.fail_job(job, "x", "decoder crashed", true).unwrap();
        assert!(!db.finalize_import_success(rec, job, &index_fields()).unwrap(), "failed");
        assert_eq!(import_row(&db, rec).0, "processing");
    }

    #[test]
    fn finalising_refuses_a_captured_recording() {
        let (db, rec) = db_with_recording();
        let job = db.enqueue(rec, JobType::ImportMedia, 1).unwrap();
        db.claim_next_job(1).unwrap();
        assert!(!db.finalize_import_success(rec, job, &index_fields()).unwrap());
    }

    #[test]
    fn a_terminal_import_failure_fails_the_job_and_the_recording_together() {
        let db = Db::open_in_memory().unwrap();
        let (rec, job) = running_import(&db);

        assert_eq!(
            db.fail_job(job, "job_failed", "This file has no audio track", false).unwrap(),
            JobState::FailedTerminal
        );

        let failed = db.job(job).unwrap().unwrap();
        assert_eq!(failed.state, JobState::FailedTerminal);
        assert_eq!(failed.error_message.as_deref(), Some("This file has no audio track"));
        assert_eq!(import_row(&db, rec).0, "failed");
        assert_eq!(db.requeue_retryable().unwrap(), 0, "nothing comes back around");
    }

    /// Running out of attempts is decided in the same operation that fails the
    /// recording, so no crash can fall between the job going terminal and the
    /// row leaving "processing".
    #[test]
    fn an_import_that_runs_out_of_attempts_fails_its_recording_in_one_step() {
        let db = Db::open_in_memory().unwrap();
        let (rec, job) = running_import(&db);
        db.conn
            .execute(
                "UPDATE jobs SET attempt = max_attempts WHERE id = ?1",
                params![job.to_string()],
            )
            .unwrap();

        assert_eq!(
            db.fail_job(job, "job_failed", "ffmpeg crashed", true).unwrap(),
            JobState::FailedTerminal
        );
        assert_eq!(db.job(job).unwrap().unwrap().state, JobState::FailedTerminal);
        assert_eq!(import_row(&db, rec).0, "failed");
    }

    #[test]
    fn an_import_with_attempts_left_stays_processing_after_a_failure() {
        let db = Db::open_in_memory().unwrap();
        let (rec, job) = running_import(&db);

        assert_eq!(
            db.fail_job(job, "job_failed", "ffmpeg crashed", true).unwrap(),
            JobState::FailedRetryable
        );
        assert_eq!(import_row(&db, rec).0, "processing");
    }

    #[test]
    fn a_terminal_failure_cannot_undo_a_finished_import() {
        let db = Db::open_in_memory().unwrap();
        let (rec, job) = running_import(&db);
        assert!(db.finalize_import_success(rec, job, &index_fields()).unwrap());

        assert!(db.fail_job(job, "job_failed", "late", false).is_err());
        assert_eq!(import_row(&db, rec).0, "ready");
        assert_eq!(db.job(job).unwrap().unwrap().state, JobState::Succeeded);
    }

    #[test]
    fn retrying_a_failed_import_puts_it_back_in_the_queue() {
        let db = Db::open_in_memory().unwrap();
        let (rec, job) = running_import(&db);
        db.fail_job(job, "job_failed", "not enough disk space", false).unwrap();

        let again = db.reset_import_for_retry(rec).unwrap().expect("a failed import retries");
        assert_ne!(again, job);
        assert_eq!(import_row(&db, rec).0, "processing");
        assert_eq!(job_types_queued(&db, rec), vec!["import_media"]);
        assert!(db.job(job).unwrap().is_none(), "the failed attempt is cleared");
    }

    /// Only a failed import is retried. Resetting one that is still running
    /// would queue a second decode of the same file alongside it.
    #[test]
    fn only_a_failed_import_is_reset() {
        let db = Db::open_in_memory().unwrap();
        let (rec, job) = running_import(&db);
        assert!(db.reset_import_for_retry(rec).unwrap().is_none(), "running");

        assert!(db.finalize_import_success(rec, job, &index_fields()).unwrap());
        assert!(db.reset_import_for_retry(rec).unwrap().is_none(), "ready");

        let (captured_db, captured) = db_with_recording();
        captured_db
            .conn
            .execute(
                "UPDATE recordings SET status = 'failed' WHERE id = ?1",
                params![captured.to_string()],
            )
            .unwrap();
        assert!(
            captured_db.reset_import_for_retry(captured).unwrap().is_none(),
            "a captured recording is never an import"
        );
    }

    /// A daemon that keeps dying while decoding a file eventually gives up on
    /// it, and the recording must say so rather than read "processing" for
    /// ever.
    #[test]
    fn an_import_that_keeps_killing_the_daemon_fails_its_recording() {
        let db = Db::open_in_memory().unwrap();
        let rec = Ulid::new();
        let job = db.create_import(&new_import(rec)).unwrap();

        for _ in 0..Job::DEFAULT_MAX_ATTEMPTS {
            db.claim_next_job(1).unwrap();
            db.recover_orphaned_jobs().unwrap();
        }

        assert_eq!(db.job(job).unwrap().unwrap().state, JobState::FailedTerminal);
        assert_eq!(import_row(&db, rec).0, "failed");
    }

    #[test]
    fn a_recovered_import_with_attempts_left_stays_processing() {
        let db = Db::open_in_memory().unwrap();
        let rec = Ulid::new();
        let job = db.create_import(&new_import(rec)).unwrap();
        db.claim_next_job(1).unwrap();
        db.recover_orphaned_jobs().unwrap();

        assert_eq!(db.job(job).unwrap().unwrap().state, JobState::Queued);
        assert_eq!(import_row(&db, rec).0, "processing");
    }

    /// Builds a database exactly as version 7 left it, with captured
    /// recordings and jobs in it, the way a real installation would be when
    /// this version first opens it.
    fn version_7_database(path: &std::path::Path) -> (Ulid, Vec<(String, i64, String)>) {
        let conn = Connection::open(path).unwrap();
        Db::configure(&conn).unwrap();
        for sql in [
            include_str!("migrations/001_initial.sql"),
            include_str!("migrations/002_library.sql"),
            include_str!("migrations/003_exports.sql"),
            include_str!("migrations/004_backoff.sql"),
            include_str!("migrations/005_progress.sql"),
            include_str!("migrations/006_derived_backup.sql"),
            include_str!("migrations/007_publish_webhook.sql"),
        ] {
            conn.execute_batch(sql).unwrap();
        }
        conn.pragma_update(None, "user_version", 7i64).unwrap();

        let rec = Ulid::new();
        conn.execute(
            "INSERT INTO recordings (id, owner_id, status, title, started_at, manifest_version,
                                     clock_started_ns, duration_ms, has_mixed, tracks_json)
             VALUES (?1, 1, 'ready', 'Standup', 1760000000, 'recording-manifest/v1',
                     5000, 60000, 1, '[]')",
            params![rec.to_string()],
        )
        .unwrap();
        let mut jobs = Vec::new();
        for (seq, (job_type, state)) in [
            ("transcribe", "succeeded"),
            ("upload_remote", "failed_retryable"),
            ("publish_webhook", "queued"),
        ]
        .into_iter()
        .enumerate()
        {
            let id = Ulid::new().to_string();
            conn.execute(
                "INSERT INTO jobs (id, enqueue_seq, recording_id, job_type, revision, state,
                                   attempt, max_attempts, next_attempt_at)
                 VALUES (?1, ?2, ?3, ?4, 1, ?5, 1, 3, 1760000100)",
                params![id, seq as i64 + 1, rec.to_string(), job_type, state],
            )
            .unwrap();
            jobs.push((id, seq as i64 + 1, state.to_string()));
        }
        (rec, jobs)
    }

    #[test]
    fn a_version_7_database_upgrades_and_keeps_its_recordings_and_jobs() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("kaseta.db");
        let (rec, jobs) = version_7_database(&path);

        let db = Db::open(&path).expect("a version 7 database must upgrade");
        let version: i64 = db
            .conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 8);

        // Existing recordings were all captured.
        let (status, origin, original_local, _, _, source_json) = import_row(&db, rec);
        assert_eq!((status.as_str(), origin.as_str()), ("ready", "captured"));
        assert_eq!(original_local, 0);
        assert_eq!(source_json, None);
        let title: String = db
            .conn
            .query_row("SELECT title FROM recordings WHERE id = ?1", params![rec.to_string()], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(title, "Standup");

        // Every job survived the rebuild with its order, state and backoff.
        for (id, seq, state) in &jobs {
            let (got_seq, got_state, next): (i64, String, Option<i64>) = db
                .conn
                .query_row(
                    "SELECT enqueue_seq, state, next_attempt_at FROM jobs WHERE id = ?1",
                    params![id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .unwrap();
            assert_eq!((&got_seq, &got_state), (seq, state));
            assert_eq!(next, Some(1760000100));
        }

        // The rebuilt table has every index the queue relies on.
        let indexes: Vec<String> = {
            let mut stmt = db
                .conn
                .prepare(
                    "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'jobs'",
                )
                .unwrap();
            let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
            rows.collect::<Result<_, _>>().unwrap()
        };
        for wanted in [
            "idx_jobs_enqueue_seq",
            "idx_jobs_state",
            "idx_jobs_recording",
            "idx_jobs_claimable",
            "idx_jobs_by_recording_state",
        ] {
            assert!(indexes.iter().any(|i| i == wanted), "{wanted} missing: {indexes:?}");
        }

        // The new stage is storable, the old ones still are, and the CHECK
        // still rejects what is not a stage.
        let import = Ulid::new();
        db.create_import(&new_import(import)).unwrap();
        db.enqueue(rec, JobType::Summarize, 1).unwrap();
        assert!(db
            .conn
            .execute(
                "INSERT INTO jobs (id, enqueue_seq, recording_id, job_type, state)
                 VALUES ('x', 999, ?1, 'make_coffee', 'queued')",
                params![rec.to_string()],
            )
            .is_err());
        // And the origin CHECK guards the new column.
        assert!(db
            .conn
            .execute(
                "UPDATE recordings SET origin = 'downloaded' WHERE id = ?1",
                params![rec.to_string()],
            )
            .is_err());

        // Claiming still walks the queue in order: the queued send from
        // before the upgrade comes before anything added after it.
        let first = db.claim_next_job(1).unwrap().unwrap();
        assert_eq!(first.job_type, JobType::PublishWebhook);
    }

    #[test]
    fn migrations_are_idempotent() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("kaseta.db");

        let db = Db::open(&path).unwrap();
        drop(db);
        // Reopening must not re-run migrations or fail on existing tables.
        let db = Db::open(&path).unwrap();
        let version: i64 = db
            .conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }
}

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
use kaseta_contracts::{Job, JobState, JobType};
use rusqlite::{params, Connection, OptionalExtension};
use ulid::Ulid;

/// Bumped whenever the schema changes. Migrations run in order at startup.
const SCHEMA_VERSION: i64 = 1;

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

        if current < 1 {
            self.conn
                .execute_batch(include_str!("migrations/001_initial.sql"))
                .context("applying migration 001_initial")?;
        }

        self.conn
            .pragma_update(None, "user_version", SCHEMA_VERSION)?;
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
                "SELECT id FROM jobs WHERE state = ?1 ORDER BY enqueue_seq ASC LIMIT 1",
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

        tx.execute(
            "UPDATE jobs SET state = ?1, worker_pid = NULL, error_code = ?2, error_message = ?3
             WHERE id = ?4",
            params![next.as_str(), code, message, id.to_string()],
        )?;
        tx.commit()?;
        Ok(next)
    }

    /// Returns retryable failures to the queue.
    pub fn requeue_retryable(&self) -> Result<usize> {
        Ok(self.conn.execute(
            "UPDATE jobs SET state = ?1, error_code = NULL, error_message = NULL
             WHERE state = ?2 AND attempt < max_attempts",
            params![JobState::Queued.as_str(), JobState::FailedRetryable.as_str()],
        )?)
    }

    /// Recovers jobs orphaned by a daemon that died mid-flight.
    ///
    /// This replaces distributed leasing. Any job still marked `running` at
    /// startup has no live worker, because the daemon that spawned it is gone.
    /// Model inference is not resumable, so orphans restart from the beginning
    /// and their partial output is discarded by the worker's own atomic write.
    pub fn recover_orphaned_jobs(&self) -> Result<Vec<Ulid>> {
        let tx = self.conn.unchecked_transaction()?;

        let ids: Vec<String> = {
            let mut stmt = tx.prepare("SELECT id FROM jobs WHERE state = ?1")?;
            let rows = stmt.query_map(params![JobState::Running.as_str()], |r| r.get(0))?;
            rows.collect::<Result<_, _>>()?
        };

        let mut recovered = Vec::new();
        for id in &ids {
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

            recovered.push(Ulid::from_string(id)?);
        }

        tx.commit()?;
        Ok(recovered)
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

/// Parsers reject unknown values rather than substituting a default.
///
/// A silent fallback would turn database corruption, or a row written by a newer
/// schema, into a job quietly misclassified as a different type or state — which
/// could resurrect finished work or discard live work.
fn parse_job_type(s: &str) -> Result<JobType> {
    Ok(match s {
        "finalize_recording" => JobType::FinalizeRecording,
        "transcribe" => JobType::Transcribe,
        "merge_transcript" => JobType::MergeTranscript,
        "summarize" => JobType::Summarize,
        "upload_remote" => JobType::UploadRemote,
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
        assert_eq!(db.requeue_retryable().unwrap(), 1);

        let job = Db::load_job(&db.conn, &id.to_string()).unwrap().unwrap();
        assert_eq!(job.state, JobState::Queued);
        assert_eq!(job.error_code, None, "requeue must clear the stale error");
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

//! Pipeline job model.
//!
//! Processing is split into independently retryable stages rather than one
//! monolithic "process recording" job, so a failed summary never costs a good
//! transcript.
//!
//! Because a single daemon owns the database, distributed leasing is
//! unnecessary: child-process supervision replaces it. A job that is `Running`
//! records the PID of the worker executing it, and on startup the daemon sweeps
//! for `Running` jobs whose process is gone and requeues them. Model inference
//! is not resumable, so a requeued job restarts from the beginning and its
//! partial output is discarded.

use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    /// Verify every declared chunk is present and hashes match, then seal the
    /// manifest.
    FinalizeRecording,
    Transcribe,
    /// Merge per-track transcripts onto the canonical timeline.
    MergeTranscript,
    Summarize,
    /// Copy sealed blobs to configured remote object storage.
    UploadRemote,
    /// Post the finished transcript to the webhook the user configured.
    ///
    /// Outside PIPELINE for the same reason backup is: nothing depends on it,
    /// and it fails for reasons that have nothing to do with the recording — a
    /// receiver that is down, a token that expired. Both are worth retrying
    /// without redoing anything else.
    PublishWebhook,
    /// Decode an uploaded file into chunks, a manifest and exports, then hand
    /// the recording to the same stages a capture gets.
    ///
    /// The first stage of an imported recording and the only one that can run
    /// before a manifest exists, since producing the manifest is its job.
    ImportMedia,
}

impl JobType {
    /// Stages that genuinely depend on the one before them.
    ///
    /// Only summarising does: it reads the transcript. Backup deliberately sits
    /// outside this — it used to be the last link in a chain, which meant a
    /// recording whose transcription failed was never copied anywhere, and that
    /// is the recording a copy matters most for. It is queued when capture ends
    /// and again whenever anything derived from the recording changes.
    ///
    /// Sealing and merging are performed where they happen rather than queued,
    /// so they are absent here too.
    pub const PIPELINE: [JobType; 2] = [JobType::Transcribe, JobType::Summarize];

    /// The stage that should follow this one, if any.
    pub fn next(self) -> Option<JobType> {
        let idx = Self::PIPELINE.iter().position(|j| *j == self)?;
        Self::PIPELINE.get(idx + 1).copied()
    }

    /// Whether failure of this stage should stop the pipeline. A failed upload
    /// or summary leaves a recording usable; a failed transcription does not.
    pub fn is_fatal_to_pipeline(self) -> bool {
        matches!(
            self,
            JobType::FinalizeRecording
                | JobType::Transcribe
                | JobType::MergeTranscript
                | JobType::ImportMedia
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            JobType::FinalizeRecording => "finalize_recording",
            JobType::Transcribe => "transcribe",
            JobType::MergeTranscript => "merge_transcript",
            JobType::Summarize => "summarize",
            JobType::UploadRemote => "upload_remote",
            JobType::PublishWebhook => "publish_webhook",
            JobType::ImportMedia => "import_media",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Running,
    Succeeded,
    /// Failed in a way worth retrying — a crashed worker, a transient network
    /// error. Returns to `Queued` until attempts are exhausted.
    FailedRetryable,
    /// Failed in a way retrying cannot fix — malformed input, missing model.
    FailedTerminal,
    Canceled,
    /// Replaced by a newer revision. Only set once the replacement has
    /// succeeded, so a failed reprocess never destroys a good result.
    Superseded,
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobState::Succeeded
                | JobState::FailedTerminal
                | JobState::Canceled
                | JobState::Superseded
        )
    }

    /// The single source of truth for legal transitions. Every state change goes
    /// through here, so an illegal transition is a caught error rather than a
    /// corrupt row.
    pub fn can_transition_to(self, next: JobState) -> bool {
        use JobState::*;
        matches!(
            (self, next),
            (Queued, Running)
                | (Queued, Canceled)
                | (Running, Succeeded)
                | (Running, FailedRetryable)
                | (Running, FailedTerminal)
                | (Running, Canceled)
                // Requeued by the startup sweep after the daemon died mid-job.
                | (Running, Queued)
                | (FailedRetryable, Queued)
                | (FailedRetryable, FailedTerminal)
                | (Succeeded, Superseded)
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Running => "running",
            JobState::Succeeded => "succeeded",
            JobState::FailedRetryable => "failed_retryable",
            JobState::FailedTerminal => "failed_terminal",
            JobState::Canceled => "canceled",
            JobState::Superseded => "superseded",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("illegal job transition {from:?} -> {to:?}")]
pub struct IllegalTransition {
    pub from: JobState,
    pub to: JobState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Job {
    pub id: Ulid,
    pub recording_id: Ulid,
    pub job_type: JobType,
    /// Incremented when a recording is reprocessed, e.g. with a different ASR
    /// model. Results from different revisions coexist until one is superseded.
    pub revision: u32,
    pub state: JobState,
    pub attempt: u32,
    pub max_attempts: u32,
    /// PID of the worker process, while running. The startup sweep uses this to
    /// tell a live job from an orphan.
    pub worker_pid: Option<u32>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl Job {
    pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;

    pub fn transition(&mut self, next: JobState) -> Result<(), IllegalTransition> {
        if !self.state.can_transition_to(next) {
            return Err(IllegalTransition {
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        if next != JobState::Running {
            self.worker_pid = None;
        }
        Ok(())
    }

    pub fn attempts_exhausted(&self) -> bool {
        self.attempt >= self.max_attempts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(state: JobState) -> Job {
        Job {
            id: Ulid::nil(),
            recording_id: Ulid::nil(),
            job_type: JobType::Transcribe,
            revision: 1,
            state,
            attempt: 0,
            max_attempts: Job::DEFAULT_MAX_ATTEMPTS,
            worker_pid: Some(4242),
            error_code: None,
            error_message: None,
        }
    }

    #[test]
    fn only_summarising_waits_on_the_stage_before_it() {
        assert_eq!(JobType::Transcribe.next(), Some(JobType::Summarize));
        assert_eq!(JobType::Summarize.next(), None);
    }

    /// Backup is not a link in the chain. Making it one meant a recording whose
    /// transcription failed was never copied anywhere, which is exactly the
    /// recording worth having a copy of.
    #[test]
    fn backup_does_not_wait_on_anything() {
        assert_eq!(JobType::UploadRemote.next(), None);
        assert!(
            !JobType::PIPELINE.contains(&JobType::UploadRemote),
            "backup must not be reachable by following the chain"
        );
    }

    #[test]
    fn only_pre_transcript_stages_stop_the_pipeline() {
        assert!(JobType::Transcribe.is_fatal_to_pipeline());
        // A recording with no summary is still a usable recording.
        assert!(!JobType::Summarize.is_fatal_to_pipeline());
        assert!(!JobType::UploadRemote.is_fatal_to_pipeline());
    }

    /// Decoding an imported file produces the audio every stage after it reads,
    /// so nothing can follow a failed import.
    #[test]
    fn importing_stops_the_pipeline_and_chains_to_nothing_by_itself() {
        assert!(JobType::ImportMedia.is_fatal_to_pipeline());
        assert_eq!(JobType::ImportMedia.as_str(), "import_media");
        assert_eq!(JobType::ImportMedia.next(), None);
        assert!(!JobType::PIPELINE.contains(&JobType::ImportMedia));
        assert_eq!(
            serde_json::to_string(&JobType::ImportMedia).unwrap(),
            "\"import_media\""
        );
    }

    #[test]
    fn a_killed_daemon_can_requeue_a_running_job() {
        let mut j = job(JobState::Running);
        j.transition(JobState::Queued).unwrap();
        assert_eq!(j.state, JobState::Queued);
        assert_eq!(j.worker_pid, None, "orphaned PID must be cleared");
    }

    #[test]
    fn rejects_illegal_transitions() {
        let mut j = job(JobState::Succeeded);
        assert!(j.transition(JobState::Running).is_err());
        assert_eq!(j.state, JobState::Succeeded, "state must not change on error");

        // A superseded result is final.
        let mut j = job(JobState::Superseded);
        assert!(j.transition(JobState::Queued).is_err());

        // Terminal failure cannot silently become a success.
        let mut j = job(JobState::FailedTerminal);
        assert!(j.transition(JobState::Succeeded).is_err());
    }

    #[test]
    fn a_good_result_is_only_superseded_from_success() {
        let mut j = job(JobState::Succeeded);
        assert!(j.transition(JobState::Superseded).is_ok());
        assert!(!job(JobState::Queued).state.can_transition_to(JobState::Superseded));
    }

    #[test]
    fn retry_budget_is_bounded() {
        let mut j = job(JobState::Running);
        j.transition(JobState::FailedRetryable).unwrap();
        j.attempt = Job::DEFAULT_MAX_ATTEMPTS;
        assert!(j.attempts_exhausted());
        assert!(j.state.can_transition_to(JobState::FailedTerminal));
    }
}

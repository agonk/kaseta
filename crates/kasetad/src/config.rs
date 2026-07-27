//! User-editable settings, including credentials.
//!
//! # Why not the database
//!
//! Settings hold API keys and object-storage credentials. The database lives in
//! the data directory alongside recordings — the directory this very feature
//! exists to upload elsewhere. Putting credentials there risks syncing them to
//! a bucket, which is precisely the accident worth designing out. They live in
//! the config directory instead, in a file only the owner can read.
//!
//! # Precedence
//!
//! The environment wins over the file. A key exported for a one-off run, or set
//! by a service unit, should not be silently overridden by something saved
//! earlier through the interface.

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Everything a user can change.
///
/// Secrets are `Option<String>` so "not configured" is distinct from "set to
/// empty", which matters when deciding whether a feature is available at all.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub summaries: SummarySettings,
    pub remote_storage: RemoteStorageSettings,
    pub retention: RetentionSettings,
    #[serde(default)]
    pub transcription: TranscriptionSettings,
}

/// Whether meetings are transcribed at all.
///
/// On by default: transcribing is the reason to run a recorder rather than a
/// tape deck, and it happens entirely on this machine, so leaving it on costs
/// nothing but time. Off is for someone who wants recordings and backup and
/// would rather not spend the CPU.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct TranscriptionSettings {
    pub enabled: bool,
}

impl Default for TranscriptionSettings {
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// When recordings are removed automatically.
///
/// Off by default. Deleting someone's meetings without being asked is not a
/// sensible default, however much disk it saves.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RetentionSettings {
    pub enabled: bool,
    /// Age past which a recording is deleted. Zero or absent means never.
    pub keep_days: Option<u32>,
    /// Keep only the audio's derived artefacts, discarding the audio itself.
    /// A transcript is a fraction of the size and is usually what is wanted
    /// months later.
    pub audio_only: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SummarySettings {
    pub api_key: Option<String>,
    pub model: Option<String>,
    /// Whether transcript text may be sent to a summary provider at all.
    ///
    /// Not merely "summarise automatically": nothing overrides this, including
    /// an explicit request and a key found in the environment. It is the one
    /// switch the interface's privacy summary reports on, and a control that
    /// something else can quietly overrule is not a control.
    ///
    /// Off by default, so a key alone does not start sending transcripts
    /// anywhere.
    pub enabled: bool,
}

/// Where recordings are copied, if anywhere.
///
/// Modelled on S3 because R2, Backblaze, MinIO and S3 itself all speak it; the
/// endpoint is what distinguishes them.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RemoteStorageSettings {
    pub enabled: bool,
    /// Full endpoint URL. Empty means Amazon S3.
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub bucket: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    /// Whether to remove local audio once it has been uploaded and verified.
    pub delete_local_after_upload: bool,
}

impl Settings {
    /// Where settings are stored, honouring the XDG config directory.
    pub fn path() -> PathBuf {
        let base = std::env::var("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| ".".into())).join(".config")
            });
        base.join("kaseta").join("settings.json")
    }

    /// Reads settings, treating an absent file as defaults.
    pub fn load() -> Result<Self> {
        Self::load_from(&Self::path())
    }

    pub fn load_from(path: &std::path::Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => {
                serde_json::from_slice(&bytes).with_context(|| {
                    format!("{} is not readable as settings", path.display())
                })
            }
            // Never configured is not an error; it is the starting state.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
        }
    }

    pub fn save(&self) -> Result<()> {
        self.save_to(&Self::path())
    }

    /// Writes settings so only the owner can read them.
    ///
    /// The file holds credentials, so the permissions are set before any content
    /// is written — creating it readable and narrowing afterwards would leave a
    /// window where another user could read the key.
    pub fn save_to(&self, path: &std::path::Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }

        let json = serde_json::to_vec_pretty(self).context("serialising settings")?;

        // Written to a temporary file and renamed, so an interrupted write
        // cannot leave settings half-written and unparseable.
        let tmp = path.with_extension("json.tmp");
        write_private(&tmp, &json)?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("committing {}", path.display()))?;
        Ok(())
    }
}

#[cfg(unix)]
fn write_private(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn write_private(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    std::fs::write(path, bytes).with_context(|| format!("writing {}", path.display()))
}

/// What the interface is allowed to see.
///
/// A secret is never sent back, only whether it is set and enough of it to
/// recognise. Returning the key would put it in every response, in browser
/// memory, and in anything that logs responses.
#[derive(Clone, Debug, Serialize)]
pub struct RedactedSettings {
    pub transcription: TranscriptionSettings,
    pub summaries: RedactedSummary,
    pub remote_storage: RedactedRemoteStorage,
    pub retention: RetentionSettings,
}

#[derive(Clone, Debug, Serialize)]
pub struct RedactedSummary {
    pub enabled: bool,
    /// Only a model the person actually chose.
    ///
    /// Emphatically not the fallback: the interface puts this in an editable
    /// field and posts it back on save, so a default rendered here would be
    /// written to disk the first time anyone saved anything, and would then
    /// outlive the default it came from. A model that stops existing — as
    /// retired ones do — would be frozen in place with no way to tell it was
    /// never a choice.
    pub model: Option<String>,
    /// What runs when no model is chosen. Shown as a prompt, never as a value.
    pub default_model: String,
    /// Set when the environment names the model, in which case what is saved
    /// here has no effect.
    pub model_from_environment: bool,
    pub api_key_set: bool,
    pub api_key_hint: Option<String>,
    /// Set when the environment provides the key, in which case editing it here
    /// would have no effect.
    pub from_environment: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct RedactedRemoteStorage {
    pub enabled: bool,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_set: bool,
    pub delete_local_after_upload: bool,
}

/// Shows enough of a secret to recognise it, and no more.
fn hint(secret: &str) -> Option<String> {
    let trimmed = secret.trim();
    if trimmed.is_empty() {
        return None;
    }
    // The last few characters are enough to tell two keys apart without being
    // useful to anyone who sees them.
    let tail: String = trimmed.chars().rev().take(4).collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    Some(format!("…{tail}"))
}

impl Settings {
    pub fn redacted(&self) -> RedactedSettings {
        let env_key = std::env::var(crate::summarize::API_KEY_ENV)
            .ok()
            .filter(|k| !k.trim().is_empty());

        RedactedSettings {
            transcription: self.transcription.clone(),
            summaries: RedactedSummary {
                enabled: self.summaries.enabled,
                model: self.summaries.model.clone(),
                default_model: crate::summarize::DEFAULT_MODEL.to_string(),
                model_from_environment: std::env::var("KASETA_OPENROUTER_MODEL")
                    .is_ok_and(|m| !m.trim().is_empty()),
                api_key_set: env_key.is_some() || self.summaries.api_key.is_some(),
                api_key_hint: env_key
                    .as_deref()
                    .or(self.summaries.api_key.as_deref())
                    .and_then(hint),
                from_environment: env_key.is_some(),
            },
            remote_storage: RedactedRemoteStorage {
                enabled: self.remote_storage.enabled,
                endpoint: self.remote_storage.endpoint.clone().unwrap_or_default(),
                region: self.remote_storage.region.clone().unwrap_or_default(),
                bucket: self.remote_storage.bucket.clone().unwrap_or_default(),
                access_key_id: self.remote_storage.access_key_id.clone().unwrap_or_default(),
                secret_set: self.remote_storage.secret_access_key.is_some(),
                delete_local_after_upload: self.remote_storage.delete_local_after_upload,
            },
            retention: self.retention.clone(),
        }
    }
}

/// A change submitted from the interface.
///
/// Every secret is optional and `None` means "leave as it was". The interface
/// never receives a secret, so it cannot send one back unchanged — without this
/// distinction, saving any unrelated setting would erase the key.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct SettingsUpdate {
    pub summaries_enabled: Option<bool>,
    pub summaries_model: Option<String>,
    pub transcription_enabled: Option<bool>,
    pub summaries_api_key: Option<String>,
    pub remote_enabled: Option<bool>,
    pub remote_endpoint: Option<String>,
    pub remote_region: Option<String>,
    pub remote_bucket: Option<String>,
    pub remote_access_key_id: Option<String>,
    pub remote_secret_access_key: Option<String>,
    pub remote_delete_local: Option<bool>,
    pub retention_enabled: Option<bool>,
    pub retention_keep_days: Option<u32>,
    pub retention_audio_only: Option<bool>,
}

impl Settings {
    /// Applies a change, leaving anything unspecified alone.
    pub fn apply(&mut self, update: SettingsUpdate) {
        if let Some(v) = update.summaries_enabled {
            self.summaries.enabled = v;
        }
        if let Some(v) = update.transcription_enabled {
            self.transcription.enabled = v;
        }
        if let Some(v) = update.summaries_model {
            self.summaries.model = non_empty(v);
        }
        if let Some(v) = update.summaries_api_key {
            // An explicitly empty value clears the key: that is how the
            // interface expresses "remove this", and it must not be confused
            // with the field being absent.
            self.summaries.api_key = non_empty(v);
        }
        if let Some(v) = update.remote_enabled {
            self.remote_storage.enabled = v;
        }
        if let Some(v) = update.remote_endpoint {
            self.remote_storage.endpoint = non_empty(v);
        }
        if let Some(v) = update.remote_region {
            self.remote_storage.region = non_empty(v);
        }
        if let Some(v) = update.remote_bucket {
            self.remote_storage.bucket = non_empty(v);
        }
        if let Some(v) = update.remote_access_key_id {
            self.remote_storage.access_key_id = non_empty(v);
        }
        if let Some(v) = update.remote_secret_access_key {
            self.remote_storage.secret_access_key = non_empty(v);
        }
        if let Some(v) = update.remote_delete_local {
            self.remote_storage.delete_local_after_upload = v;
        }
        if let Some(v) = update.retention_enabled {
            self.retention.enabled = v;
        }
        if let Some(v) = update.retention_keep_days {
            // Zero would mean "delete everything immediately", which is never
            // what someone means by a retention period.
            self.retention.keep_days = (v > 0).then_some(v);
        }
        if let Some(v) = update.retention_audio_only {
            self.retention.audio_only = v;
        }
    }
}

fn non_empty(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn absent_settings_are_defaults_rather_than_an_error() {
        let dir = TempDir::new().unwrap();
        let settings = Settings::load_from(&dir.path().join("nothing.json")).unwrap();
        assert!(!settings.summaries.enabled);
        assert!(settings.summaries.api_key.is_none());
    }

    #[test]
    fn settings_round_trip_through_the_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");

        let mut settings = Settings::default();
        settings.summaries.api_key = Some("sk-or-secret".into());
        settings.remote_storage.bucket = Some("meetings".into());
        settings.save_to(&path).unwrap();

        let back = Settings::load_from(&path).unwrap();
        assert_eq!(back.summaries.api_key.as_deref(), Some("sk-or-secret"));
        assert_eq!(back.remote_storage.bucket.as_deref(), Some("meetings"));
    }

    #[cfg(unix)]
    #[test]
    fn the_settings_file_is_readable_only_by_its_owner() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        Settings::default().save_to(&path).unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "credentials must not be world-readable");
    }

    #[cfg(unix)]
    #[test]
    fn rewriting_settings_keeps_them_private() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        Settings::default().save_to(&path).unwrap();

        let mut settings = Settings::default();
        settings.summaries.api_key = Some("sk-or-second".into());
        settings.save_to(&path).unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn saving_leaves_no_temporary_file_behind() {
        let dir = TempDir::new().unwrap();
        Settings::default().save_to(&dir.path().join("settings.json")).unwrap();

        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty());
    }

    #[test]
    fn a_secret_is_never_returned_to_the_interface() {
        let mut settings = Settings::default();
        settings.summaries.api_key = Some("sk-or-v1-abcdef123456".into());
        settings.remote_storage.secret_access_key = Some("very-secret".into());

        let json = serde_json::to_string(&settings.redacted()).unwrap();

        assert!(!json.contains("sk-or-v1-abcdef123456"));
        assert!(!json.contains("very-secret"));
        assert!(json.contains("api_key_set\":true"));
        assert!(json.contains("secret_set\":true"));
    }

    #[test]
    fn the_hint_identifies_a_key_without_revealing_it() {
        assert_eq!(hint("sk-or-v1-abcdef123456").as_deref(), Some("…3456"));
        assert_eq!(hint("   "), None);
        assert_eq!(hint(""), None);
    }

    #[test]
    fn an_unrelated_change_does_not_erase_the_key() {
        // The interface never receives the secret, so it cannot send it back.
        // Treating an absent field as "clear it" would wipe the key whenever
        // any other setting was saved.
        let mut settings = Settings::default();
        settings.summaries.api_key = Some("sk-or-keep-me".into());

        settings.apply(SettingsUpdate {
            summaries_enabled: Some(true),
            ..SettingsUpdate::default()
        });

        assert_eq!(settings.summaries.api_key.as_deref(), Some("sk-or-keep-me"));
        assert!(settings.summaries.enabled);
    }

    #[test]
    fn an_explicitly_empty_value_clears_a_secret() {
        let mut settings = Settings::default();
        settings.summaries.api_key = Some("sk-or-remove-me".into());

        settings.apply(SettingsUpdate {
            summaries_api_key: Some(String::new()),
            ..SettingsUpdate::default()
        });

        assert_eq!(settings.summaries.api_key, None);
    }

    #[test]
    fn values_are_trimmed_so_a_pasted_key_still_works() {
        let mut settings = Settings::default();
        settings.apply(SettingsUpdate {
            summaries_api_key: Some("  sk-or-pasted \n".into()),
            ..SettingsUpdate::default()
        });
        assert_eq!(settings.summaries.api_key.as_deref(), Some("sk-or-pasted"));
    }

    #[test]
    fn settings_are_not_stored_beside_recordings() {
        // The data directory is what remote storage uploads. A credential there
        // could be synced to a bucket.
        let path = Settings::path();
        assert!(
            path.to_string_lossy().contains("config"),
            "settings belong in the config directory, not {path:?}"
        );
    }
}

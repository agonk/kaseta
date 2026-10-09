//! The local HTTP API, and the interface it serves.
//!
//! The daemon serves its own interface as a static page rather than shipping a
//! desktop shell. Closing the browser tab leaves no process resident, which is
//! what keeps idle cost to the daemon alone.
//!
//! # Binding
//!
//! Listening is restricted to loopback. The API has no authentication because
//! it has no remote surface; binding it to anything routable would expose a
//! recorder with no access control to the network.

use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};

use anyhow::{Context, Result};
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use kaseta_contracts::manifest::RecordingNotes;
use kaseta_contracts::{BlobKey, ImportSource};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::blobstore::BlobStore;
use crate::db::Db;
use crate::import::upload::{self, UploadError, UploadRequest};
use crate::import::ImportRuntime;
use crate::library;
use crate::supervisor::{DaemonStatus, Supervisor};

/// The interface, compiled into the binary so the daemon is a single file with
/// nothing to install alongside it.
const INDEX_HTML: &str = include_str!("../ui/index.html");

/// Header carrying the per-run token on mutating requests.
const TOKEN_HEADER: &str = "x-kaseta-token";

/// A random token minted per daemon run and embedded in the page.
///
/// The API is unauthenticated because it has no remote surface, but "no remote
/// surface" is not the same as "unreachable": any page in the browser can POST
/// to `http://127.0.0.1:7777` cross-origin. Without this, visiting a hostile
/// site would let it start or stop recording. The same-origin policy stops that
/// site reading the token out of our page, so requiring it on every mutation is
/// what makes the difference.
///
/// Drawn from the operating system's generator. A token derived from the PID
/// and the clock is guessable by anyone who can estimate when the daemon
/// started, and it now guards uploads of arbitrary size as well as recording.
fn mint_token() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|e| anyhow::anyhow!("reading the system random number generator: {e}"))?;
    Ok(hex::encode(bytes))
}

/// Whether a request's `Host` names this daemon.
///
/// DNS rebinding is the attack the token cannot stop: a hostile page whose
/// name is re-pointed at 127.0.0.1 becomes same-origin with itself while
/// talking to us, so the browser lets it read our replies, the token
/// included. Its requests still carry its own name in `Host`, which no page
/// script can change. Only the loopback names, on our port, are accepted.
fn host_allowed(host: Option<&str>, port: u16) -> bool {
    let Some(host) = host else { return false };
    let host = host.trim().to_ascii_lowercase();
    ["127.0.0.1", "localhost", "[::1]"].iter().any(|name| {
        host == format!("{name}:{port}") || (port == 80 && host == *name)
    })
}

/// Refuses every request whose `Host` is not this daemon's own.
fn guard_host(router: Router, port: u16) -> Router {
    router.layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| async move {
            // HTTP/2 carries the name in the URI rather than a header.
            let host = request
                .headers()
                .get(header::HOST)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
                .or_else(|| request.uri().authority().map(|a| a.to_string()));
            if host_allowed(host.as_deref(), port) {
                next.run(request).await
            } else {
                ApiError::with_status(
                    StatusCode::MISDIRECTED_REQUEST,
                    "this address does not name the Kaseta daemon; open it through \
                     127.0.0.1 or localhost",
                )
                .into_response()
            }
        },
    ))
}

#[derive(Clone)]
pub struct AppState {
    supervisor: Arc<Supervisor>,
    store: Arc<dyn BlobStore>,
    /// SQLite allows one writer and its connection is not `Sync`, so handlers
    /// take it in turn. Every access happens inside `spawn_blocking`, never on
    /// a runtime thread.
    db: Arc<Mutex<Db>>,
    token: Arc<String>,
    /// Whether files can be imported, and the one upload slot.
    imports: Arc<ImportRuntime>,
}

pub fn router(
    supervisor: Arc<Supervisor>,
    store: Arc<dyn BlobStore>,
    db: Arc<Mutex<Db>>,
    imports: Arc<ImportRuntime>,
    token: Arc<String>,
    port: u16,
) -> Router {
    let state = AppState {
        supervisor,
        store,
        db,
        token,
        imports,
    };

    let routes = Router::new()
        .route("/", get(index))
        .route("/api/v1/status", get(status))
        .route("/api/v1/token", get(session_token))
        .route("/api/v1/devices", get(devices))
        .route("/api/v1/recordings", get(list_recordings).post(start_recording))
        .route("/api/v1/recordings/active/stop", post(stop_recording))
        .route("/api/v1/recordings/{id}", get(get_recording))
        .route("/api/v1/recordings/{id}", patch(rename_recording))
        .route("/api/v1/recordings/{id}", delete(delete_recording))
        .route("/api/v1/recordings/{id}/audio/{file}", get(audio))
        .route("/api/v1/recordings/{id}/original", get(original))
        // A file to import may be gigabytes, and arrives as a stream that is
        // written to disk as it comes; the size limit is the import's own.
        .route(
            "/api/v1/imports",
            post(import_file).layer(DefaultBodyLimit::disable()),
        )
        .route("/api/v1/recordings/{id}/transcript", get(transcript))
        .route("/api/v1/recordings/{id}/summary", get(summary))
        .route("/api/v1/recordings/{id}/waveform", get(waveform))
        .route("/api/v1/recordings/{id}/stages/{stage}", post(run_stage))
        .route("/api/v1/recordings/{id}/transcript.txt", get(transcript_text))
        .route("/api/v1/search", get(search))
        .route("/api/v1/settings", get(get_settings).put(put_settings))
        .with_state(state);

    guard_host(routes, port)
}

/// Serves the interface on loopback.
pub async fn serve(
    supervisor: Arc<Supervisor>,
    store: Arc<dyn BlobStore>,
    db: Arc<Mutex<Db>>,
    imports: Arc<ImportRuntime>,
    port: u16,
) -> Result<()> {
    let token = Arc::new(mint_token()?);
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr} (is another kasetad already running?)"))?;

    println!("Kaseta is running at http://{addr}");
    println!("Press Ctrl-C to stop.\n");

    axum::serve(listener, router(supervisor, store, db, imports, token, port))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serving HTTP")?;
    Ok(())
}

/// Waits for Ctrl-C so a recording in progress is sealed rather than abandoned.
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
}

async fn index(State(state): State<AppState>) -> Html<String> {
    // The token is handed to the page it protects, and to nothing else.
    Html(INDEX_HTML.replace("__KASETA_TOKEN__", &state.token))
}

/// Rejects a mutating request that did not come from the served page.
///
/// Two independent checks: the token, which a cross-origin page cannot read,
/// and `Sec-Fetch-Site`, which the browser sets and script cannot forge.
fn authorize(state: &AppState, headers: &axum::http::HeaderMap) -> Result<(), ApiError> {
    if let Some(site) = headers.get("sec-fetch-site").and_then(|v| v.to_str().ok()) {
        if site != "same-origin" && site != "none" {
            return Err(ApiError::forbidden("request did not come from the Kaseta page"));
        }
    }

    let presented = headers.get(TOKEN_HEADER).and_then(|v| v.to_str().ok());
    match presented {
        Some(t) if t == state.token.as_str() => Ok(()),
        _ => Err(ApiError::forbidden("missing or invalid token")),
    }
}

/// Runs a database operation off the runtime thread.
///
/// The connection is a blocking resource behind a std `Mutex`. Touching it from
/// an async handler would stall every other request, including status, for as
/// long as the query and any I/O took.
async fn with_db<T, F>(state: &AppState, f: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&Db) -> Result<T, ApiError> + Send + 'static,
{
    let db = Arc::clone(&state.db);
    tokio::task::spawn_blocking(move || {
        let guard = db.lock().map_err(|_| ApiError::internal("database lock poisoned"))?;
        f(&guard)
    })
    .await
    .map_err(|e| ApiError::internal(format!("database task failed: {e}")))?
}

/// What the daemon is doing, and what it can do.
#[derive(Debug, Serialize)]
struct StatusResponse {
    #[serde(flatten)]
    daemon: DaemonStatus,
    import: ImportStatus,
}

/// Whether files can be imported. When they cannot, the reason names what to
/// install, so the interface can say so before anyone picks a file.
#[derive(Debug, Serialize)]
struct ImportStatus {
    available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

async fn status(State(state): State<AppState>) -> Json<StatusResponse> {
    let tools = state.imports.toolchain();
    Json(StatusResponse {
        daemon: state.supervisor.status(),
        import: ImportStatus {
            available: tools.is_ok(),
            reason: tools.err().map(str::to_string),
        },
    })
}

/// Hands the run's token to local tooling.
///
/// Safe to expose, and not a way around the token's purpose. That purpose is to
/// stop a page on another origin driving the recorder — and such a page can
/// issue this request but cannot read the reply, because the same-origin policy
/// withholds the response body without CORS headers, which are deliberately not
/// sent. A local process could read the token out of the served page anyway;
/// this only means it does not have to scrape markup that may be restyled.
async fn session_token(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<String, ApiError> {
    // Belt and braces: browsers label cross-site requests, and script cannot
    // forge the header.
    if let Some(site) = headers.get("sec-fetch-site").and_then(|v| v.to_str().ok()) {
        if site != "same-origin" && site != "none" {
            return Err(ApiError::forbidden("cross-site requests cannot read the token"));
        }
    }
    Ok(state.token.to_string())
}

async fn devices() -> Result<Json<Vec<crate::capture::AudioDevice>>, ApiError> {
    // Enumeration touches PipeWire, which is blocking and not async-friendly.
    let devices = tokio::task::spawn_blocking(crate::capture::list_devices)
        .await
        .map_err(|e| ApiError::internal(format!("device enumeration panicked: {e}")))?
        .map_err(|e| ApiError::internal(format!("{e:#}")))?;
    Ok(Json(devices))
}

#[derive(Debug, Deserialize, Default)]
struct StartRequest {
    #[serde(default)]
    title: Option<String>,
}

#[derive(Debug, Serialize)]
struct StartResponse {
    recording_id: Ulid,
}

async fn start_recording(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Option<Json<StartRequest>>,
) -> Result<(StatusCode, Json<StartResponse>), ApiError> {
    authorize(&state, &headers)?;
    let request = body.map(|Json(b)| b).unwrap_or_default();
    let notes = RecordingNotes {
        title: request.title.map(|t| t.trim().to_string()).filter(|t| !t.is_empty()),
        ..RecordingNotes::default()
    };

    let recording_id = state
        .supervisor
        .start(notes)
        .await
        .map_err(ApiError::from_supervisor)?;

    Ok((StatusCode::CREATED, Json(StartResponse { recording_id })))
}

#[derive(Debug, Serialize)]
struct StopResponse {
    recording_id: Ulid,
    duration_ms: i64,
}

async fn stop_recording(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<(StatusCode, Json<StopResponse>), ApiError> {
    authorize(&state, &headers)?;

    // Returns once the manifest is durable. Exports continue on their own
    // thread and are reported through `/status`, so a long meeting does not
    // hold the request open for the minutes it takes to re-encode.
    let stopped = state
        .supervisor
        .stop()
        .await
        .map_err(ApiError::from_supervisor)?;

    // Index just this recording. Rescanning every manifest would make stop
    // latency grow with the size of the library.
    let manifest = stopped.manifest.clone();
    let store = Arc::clone(&state.store);
    if let Err(e) = with_db(&state, move |db| {
        library::index_recording(&*store, db, &manifest).map_err(ApiError::from_anyhow)
    })
    .await
    {
        tracing::error!(error = ?e, "indexing the new recording failed");
    }

    let duration_ms = stopped
        .manifest
        .tracks
        .iter()
        .filter_map(|t| {
            let rate = t.format.sample_rate_hz?;
            (rate > 0).then(|| (t.sample_count() as i64 * 1_000) / rate as i64)
        })
        .max()
        .unwrap_or(0);

    Ok((
        StatusCode::ACCEPTED,
        Json(StopResponse {
            recording_id: stopped.manifest.recording_id,
            duration_ms,
        }),
    ))
}

#[derive(Debug, Serialize)]
struct ListResponse {
    items: Vec<library::LibraryItem>,
}

async fn list_recordings(State(state): State<AppState>) -> Result<Json<ListResponse>, ApiError> {
    let items = with_db(&state, |db| library::list(db).map_err(ApiError::from_anyhow)).await?;
    Ok(Json(ListResponse { items }))
}

async fn get_recording(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<library::LibraryItem>, ApiError> {
    let id = parse_id(&id)?;
    with_db(&state, move |db| {
        library::get(db, id)
            .map_err(ApiError::from_anyhow)?
            .ok_or_else(|| ApiError::not_found("no such recording"))
    })
    .await
    .map(Json)
}

#[derive(Debug, Deserialize)]
struct RenameRequest {
    /// `null` or empty clears the override, restoring the captured or generated
    /// name rather than leaving the recording blank.
    title: Option<String>,
}

async fn rename_recording(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<RenameRequest>,
) -> Result<StatusCode, ApiError> {
    authorize(&state, &headers)?;
    let id = parse_id(&id)?;
    let title = body.title.clone();

    let store = Arc::clone(&state.store);
    with_db(&state, move |db| {
        library::set_title(&*store, db, id, title.as_deref())
            .map_err(ApiError::from_anyhow)?
            .then_some(StatusCode::NO_CONTENT)
            .ok_or_else(|| ApiError::not_found("no such recording"))
    })
    .await
}

async fn delete_recording(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    authorize(&state, &headers)?;
    let id = parse_id(&id)?;
    let store = Arc::clone(&state.store);

    with_db(&state, move |db| {
        library::delete(&*store, db, id)
            .map_err(ApiError::from_anyhow)?
            .then_some(StatusCode::NO_CONTENT)
            .ok_or_else(|| ApiError::not_found("no such recording"))
    })
    .await
}

/// Peak data for drawing the player's scrubber.
async fn waveform(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<f32>>, ApiError> {
    let id = parse_id(&id)?;
    let started_at = with_db(&state, move |db| {
        library::get(db, id)
            .map_err(ApiError::from_anyhow)?
            .map(|r| r.started_at)
            .ok_or_else(|| ApiError::not_found("no such recording"))
    })
    .await?;

    let store = Arc::clone(&state.store);
    let peaks = tokio::task::spawn_blocking(move || {
        let prefix = kaseta_contracts::RecordingPrefix::new(id, started_at);
        let key = prefix.export("mixed.peaks.json").ok()?;
        let bytes = store.get(&key).ok()?;
        serde_json::from_slice::<Vec<f32>>(&bytes).ok()
    })
    .await
    .map_err(|e| ApiError::internal(format!("reading the waveform failed: {e}")))?;

    // An absent waveform is not an error: the recording predates them, or the
    // mix has not been produced yet. The player falls back to a plain bar.
    Ok(Json(peaks.unwrap_or_default()))
}

/// A transcript as plain text, for saving or pasting elsewhere.
async fn transcript_text(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let id = parse_id(&id)?;
    let (transcript, title) = with_db(&state, move |db| {
        let transcript = library::transcript(db, id)
            .map_err(ApiError::from_anyhow)?
            .ok_or_else(|| ApiError::not_found("this recording has not been transcribed"))?;
        let title = library::get(db, id)
            .map_err(ApiError::from_anyhow)?
            .map(|r| r.title)
            .unwrap_or_else(|| "Recording".into());
        Ok((transcript, title))
    })
    .await?;

    let body = library::plain_text(&transcript);
    let filename = sanitise_filename(&title);

    Ok((
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}.txt\""),
            ),
        ],
        body,
    )
        .into_response())
}

/// Makes a title safe to offer as a download filename.
///
/// A title is user-supplied and may contain quotes, slashes or newlines, any of
/// which would break the header or suggest a path.
fn sanitise_filename(title: &str) -> String {
    let cleaned: String = title
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == ' ' { c } else { '_' })
        .collect();
    let trimmed = cleaned.trim().trim_matches('_').trim();
    if trimmed.is_empty() {
        "recording".into()
    } else {
        trimmed.chars().take(80).collect()
    }
}

async fn get_settings(
    State(_state): State<AppState>,
) -> Result<Json<crate::config::RedactedSettings>, ApiError> {
    let settings = tokio::task::spawn_blocking(crate::config::Settings::load)
        .await
        .map_err(|e| ApiError::internal(format!("reading settings failed: {e}")))?
        .map_err(ApiError::from_anyhow)?;
    Ok(Json(settings.redacted()))
}

async fn put_settings(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(update): Json<crate::config::SettingsUpdate>,
) -> Result<Json<crate::config::RedactedSettings>, ApiError> {
    authorize(&state, &headers)?;

    // Read, modify and write on the blocking pool: this touches the filesystem,
    // and the file it writes must not be built from a stale copy.
    let db = Arc::clone(&state.db);
    let updated = tokio::task::spawn_blocking(move || -> Result<_, ApiError> {
        let mut settings = crate::config::Settings::load().map_err(ApiError::from_anyhow)?;
        let before = settings.clone();
        settings.apply(update);

        // Marked before the settings are written, which is the order that
        // survives a failure. The decision is made by comparing the old state
        // with the new, so saving first and failing here would leave the new
        // state on disk and the same save decided against ever repeating —
        // stranding recordings that should have been revisited, permanently.
        //
        // This way round, a failure leaves the settings untouched and the same
        // request can simply be made again. The worst case is marking work that
        // turns out to be unnecessary, which costs an upload that finds every
        // object already present and skips it.
        let revisit = affects_finished_recordings(&before, &settings);
        let mark = || -> Result<(), ApiError> {
            let guard = db
                .lock()
                .map_err(|_| ApiError::internal("database lock poisoned"))?;
            crate::derived::mark_uploaded_for_revisit(&guard)
                .map(|_| ())
                .map_err(ApiError::from_anyhow)
        };

        if revisit {
            mark()?;
        }

        settings.save().map_err(ApiError::from_anyhow)?;

        // Marked again, because the scheduler reads settings from disk on its
        // own schedule. Between the first mark and this save it could have
        // queued and finished an upload under the old rules, consuming the
        // flag and clearing it — leaving the new setting on disk with the
        // revisit already spent under settings that no longer apply.
        //
        // Cheap to repeat: the mark only sets a flag on recordings already
        // uploaded, so doing it twice costs one statement.
        if revisit {
            mark()?;
        }

        Ok(settings.redacted())
    })
    .await
    .map_err(|e| ApiError::internal(format!("saving settings failed: {e}")))??;

    Ok(Json(updated))
}

/// Whether a settings change alters what should already have happened, rather
/// than only what happens next.
///
/// Switching transcription off releases every recording that was holding its
/// audio waiting for a transcript. Asking to reclaim space says the same of
/// every recording already uploaded. Neither is revisited on its own, because a
/// finished backup clears the flag that brings a recording back around.
fn affects_finished_recordings(
    before: &crate::config::Settings,
    after: &crate::config::Settings,
) -> bool {
    // Only when the *combination* becomes actionable. Switching transcription
    // off on its own changes nothing about already-uploaded recordings, and
    // treating it as though it did would re-upload a whole library to discover
    // there was nothing to do.
    let cleanup_now_possible = |s: &crate::config::Settings| {
        s.remote_storage.delete_local_after_upload && !s.transcription.enabled
    };
    !cleanup_now_possible(before) && cleanup_now_possible(after)
}

#[derive(Debug, Serialize)]
struct StageQueued {
    stage: String,
    queued: bool,
}

/// Runs a pipeline stage for a recording, or queues it again after a failure.
///
/// The same endpoint serves "this was never done" and "this failed, try again":
/// both mean the same thing to the pipeline, and distinguishing them in the API
/// would only make the interface decide something it does not need to know.
async fn run_stage(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path((id, stage)): Path<(String, String)>,
) -> Result<(StatusCode, Json<StageQueued>), ApiError> {
    authorize(&state, &headers)?;
    let id = parse_id(&id)?;

    let job_type = match stage.as_str() {
        "transcribe" => kaseta_contracts::JobType::Transcribe,
        "summarize" => kaseta_contracts::JobType::Summarize,
        "backup" => kaseta_contracts::JobType::UploadRemote,
        "publish" => kaseta_contracts::JobType::PublishWebhook,
        "import" => kaseta_contracts::JobType::ImportMedia,
        other => return Err(ApiError::bad_request(format!("unknown stage: {other}"))),
    };

    let store = Arc::clone(&state.store);
    let queued = with_db(&state, move |db| {
        // A recording that no longer exists must not leave work queued against
        // it: the job would fail on every attempt with nothing to act on.
        let exists = library::get(db, id).map_err(ApiError::from_anyhow)?.is_some();
        if !exists {
            return Err(ApiError::not_found("no such recording"));
        }

        // Decoding again needs the upload, which lives in storage rather than
        // in anything the database can vouch for, and only a failed import is
        // waiting for another go: one still decoding already has its attempt.
        if job_type == kaseta_contracts::JobType::ImportMedia {
            if !crate::import::upload_present(&*store, id).map_err(ApiError::from_anyhow)? {
                return Err(ApiError::bad_request(crate::import::job::UPLOAD_GONE));
            }
            return db
                .reset_import_for_retry(id)
                .map_err(ApiError::from_anyhow)?
                .map(|_| true)
                .ok_or_else(|| ApiError::bad_request("only an import that failed can be retried"));
        }

        // Pressing a button is a different question from the chain reaching a
        // stage on its own. The chain skips quietly; a person who asked for
        // this deserves the reason rather than a job that reports success
        // having done nothing.
        let settings = crate::config::Settings::load().unwrap_or_default();
        if let Some(reason) =
            crate::scheduler::why_not_runnable(db, &settings, id, job_type)
                .map_err(ApiError::from_anyhow)?
        {
            return Err(ApiError::bad_request(reason));
        }

        // Any earlier attempt is cleared first, so asking again after a failure
        // actually re-runs rather than being refused as already-attempted. A
        // skip counts: it finished successfully having done nothing, and
        // without clearing it, fixing the setting that caused the skip would
        // leave the button permanently refusing to act.
        db.conn()
            .execute(
                "DELETE FROM jobs WHERE recording_id = ?1 AND job_type = ?2
                 AND (state IN ('failed_terminal','failed_retryable','canceled')
                      OR (state = 'succeeded' AND error_code = 'skipped'))",
                rusqlite::params![id.to_string(), job_type.as_str()],
            )
            .map_err(|e| ApiError::internal(e.to_string()))?;

        // Handing over is keyed on the transcript's content rather than on 1,
        // so that asking twice for the same text is one job and asking after a
        // re-transcription is a new one. A send that already succeeded is
        // cleared too: pressing the button is a request to send it, and
        // a receiver is expected to treat a repeat as the no-op it is.
        let revision = if job_type == kaseta_contracts::JobType::PublishWebhook {
            db.conn()
                .execute(
                    "DELETE FROM jobs WHERE recording_id = ?1 AND job_type = ?2",
                    rusqlite::params![id.to_string(), job_type.as_str()],
                )
                .map_err(|e| ApiError::internal(e.to_string()))?;
            crate::webhook::transcript_revision(db, id)
                .map_err(ApiError::from_anyhow)?
                .ok_or_else(|| ApiError::bad_request("there is no transcript to send yet"))?
        } else {
            1
        };

        db.enqueue_once(id, job_type, revision)
            .map_err(ApiError::from_anyhow)
            .map(|queued| queued.is_some())
    })
    .await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(StageQueued { stage, queued }),
    ))
}

async fn transcript(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<library::Transcript>, ApiError> {
    let id = parse_id(&id)?;
    with_db(&state, move |db| {
        library::transcript(db, id)
            .map_err(ApiError::from_anyhow)?
            .ok_or_else(|| ApiError::not_found("this recording has not been transcribed"))
    })
    .await
    .map(Json)
}

async fn summary(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<crate::summarize::Summary>, ApiError> {
    let id = parse_id(&id)?;
    with_db(&state, move |db| {
        crate::summarize::load(db, id)
            .map_err(ApiError::from_anyhow)?
            .ok_or_else(|| ApiError::not_found("this recording has not been summarised"))
    })
    .await
    .map(Json)
}

#[derive(Debug, Deserialize)]
struct SearchQuery {
    q: String,
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    items: Vec<library::LibraryItem>,
}

/// Finds recordings whose transcript contains the query.
async fn search(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<SearchQuery>,
) -> Result<Json<SearchResponse>, ApiError> {
    let items = with_db(&state, move |db| {
        let matching = library::search(db, &query.q).map_err(ApiError::from_anyhow)?;
        let all = library::list(db).map_err(ApiError::from_anyhow)?;
        Ok(all
            .into_iter()
            .filter(|item| matching.contains(&item.id))
            .collect::<Vec<_>>())
    })
    .await?;
    Ok(Json(SearchResponse { items }))
}

/// Serves an exported audio file, honouring range requests.
///
/// Range support is what makes seeking work in a media element: without it the
/// browser must fetch the whole file before it can play from the middle, and a
/// two-hour recording is hundreds of megabytes.
async fn audio(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, file)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let id = parse_id(&id)?;

    // The filename arrives from the URL, so it is validated as a single safe
    // segment before being used to build a storage key.
    if file.contains('/') || file.contains("..") || !file.ends_with(".flac") {
        return Err(ApiError::bad_request("not an audio file"));
    }

    let started_at = with_db(&state, move |db| {
        library::get(db, id)
            .map_err(ApiError::from_anyhow)?
            .map(|r| r.started_at)
            .ok_or_else(|| ApiError::not_found("no such recording"))
    })
    .await?;

    let prefix = kaseta_contracts::RecordingPrefix::new(id, started_at);
    let key = prefix
        .export(&file)
        .map_err(|_| ApiError::bad_request("not an audio file"))?;

    let mut response = serve_file(
        Arc::clone(&state.store),
        key,
        &headers,
        "that audio has not been exported",
    )
    .await?;
    let h = response.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static("audio/flac"));
    // Exports are immutable once written, so a browser may keep them.
    h.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=31536000, immutable"),
    );
    Ok(response)
}

#[derive(Debug, Deserialize)]
struct OriginalQuery {
    /// `1` to save the file rather than play it.
    #[serde(default)]
    download: Option<String>,
}

/// Serves an imported recording's kept original, honouring range requests,
/// so a video can be played and sought in the page.
///
/// Served as the type decided from its content when it was imported, never
/// from its extension, and offered under the name it had on the person's
/// machine.
async fn original(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<OriginalQuery>,
) -> Result<Response, ApiError> {
    let id = parse_id(&id)?;

    let (source, local) = with_db(&state, move |db| {
        use rusqlite::OptionalExtension;
        let row: Option<(Option<String>, bool)> = db
            .conn()
            .query_row(
                "SELECT source_json, original_local FROM recordings
                 WHERE id = ?1 AND deleted_at IS NULL",
                rusqlite::params![id.to_string()],
                |r| Ok((r.get(0)?, r.get::<_, i64>(1)? != 0)),
            )
            .optional()
            .map_err(|e| ApiError::internal(e.to_string()))?;
        let (source_json, local) = row.ok_or_else(|| ApiError::not_found("no such recording"))?;
        let source: ImportSource = source_json
            .and_then(|json| serde_json::from_str(&json).ok())
            .ok_or_else(|| ApiError::not_found("this recording was not imported from a file"))?;
        Ok((source, local))
    })
    .await?;

    let key = source
        .original_key
        .clone()
        .ok_or_else(|| ApiError::not_found("the original file was not kept"))?;
    if !local {
        return Err(ApiError::not_found(
            "the original file is in your bucket, not on this machine",
        ));
    }

    let mut response =
        serve_file(Arc::clone(&state.store), key, &headers, "the original file is missing").await?;
    let disposition = if query.download.as_deref() == Some("1") {
        "attachment"
    } else {
        "inline"
    };
    let h = response.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&source.content_type)
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    h.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&content_disposition(disposition, &source.original_filename))
            .map_err(|e| ApiError::internal(format!("naming the download: {e}")))?,
    );
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("private, no-cache"));
    Ok(response)
}

/// Streams one stored object, or the range of it a `Range` header asks for.
///
/// The bytes go from the file to the socket a block at a time. Reading the
/// range into memory first, as this once did, made a seek in a multi-gigabyte
/// video cost a buffer the size of the rest of the file.
async fn serve_file(
    store: Arc<dyn BlobStore>,
    key: BlobKey,
    headers: &HeaderMap,
    missing: &'static str,
) -> Result<Response, ApiError> {
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let (file, total) = tokio::task::spawn_blocking(move || {
        if !store.exists(&key).map_err(ApiError::from_anyhow)? {
            return Err(ApiError::not_found(missing));
        }
        let file = store.open_file(&key).map_err(ApiError::from_anyhow)?;
        let total = file
            .metadata()
            .map_err(|e| ApiError::internal(format!("reading {key}: {e}")))?
            .len();
        Ok((file, total))
    })
    .await
    .map_err(|e| ApiError::internal(format!("opening the file failed: {e}")))??;

    let (start, end) = match range.as_deref().and_then(|r| parse_range(r, total)) {
        Some(r) => r,
        // A syntactically valid but unsatisfiable range must say so rather
        // than silently returning the whole file.
        None if range.is_some() => return Err(ApiError::range_not_satisfiable(total)),
        None => (0, total.saturating_sub(1)),
    };
    let len = if total == 0 { 0 } else { end - start + 1 };

    let mut file = tokio::fs::File::from_std(file);
    if start > 0 {
        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::Start(start))
            .await
            .map_err(|e| ApiError::internal(format!("seeking: {e}")))?;
    }

    let status = if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let mut response = (status, Body::new(FileBody::new(file, len))).into_response();
    let h = response.headers_mut();
    h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    h.insert(header::CONTENT_LENGTH, HeaderValue::from(len));
    if range.is_some() {
        h.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {start}-{end}/{total}"))
                .expect("digits and punctuation are a valid header"),
        );
    }
    Ok(response)
}

/// A response body read from a file, `remaining` bytes from wherever the file
/// is positioned, one block at a time.
struct FileBody {
    file: tokio::fs::File,
    remaining: u64,
    buf: Box<[u8]>,
}

impl FileBody {
    /// Large enough that a video streams without a trip to the blocking pool
    /// per network packet; small enough that many open players cost little.
    const BLOCK_BYTES: usize = 64 * 1024;

    fn new(file: tokio::fs::File, len: u64) -> Self {
        Self {
            file,
            remaining: len,
            buf: vec![0; Self::BLOCK_BYTES].into_boxed_slice(),
        }
    }
}

impl http_body::Body for FileBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<std::result::Result<http_body::Frame<Bytes>, std::io::Error>>> {
        use tokio::io::AsyncRead;

        let this = self.get_mut();
        if this.remaining == 0 {
            return Poll::Ready(None);
        }
        let want = this.buf.len().min(usize::try_from(this.remaining).unwrap_or(usize::MAX));
        let mut read = tokio::io::ReadBuf::new(&mut this.buf[..want]);
        match Pin::new(&mut this.file).poll_read(cx, &mut read) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Ok(())) => {
                let filled = read.filled();
                if filled.is_empty() {
                    // The file is shorter than it was a moment ago. Ending
                    // quietly would hand the player a truncated body that
                    // claims to be complete.
                    return Poll::Ready(Some(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "the file ended before the range did",
                    ))));
                }
                this.remaining -= filled.len() as u64;
                Poll::Ready(Some(Ok(http_body::Frame::data(Bytes::copy_from_slice(filled)))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.remaining)
    }
}

/// A `Content-Disposition` naming `filename`, which may be anything a person
/// called a file.
///
/// Two forms, as RFC 6266 recommends: a plain `filename` with everything
/// outside printable ASCII (and the quote and backslash that would end or
/// escape it) replaced, for clients that know only that; and `filename*`
/// carrying the exact UTF-8 name, percent-encoded, which current browsers
/// prefer.
fn content_disposition(kind: &str, filename: &str) -> String {
    let fallback: String = filename
        .chars()
        .map(|c| match c {
            '"' | '\\' | '/' => '_',
            c if c == ' ' || c.is_ascii_graphic() => c,
            _ => '_',
        })
        .collect();
    let fallback = match fallback.trim() {
        "" => "original",
        trimmed => trimmed,
    };

    let mut encoded = String::new();
    for byte in filename.bytes() {
        // RFC 5987 `attr-char`: what may appear in the value unencoded.
        if byte.is_ascii_alphanumeric() || b"!#$&+-.^_`|~".contains(&byte) {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    format!("{kind}; filename=\"{fallback}\"; filename*=UTF-8''{encoded}")
}

#[derive(Debug, Serialize)]
struct ImportResponse {
    recording_id: Ulid,
}

/// Receives a file to import.
///
/// Everything that can be decided from the headers is decided before a byte
/// of the body is read: whether importing is possible at all, whether the
/// declared size is within the limit and fits on the disk, whether another
/// upload is already arriving. Finding any of those out after a
/// multi-gigabyte upload would waste the whole upload.
async fn import_file(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<ImportResponse>), ApiError> {
    authorize(&state, &headers)?;

    if let Err(reason) = state.imports.toolchain() {
        return Err(ApiError::with_status(StatusCode::SERVICE_UNAVAILABLE, reason));
    }

    let length = content_length(&headers)?;
    if length == 0 {
        return Err(ApiError::bad_request("the file is empty"));
    }
    let request = UploadRequest::from_headers(&headers).map_err(ApiError::bad_request)?;

    let limits = tokio::task::spawn_blocking(crate::config::Settings::load)
        .await
        .map_err(|e| ApiError::internal(format!("reading settings failed: {e}")))?
        .unwrap_or_default()
        .imports;
    if length > limits.max_upload_bytes() {
        return Err(ApiError::with_status(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "the file is {}; the largest file that can be imported is {} GB \
                 (imports.max_upload_gb in settings)",
                gigabytes(length),
                limits.max_upload_gb()
            ),
        ));
    }

    // Held until this handler ends, however it ends.
    let _permit = state.imports.begin_upload().ok_or_else(|| {
        ApiError::conflict("another file is being uploaded; import this one when it has finished")
    })?;

    let root = state
        .store
        .local_root()
        .ok_or_else(|| ApiError::internal("imports need a store on the local filesystem"))?;
    let free = state.imports.free_space(root).map_err(ApiError::from_anyhow)?;
    if !upload::room_for(length, free) {
        return Err(ApiError::with_status(
            StatusCode::INSUFFICIENT_STORAGE,
            format!(
                "not enough disk space: importing this file needs {} free, and {} is",
                gigabytes(upload::space_needed(length)),
                gigabytes(free)
            ),
        ));
    }

    let recording_id = upload::receive(
        Arc::clone(&state.store),
        Arc::clone(&state.db),
        request,
        length,
        body,
        upload::BODY_IDLE_TIMEOUT,
    )
    .await
    .map_err(|e| match e {
        UploadError::Request(message) => ApiError::bad_request(message),
        UploadError::Internal(e) => ApiError::from_anyhow(e),
    })?;

    Ok((StatusCode::ACCEPTED, Json(ImportResponse { recording_id })))
}

/// The declared body length, which an upload must state.
fn content_length(headers: &HeaderMap) -> Result<u64, ApiError> {
    let Some(value) = headers.get(header::CONTENT_LENGTH) else {
        return Err(ApiError::with_status(
            StatusCode::LENGTH_REQUIRED,
            "an import must state its size in Content-Length",
        ));
    };
    value
        .to_str()
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .ok_or_else(|| ApiError::bad_request("Content-Length is not a number"))
}

fn gigabytes(bytes: u64) -> String {
    format!("{:.1} GB", bytes as f64 / f64::from(1u32 << 30))
}

/// Parses a single-range `Range: bytes=…` header against a known total size.
///
/// Returns inclusive start and end. Multi-range requests are not supported and
/// yield `None`, which serves the whole file — a valid response.
fn parse_range(header: &str, total: u64) -> Option<(u64, u64)> {
    if total == 0 {
        return None;
    }
    let spec = header.strip_prefix("bytes=")?.trim();
    if spec.contains(',') {
        return None;
    }
    let (start, end) = spec.split_once('-')?;

    let (start, end) = match (start.trim(), end.trim()) {
        // `-N`: the final N bytes.
        ("", last) => {
            let n: u64 = last.parse().ok()?;
            (total.saturating_sub(n.min(total)), total - 1)
        }
        // `N-`: from N to the end.
        (first, "") => (first.parse().ok()?, total - 1),
        (first, last) => (first.parse().ok()?, last.parse::<u64>().ok()?.min(total - 1)),
    };

    (start <= end && start < total).then_some((start, end))
}

fn parse_id(raw: &str) -> Result<Ulid, ApiError> {
    Ulid::from_string(raw).map_err(|_| ApiError::bad_request("not a valid recording id"))
}

/// An error shaped for the interface rather than for a log.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
    /// The size to report in `Content-Range` with a 416, which a client needs
    /// to ask again for something that exists.
    unsatisfied_range_of: Option<u64>,
}

impl ApiError {
    fn with_status(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            unsatisfied_range_of: None,
        }
    }
    fn internal(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::INTERNAL_SERVER_ERROR, message)
    }
    fn bad_request(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::BAD_REQUEST, message)
    }
    fn not_found(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::NOT_FOUND, message)
    }
    fn conflict(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::CONFLICT, message)
    }
    fn forbidden(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::FORBIDDEN, message)
    }
    fn range_not_satisfiable(total: u64) -> Self {
        Self {
            unsatisfied_range_of: Some(total),
            ..Self::with_status(
                StatusCode::RANGE_NOT_SATISFIABLE,
                format!("the requested range lies outside the {total}-byte file"),
            )
        }
    }

    /// Distinguishes a caller asking for something that does not apply from a
    /// genuine fault. Reporting a disk-full or PipeWire failure as a conflict
    /// would tell the interface to say "already recording" when it is not.
    fn from_supervisor(e: anyhow::Error) -> Self {
        match e.downcast_ref::<crate::supervisor::SupervisorError>() {
            Some(known) => Self::conflict(known.to_string()),
            None => Self::internal(format!("{e:#}")),
        }
    }
    fn from_anyhow(e: anyhow::Error) -> Self {
        Self::internal(format!("{e:#}"))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        tracing::debug!(status = %self.status, message = %self.message, "request failed");
        let mut response =
            (self.status, Json(serde_json::json!({ "error": self.message }))).into_response();
        if let Some(total) = self.unsatisfied_range_of {
            if let Ok(value) = HeaderValue::from_str(&format!("bytes */{total}")) {
                response.headers_mut().insert(header::CONTENT_RANGE, value);
            }
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_audio_paths_that_escape_the_recording() {
        // The filename arrives from the URL, so traversal must be refused before
        // it reaches key construction.
        for bad in ["../../etc/passwd", "a/b.flac", "..%2Fx.flac", "notes.txt"] {
            let escaping = bad.contains('/') || bad.contains("..") || !bad.ends_with(".flac");
            assert!(escaping, "{bad} should be rejected");
        }
        assert!(
            !("mixed.flac".contains('/') || "mixed.flac".contains("..")),
            "a plain export name must be accepted"
        );
    }

    #[test]
    fn parses_the_range_forms_a_media_element_sends() {
        // Seeking sends an open-ended range from the target offset.
        assert_eq!(parse_range("bytes=0-", 1000), Some((0, 999)));
        assert_eq!(parse_range("bytes=500-", 1000), Some((500, 999)));
        assert_eq!(parse_range("bytes=0-499", 1000), Some((0, 499)));
        // A suffix range asks for the final N bytes.
        assert_eq!(parse_range("bytes=-100", 1000), Some((900, 999)));
    }

    #[test]
    fn clamps_a_range_that_runs_past_the_end() {
        assert_eq!(parse_range("bytes=900-5000", 1000), Some((900, 999)));
        assert_eq!(parse_range("bytes=-5000", 1000), Some((0, 999)));
    }

    #[test]
    fn rejects_ranges_that_cannot_be_served() {
        // Starting past the end is unsatisfiable, not "the whole file".
        assert_eq!(parse_range("bytes=1000-", 1000), None);
        assert_eq!(parse_range("bytes=800-700", 1000), None);
        assert_eq!(parse_range("bytes=abc-", 1000), None);
        assert_eq!(parse_range("items=0-10", 1000), None);
        assert_eq!(parse_range("bytes=0-10", 0), None);
    }

    #[test]
    fn multi_range_requests_fall_back_to_the_whole_file() {
        // Serving the entire body is a valid response to a multi-range request.
        assert_eq!(parse_range("bytes=0-10,20-30", 1000), None);
    }

    #[test]
    fn every_run_mints_a_different_token() {
        // A predictable token would defeat the point: a hostile page could
        // simply guess it.
        let a = mint_token().unwrap();
        let b = mint_token().unwrap();
        assert_ne!(a, b);
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    /// Sends one request with the given `Host` through the same guard the
    /// daemon's router uses, and reports the status it got back.
    async fn status_for_host(host: Option<&str>) -> StatusCode {
        use tower::ServiceExt;

        let routes = Router::new().route("/api/v1/token", get(|| async { "secret" }));
        let app = guard_host(routes, 7777);
        let mut request = axum::http::Request::builder().uri("/api/v1/token");
        if let Some(host) = host {
            request = request.header(header::HOST, host);
        }
        app.oneshot(request.body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn the_daemon_answers_to_its_own_loopback_names() {
        for host in ["127.0.0.1:7777", "localhost:7777", "[::1]:7777", "LocalHost:7777"] {
            assert_eq!(status_for_host(Some(host)).await, StatusCode::OK, "{host}");
        }
    }

    /// DNS rebinding: a hostile page whose name has been re-pointed at
    /// 127.0.0.1 is same-origin with itself, so the browser lets it read the
    /// reply, including the token. The `Host` it sends still names the hostile
    /// site, and that is what is refused.
    #[tokio::test]
    async fn any_other_host_is_refused() {
        for host in [
            Some("evil.example:7777"),
            Some("evil.example"),
            Some("127.0.0.1.evil.example:7777"),
            Some("localhost.evil.example:7777"),
            // The right name on the wrong port is a different service.
            Some("127.0.0.1:8080"),
            Some("localhost"),
            Some("127.0.0.1:7777.evil.example"),
            Some("127.0.0.2:7777"),
            Some("0.0.0.0:7777"),
            Some(""),
            None,
        ] {
            assert_eq!(
                status_for_host(host).await,
                StatusCode::MISDIRECTED_REQUEST,
                "{host:?} must be refused"
            );
        }
    }

    #[test]
    fn the_default_http_port_may_be_left_out_of_the_host() {
        // Browsers omit `:80`, so a daemon on port 80 sees a bare name.
        assert!(host_allowed(Some("localhost"), 80));
        assert!(host_allowed(Some("localhost:80"), 80));
        assert!(!host_allowed(Some("localhost"), 7777));
    }

    #[test]
    fn the_page_carries_a_placeholder_for_the_token() {
        assert!(
            INDEX_HTML.contains("__KASETA_TOKEN__"),
            "the page must receive a token, or every mutation will be rejected"
        );
    }

    #[test]
    fn a_download_filename_cannot_break_the_header_or_suggest_a_path() {
        // Titles are user-supplied; a quote would terminate the header early
        // and a slash would read as a directory.
        assert_eq!(sanitise_filename("Weekly sync"), "Weekly sync");
        assert!(!sanitise_filename("../../etc/passwd").contains('/'));
        assert!(!sanitise_filename("say \"hello\"").contains('"'));
        assert!(!sanitise_filename("line\nbreak").contains('\n'));
    }

    #[test]
    fn an_unnameable_recording_still_downloads() {
        assert_eq!(sanitise_filename(""), "recording");
        assert_eq!(sanitise_filename("///"), "recording");
        assert_eq!(sanitise_filename("   "), "recording");
    }

    #[test]
    fn a_very_long_title_is_shortened() {
        let long = "a".repeat(500);
        assert!(sanitise_filename(&long).len() <= 80);
    }

    #[test]
    fn rejects_ids_that_are_not_ulids() {
        assert!(parse_id("not-a-ulid").is_err());
        assert!(parse_id(&Ulid::nil().to_string()).is_ok());
    }

    #[test]
    fn errors_serialise_as_a_message_the_interface_can_show() {
        let response = ApiError::conflict("a recording is already running").into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn the_interface_is_compiled_into_the_binary() {
        assert!(INDEX_HTML.contains("<!doctype html>") || INDEX_HTML.contains("<html"));
        assert!(
            INDEX_HTML.contains("/api/v1/"),
            "the page must talk to the API it is served by"
        );
    }

    #[test]
    fn the_page_sends_what_the_import_route_reads() {
        // The page sends a file with a hand-built request rather than through
        // its JSON helper, so nothing else ties the header names it writes to
        // the ones the route reads. A mismatch would refuse every import from
        // the window while the command line went on working.
        use crate::import::upload::{FILENAME_HEADER, KEEP_ORIGINAL_HEADER, TITLE_HEADER};
        for header in [FILENAME_HEADER, TITLE_HEADER, KEEP_ORIGINAL_HEADER, TOKEN_HEADER] {
            assert!(
                INDEX_HTML.contains(&format!("\"{header}\"")),
                "the page never sends {header}"
            );
        }
        assert!(INDEX_HTML.contains("\"/api/v1/imports\""), "the page never posts an import");
    }

    #[test]
    fn the_page_retries_an_import_through_a_stage_the_daemon_knows() {
        // The Import chip's state is found by job name and its retry is
        // posted by stage name; either drifting leaves a failed import with
        // no chip, or a retry the daemon refuses as an unknown stage.
        assert_eq!(kaseta_contracts::JobType::ImportMedia.as_str(), "import_media");
        assert!(INDEX_HTML.contains("import: \"import_media\""));
    }

    #[test]
    fn the_page_offers_delete_when_the_upload_is_gone() {
        // Retrying decodes the upload again, so once it is gone the only
        // useful action is deleting the item. The page recognises that case
        // by the start of the daemon's message.
        const PHRASE: &str = "the uploaded file is gone";
        assert!(crate::import::job::UPLOAD_GONE.starts_with(PHRASE));
        assert!(INDEX_HTML.contains(&format!("\"{PHRASE}\"")));
    }

    // The routes, driven through the same router the daemon serves, with no
    // socket. The supervisor is real; nothing here starts a recording, so it
    // never touches an audio device.

    use crate::import::sandbox::{HostLayout, Toolchain};
    use crate::import::upload::{FILENAME_HEADER, KEEP_ORIGINAL_HEADER, TITLE_HEADER};
    use tower::ServiceExt;

    const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    struct App {
        _dir: tempfile::TempDir,
        store: Arc<dyn BlobStore>,
        db: Arc<Mutex<Db>>,
        imports: Arc<ImportRuntime>,
        router: Router,
    }

    /// A runtime that would import: tools named, never run by these tests,
    /// and a disk with room to spare.
    fn ready() -> ImportRuntime {
        ImportRuntime::with(Toolchain::at(
            "/usr/bin/bwrap".into(),
            "/usr/bin/ffmpeg".into(),
            "/usr/bin/ffprobe".into(),
            HostLayout::of_host(),
        ))
        .with_free_space(|_| Ok(1 << 40))
    }

    fn app(imports: ImportRuntime) -> App {
        let dir = tempfile::TempDir::new().unwrap();
        let store: Arc<dyn BlobStore> =
            Arc::new(crate::blobstore::LocalFsStore::new(dir.path()).unwrap());
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        let supervisor =
            Arc::new(Supervisor::spawn(Arc::clone(&store), Arc::clone(&db)).unwrap());
        let imports = Arc::new(imports);
        let router = router(
            supervisor,
            Arc::clone(&store),
            Arc::clone(&db),
            Arc::clone(&imports),
            Arc::new(TOKEN.to_string()),
            7777,
        );
        App {
            _dir: dir,
            store,
            db,
            imports,
            router,
        }
    }

    impl App {
        async fn send(&self, request: axum::http::Request<Body>) -> (StatusCode, HeaderMap, Vec<u8>) {
            let response = self.router.clone().oneshot(request).await.unwrap();
            let status = response.status();
            let headers = response.headers().clone();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec();
            (status, headers, body)
        }

        async fn get(&self, uri: &str, range: Option<&str>) -> (StatusCode, HeaderMap, Vec<u8>) {
            let mut request = axum::http::Request::get(uri).header(header::HOST, "127.0.0.1:7777");
            if let Some(range) = range {
                request = request.header(header::RANGE, range);
            }
            self.send(request.body(Body::empty()).unwrap()).await
        }

        /// Posts `body` as an import. `length` is what the request claims,
        /// which a test may make disagree with the body.
        async fn import(&self, length: Option<u64>, body: Vec<u8>) -> (StatusCode, serde_json::Value) {
            let mut request = axum::http::Request::post("/api/v1/imports")
                .header(header::HOST, "127.0.0.1:7777")
                .header(TOKEN_HEADER, TOKEN)
                .header(FILENAME_HEADER, "Lecture%203%20%C3%9Cbung.mp4")
                .header(TITLE_HEADER, "Week%203")
                .header(KEEP_ORIGINAL_HEADER, "1");
            if let Some(length) = length {
                request = request.header(header::CONTENT_LENGTH, length);
            }
            let (status, _, body) = self.send(request.body(Body::from(body)).unwrap()).await;
            (status, serde_json::from_slice(&body).unwrap_or_default())
        }

        fn count(&self, sql: &str) -> i64 {
            self.db.lock().unwrap().conn().query_row(sql, [], |r| r.get(0)).unwrap()
        }

        fn staging_is_empty(&self) -> bool {
            let root = self.store.local_root().unwrap().join("imports");
            std::fs::read_dir(root).map_or(true, |mut d| d.next().is_none())
        }
    }

    #[tokio::test]
    async fn an_upload_becomes_an_import_waiting_to_be_decoded() {
        let app = app(ready());
        // Larger than the router's default body limit, which this route lifts.
        let body: Vec<u8> = (0..3 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let (status, json) = app.import(Some(body.len() as u64), body.clone()).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{json}");
        let id = Ulid::from_string(json["recording_id"].as_str().unwrap()).unwrap();

        let intent = crate::import::intent::Intent::read(&*app.store, id).unwrap().unwrap();
        assert_eq!(intent.original_filename, "Lecture 3 \u{dc}bung.mp4");
        assert_eq!(intent.title, "Week 3");
        assert!(intent.keep_original);
        assert_eq!(intent.bytes, Some(body.len() as u64));
        let staged = app.store.local_root().unwrap().join(intent.upload_key().unwrap().as_str());
        assert_eq!(std::fs::read(staged).unwrap(), body);

        let item = {
            let db = app.db.lock().unwrap();
            library::get(&db, id).unwrap().unwrap()
        };
        assert_eq!(item.status, "processing");
        assert_eq!(item.origin, kaseta_contracts::Origin::Imported);
        assert_eq!(item.title, "Week 3");
        assert_eq!(item.stages[0].stage, "import_media");
        assert_eq!(item.stages[0].state, "queued");
    }

    #[tokio::test]
    async fn an_upload_without_the_token_is_refused() {
        let app = app(ready());
        let request = axum::http::Request::post("/api/v1/imports")
            .header(header::HOST, "127.0.0.1:7777")
            .header(header::CONTENT_LENGTH, 3)
            .header(FILENAME_HEADER, "a.mp3")
            .body(Body::from("abc"))
            .unwrap();
        let (status, _, _) = app.send(request).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(app.staging_is_empty());
        assert_eq!(app.count("SELECT COUNT(*) FROM recordings"), 0);
    }

    /// Without the tools or the sandbox, importing is refused before the
    /// upload, and the status says why so the page can too.
    #[tokio::test]
    async fn importing_without_the_tools_is_unavailable_and_says_why() {
        let reason = "Importing needs bubblewrap: sudo pacman -S --needed bubblewrap";
        let app = app(ImportRuntime::unavailable(reason));

        let (status, json) = app.import(Some(3), b"abc".to_vec()).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["error"], reason);
        assert!(app.staging_is_empty());

        let (_, _, body) = app.get("/api/v1/status", None).await;
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["import"]["available"], false);
        assert_eq!(status["import"]["reason"], reason);
        assert_eq!(status["state"], "idle", "the daemon's own status is still there");
    }

    #[tokio::test]
    async fn the_status_says_importing_is_available() {
        let app = app(ready());
        let (_, _, body) = app.get("/api/v1/status", None).await;
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["import"]["available"], true);
        assert!(status["import"].get("reason").is_none());
    }

    #[tokio::test]
    async fn an_upload_must_state_its_size() {
        let app = app(ready());
        let (status, json) = app.import(None, b"abc".to_vec()).await;
        assert_eq!(status, StatusCode::LENGTH_REQUIRED, "{json}");
        let (status, _) = app.import(Some(0), Vec::new()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "an empty file is nothing to import");
        assert!(app.staging_is_empty());
    }

    /// Refused on the declared size, before a byte is read. Past the highest
    /// limit that can be configured, so the machine's own settings cannot
    /// change the answer.
    #[tokio::test]
    async fn an_upload_over_the_limit_is_refused_before_it_is_read() {
        let app = app(ready());
        let too_big = (u64::from(crate::config::ImportSettings::MAX_UPLOAD_GB_CEILING) << 30) + 1;
        let (status, json) = app.import(Some(too_big), Vec::new()).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{json}");
        assert!(json["error"].as_str().unwrap().contains("imports.max_upload_gb"));
        assert!(app.staging_is_empty());
    }

    #[tokio::test]
    async fn an_upload_the_disk_cannot_hold_is_refused_before_it_is_read() {
        let app = app(ready().with_free_space(|_| Ok(1 << 30)));
        let (status, json) = app.import(Some(3), b"abc".to_vec()).await;
        assert_eq!(status, StatusCode::INSUFFICIENT_STORAGE, "{json}");
        assert!(json["error"].as_str().unwrap().contains("not enough disk space"));
        assert!(app.staging_is_empty());
    }

    #[tokio::test]
    async fn a_second_upload_waits_for_the_first() {
        let app = app(ready());
        let first = app.imports.begin_upload().unwrap();

        let (status, json) = app.import(Some(3), b"abc".to_vec()).await;
        assert_eq!(status, StatusCode::CONFLICT, "{json}");
        assert!(app.staging_is_empty());

        drop(first);
        let (status, _) = app.import(Some(3), b"abc".to_vec()).await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }

    /// A body that disagrees with its declared size leaves no staging and no
    /// recording, and frees the slot for the next upload.
    #[tokio::test]
    async fn a_body_that_disagrees_with_its_size_leaves_nothing_and_frees_the_slot() {
        let app = app(ready());
        for (declared, sent) in [(10u64, &b"abc"[..]), (2, &b"abc"[..])] {
            let (status, json) = app.import(Some(declared), sent.to_vec()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
            assert!(app.staging_is_empty());
            assert_eq!(app.count("SELECT COUNT(*) FROM recordings"), 0);
        }
        let (status, _) = app.import(Some(3), b"abc".to_vec()).await;
        assert_eq!(status, StatusCode::ACCEPTED, "the slot was released");
        assert_eq!(app.count("SELECT COUNT(*) FROM jobs WHERE job_type = 'import_media'"), 1);
    }

    #[tokio::test]
    async fn unreadable_metadata_is_refused() {
        let app = app(ready());
        let request = axum::http::Request::post("/api/v1/imports")
            .header(header::HOST, "127.0.0.1:7777")
            .header(TOKEN_HEADER, TOKEN)
            .header(header::CONTENT_LENGTH, 3)
            .header(FILENAME_HEADER, "%FF%FE.mp4")
            .body(Body::from("abc"))
            .unwrap();
        let (status, _, body) = app.send(request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(String::from_utf8_lossy(&body).contains("not readable"));
        assert!(app.staging_is_empty());
    }

    /// The bytes of a kept original, distinguishable at every offset.
    fn original_bytes() -> Vec<u8> {
        (0..1000u32).map(|i| (i % 256) as u8).collect()
    }

    /// An imported recording, finished, whose original was kept (and is
    /// still here when `local`).
    fn imported(app: &App, keep: bool, local: bool) -> Ulid {
        let id = Ulid::new();
        let started_at = time::macros::datetime!(2026-10-09 08:00:00 UTC);
        let prefix = kaseta_contracts::RecordingPrefix::new(id, started_at);
        let key = prefix.original("mp4").unwrap();
        if keep && local {
            app.store.put(&key, &original_bytes()).unwrap();
        }
        let source = ImportSource {
            original_filename: "Vortrag \u{dc}ber \"Rust\".mp4".into(),
            original_key: keep.then_some(key),
            original_bytes: 1000,
            original_sha256: "ab".repeat(32),
            container: "mov".into(),
            codec: "aac".into(),
            content_type: "video/mp4".into(),
            media_kind: kaseta_contracts::MediaType::Video,
            media_created_at: None,
            imported_at: started_at,
            duration_s: 3.0,
        };
        let db = app.db.lock().unwrap();
        db.create_import(&crate::db::NewImport {
            recording_id: id,
            started_at,
            title: "Vortrag".into(),
        })
        .unwrap();
        db.conn()
            .execute(
                "UPDATE recordings SET status = 'ready', source_json = ?2, original_local = ?3
                 WHERE id = ?1",
                rusqlite::params![
                    id.to_string(),
                    serde_json::to_string(&source).unwrap(),
                    (keep && local) as i64
                ],
            )
            .unwrap();
        id
    }

    #[tokio::test]
    async fn the_original_is_served_whole_with_its_type() {
        let app = app(ready());
        let id = imported(&app, true, true);
        let (status, headers, body) = app.get(&format!("/api/v1/recordings/{id}/original"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, original_bytes());
        assert_eq!(headers[header::CONTENT_TYPE], "video/mp4");
        assert_eq!(headers[header::ACCEPT_RANGES], "bytes");
        assert_eq!(headers[header::CONTENT_LENGTH], "1000");
        assert!(headers[header::CONTENT_DISPOSITION].to_str().unwrap().starts_with("inline;"));
    }

    /// Every range form a media element sends when seeking.
    #[tokio::test]
    async fn the_original_is_served_in_ranges() {
        let app = app(ready());
        let id = imported(&app, true, true);
        let uri = format!("/api/v1/recordings/{id}/original");
        let all = original_bytes();

        for (range, start, end) in [
            ("bytes=100-199", 100usize, 199usize),
            ("bytes=-50", 950, 999),
            ("bytes=990-", 990, 999),
            ("bytes=0-", 0, 999),
            ("bytes=900-5000", 900, 999),
        ] {
            let (status, headers, body) = app.get(&uri, Some(range)).await;
            assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{range}");
            assert_eq!(body, &all[start..=end], "{range}");
            assert_eq!(
                headers[header::CONTENT_RANGE],
                format!("bytes {start}-{end}/1000").as_str(),
                "{range}"
            );
            assert_eq!(headers[header::CONTENT_LENGTH], (end - start + 1).to_string().as_str());
        }

        let (status, headers, _) = app.get(&uri, Some("bytes=1000-")).await;
        assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(headers[header::CONTENT_RANGE], "bytes */1000");
    }

    /// Saved under the name it had, exactly, with a plain fallback for
    /// clients that only read that.
    #[tokio::test]
    async fn the_original_downloads_under_its_own_name() {
        let app = app(ready());
        let id = imported(&app, true, true);
        let (status, headers, body) =
            app.get(&format!("/api/v1/recordings/{id}/original?download=1"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.len(), 1000);
        assert_eq!(
            headers[header::CONTENT_DISPOSITION],
            "attachment; filename=\"Vortrag _ber _Rust_.mp4\"; \
             filename*=UTF-8''Vortrag%20%C3%9Cber%20%22Rust%22.mp4"
        );
    }

    #[tokio::test]
    async fn an_original_that_is_not_here_is_not_found() {
        let app = app(ready());
        for (keep, local, expected) in [
            (true, false, "in your bucket"),
            (false, false, "was not kept"),
        ] {
            let id = imported(&app, keep, local);
            let (status, _, body) = app.get(&format!("/api/v1/recordings/{id}/original"), None).await;
            assert_eq!(status, StatusCode::NOT_FOUND);
            assert!(String::from_utf8_lossy(&body).contains(expected), "{expected}");
        }

        // A captured recording has no original at all.
        let captured = Ulid::new();
        app.db
            .lock()
            .unwrap()
            .conn()
            .execute(
                "INSERT INTO recordings (id, owner_id, status, started_at, manifest_version)
                 VALUES (?1, ?2, 'ready', 0, 'v')",
                rusqlite::params![captured.to_string(), crate::db::LOCAL_OWNER_ID],
            )
            .unwrap();
        let (status, _, body) =
            app.get(&format!("/api/v1/recordings/{captured}/original"), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(String::from_utf8_lossy(&body).contains("not imported"));
    }

    /// The exported audio goes through the same streaming ranges.
    #[tokio::test]
    async fn exported_audio_is_streamed_in_ranges() {
        let app = app(ready());
        let id = imported(&app, false, false);
        let prefix = kaseta_contracts::RecordingPrefix::new(
            id,
            time::macros::datetime!(2026-10-09 08:00:00 UTC),
        );
        app.store
            .put(&prefix.export("mixed.flac").unwrap(), &original_bytes())
            .unwrap();
        let uri = format!("/api/v1/recordings/{id}/audio/mixed.flac");

        let (status, headers, body) = app.get(&uri, Some("bytes=10-19")).await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(body, &original_bytes()[10..20]);
        assert_eq!(headers[header::CONTENT_TYPE], "audio/flac");

        let (status, _, body) = app.get(&uri, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.len(), 1000);

        let (status, _, _) =
            app.get(&format!("/api/v1/recordings/{id}/audio/a_local-mic_01.flac"), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn a_plain_name_needs_no_encoding_but_gets_both_forms() {
        assert_eq!(
            content_disposition("inline", "talk.mp4"),
            "inline; filename=\"talk.mp4\"; filename*=UTF-8''talk.mp4"
        );
        // Nothing usable in ASCII still names something.
        assert!(content_disposition("attachment", "\u{1f3a4}")
            .starts_with("attachment; filename=\"_\"; filename*=UTF-8''%F0%9F%8E%A4"));
        assert!(content_disposition("attachment", "a\\b/c")
            .starts_with("attachment; filename=\"a_b_c\""));
    }

    /// The transcript as text names each line the way the page does.
    #[tokio::test]
    async fn the_text_transcript_uses_the_labels() {
        let app = app(ready());
        let id = imported(&app, false, false);
        {
            let db = app.db.lock().unwrap();
            let transcript_id = Ulid::new().to_string();
            db.conn()
                .execute(
                    "INSERT INTO transcripts (id, recording_id, revision, engine_name, engine_model,
                                              language)
                     VALUES (?1, ?2, 1, 'parakeet', 'tdt-0.6b', 'en')",
                    rusqlite::params![transcript_id, id.to_string()],
                )
                .unwrap();
            db.conn()
                .execute(
                    "INSERT INTO transcript_segments
                         (id, transcript_id, track_id, seq, start_boottime_ns, end_boottime_ns,
                          speaker_hint, text)
                     VALUES (?1, ?2, 'a_imported_01', 0, 0, 1, 'unknown', 'Welcome.')",
                    rusqlite::params![Ulid::new().to_string(), transcript_id],
                )
                .unwrap();
        }
        let (status, _, body) =
            app.get(&format!("/api/v1/recordings/{id}/transcript.txt"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(String::from_utf8(body).unwrap(), "[00:00] Speaker: Welcome.");

        let (_, _, body) = app.get(&format!("/api/v1/recordings/{id}/transcript"), None).await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["lines"][0]["label"], "Speaker");
        assert_eq!(json["lines"][0]["speaker"], "unknown");
    }
}

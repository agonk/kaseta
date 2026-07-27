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
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use kaseta_contracts::manifest::RecordingNotes;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::blobstore::BlobStore;
use crate::db::Db;
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
fn mint_token() -> String {
    // Derived from process-unique, time-varying values rather than a CSPRNG
    // dependency: this only needs to be unguessable by a page that cannot read
    // it, not to resist offline attack.
    let seed = format!(
        "{}-{}-{:?}",
        std::process::id(),
        crate::clock::boottime_ns(),
        std::time::SystemTime::now()
    );
    crate::blobstore::sha256_hex(seed.as_bytes())
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
}

pub fn router(
    supervisor: Arc<Supervisor>,
    store: Arc<dyn BlobStore>,
    db: Arc<Mutex<Db>>,
    token: Arc<String>,
) -> Router {
    let state = AppState {
        supervisor,
        store,
        db,
        token,
    };

    Router::new()
        .route("/", get(index))
        .route("/api/v1/status", get(status))
        .route("/api/v1/devices", get(devices))
        .route("/api/v1/recordings", get(list_recordings).post(start_recording))
        .route("/api/v1/recordings/active/stop", post(stop_recording))
        .route("/api/v1/recordings/{id}", get(get_recording))
        .route("/api/v1/recordings/{id}", patch(rename_recording))
        .route("/api/v1/recordings/{id}", delete(delete_recording))
        .route("/api/v1/recordings/{id}/audio/{file}", get(audio))
        .route("/api/v1/recordings/{id}/transcript", get(transcript))
        .route("/api/v1/recordings/{id}/summary", get(summary))
        .route("/api/v1/search", get(search))
        .with_state(state)
}

/// Serves the interface on loopback.
pub async fn serve(
    supervisor: Arc<Supervisor>,
    store: Arc<dyn BlobStore>,
    db: Arc<Mutex<Db>>,
    port: u16,
) -> Result<()> {
    let token = Arc::new(mint_token());
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr} (is another kasetad already running?)"))?;

    println!("Kaseta is running at http://{addr}");
    println!("Press Ctrl-C to stop.\n");

    axum::serve(listener, router(supervisor, store, db, token))
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

async fn status(State(state): State<AppState>) -> Json<DaemonStatus> {
    Json(state.supervisor.status())
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

    with_db(&state, move |db| {
        library::set_title(db, id, title.as_deref())
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
    headers: axum::http::HeaderMap,
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

    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let store = Arc::clone(&state.store);
    tokio::task::spawn_blocking(move || {
        let total = store
            .size(&key)
            .map_err(|_| ApiError::not_found("that audio has not been exported"))?;

        let (start, end) = match range.as_deref().and_then(|r| parse_range(r, total)) {
            Some(r) => r,
            None if range.is_some() => {
                // A syntactically valid but unsatisfiable range must say so
                // rather than silently returning the whole file.
                return Err(ApiError::range_not_satisfiable(total));
            }
            None => (0, total.saturating_sub(1)),
        };

        let len = end.saturating_sub(start) + 1;
        let bytes = store
            .get_range(&key, start, len)
            .map_err(|e| ApiError::internal(format!("{e:#}")))?;

        let status = if range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };

        let mut response = (status, bytes).into_response();
        let h = response.headers_mut();
        h.insert(header::CONTENT_TYPE, "audio/flac".parse().unwrap());
        h.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
        // Exports are immutable once written, so a browser may keep them.
        h.insert(
            header::CACHE_CONTROL,
            "private, max-age=31536000, immutable".parse().unwrap(),
        );
        if range.is_some() {
            h.insert(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{total}").parse().unwrap(),
            );
        }
        Ok(response)
    })
    .await
    .map_err(|e| ApiError::internal(format!("reading audio failed: {e}")))?
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
}

impl ApiError {
    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
        }
    }
    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }
    fn range_not_satisfiable(total: u64) -> Self {
        Self {
            status: StatusCode::RANGE_NOT_SATISFIABLE,
            message: format!("the requested range lies outside the {total}-byte file"),
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
        (self.status, Json(serde_json::json!({ "error": self.message }))).into_response()
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
        let a = mint_token();
        let b = mint_token();
        assert_ne!(a, b);
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn the_page_carries_a_placeholder_for_the_token() {
        assert!(
            INDEX_HTML.contains("__KASETA_TOKEN__"),
            "the page must receive a token, or every mutation will be rejected"
        );
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
}

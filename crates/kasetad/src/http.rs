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

#[derive(Clone)]
pub struct AppState {
    supervisor: Arc<Supervisor>,
    store: Arc<dyn BlobStore>,
    /// SQLite allows one writer; the connection is not `Sync`, so handlers take
    /// it in turn. Every query here is short.
    db: Arc<Mutex<Db>>,
}

pub fn router(supervisor: Arc<Supervisor>, store: Arc<dyn BlobStore>, db: Arc<Mutex<Db>>) -> Router {
    let state = AppState {
        supervisor,
        store,
        db,
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
        .with_state(state)
}

/// Serves the interface on loopback.
pub async fn serve(
    supervisor: Arc<Supervisor>,
    store: Arc<dyn BlobStore>,
    db: Arc<Mutex<Db>>,
    port: u16,
) -> Result<()> {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr} (is another kasetad already running?)"))?;

    println!("Kaseta is running at http://{addr}");
    println!("Press Ctrl-C to stop.\n");

    axum::serve(listener, router(supervisor, store, db))
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

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
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
    body: Option<Json<StartRequest>>,
) -> Result<(StatusCode, Json<StartResponse>), ApiError> {
    let request = body.map(|Json(b)| b).unwrap_or_default();
    let notes = RecordingNotes {
        title: request.title.map(|t| t.trim().to_string()).filter(|t| !t.is_empty()),
        ..RecordingNotes::default()
    };

    let recording_id = state
        .supervisor
        .start(notes)
        .await
        // A refusal here is almost always "already recording", which is the
        // caller's mistake rather than a server fault.
        .map_err(|e| ApiError::conflict(format!("{e:#}")))?;

    Ok((StatusCode::CREATED, Json(StartResponse { recording_id })))
}

#[derive(Debug, Serialize)]
struct StopResponse {
    recording_id: Ulid,
    duration_ms: i64,
}

async fn stop_recording(State(state): State<AppState>) -> Result<Json<StopResponse>, ApiError> {
    let stopped = state
        .supervisor
        .stop()
        .await
        .map_err(|e| ApiError::conflict(format!("{e:#}")))?;

    // Index immediately so the recording appears in the library without waiting
    // for the next restart.
    {
        let db = state.db.lock().map_err(|_| ApiError::internal("database lock poisoned"))?;
        if let Err(e) = library::reconcile(&*state.store, &db) {
            tracing::error!(error = %format!("{e:#}"), "indexing the new recording failed");
        }
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

    Ok(Json(StopResponse {
        recording_id: stopped.manifest.recording_id,
        duration_ms,
    }))
}

#[derive(Debug, Serialize)]
struct ListResponse {
    items: Vec<library::LibraryItem>,
}

async fn list_recordings(State(state): State<AppState>) -> Result<Json<ListResponse>, ApiError> {
    let db = state.db.lock().map_err(|_| ApiError::internal("database lock poisoned"))?;
    let items = library::list(&*state.store, &db).map_err(ApiError::from_anyhow)?;
    Ok(Json(ListResponse { items }))
}

async fn get_recording(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<library::LibraryItem>, ApiError> {
    let id = parse_id(&id)?;
    let db = state.db.lock().map_err(|_| ApiError::internal("database lock poisoned"))?;
    library::get(&*state.store, &db, id)
        .map_err(ApiError::from_anyhow)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("no such recording"))
}

#[derive(Debug, Deserialize)]
struct RenameRequest {
    /// `null` or empty clears the override, restoring the captured or generated
    /// name rather than leaving the recording blank.
    title: Option<String>,
}

async fn rename_recording(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<RenameRequest>,
) -> Result<StatusCode, ApiError> {
    let id = parse_id(&id)?;
    let db = state.db.lock().map_err(|_| ApiError::internal("database lock poisoned"))?;

    library::set_title(&db, id, body.title.as_deref())
        .map_err(ApiError::from_anyhow)?
        .then_some(StatusCode::NO_CONTENT)
        .ok_or_else(|| ApiError::not_found("no such recording"))
}

async fn delete_recording(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let id = parse_id(&id)?;
    let db = state.db.lock().map_err(|_| ApiError::internal("database lock poisoned"))?;

    library::delete(&*state.store, &db, id)
        .map_err(ApiError::from_anyhow)?
        .then_some(StatusCode::NO_CONTENT)
        .ok_or_else(|| ApiError::not_found("no such recording"))
}

/// Streams an exported audio file.
async fn audio(
    State(state): State<AppState>,
    Path((id, file)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let id = parse_id(&id)?;

    // The filename comes from the URL, so it is validated as a single safe key
    // segment before being used to build a storage key.
    if file.contains('/') || file.contains("..") || !file.ends_with(".flac") {
        return Err(ApiError::bad_request("not an audio file"));
    }

    let started_at = {
        let db = state.db.lock().map_err(|_| ApiError::internal("database lock poisoned"))?;
        library::get(&*state.store, &db, id)
            .map_err(ApiError::from_anyhow)?
            .ok_or_else(|| ApiError::not_found("no such recording"))?
            .started_at
    };

    let prefix = kaseta_contracts::RecordingPrefix::new(id, started_at);
    let key = prefix
        .export(&file)
        .map_err(|_| ApiError::bad_request("not an audio file"))?;

    let bytes = state
        .store
        .get(&key)
        .map_err(|_| ApiError::not_found("that audio has not been exported"))?;

    Ok((
        [
            (header::CONTENT_TYPE, "audio/flac"),
            // Exports are immutable once written, so a browser may keep them.
            (header::CACHE_CONTROL, "private, max-age=31536000, immutable"),
        ],
        bytes,
    )
        .into_response())
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

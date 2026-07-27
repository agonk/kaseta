use std::sync::Arc;

use anyhow::{Context, Result};

mod blobstore;
mod capture;
mod config;
mod clock;
mod db;
mod derived;
mod export;
mod http;
mod library;
mod remote;
mod retention;
mod scheduler;
mod summarize;
mod supervisor;
mod transcribe;

const USAGE: &str = "\
kasetad — Kaseta recording daemon

USAGE:
    kasetad <COMMAND>

COMMANDS:
    devices              List recordable audio devices and exit
    doctor               Check that this machine can capture audio
    record [SECONDS]     Record both sides of a meeting (default 30s)
    serve [PORT]         Run the daemon and its interface (default 7777)
    export [ID]          Merge a recording's chunks into one file per track
                         (defaults to the most recent recording)
    transcribe [ID]      Transcribe a recording (defaults to the most recent)
    help                 Show this message

ENVIRONMENT:
    KASETA_WORKER_PYTHON  Interpreter with the transcription worker installed
    KASETA_LOG   Log filter, e.g. `kasetad=debug`
    KASETA_DATA  Where recordings are written (default ./data)
    KASETA_PORT  Port for `serve` (default 7777)
";

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("KASETA_LOG")
                .unwrap_or_else(|_| "kasetad=info".into()),
        )
        .with_target(false)
        .init();

    match std::env::args().nth(1).as_deref() {
        Some("devices") => cmd_devices(),
        Some("doctor") => cmd_doctor(),
        Some("export") => cmd_export(std::env::args().nth(2)),
        Some("transcribe") => cmd_transcribe(std::env::args().nth(2)),
        Some("serve") => {
            let port = std::env::args()
                .nth(2)
                .or_else(|| std::env::var("KASETA_PORT").ok())
                .map(|p| p.parse::<u16>())
                .transpose()
                .context("PORT must be a number between 1 and 65535")?
                .unwrap_or(7777);
            cmd_serve(port)
        }
        Some("record") => {
            let seconds = std::env::args()
                .nth(2)
                .map(|s| s.parse::<u64>())
                .transpose()
                .context("SECONDS must be a whole number")?
                .unwrap_or(30);
            cmd_record(seconds)
        }
        Some("help") | Some("--help") | Some("-h") => {
            println!("{USAGE}");
            Ok(())
        }
        Some(other) => {
            eprintln!("unknown command: {other}\n\n{USAGE}");
            std::process::exit(2);
        }
        None => {
            println!("{USAGE}");
            Ok(())
        }
    }
}

/// Lists what this machine can record from.
///
/// Recording a meeting needs two devices: a microphone for the operator, and
/// the monitor of whichever output the meeting is playing through for the far
/// end.
fn cmd_devices() -> Result<()> {
    let devices = capture::list_devices()?;

    if devices.is_empty() {
        println!("No recordable audio devices found.");
        println!("PipeWire is reachable but exposes no sources or sinks.");
        return Ok(());
    }

    let width = devices
        .iter()
        .map(|d| d.display_name.len())
        .max()
        .unwrap_or(20)
        .max(12);

    println!("  {:<width$}  {:<12}  {}", "DEVICE", "KIND", "NODE NAME");
    for d in &devices {
        let kind = match d.kind {
            capture::DeviceKind::Microphone => "microphone",
            capture::DeviceKind::SinkMonitor => "playback",
        };
        // The session default is what the user is actually speaking into and
        // listening to, and is what a recording will open.
        let marker = if d.is_default { "*" } else { " " };
        println!(
            "{marker} {:<width$}  {:<12}  {}",
            d.display_name, kind, d.node_name
        );
    }
    println!("\n* = session default, used when recording");

    let mics = devices
        .iter()
        .filter(|d| d.kind == capture::DeviceKind::Microphone)
        .count();
    let monitors = devices.len() - mics;
    println!("\n{mics} microphone(s), {monitors} playback monitor(s)");

    Ok(())
}

/// Reports whether this machine can actually record, and why not if it cannot.
///
/// Capture depends on a running PipeWire session, which is absent on headless
/// machines and inside containers. Diagnosing that here gives a clear answer
/// instead of a failure partway into a recording.
fn cmd_doctor() -> Result<()> {
    println!("Kaseta doctor\n");

    let session = std::env::var("XDG_SESSION_TYPE").unwrap_or_else(|_| "unknown".into());
    println!("  session type      {session}");

    let runtime_dir = std::env::var("XDG_RUNTIME_DIR").ok();
    match &runtime_dir {
        Some(dir) => println!("  runtime dir       {dir}"),
        None => println!("  runtime dir       (unset — PipeWire will not be reachable)"),
    }

    match capture::list_devices() {
        Ok(devices) => {
            let mics = devices
                .iter()
                .filter(|d| d.kind == capture::DeviceKind::Microphone)
                .count();
            let monitors = devices.len() - mics;
            println!("  pipewire          reachable");
            println!("  microphones       {mics}");
            println!("  playback monitors {monitors}");

            let default_mic = devices
                .iter()
                .find(|d| d.is_default && d.kind == capture::DeviceKind::Microphone);
            let default_playback = devices
                .iter()
                .find(|d| d.is_default && d.kind == capture::DeviceKind::SinkMonitor);
            println!(
                "  default mic       {}",
                default_mic.map_or("(none detected)", |d| d.display_name.as_str())
            );
            println!(
                "  default playback  {}",
                default_playback.map_or("(none detected)", |d| d.display_name.as_str())
            );

            println!();
            if mics == 0 {
                println!("No microphone found — the operator's own audio cannot be recorded.");
            }
            if monitors == 0 {
                println!("No playback monitor found — the far end of a call cannot be recorded.");
            }
            if mics > 0 && monitors > 0 {
                println!("This machine can record both sides of a meeting.");
            }
        }
        Err(e) => {
            println!("  pipewire          unreachable");
            println!("\nCapture is unavailable: {e:#}");
            println!("\nKaseta needs a running PipeWire session. This is expected on a");
            println!("headless server; run the daemon on the desktop machine instead.");
        }
    }

    // Reported regardless of whether capture works: a worker that cannot start
    // fails every transcription in the background, and the only sign is a
    // recording that never gains a transcript.
    println!();
    match transcribe::worker_status() {
        Ok(python) => println!("  transcription     ready ({python})"),
        Err(e) => {
            println!("  transcription     unavailable");
            println!("                    {e:#}");
            println!("\nRecordings will be captured but not transcribed. Set it up with:");
            println!("  cd worker && python -m venv .venv && .venv/bin/pip install -e .");
        }
    }

    Ok(())
}

/// Records both sides of a meeting for a fixed duration.
///
/// This is the end-to-end check that capture works on a given machine: it opens
/// a microphone and a playback monitor as separate tracks, seals chunks as it
/// goes, and reports what it measured — including per-track clock drift, which
/// is the failure that would otherwise go unnoticed until transcripts came out
/// misaligned.
fn cmd_record(seconds: u64) -> Result<()> {
    use blobstore::BlobStore;
    use capture::devices::DeviceKind;
    use capture::session::{RecordingSession, TrackSpec};

    let devices = capture::list_devices()
        .context("could not reach PipeWire — run `kasetad doctor` to diagnose")?;

    let mic = devices
        .iter()
        .find(|d| d.kind == DeviceKind::Microphone)
        .context("no microphone found — cannot record your own audio")?
        .clone();
    let playback = devices
        .iter()
        .find(|d| d.kind == DeviceKind::SinkMonitor)
        .context("no playback monitor found — cannot record the far end")?
        .clone();

    let root = std::env::var("KASETA_DATA").unwrap_or_else(|_| "./data".into());
    let store = Arc::new(blobstore::LocalFsStore::new(&root)?);

    // Devices are ordered defaults-first, so taking the first of each kind
    // selects what the user is actually using.
    println!("Recording {seconds}s");
    println!(
        "  microphone  {}{}",
        mic.display_name,
        if mic.is_default { "" } else { "  (NOT the session default)" }
    );
    println!(
        "  playback    {}{}",
        playback.display_name,
        if playback.is_default { "" } else { "  (NOT the session default)" }
    );
    println!("  writing to  {}\n", store.root().display());

    let specs = TrackSpec::meeting(mic, playback)?;
    let started_at = time::OffsetDateTime::now_utc();
    // Headphones are the supported configuration: on speakers the far end
    // bleeds into the microphone track and local-versus-remote attribution
    // degrades. The CLI cannot know, so it records that it does not.
    let notes = kaseta_contracts::manifest::RecordingNotes::default();
    let session = RecordingSession::start(specs, store.clone(), started_at, notes)?;
    let prefix = session.prefix().clone();

    // Poll rather than sleep straight through: if persistence fails, the
    // collector stops and every later buffer is discarded, so continuing to the
    // end of the requested duration would record nothing while appearing to work.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(seconds);
    let mut aborted = None;
    let mut reported_degraded = std::collections::HashSet::new();
    while std::time::Instant::now() < deadline {
        let health = session.health();
        if let Some(error) = health.failure {
            aborted = Some(error);
            break;
        }
        for (track, reason) in &health.degraded_tracks {
            if reported_degraded.insert(track.clone()) {
                eprintln!("  {track} stopped capturing: {reason}");
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    if let Some(error) = &aborted {
        eprintln!("\nRecording stopped early: {error}\n");
    }

    let outcome = session.stop()?;
    let manifest = &outcome.manifest;

    // The manifest is what every later stage reads; without it the chunks on
    // disk are unattributed audio.
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    store.put(&prefix.manifest(), &manifest_json)?;

    println!("Recorded {}\n", manifest.recording_id);

    for track in &manifest.tracks {
        let bytes: u64 = track.chunks.iter().map(|c| c.bytes).sum();
        let gaps = track.chunks.iter().filter(|c| c.discontinuity).count();
        let seconds_captured = track.sample_count() as f64
            / track.format.sample_rate_hz.unwrap_or(48_000) as f64;

        println!("  {}", track.track_id);
        println!(
            "    {} chunks, {:.1}s captured, {:.1} MiB",
            track.chunks.len(),
            seconds_captured,
            bytes as f64 / (1024.0 * 1024.0)
        );

        match track.drift() {
            Some(d) => {
                let verdict = if d.requires_resample() {
                    "exceeds threshold — merged audio needs resampling"
                } else {
                    "within tolerance"
                };
                println!(
                    "    drift {:+.1} ms over {:.0}s ({verdict})",
                    d.offset_ms, seconds_captured
                );
            }
            None => println!("    drift  not measurable (no audio captured)"),
        }

        // Peak level is what separates "captured silence" from "captured
        // nothing" — both produce chunks, and FLAC shrinks silence to almost
        // nothing, so byte counts cannot tell them apart.
        if let Some(o) = outcome.tracks.iter().find(|o| o.track_id == track.track_id) {
            if o.peak_level <= 0.0 {
                // Every sample was exactly zero. A live input always carries a
                // noise floor, so this means the source is muted or the wrong
                // device was opened — not that the room was quiet.
                println!("    peak  SILENT — every sample was zero");
                println!("          the source is muted or this is the wrong device");
            } else {
                let dbfs = 20.0 * o.peak_level.log10();
                println!("    peak  {:.1} dBFS", dbfs);
            }
        }

        if let Some(o) = outcome.tracks.iter().find(|o| o.track_id == track.track_id) {
            if o.drops > 0 {
                println!("    {} buffer(s) dropped by the audio server", o.drops);
            }
        }
        if gaps > 0 {
            println!(
                "    {gaps} discontinuit(ies), {:.1} ms of audio missing",
                track.missing_ns() as f64 / 1e6
            );
        }
        if track.chunks.is_empty() {
            println!("    NO AUDIO CAPTURED");
        }
        println!();
    }

    // Chunks are the durable format but not a listenable one, so a merged file
    // per track is produced immediately rather than left as a separate step.
    // A failed export must not discard a good recording; the chunks are intact
    // and `export` can retry.
    if let Err(e) = write_exports(&*store, manifest, &prefix) {
        eprintln!("Could not export: {e:#}");
        eprintln!("The chunks are intact; run `kasetad export` to retry.\n");
    }

    println!("Manifest: {}", prefix.manifest());

    if let Some(error) = aborted {
        anyhow::bail!("recording did not complete: {error}");
    }
    Ok(())
}

/// Merges a recording's chunks into one continuous file per track.
///
/// Chunks stay the durable format; this produces something listenable without
/// discarding them.
fn cmd_export(id: Option<String>) -> Result<()> {
    use blobstore::BlobStore;

    let root = std::env::var("KASETA_DATA").unwrap_or_else(|_| "./data".into());
    let store = blobstore::LocalFsStore::new(&root)?;

    let manifest_key = match &id {
        Some(id) => find_manifest_matching(&store, id)?,
        None => latest_manifest(&store)?,
    };

    let manifest: kaseta_contracts::RecordingManifest =
        serde_json::from_slice(&store.get(&manifest_key)?)
            .with_context(|| format!("reading manifest {manifest_key}"))?;

    let prefix = kaseta_contracts::RecordingPrefix::new(manifest.recording_id, manifest.started_at);
    println!("Merging {}\n", manifest.recording_id);

    write_exports(&store, &manifest, &prefix)
}

/// Produces the listenable artifacts for a recording: one file per track, and
/// one combined stereo mix.
///
/// Both are kept. The per-track files carry the separation that lets a
/// transcript attribute every line without a speaker model; the mix is what a
/// person actually wants to play back.
fn write_exports(
    store: &dyn blobstore::BlobStore,
    manifest: &kaseta_contracts::RecordingManifest,
    prefix: &kaseta_contracts::RecordingPrefix,
) -> Result<()> {
    let merged = export::merge_recording(store, manifest, prefix)?;
    if merged.is_empty() {
        println!("Nothing to export: this recording captured no audio.");
        return Ok(());
    }

    println!("Exported:");
    for m in &merged {
        let rate = manifest
            .tracks
            .iter()
            .find(|t| m.key.as_str().contains(t.track_id.as_str()))
            .and_then(|t| t.format.sample_rate_hz)
            .unwrap_or(48_000);

        println!(
            "  {}  ({:.1}s, {:.1} MiB)",
            m.key,
            export::frames_to_seconds(m.frames, rate),
            m.bytes as f64 / (1024.0 * 1024.0)
        );
        if m.padded_frames > 0 {
            println!(
                "      {:.1}s of silence inserted where audio was dropped",
                export::frames_to_seconds(m.padded_frames, rate)
            );
        }
    }

    if let Some(mix) = export::mix_recording(store, manifest, prefix)? {
        println!(
            "\n  {}  ({:.1}s, {:.1} MiB)  <- both sides together",
            mix.key,
            export::frames_to_seconds(mix.frames, mix.sample_rate_hz),
            mix.bytes as f64 / (1024.0 * 1024.0)
        );
        if mix.gain < 1.0 {
            println!(
                "      attenuated {:.1} dB so the combined signal does not clip",
                20.0 * mix.gain.log10()
            );
        }
    }
    println!();

    Ok(())
}

/// The manifest of the most recent recording.
///
/// Keys are time-partitioned and zero-padded, so the last one lexicographically
/// is the newest.
fn latest_manifest(store: &blobstore::LocalFsStore) -> Result<kaseta_contracts::BlobKey> {
    use blobstore::BlobStore;
    store
        .list_prefix("recordings")?
        .into_iter()
        .filter(|k| k.as_str().ends_with("/manifest.json"))
        .next_back()
        .context("no completed recordings found")
}

fn find_manifest_matching(
    store: &blobstore::LocalFsStore,
    id: &str,
) -> Result<kaseta_contracts::BlobKey> {
    use blobstore::BlobStore;
    store
        .list_prefix("recordings")?
        .into_iter()
        .filter(|k| k.as_str().ends_with("/manifest.json"))
        .find(|k| k.as_str().contains(id))
        .with_context(|| format!("no recording matching {id:?}"))
}

/// Runs the daemon: capture supervisor, HTTP API, and the interface.
///
/// The interface is served as a static page rather than shipped as a desktop
/// shell, so closing the browser tab leaves only this process resident.
fn cmd_serve(port: u16) -> Result<()> {
    use std::sync::Mutex;

    let root = std::env::var("KASETA_DATA").unwrap_or_else(|_| "./data".into());
    let local = blobstore::LocalFsStore::new(&root)?;
    // The worker resolves blob keys against this, so it must be the resolved
    // path rather than whatever was configured.
    let storage_root = local.root().display().to_string();
    let store: Arc<dyn blobstore::BlobStore> = Arc::new(local);

    let db = db::Db::open(&std::path::Path::new(&root).join("kaseta.db"))?;

    // Storage is the source of truth; the index is derived. Rebuilding it at
    // startup means a database that was deleted, or that missed a recording
    // because the daemon died mid-write, converges on what storage holds.
    match library::reconcile(&*store, &db) {
        Ok(count) => tracing::info!(recordings = count, "library ready"),
        Err(e) => tracing::error!(error = %format!("{e:#}"), "could not index existing recordings"),
    }

    // Recordings made before transcripts and summaries were stored have them
    // only in the index, and so have backups that silently omit them. Working
    // through that backlog once, here, is what stops those backups staying
    // incomplete until each recording next happens to be touched.
    match derived::heal(&*store, &db) {
        Ok(healed) if healed.written > 0 => {
            tracing::info!(
                recordings = healed.written,
                "stored transcripts and summaries that were only indexed"
            );
        }
        Ok(_) => {}
        Err(e) => tracing::error!(
            error = %format!("{e:#}"),
            "could not store previously indexed transcripts"
        ),
    }

    // A purge interrupted by a crash left objects behind and a tombstone hiding
    // them; finish the job before anything indexes them again.
    match library::purge_pending(&*store, &db) {
        Ok(n) if n > 0 => tracing::info!(recordings = n, "completed interrupted deletions"),
        Ok(_) => {}
        Err(e) => tracing::error!(error = %format!("{e:#}"), "retrying deletions failed"),
    }

    // Jobs left running by a previous process have no live worker; requeue them
    // before anything new is scheduled.
    match db.recover_orphaned_jobs() {
        Ok(ids) if !ids.is_empty() => tracing::warn!(count = ids.len(), "requeued interrupted jobs"),
        Ok(_) => {}
        Err(e) => tracing::error!(error = %format!("{e:#}"), "job recovery failed"),
    }

    let db = Arc::new(Mutex::new(db));
    let supervisor = Arc::new(supervisor::Supervisor::spawn(
        Arc::clone(&store),
        Arc::clone(&db),
    )?);

    // Applied once at startup and then daily by the scheduler. Running it here
    // means a policy set while the daemon was stopped takes effect on the next
    // launch rather than waiting a day.
    {
        let settings = config::Settings::load().unwrap_or_default();
        if let Err(e) = retention::sweep(
            &*store,
            &db,
            &settings.retention,
            time::OffsetDateTime::now_utc(),
        ) {
            tracing::error!(error = %format!("{e:#}"), "retention sweep failed");
        }
    }

    // Held for the lifetime of the daemon: dropping it stops the worker thread.
    let _scheduler = scheduler::Scheduler::spawn(
        Arc::clone(&store),
        Arc::clone(&db),
        storage_root,
    )?;

    // A multi-thread runtime, so one slow request cannot stall the others.
    // Blocking work is additionally moved off the runtime with `spawn_blocking`;
    // this is defence in depth rather than the primary mechanism.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting the async runtime")?;

    runtime.block_on(http::serve(supervisor, store, db, port))
}

/// Transcribes a recording and stores the result.
///
/// Each side is transcribed from its own track, so every line already knows who
/// said it — no speaker model involved.
fn cmd_transcribe(id: Option<String>) -> Result<()> {
    use blobstore::BlobStore;

    let root = std::env::var("KASETA_DATA").unwrap_or_else(|_| "./data".into());
    let store = blobstore::LocalFsStore::new(&root)?;
    let db = db::Db::open(&std::path::Path::new(&root).join("kaseta.db"))?;

    let manifest_key = match &id {
        Some(id) => find_manifest_matching(&store, id)?,
        None => latest_manifest(&store)?,
    };
    let manifest: kaseta_contracts::RecordingManifest =
        serde_json::from_slice(&store.get(&manifest_key)?)
            .with_context(|| format!("reading manifest {manifest_key}"))?;

    println!("Transcribing {}", manifest.recording_id);
    println!("This runs a model on CPU; expect it to take a fraction of the recording's length.\n");

    let root = store.root().display().to_string();
    let transcription = transcribe::transcribe(&store, &root, &manifest)?;
    let prefix =
        kaseta_contracts::RecordingPrefix::new(manifest.recording_id, manifest.started_at);
    let segments = transcribe::store_transcript(
        &store,
        &prefix,
        &db,
        manifest.recording_id,
        &transcription,
    )?;

    if segments == 0 {
        println!("No speech was found.");
        return Ok(());
    }

    println!("{segments} segment(s) transcribed.\n");
    print_transcript(&db, manifest.recording_id)?;
    Ok(())
}

/// Prints a transcript as a conversation, in the order things were said.
fn print_transcript(db: &db::Db, recording_id: ulid::Ulid) -> Result<()> {
    let mut stmt = db.conn().prepare(
        "SELECT s.start_boottime_ns, s.speaker_hint, s.text
         FROM transcript_segments s
         JOIN transcripts t ON t.id = s.transcript_id
         WHERE t.recording_id = ?1
         ORDER BY s.start_boottime_ns",
    )?;

    let base: Option<i64> = db
        .conn()
        .query_row(
            "SELECT clock_started_ns FROM recordings WHERE id = ?1",
            rusqlite::params![recording_id.to_string()],
            |r| r.get(0),
        )
        .unwrap_or(None);
    let base = base.unwrap_or(0) as u64;

    let rows = stmt.query_map(rusqlite::params![recording_id.to_string()], |r| {
        Ok((
            r.get::<_, i64>(0)? as u64,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
        ))
    })?;

    for row in rows {
        let (start_ns, speaker, text) = row?;
        // Shown relative to the recording, not to system boot.
        let offset = start_ns.saturating_sub(base) / 1_000_000_000;
        let who = match speaker.as_str() {
            "local" => "You",
            "remote" => "Them",
            _ => "?",
        };
        println!("[{:02}:{:02}] {who:>4}  {text}", offset / 60, offset % 60);
    }
    Ok(())
}

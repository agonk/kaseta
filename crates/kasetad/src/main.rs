use std::sync::Arc;

use anyhow::{Context, Result};

mod blobstore;
mod capture;
mod clock;
mod db;
mod export;

const USAGE: &str = "\
kasetad — Kaseta recording daemon

USAGE:
    kasetad <COMMAND>

COMMANDS:
    devices              List recordable audio devices and exit
    doctor               Check that this machine can capture audio
    record [SECONDS]     Record both sides of a meeting (default 30s)
    export [ID]          Merge a recording's chunks into one file per track
                         (defaults to the most recent recording)
    help                 Show this message

ENVIRONMENT:
    KASETA_LOG   Log filter, e.g. `kasetad=debug`
    KASETA_DATA  Where recordings are written (default ./data)
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
    let session = RecordingSession::start(specs, store.clone(), started_at)?;
    let prefix = session.prefix().clone();

    // Poll rather than sleep straight through: if persistence fails, the
    // collector stops and every later buffer is discarded, so continuing to the
    // end of the requested duration would record nothing while appearing to work.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(seconds);
    let mut aborted = None;
    while std::time::Instant::now() < deadline {
        if let Some(error) = session.failure() {
            aborted = Some(error);
            break;
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

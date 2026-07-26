use anyhow::Result;

mod blobstore;
mod capture;
mod clock;
mod db;

const USAGE: &str = "\
kasetad — Kaseta recording daemon

USAGE:
    kasetad <COMMAND>

COMMANDS:
    devices     List recordable audio devices and exit
    doctor      Check that this machine can capture audio
    help        Show this message

ENVIRONMENT:
    KASETA_LOG  Log filter, e.g. `kasetad=debug`
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

    println!("{:<width$}  {:<12}  {}", "DEVICE", "KIND", "NODE NAME");
    for d in &devices {
        let kind = match d.kind {
            capture::DeviceKind::Microphone => "microphone",
            capture::DeviceKind::SinkMonitor => "playback",
        };
        println!(
            "{:<width$}  {:<12}  {}",
            d.display_name, kind, d.node_name
        );
    }

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

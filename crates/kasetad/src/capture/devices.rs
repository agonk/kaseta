//! Device discovery.
//!
//! Enumerates what Kaseta can record from: microphones, and the monitor of each
//! output sink. The monitor is what carries the far end of a call — it is the
//! audio the machine played back — and capturing it separately from the
//! microphone is what allows every transcript line to be attributed to the
//! operator or the far end without any speaker model.
//!
//! # Identity
//!
//! PipeWire node *IDs* are integers that are reused after a node disappears, so
//! they are useless as durable identity. `node.name` is stable across restarts
//! for real devices and is what gets persisted and matched on reconnection.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioDevice {
    /// Stable identifier, persisted and used to reattach. Not the node ID.
    pub node_name: String,
    /// Human-readable label for the interface.
    pub display_name: String,
    pub kind: DeviceKind,
    pub channels: Option<u16>,
    /// Whether this is the server's current default for its kind.
    pub is_default: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceKind {
    /// A capture device — a microphone or line input.
    Microphone,
    /// The monitor of an output sink: everything the machine played.
    SinkMonitor,
}

/// Lists every recordable audio device the server currently exposes.
///
/// This connects, drains the registry, and disconnects. It is called when the
/// interface asks what is available, not held open, so idle cost is zero.
pub fn list_devices() -> Result<Vec<AudioDevice>> {
    // `init` is idempotent and must run before any other PipeWire call.
    pipewire::init();

    let mainloop = pipewire::main_loop::MainLoopRc::new(None)
        .context("creating PipeWire main loop (is PipeWire running?)")?;
    let context = pipewire::context::ContextRc::new(&mainloop, None)
        .context("creating PipeWire context")?;
    let core = context
        .connect_rc(None)
        .context("connecting to PipeWire (is a session available?)")?;
    let registry = core
        .get_registry_rc()
        .context("obtaining PipeWire registry")?;

    let found: Rc<RefCell<Vec<AudioDevice>>> = Rc::new(RefCell::new(Vec::new()));
    // Which nodes the session considers default. Recording anything else means
    // recording a device the user is not actually speaking into or listening to.
    let defaults: Rc<RefCell<Defaults>> = Rc::new(RefCell::new(Defaults::default()));
    // Bound proxies and their listeners must outlive the loop, or the server
    // stops delivering their events.
    let bound: Rc<RefCell<Vec<(pipewire::metadata::Metadata, pipewire::metadata::MetadataListener)>>> =
        Rc::new(RefCell::new(Vec::new()));

    // The server replays every existing object on connect, then answers our
    // sync. Waiting for that answer is what makes enumeration complete rather
    // than merely "whatever arrived before a timeout".
    let pending = core.sync(0).map_err(|e| anyhow::anyhow!("sync failed: {e}"))?;
    let done = Rc::new(Cell::new(false));

    let finished = done.clone();
    let quit_loop = mainloop.clone();
    let _core_listener = core
        .add_listener_local()
        .done(move |id, seq| {
            if id == pipewire::core::PW_ID_CORE && seq == pending {
                finished.set(true);
                quit_loop.quit();
            }
        })
        .register();

    let collector = found.clone();
    let default_sink_source = defaults.clone();
    let keep_alive = bound.clone();
    let registry_for_bind = registry.clone();
    let _registry_listener = registry
        .add_listener_local()
        .global(move |global| {
            if let Some(device) = device_from_global(global) {
                collector.borrow_mut().push(device);
                return;
            }

            // WirePlumber publishes the session's default sink and source in a
            // metadata object named `default`. There is no other authoritative
            // source for it: the node list carries no such flag.
            if global.type_ != pipewire::types::ObjectType::Metadata {
                return;
            }
            let is_default_metadata = global
                .props
                .and_then(|p| p.get("metadata.name"))
                .is_some_and(|name| name == "default");
            if !is_default_metadata {
                return;
            }

            let Ok(metadata) = registry_for_bind.bind::<pipewire::metadata::Metadata, _>(global)
            else {
                tracing::debug!("could not bind default metadata; falling back to first device");
                return;
            };

            let sink_source = default_sink_source.clone();
            let listener = metadata
                .add_listener_local()
                .property(move |_subject, key, _type, value| {
                    let (Some(key), Some(value)) = (key, value) else {
                        return 0;
                    };
                    // Values are JSON of the form {"name":"<node.name>"}.
                    let Some(name) = parse_default_node_name(value) else {
                        return 0;
                    };
                    let mut slot = sink_source.borrow_mut();
                    match key {
                        "default.audio.sink" => slot.sink = Some(name),
                        "default.audio.source" => slot.source = Some(name),
                        _ => {}
                    }
                    0
                })
                .register();

            keep_alive.borrow_mut().push((metadata, listener));
        })
        .register();

    while !done.get() {
        mainloop.run();
    }

    // Listeners are dropped here, releasing the collectors' second handles.
    drop(_registry_listener);
    bound.borrow_mut().clear();

    let defaults = defaults.borrow().clone();
    let mut devices = Rc::try_unwrap(found)
        .map_err(|_| anyhow::anyhow!("device collector outlived enumeration"))?
        .into_inner();

    for device in &mut devices {
        let default_name = match device.kind {
            DeviceKind::Microphone => defaults.source.as_deref(),
            DeviceKind::SinkMonitor => defaults.sink.as_deref(),
        };
        device.is_default = default_name == Some(device.node_name.as_str());
    }

    // Microphones before monitors, the session default first within each, then
    // alphabetically. The default is what the user is actually speaking into
    // and listening to, so it must sort ahead of an unused internal input.
    // Ordering depends on `is_default`, so it is resolved above first.
    devices.sort_by(|a, b| {
        (a.kind as u8, !a.is_default, &a.display_name)
            .cmp(&(b.kind as u8, !b.is_default, &b.display_name))
    });
    devices.dedup_by(|a, b| a.node_name == b.node_name);

    Ok(devices)
}

/// The session's default sink and source, by node name.
#[derive(Clone, Debug, Default)]
struct Defaults {
    sink: Option<String>,
    source: Option<String>,
}

/// Extracts the node name from a default-device metadata value.
///
/// The value is JSON of the form `{"name":"alsa_output..."}`. A malformed or
/// unexpected value yields `None`, which falls back to no default rather than
/// to a wrong one.
fn parse_default_node_name(value: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(value).ok()?;
    let name = parsed.get("name")?.as_str()?;
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Classifies a registry object, returning `None` for anything not recordable.
fn device_from_global(global: &pipewire::registry::GlobalObject<&pipewire::spa::utils::dict::DictRef>) -> Option<AudioDevice> {
    if global.type_ != pipewire::types::ObjectType::Node {
        return None;
    }
    let props = global.props?;

    let media_class = props.get("media.class").unwrap_or_default();
    let node_name = props.get("node.name")?.to_string();

    // `Audio/Source` is a capture device. `Audio/Sink` exposes a monitor
    // carrying whatever it played, which is the far end of a call.
    //
    // `Audio/Source/Virtual` is excluded: those are loopbacks and null sinks
    // that would duplicate audio already captured elsewhere.
    let kind = match media_class {
        "Audio/Source" => DeviceKind::Microphone,
        "Audio/Sink" => DeviceKind::SinkMonitor,
        _ => return None,
    };

    let display_name = props
        .get("node.description")
        .or_else(|| props.get("node.nick"))
        .unwrap_or(&node_name)
        .to_string();

    let display_name = match kind {
        DeviceKind::SinkMonitor => format!("{display_name} (playback)"),
        DeviceKind::Microphone => display_name,
    };

    Some(AudioDevice {
        node_name,
        display_name,
        kind,
        channels: props
            .get("audio.channels")
            .and_then(|c| c.parse().ok()),
        is_default: false,
    })
}

/// The PipeWire node to open in order to record `device`.
///
/// Recording a sink's monitor means connecting an input stream to the sink node
/// itself and asking the server for its monitor ports, which is what
/// `stream.capture.sink` requests.
pub fn capture_target(device: &AudioDevice) -> CaptureTarget {
    CaptureTarget {
        node_name: device.node_name.clone(),
        capture_sink: device.kind == DeviceKind::SinkMonitor,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureTarget {
    pub node_name: String,
    /// Whether to ask PipeWire for the node's monitor ports rather than its
    /// capture ports.
    pub capture_sink: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn device(name: &str, display: &str, kind: DeviceKind) -> AudioDevice {
        AudioDevice {
            node_name: name.into(),
            display_name: display.into(),
            kind,
            channels: Some(2),
            is_default: false,
        }
    }

    #[test]
    fn a_sink_is_captured_through_its_monitor_ports() {
        let sink = device("alsa_output.pci-0000_00_1f.3", "Speakers", DeviceKind::SinkMonitor);
        let target = capture_target(&sink);

        assert!(
            target.capture_sink,
            "recording playback requires the sink's monitor, not its input"
        );
        assert_eq!(target.node_name, "alsa_output.pci-0000_00_1f.3");
    }

    #[test]
    fn a_microphone_is_captured_directly() {
        let mic = device("alsa_input.usb-mic", "USB Mic", DeviceKind::Microphone);
        let target = capture_target(&mic);
        assert!(!target.capture_sink);
    }

    #[test]
    fn parses_the_default_node_name_from_metadata() {
        assert_eq!(
            parse_default_node_name(r#"{"name":"alsa_output.usb-Jabra_EVOLVE_20_MS.analog-stereo"}"#),
            Some("alsa_output.usb-Jabra_EVOLVE_20_MS.analog-stereo".into())
        );
    }

    #[test]
    fn a_malformed_default_yields_no_default_rather_than_a_wrong_one() {
        assert_eq!(parse_default_node_name("not json"), None);
        assert_eq!(parse_default_node_name(r#"{"other":"x"}"#), None);
        assert_eq!(parse_default_node_name(r#"{"name":""}"#), None);
        assert_eq!(parse_default_node_name(r#"{"name":123}"#), None);
    }

    #[test]
    fn the_session_default_sorts_ahead_of_other_devices_of_its_kind() {
        // A laptop's unused internal input enumerates before a plugged-in
        // headset; recording the first-found device would capture silence.
        let mut internal = device("alsa_input.internal", "Internal Mic", DeviceKind::Microphone);
        let mut headset = device("alsa_input.jabra", "Jabra EVOLVE 20 MS", DeviceKind::Microphone);
        headset.is_default = true;
        internal.is_default = false;

        let mut devices = vec![internal, headset];
        devices.sort_by(|a, b| {
            (a.kind as u8, !a.is_default, &a.display_name)
                .cmp(&(b.kind as u8, !b.is_default, &b.display_name))
        });

        assert_eq!(
            devices[0].node_name, "alsa_input.jabra",
            "the default device must be chosen over an alphabetically earlier one"
        );
    }

    #[test]
    fn devices_are_ordered_microphones_first_then_alphabetically() {
        let mut devices = vec![
            device("sink.b", "Zeta Speakers (playback)", DeviceKind::SinkMonitor),
            device("mic.b", "Beta Mic", DeviceKind::Microphone),
            device("sink.a", "Alpha Speakers (playback)", DeviceKind::SinkMonitor),
            device("mic.a", "Alpha Mic", DeviceKind::Microphone),
        ];

        devices.sort_by(|a, b| (a.kind as u8, &a.display_name).cmp(&(b.kind as u8, &b.display_name)));

        let names: Vec<_> = devices.iter().map(|d| d.display_name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "Alpha Mic",
                "Beta Mic",
                "Alpha Speakers (playback)",
                "Zeta Speakers (playback)"
            ]
        );
    }

    #[test]
    fn device_identity_survives_serialisation() {
        let d = device("alsa_input.usb-mic", "USB Mic", DeviceKind::Microphone);
        let json = serde_json::to_string(&d).unwrap();
        let back: AudioDevice = serde_json::from_str(&json).unwrap();
        assert_eq!(back, d);
        assert!(
            json.contains("node_name"),
            "the stable identifier must be what is persisted"
        );
    }
}

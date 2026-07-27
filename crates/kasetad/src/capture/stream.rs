//! A single live capture stream.
//!
//! Each stream owns a dedicated OS thread running its own PipeWire main loop,
//! because PipeWire objects are neither `Send` nor `Sync` and must be created,
//! used, and dropped on one thread. Captured audio leaves that thread through a
//! channel; control commands enter through PipeWire's own cross-thread channel,
//! which wakes the loop rather than requiring it to poll.
//!
//! Recording two tracks means two of these, each on its own clock. They are
//! never synchronised live — see [`super::chunk`] for why alignment is computed
//! after the fact instead.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::JoinHandle;

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use pipewire::spa;
use pipewire::spa::pod::Pod;

use super::devices::CaptureTarget;
use crate::clock::boottime_ns;

/// What a stream reports to its owner.
#[derive(Debug)]
pub enum CaptureEvent {
    /// The server negotiated a format. Always arrives before any `Buffer`, and
    /// carries the rate and channel count actually in use, which may differ from
    /// what was requested.
    Negotiated { sample_rate_hz: u32, channels: u16 },
    Buffer {
        /// Interleaved 16-bit samples.
        samples: Vec<i16>,
        /// Canonical clock at the moment the buffer was received.
        arrived_at_ns: u64,
        /// The stream's own position, for cross-checking the canonical clock.
        source_pts_ns: Option<u64>,
    },
    /// The server had audio ready that could not be collected. Reported rather
    /// than logged because elapsed-time gap detection cannot see this loss: the
    /// next buffer still arrives on schedule.
    Dropped,
    /// The stream dropped and capture is being reattempted. Reported so the
    /// interface can show that a device went away without implying the
    /// recording is over.
    Reconnecting { reason: String, attempt: u32 },
    /// The stream stopped on its own — the device disappeared, or the server
    /// went away. The owner decides whether to reconnect.
    Ended { reason: String },
}

/// Sent into the capture thread to wind it down.
struct Terminate;

pub struct CaptureStream {
    thread: Option<JoinHandle<()>>,
    terminate: pipewire::channel::Sender<Terminate>,
    /// Distinguishes a deliberate stop from a device disappearing, so a
    /// requested stop is not mistaken for something to reconnect to.
    stopping: Arc<AtomicBool>,
}

impl CaptureStream {
    /// Opens `target` and begins delivering buffers to `events`.
    ///
    /// Returns once the thread is running; format negotiation happens
    /// asynchronously and is reported as [`CaptureEvent::Negotiated`].
    pub fn start(target: CaptureTarget, events: Sender<CaptureEvent>) -> Result<Self> {
        let (terminate, receiver) = pipewire::channel::channel::<Terminate>();
        let stopping = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stopping);

        let thread = std::thread::Builder::new()
            .name(format!("kaseta-capture-{}", target.node_name))
            .spawn(move || {
                let reason = run_with_reconnect(&target, &events, receiver, &stop_flag);
                // Best effort: the owner may already have dropped the receiver.
                let _ = events.send(CaptureEvent::Ended { reason });
            })
            .context("spawning capture thread")?;

        Ok(Self {
            thread: Some(thread),
            terminate,
            stopping,
        })
    }

    /// Signals the stream to stop and waits for its thread to finish.
    ///
    /// Joining matters: the thread must finish draining and flush its final
    /// buffers before the caller seals the recording.
    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.stopping.store(true, Ordering::SeqCst);
        // A send failure means the loop has already exited, which is fine.
        let _ = self.terminate.send(Terminate);
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                tracing::error!("capture thread panicked");
            }
        }
    }
}

impl Drop for CaptureStream {
    fn drop(&mut self) {
        // A stream dropped without `stop` must still not leak its thread.
        self.shutdown();
    }
}

/// How long to wait between reconnection attempts.
///
/// Unplugging a headset and plugging in another takes seconds, and the
/// replacement device takes a moment to appear. Retrying faster would burn the
/// budget before the device exists; slower would lose more of the meeting.
const RECONNECT_DELAY: Duration = Duration::from_millis(750);

/// How many times a dropped stream is reattached before giving up.
///
/// Bounded so a device that is genuinely gone ends the track rather than
/// retrying for the rest of the meeting.
const RECONNECT_ATTEMPTS: u32 = 8;

/// Runs capture, reattaching if the device disappears.
///
/// Unplugging a headset mid-meeting destroys the stream. Without this the track
/// simply ended and the rest of the conversation was lost from that side, with
/// nothing recorded but a log line.
fn run_with_reconnect(
    target: &CaptureTarget,
    events: &Sender<CaptureEvent>,
    receiver: pipewire::channel::Receiver<Terminate>,
    stopping: &Arc<AtomicBool>,
) -> String {
    // The terminate channel is consumed by the loop it is attached to, so only
    // the first attempt can own it. Later attempts poll the stop flag instead,
    // which is set before the terminate signal is ever sent.
    let mut receiver = Some(receiver);

    for attempt in 0..=RECONNECT_ATTEMPTS {
        let outcome = match receiver.take() {
            Some(rx) => run_capture_loop(target, events, Some(rx), stopping),
            None => run_capture_loop(target, events, None, stopping),
        };

        if stopping.load(Ordering::SeqCst) {
            return "stopped".to_string();
        }

        let reason = match outcome {
            // A clean end without a stop having been requested means the server
            // dropped the stream — the device went away.
            Ok(()) => "the device stopped providing audio".to_string(),
            Err(e) => format!("{e:#}"),
        };

        if attempt == RECONNECT_ATTEMPTS {
            return format!("{reason} (gave up after {RECONNECT_ATTEMPTS} attempts)");
        }

        tracing::warn!(
            device = %target.node_name,
            attempt = attempt + 1,
            %reason,
            "capture dropped; reattaching"
        );
        let _ = events.send(CaptureEvent::Reconnecting {
            reason: reason.clone(),
            attempt: attempt + 1,
        });

        // Sleep in slices so a stop during the wait is noticed promptly rather
        // than after the full delay.
        let deadline = std::time::Instant::now() + RECONNECT_DELAY;
        while std::time::Instant::now() < deadline {
            if stopping.load(Ordering::SeqCst) {
                return "stopped".to_string();
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    "stopped".to_string()
}

/// Per-stream state shared between the PipeWire callbacks.
///
/// Named to avoid colliding with PipeWire's own `StreamState`, which describes
/// the connection rather than what has been captured.
#[derive(Default)]
struct StreamCapture {
    format: spa::param::audio::AudioInfoRaw,
    negotiated: bool,
}

fn run_capture_loop(
    target: &CaptureTarget,
    events: &Sender<CaptureEvent>,
    receiver: Option<pipewire::channel::Receiver<Terminate>>,
    stopping: &Arc<AtomicBool>,
) -> Result<()> {
    pipewire::init();

    let mainloop = pipewire::main_loop::MainLoopRc::new(None)
        .context("creating capture main loop")?;
    let context = pipewire::context::ContextRc::new(&mainloop, None)
        .context("creating capture context")?;
    let core = context
        .connect_rc(None)
        .context("connecting to PipeWire")?;

    let mut props = pipewire::properties::properties! {
        *pipewire::keys::MEDIA_TYPE => "Audio",
        *pipewire::keys::MEDIA_CATEGORY => "Capture",
        // Identifies Kaseta in the server's stream list, so a user can see what
        // is recording them.
        *pipewire::keys::APP_NAME => "Kaseta",
        *pipewire::keys::NODE_NAME => "kaseta-capture",
    };
    props.insert(*pipewire::keys::TARGET_OBJECT, target.node_name.clone());

    if target.capture_sink {
        // Recording playback means reading the sink's monitor ports rather than
        // its inputs. Without this, connecting to a sink captures nothing.
        props.insert(*pipewire::keys::STREAM_CAPTURE_SINK, "true");
    }

    let stream = pipewire::stream::StreamBox::new(&core, "kaseta-capture", props)
        .context("creating capture stream")?;

    let events_for_format = events.clone();
    let events_for_process = events.clone();

    let ended = Arc::new(AtomicBool::new(false));
    let ended_flag = Arc::clone(&ended);
    let state_quit = mainloop.clone();

    let _listener = stream
        .add_local_listener_with_user_data(StreamCapture::default())
        .state_changed(move |_, _, _old, new| {
            // A device disappearing moves the stream out of Streaming without
            // anything else noticing. Nothing else exits the loop on that path,
            // so without this the track would stay silently dead for the rest
            // of the meeting instead of being reattached.
            match new {
                pipewire::stream::StreamState::Error(ref reason) => {
                    tracing::warn!(%reason, "capture stream errored");
                    ended_flag.store(true, Ordering::SeqCst);
                    state_quit.quit();
                }
                pipewire::stream::StreamState::Unconnected => {
                    tracing::warn!("capture stream disconnected");
                    ended_flag.store(true, Ordering::SeqCst);
                    state_quit.quit();
                }
                _ => {}
            }
        })
        .param_changed(move |_, state, id, param| {
            let Some(param) = param else { return };
            if id != spa::param::ParamType::Format.as_raw() {
                return;
            }
            let Ok((media_type, media_subtype)) = spa::param::format_utils::parse_format(param)
            else {
                return;
            };
            if media_type != spa::param::format::MediaType::Audio
                || media_subtype != spa::param::format::MediaSubtype::Raw
            {
                return;
            }
            if state.format.parse(param).is_err() {
                tracing::error!("failed to parse negotiated audio format");
                return;
            }

            state.negotiated = true;
            let _ = events_for_format.send(CaptureEvent::Negotiated {
                sample_rate_hz: state.format.rate(),
                channels: state.format.channels() as u16,
            });
        })
        .process(move |stream, state| {
            if !state.negotiated {
                return;
            }
            let Some(mut buffer) = stream.dequeue_buffer() else {
                // Audio is lost here, and the canonical clock cannot reveal it:
                // the next buffer still arrives on schedule, so no gap appears.
                // It is reported so the chunk can carry an explicit drop count.
                tracing::warn!("no capture buffer available; audio dropped");
                let _ = events_for_process.send(CaptureEvent::Dropped);
                return;
            };

            let datas = buffer.datas_mut();
            if datas.is_empty() {
                return;
            }
            let data = &mut datas[0];
            let size = data.chunk().size() as usize;
            let Some(bytes) = data.data() else { return };
            if size == 0 || size > bytes.len() {
                return;
            }

            let samples = decode_samples(&bytes[..size], state.format.format());
            if samples.is_empty() {
                return;
            }

            // Read the clock as close to receipt as possible; every later
            // timestamp is derived from it.
            let arrived_at_ns = boottime_ns();
            let source_pts_ns = stream.time().ok().and_then(|t| ticks_to_ns(&t));

            // A send failure means the owner has gone away; the loop will be
            // torn down through the terminate channel.
            let _ = events_for_process.send(CaptureEvent::Buffer {
                samples,
                arrived_at_ns,
                source_pts_ns,
            });
        })
        .register()
        .context("registering stream listener")?;

    // Ask for signed 16-bit, matching what chunks are stored as, so the server's
    // converter does the work once rather than every chunk doing it again. Rate
    // and channels are left unset to accept the graph's native values, which
    // avoids inserting a resampler that would add its own drift.
    let mut audio_info = spa::param::audio::AudioInfoRaw::new();
    audio_info.set_format(spa::param::audio::AudioFormat::S16LE);
    let obj = spa::pod::Object {
        type_: spa::utils::SpaTypes::ObjectParamFormat.as_raw(),
        id: spa::param::ParamType::EnumFormat.as_raw(),
        properties: audio_info.into(),
    };
    let values: Vec<u8> = spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &spa::pod::Value::Object(obj),
    )
    .map_err(|e| anyhow!("serialising format request: {e:?}"))?
    .0
    .into_inner();
    let mut params = [Pod::from_bytes(&values).ok_or_else(|| anyhow!("invalid format pod"))?];

    // `RT_PROCESS` is deliberately omitted. It would run the process callback on
    // a realtime thread, where allocating or sending on a channel risks an
    // xrun. Recording tolerates a few milliseconds of scheduling latency far
    // better than it tolerates dropped audio, so the callback runs on the loop
    // thread instead.
    stream
        .connect(
            spa::utils::Direction::Input,
            None,
            pipewire::stream::StreamFlags::AUTOCONNECT
                | pipewire::stream::StreamFlags::MAP_BUFFERS,
            &mut params,
        )
        .with_context(|| format!("connecting capture stream to {}", target.node_name))?;

    // Waking the loop from another thread requires attaching the receiver to it.
    let quit_loop = mainloop.clone();
    let _terminate = receiver.map(|rx| {
        rx.attach(mainloop.loop_(), move |_| {
            quit_loop.quit();
        })
    });

    // A reconnected stream has no terminate channel of its own, so a stop is
    // noticed by polling the shared flag on a timer.
    let poll_quit = mainloop.clone();
    let stop_check = Arc::clone(stopping);
    let _timer = {
        let timer = mainloop.loop_().add_timer(move |_| {
            if stop_check.load(Ordering::SeqCst) {
                poll_quit.quit();
            }
        });
        let _ = timer.update_timer(
            Some(Duration::from_millis(100)),
            Some(Duration::from_millis(100)),
        );
        timer
    };

    mainloop.run();

    // Drain anything the server still holds before the thread exits, so a
    // deliberate stop does not truncate the final buffer.
    let _ = stream.flush(true);
    let _ = stream.disconnect();

    // Distinguishes "the device went away" from "we were asked to stop", which
    // is what decides whether to reattach.
    if ended.load(Ordering::SeqCst) && !stopping.load(Ordering::SeqCst) {
        bail!("the capture device stopped providing audio");
    }
    Ok(())
}

/// Converts a raw PipeWire buffer into interleaved 16-bit samples.
///
/// S16LE is what gets requested, but the server is free to negotiate something
/// else, so the common float case is handled rather than producing silence.
fn decode_samples(bytes: &[u8], format: spa::param::audio::AudioFormat) -> Vec<i16> {
    use spa::param::audio::AudioFormat;

    match format {
        AudioFormat::S16LE => bytes
            .chunks_exact(2)
            .map(|b| i16::from_le_bytes([b[0], b[1]]))
            .collect(),
        AudioFormat::F32LE => bytes
            .chunks_exact(4)
            .map(|b| f32_to_i16(f32::from_le_bytes([b[0], b[1], b[2], b[3]])))
            .collect(),
        AudioFormat::S32LE => bytes
            .chunks_exact(4)
            .map(|b| (i32::from_le_bytes([b[0], b[1], b[2], b[3]]) >> 16) as i16)
            .collect(),
        other => {
            tracing::error!(?other, "unsupported capture sample format");
            Vec::new()
        }
    }
}

/// Scales a normalised float sample to 16-bit, clamping rather than wrapping.
///
/// Wrapping would turn a moment of loudness into a burst of noise, which is
/// both audible and damaging to transcription accuracy.
fn f32_to_i16(sample: f32) -> i16 {
    (sample.clamp(-1.0, 1.0) * i16::MAX as f32) as i16
}

/// Converts a stream time report into nanoseconds of stream position.
fn ticks_to_ns(time: &pipewire::stream::Time) -> Option<u64> {
    let rate = time.rate();
    if rate.denom == 0 {
        return None;
    }
    // `ticks` counts at `rate`, usually 1/samplerate.
    Some((time.ticks() as u128 * 1_000_000_000u128 * rate.num as u128 / rate.denom as u128) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use spa::param::audio::AudioFormat;

    #[test]
    fn decodes_signed_16_bit_natively() {
        // Little-endian: 0x0000 = 0, 0x7FFF = i16::MAX, 0x8000 = i16::MIN.
        let bytes = [0x00, 0x00, 0xFF, 0x7F, 0x00, 0x80];
        assert_eq!(
            decode_samples(&bytes, AudioFormat::S16LE),
            vec![0, i16::MAX, i16::MIN]
        );
    }

    #[test]
    fn decodes_float_samples() {
        let mut bytes = Vec::new();
        for v in [0.0f32, 1.0, -1.0, 0.5] {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        let samples = decode_samples(&bytes, AudioFormat::F32LE);

        assert_eq!(samples[0], 0);
        assert_eq!(samples[1], i16::MAX);
        assert_eq!(samples[2], -i16::MAX);
        assert!((samples[3] - i16::MAX / 2).abs() <= 1);
    }

    #[test]
    fn clamps_rather_than_wrapping_on_overload() {
        // A float stream can exceed unity; wrapping would turn loudness into
        // noise.
        let mut bytes = Vec::new();
        for v in [2.5f32, -2.5] {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        let samples = decode_samples(&bytes, AudioFormat::F32LE);
        assert_eq!(samples, vec![i16::MAX, -i16::MAX]);
    }

    #[test]
    fn truncates_32_bit_samples_to_16() {
        let mut bytes = Vec::new();
        for v in [0i32, i32::MAX, i32::MIN] {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        assert_eq!(
            decode_samples(&bytes, AudioFormat::S32LE),
            vec![0, i16::MAX, i16::MIN]
        );
    }

    #[test]
    fn ignores_a_trailing_partial_sample() {
        // A buffer size that is not a whole number of samples must not panic.
        let bytes = [0x00, 0x00, 0xFF];
        assert_eq!(decode_samples(&bytes, AudioFormat::S16LE), vec![0]);
    }

    #[test]
    fn an_unsupported_format_yields_no_samples_rather_than_noise() {
        let bytes = [0u8; 16];
        assert!(decode_samples(&bytes, AudioFormat::U8).is_empty());
    }
}

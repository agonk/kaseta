//! Audio capture.
//!
//! Split so that everything decision-bearing is testable without an audio
//! server: [`chunk`] owns buffering, timing, and encoding, while the PipeWire
//! layer does nothing but move buffers and report device state.

pub mod chunk;
pub mod devices;
pub mod session;
pub mod stream;

// The capture surface, re-exported for callers. Not every item has a consumer
// inside the binary yet, so the module-level allow keeps the public shape
// intact without warning noise.
#[allow(unused_imports)]
pub use chunk::{CapturedBuffer, ChunkConfig, ChunkWriter, SealedChunk, DEFAULT_CHUNK_DURATION_S};
#[allow(unused_imports)]
pub use devices::{list_devices, AudioDevice, DeviceKind};

//! The daemon's storage and audio-export core.
//!
//! Everything else in the daemon lives in the binary. These modules are a
//! library as well so that tests which must own their whole process can reach
//! them: the export memory bound is measured with a counting allocator, and an
//! allocator is process-wide, so that test runs in its own test binary under
//! `tests/` rather than among the binary's unit tests.

pub mod blobstore;
pub mod clock;
pub mod export;
pub mod flac;
pub mod staging;

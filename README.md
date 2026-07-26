# Kaseta

A Linux meeting recorder that captures both sides of a conversation, transcribes
it locally, and summarises it — without a browser extension, a bot joining your
call, or audio leaving the machine.

> Working name. Status: **in development, not yet usable end to end.**

## What it does

Records your microphone and your system playback as **two separate tracks**.
That separation is the design's centre of gravity: everything on the microphone
track was said by you, everything on the playback track was said by the far end,
so a transcript is attributed without any speaker model. It also means the two
sides can be re-mixed, re-transcribed, or diarized later without re-recording.

Works with anything that makes sound — Zoom, Meet, Teams, a native desktop app,
a browser tab — because it captures at the audio-server level rather than
hooking a specific application.

## Design constraints

**Memory.** A daemon holds the recording and the job pipeline; the interface is
a static page it serves, so closing the interface leaves no process resident.
Transcription runs in a child process that exits when the job finishes, making
its steady-state cost zero.

**Storage is addressed, not pathed.** Every byte is written through a
`BlobKey` — an opaque object key — never a filesystem path. The local directory
layout and an S3/R2 bucket layout are byte-identical, so moving between them is
a copy rather than a translation.

**Timing is measured, not assumed.** A microphone and a sink monitor run on
independent hardware clocks and drift apart over a long meeting. Every chunk
records the canonical clock (`CLOCK_BOOTTIME`, which unlike `CLOCK_MONOTONIC`
keeps counting across suspend) at its first and last sample, so alignment is
computed from measured time. Drift stays correctable rather than baked in.

**Crashes cost one chunk.** Audio is sealed into short immutable chunks as it is
captured. A daemon killed mid-recording loses at most the chunk in flight, and
jobs orphaned by the crash are swept back onto the queue at startup.

## Layout

```
crates/
  kaseta-contracts/   Types crossing a process or storage boundary. No I/O.
  kasetad/            The daemon: capture, storage, jobs, local API.
docs/
  PLAN.md             Architecture and milestones.
  CONTRACTS-DRAFT.md  Schemas and protocols.
```

## Requirements

- Linux with PipeWire (any current desktop; developed against 1.0+)
- Rust stable

Capture needs a real PipeWire session, so it cannot run on a headless server.

## Try it

```bash
cargo run -p kasetad -- doctor     # can this machine record?
cargo run -p kasetad -- devices    # what can it record from?
cargo test                         # 60 tests, no audio hardware needed
```

`doctor` reports whether both a microphone and a playback monitor are present —
the two devices a meeting recording needs.

## Licence

AGPL-3.0-only.

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

## Install

Kaseta needs a Rust toolchain, a C compiler toolchain for the PipeWire bindings,
and PipeWire itself.

**`clang` is not optional.** The `pipewire-sys` crate generates its bindings with
bindgen, which loads `libclang` at build time. Without it the build fails partway
through the dependency tree with an error that does not mention clang.

### Arch / EndeavourOS

```bash
sudo pacman -S --needed rustup clang pkgconf pipewire base-devel
rustup default stable
```

`base-devel` provides the C compiler that builds the bundled SQLite. It is
already present on a normal EndeavourOS install, and `--needed` makes listing it
a no-op if so.

Arch ships C headers inside the main package rather than a separate `-dev` one,
so `pipewire` covers both the runtime and the headers.

### Debian / Ubuntu

```bash
sudo apt install -y libpipewire-0.3-dev libclang-dev pkg-config build-essential
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
```

### Fedora

```bash
sudo dnf install -y pipewire-devel clang-devel pkgconf-pkg-config
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
```

### Verify

```bash
pkg-config --modversion libpipewire-0.3   # expect 1.0 or newer
cargo --version
```

Nothing else is vendored in: SQLite is compiled from source by `rusqlite`, so the
first build is slow and later ones are incremental.

## Run

```bash
cargo run -p kasetad -- doctor     # can this machine record?
cargo run -p kasetad -- devices    # what can it record from?
cargo run -p kasetad -- record 60  # record both sides for 60 seconds
cargo test                         # 70 tests, no audio hardware needed
```

`doctor` reports whether both a microphone and a playback monitor are present —
the two devices a meeting recording needs — and explains what is missing if not.

`record` writes FLAC chunks and a manifest under `./data` (override with
`KASETA_DATA`), then reports per-track chunk counts, duration, and **measured
clock drift**. Drift beyond ±20 ms means a merged audio export would need
resampling; transcripts are unaffected either way.

Use headphones when testing. On speakers the far end bleeds back into the
microphone track, which degrades the local-versus-remote attribution the
transcript depends on.

### Environment

| Variable | Default | Purpose |
|---|---|---|
| `KASETA_DATA` | `./data` | Where recordings are written |
| `KASETA_LOG` | `kasetad=info` | Log filter, e.g. `kasetad=debug` |

### Headless machines

Capture needs a live PipeWire session, so it cannot run on a server or in a
container without one. `doctor` detects this and says so rather than failing
partway into a recording. The crates still build and the full test suite still
passes there, so a headless box is fine for development — just not for capture.

## Licence

AGPL-3.0-only.

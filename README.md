# Kaseta

A Linux meeting recorder that captures both sides of a conversation, transcribes
it locally, and summarises it — without a browser extension, a bot joining your
call, or audio leaving the machine.

> Working name.

## What it does

Records a meeting, transcribes it locally, and summarises it. Start and stop
from the window or a keyboard shortcut; the transcript and summary appear on
their own.

Optionally backs recordings up to S3-compatible storage — Cloudflare R2,
Backblaze, MinIO — and expires old ones on a policy you set, either deleting
them outright or keeping the transcript and discarding the audio.

Records your microphone and your system playback as **two separate tracks**.
That separation is the design's centre of gravity: everything on the microphone
track was said by you, everything on the playback track was said by the far end,
so **every line of the transcript already knows who said it** — no diarization,
no speaker model. It also means the two sides can be re-mixed, re-transcribed,
or diarized later without re-recording.

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

**Devices can come and go.** Unplugging a headset mid-meeting reattaches capture
rather than ending that track. The gap is recorded as a discontinuity and padded,
so everything afterwards keeps its true position on the timeline.

## Layout

```
crates/
  kaseta-contracts/   Types crossing a process or storage boundary. No I/O.
  kasetad/            The daemon: capture, storage, jobs, local API, interface.
worker/               Transcription worker. Short-lived, spawned per job.
packaging/            Desktop entry, user service, icon.
docs/
  PLAN.md             Architecture and milestones.
  CONTRACTS-DRAFT.md  Schemas and protocols.
```

## Install

```bash
git clone git@github.com:agonk/kaseta.git
cd kaseta
./install.sh
```

That builds Kaseta, installs it under `~/.local`, sets up the transcription
worker, and enables the recorder as a user service so it starts with your
session. **Kaseta** then appears in your launcher as an ordinary application,
with its own window and icon. Nothing runs as root and nothing is written
outside your home directory.

Open **Settings** in the application to configure everything: an
[OpenRouter](https://openrouter.ai/keys) key for summaries, S3-compatible cloud
backup, and how long recordings are kept. Without a key, meetings are still
recorded and transcribed — only the summary is skipped.

Summaries are off until you switch them on, and **only that switch decides**.
Supplying a key through the environment does not turn them on, because that
switch is what the privacy note in the window reports on, and a control
something else can overrule is not a control. It is the only path by which
transcript text reaches a third party; transcription itself is local either
way, and cloud backup — the other thing that leaves the machine — has a switch
of its own.

Recording can also be started without opening the window. Bind
`kaseta-tray toggle` to a key in your desktop's shortcut settings, or use the
**Kaseta — Start or stop recording** entry your launcher now has.

Credentials are written to `~/.config/kaseta/settings.json`, readable only by
you, and deliberately **not** in the data directory — that is the directory
cloud backup uploads, and a key does not belong in a bucket.

### What it installs

| | |
|---|---|
| `~/.local/bin/kasetad` | the recorder |
| `~/.local/share/applications/kaseta.desktop` | launcher entry |
| `~/.config/systemd/user/kaseta.service` | starts it with your session |
| `~/.local/share/kaseta/` | recordings, transcripts, database |
| `~/.config/kaseta/settings.json` | keys and cloud settings, mode 0600 |
| `~/.config/kaseta/env` | optional environment overrides |

### Uninstall

```bash
systemctl --user disable --now kaseta.service
rm -f ~/.local/bin/kasetad ~/.local/bin/kaseta-open \
      ~/.local/share/applications/kaseta.desktop \
      ~/.config/systemd/user/kaseta.service
```

Recordings in `~/.local/share/kaseta/` are left alone; delete that directory
too if you want them gone.

## Build prerequisites

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

## Command line

Everything the application does is also available directly, which is useful for
diagnosing a machine that cannot record.

```bash
cargo run -p kasetad -- doctor     # can this machine record?
cargo run -p kasetad -- devices    # what can it record from?
cargo run -p kasetad -- record 60  # record both sides for 60 seconds
cargo run -p kasetad -- transcribe # transcribe the most recent recording
cargo run -p kasetad -- serve      # run the daemon and its interface
cargo test                         # no audio hardware needed
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
| `KASETA_PORT` | `7777` | Port the interface is served on |
| `KASETA_WORKER_PYTHON` | `worker/.venv/bin/python` | Interpreter with the transcription worker |
| `KASETA_OPENROUTER_KEY` | — | Key for summaries; a key alone does not switch them on |
| `KASETA_OPENROUTER_MODEL` | `anthropic/claude-haiku-4.5` | Model used for summaries |

Most of these are better set in **Settings**, which writes them to
`~/.config/kaseta/settings.json` with owner-only permissions. Credentials are
deliberately kept out of the data directory, since that is what cloud backup
uploads.

Installed setups read these from `~/.config/kaseta/env`. The environment wins
over anything saved in Settings, so a key exported for a one-off run is not
silently overridden.

### Headless machines

Capture needs a live PipeWire session, so it cannot run on a server or in a
container without one. `doctor` detects this and says so rather than failing
partway into a recording. The crates still build and the full test suite still
passes there, so a headless box is fine for development — just not for capture.

## Licence

AGPL-3.0-only.

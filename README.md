# Kaseta

A Linux meeting recorder that captures both sides of a conversation, transcribes
it on your own machine, and summarises it — without a browser extension and
without a bot joining your call. Nothing leaves the computer unless you switch
on summaries or cloud backup, and both are off until you do.

> Working name. Alpha: it works, and it has not been through many hands yet.

![The Kaseta window: a recording with its transcript and summary](assets/library.png)

A recording, its transcript attributed line by line, and the summary drawn from
it. The chips along the top say what has happened to it and what has not.

## What it does

Records a meeting and, if you want, transcribes and summarises it. Start and
stop from the window or a keyboard shortcut; whatever you switched on appears on
its own.

Records your microphone and your system playback as **two separate tracks**.
That separation is the design's centre of gravity: everything on the microphone
track was said by you, everything on the playback track was said by the far end,
so **every line of the transcript already knows who said it** — no diarization,
no speaker model. It also means the two sides can be re-mixed, re-transcribed,
or diarized later without re-recording.

Works with anything that makes sound — Zoom, Meet, Teams, a native desktop app,
a browser tab — because it captures at the audio-server level rather than
hooking a specific application.

Optionally copies recordings to S3-compatible storage — Cloudflare R2,
Backblaze, MinIO — and expires old ones on a policy you set.

Also takes an audio or video file you already have, a recorded talk, a
voice memo, a meeting someone else recorded, and turns it into a recording like
any other: transcribed, summarised, playable, backed up. See
[Importing a file](#importing-a-file).

## Importing a file

Press **Import** next to the record button, or drop a file anywhere on the
window. The dialog shows the file, a title to change if you like, and a **Keep
original file** switch; the upload shows its progress and can be cancelled. The
recording appears in the library straight away with an **Import** chip, which
turns into the usual Transcript and Summary chips once the file is decoded. If
decoding fails, the chip says why and offers to retry.

From a terminal, with the daemon running:

```bash
kasetad import talk.mp4                       # title taken from the file name
kasetad import interview.mkv --title "Interview with Ana" --keep-original
```

It sends the file to the daemon on `KASETA_PORT` (7777 unless set), prints the
new recording's id, and waits until the file is decoded or fails, saying which.

**Supported:** MP4/MOV/M4A/3GP, MKV/WebM, MP3, WAV/W64, FLAC, Ogg/Opus, AAC,
WMA/ASF, AVI, MPEG-TS, MPEG-PS, CAF, AIFF, AMR, AC-3/E-AC-3 and AU. Other formats
are rejected. What a file is gets decided by reading it, not by its extension,
so an MP3 named `.mp4` imports and a text file named `.mp4` does not. A video
contributes its default audio track (or its first, if none is marked default);
the picture is not used.

**An imported file is one track.** Kaseta's attribution comes from recording
each side of a call separately, and a file has already mixed everyone together.
Its transcript lines are therefore not attributed to anyone and read as
**Speaker**, and its summary is written without a "me and them" framing. The
lines are not split by voice.

**Keep original file** is off by default. Without it, Kaseta keeps the audio it
decoded and discards the file once the import succeeds. With it, the file is
stored with the recording: a video plays in the recording's view, alongside its
transcript, and **Download original** gives it back under its own name. A kept
original is part of the recording, so cloud backup copies it, and removing local
copies after upload or keeping transcripts only under retention removes it from
this machine just as it removes the audio.

**Limits.** An upload larger than 8 GB, or a file longer than 6 hours, is
refused. Both are set in `~/.config/kaseta/settings.json`:

```json
{ "imports": { "max_upload_gb": 8, "max_duration_hours": 6 } }
```

The upload ceiling can be raised to 64 GB and the duration to 24 hours. An
upload also needs its own size plus 1 GB free on the disk holding your
recordings, and decoding checks for room as it goes, so a full disk fails the
import with a message rather than filling up. One file uploads at a time.

**Contained.** A file from elsewhere is untrusted input, and ffmpeg is a large
parser. Kaseta runs `ffprobe` and `ffmpeg` inside a
[bubblewrap](https://github.com/containers/bubblewrap) sandbox with no network,
nothing of your home directory but a read-only view of the upload's own
folder, and they read only the formats listed above. Importing is unavailable without
bubblewrap rather than running unsandboxed, and the window and `kasetad doctor`
say what to install.

## Webhook

A finished recording can be posted to a URL of your choosing as JSON — a task
tracker, a notes system, a script you wrote. Off until you switch it on, in
**Settings**: a URL, a token, and when to send — as soon as the transcript is
ready, after the summary too, or only when you press **Send** on a recording.

The URL is used exactly as you write it, path included, and the reply is not
interpreted beyond whether it succeeded. A recorder that appended a path or
insisted on a particular response would work with one receiver and silently fail
with every other.

The request is a POST with your token as a bearer credential:

```json
{
  "recording_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
  "transcript_fingerprint": "9f2a1c4e7b03",
  "title": "Weekly sync",
  "recorded_at": "2026-07-28T09:15:00Z",
  "duration_s": 1820.0,
  "transcript": "You: I will send the report by Friday.\nThem: Thanks.\n",
  "summary": { }
}
```

Every line of the transcript is attributed, because each side is recorded on its
own track — `You` is whoever is at this machine, `Them` is the far end.

An imported file carries two more fields, and its lines read `Speaker`, since a
file has every voice on one track:

```json
{
  "source": "import",
  "original_filename": "all-hands.mp4",
  "recorded_at": "2026-07-01T16:00:00Z",
  "transcript": "Speaker: Welcome, everyone.\n"
}
```

`source` is `capture` for a recording made here and `import` for a file.
`original_filename` is present only for imports. For an import, `recorded_at` is
the date the file says it was made when it says one, and the time it was
imported otherwise.

The token should be scoped to depositing recordings and nothing else, which is
the right shape for something living on a laptop.

Only the transcript and the summary are sent. The audio never is.

## What leaves your machine

Nothing, until you switch something on. Each of these is a separate switch in
**Settings**.

| | Where it runs | Default |
|---|---|---|
| Recording | This machine | — |
| Importing a file | This machine; the file goes only to the local daemon | n/a |
| Transcription | This machine, in a local model | On |
| Summaries | Sends **transcript text** to a provider you choose | **Off** |
| Cloud backup | Sends audio, transcripts, summaries and kept originals to your bucket | **Off** |
| Webhook | Sends **transcript text** to a URL you choose | **Off** |

Three of these send data off the machine, and they send different things.
**Summaries** send transcript text to a provider you choose. **Cloud backup**
sends everything to storage you control: audio, transcripts, summaries, and the
original of an imported file you chose to keep. **Webhook** sends
transcript text to a URL you choose. None happens unless you switch it on.

For summaries, **only that switch decides**. Supplying an API key through the
environment does not turn them on: a control something else can quietly overrule
is not a control.

When summaries are on, requests decline any provider that retains what it is
sent. Open-weight models are served by many companies at once and a router would
otherwise pick between them per request, which makes "who has this transcript"
unanswerable.

Credentials live in `~/.config/kaseta/` — in `settings.json` when saved through
the interface, readable only by you, or in `env` if you would rather set them
there. Deliberately **not** in the data directory: that is what cloud backup
uploads, and a key does not belong in a bucket.

## Design constraints

**Memory.** A daemon holds the recording and the job pipeline; the interface is
a static page it serves, so closing the interface leaves no process resident.
Transcription runs in a child process that exits when the job finishes, making
its steady-state cost zero.

**Storage is addressed, not pathed.** Every byte is written through a
`BlobKey` — an opaque object key — never a filesystem path. The local directory
layout and an S3/R2 bucket layout are byte-identical, so moving between them is
a copy rather than a translation.

**Derived work lives beside what it came from.** Transcripts, summaries and a
title you typed are written as objects under the recording's own prefix, not
only into the index. The index is rebuildable from storage — delete the database
and it comes back — which is only true if everything worth keeping is in
storage. It is also what makes a backup a backup: a bucket holding audio and
nothing made from it would mean transcribing every meeting again.

**Timing is measured, not assumed.** A microphone and a sink monitor run on
independent hardware clocks and drift apart over a long meeting. Every chunk
records the canonical clock (`CLOCK_BOOTTIME`, which unlike `CLOCK_MONOTONIC`
keeps counting across suspend) at its first and last sample, so alignment is
computed from measured time. Drift stays correctable rather than baked in.

**Crashes cost one chunk.** Audio is sealed into short immutable chunks as it is
captured. A daemon killed mid-recording loses at most the chunk in flight, and
jobs orphaned by the crash are swept back onto the queue at startup.

**Devices can come and go.** Unplugging a headset mid-meeting reattaches capture
rather than ending that track. The gap is recorded as a discontinuity and
padded, so everything afterwards keeps its true position on the timeline.

**Backup does not wait on anything.** It is queued when a recording is sealed,
not behind transcription — a recording whose transcription failed is the one
most worth having a copy of. Whatever arrives later brings the recording round
for another pass.

**Large files never sit in memory.** An imported file is streamed to disk as it
arrives, decoded in small pieces, and exported and backed up from disk, with
anything over 64 MB sent to the bucket in parts. A three-hour video costs disk
space, not memory.

**A stage that does nothing says so.** Switching summaries off does not make the
pipeline report a summary; it reports a skip, with the reason, and offers to run
it once you change your mind.

## Requirements

- Linux with **PipeWire** (any current Arch, Fedora, or Ubuntu 22.10+)
- **Rust** toolchain, a **C compiler**, and **`clang`**
- **Python 3.10+** with `venv`, for the transcription worker

Capture needs a live PipeWire session, so this cannot run on a headless server.
`kasetad doctor` detects that and says so rather than failing halfway into a
recording. The crates still build and the tests still pass there, so a headless
box is fine for development — just not for capture.

**`clang` is not optional.** The `pipewire-sys` crate generates its bindings with
bindgen, which loads `libclang` at build time. Without it the build fails partway
through the dependency tree with an error that does not mention clang.

**Importing files** additionally needs **ffmpeg** (with `ffprobe`) and
**bubblewrap**, both from your distribution's packages so they live under
`/usr`. Recording works without them; `./install.sh` warns when they are missing
and prints the command for your distribution.

### Arch / EndeavourOS

```bash
sudo pacman -S --needed rustup clang pkgconf pipewire python base-devel
rustup default stable
sudo pacman -S --needed ffmpeg bubblewrap   # for importing files
```

Arch ships C headers inside the main package rather than a separate `-dev` one,
so `pipewire` covers both the runtime and the headers.

### Debian / Ubuntu

```bash
sudo apt install -y libpipewire-0.3-dev libclang-dev pkg-config build-essential \
                    python3 python3-venv
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
sudo apt install -y ffmpeg bubblewrap   # for importing files
```

### Fedora

```bash
sudo dnf install -y pipewire-devel clang-devel pkgconf-pkg-config python3
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
sudo dnf install -y ffmpeg bubblewrap   # for importing files; ffmpeg is in RPM Fusion
```

## Install

```bash
git clone https://github.com/agonk/kaseta.git
cd kaseta
./install.sh
```

That builds Kaseta, installs it under `~/.local`, creates the transcription
worker's virtual environment, and enables the recorder as a user service so it
starts with your session. **Kaseta** then appears in your launcher as an
ordinary application. Nothing runs as root and nothing is written outside your
home directory.

> **Keep the checkout.** The transcription worker's virtual environment lives in
> `worker/.venv` inside this directory, and its path is recorded in
> `~/.config/kaseta/env`. Moving or deleting the checkout breaks transcription
> until you run `./install.sh` again from wherever it ended up. Recordings are
> unaffected — they live in `~/.local/share/kaseta/`.

Re-running `./install.sh` is how you upgrade. It rebuilds, reinstalls, restarts
the service, and rebuilds the worker environment if the Python it was built
against has gone — which a rolling distribution does on a point-release upgrade.

Recording can also be started without opening the window. Bind
`kaseta-tray toggle` to a key in your desktop's shortcut settings, or use the
**Kaseta — Start or stop recording** entry in your launcher.

### Configuring it

Everything is set from the application — there is no configuration file to edit
by hand, except for the two [import limits](#importing-a-file), which few people
will need to change.

![Kaseta's settings: transcription, summaries, cloud backup and retention](assets/settings.png)

Each switch governs one thing that would otherwise happen without being asked.
Transcription runs locally and is on; summaries and cloud backup are off until
you turn them on, because both send something off the machine.

### What it installs

| | |
|---|---|
| `~/.local/bin/kasetad` | the recorder |
| `~/.local/bin/kaseta-open` | opens the window |
| `~/.local/bin/kaseta-tray` | start/stop from a shortcut |
| `~/.local/share/applications/kaseta.desktop` | launcher entry |
| `~/.local/share/applications/kaseta-record.desktop` | start/stop entry |
| `~/.local/share/icons/hicolor/scalable/apps/kaseta.svg` | icon |
| `~/.local/share/icons/hicolor/scalable/apps/kaseta-symbolic.svg` | symbolic icon |
| `~/.config/systemd/user/kaseta.service` | starts it with your session |
| `~/.config/kaseta/` | config directory; `settings.json` appears here, mode 0600, once you save settings |
| `~/.config/kaseta/env` | environment for the service |
| `~/.local/share/kaseta/` | recordings, transcripts, database |
| `worker/.venv/` | in this checkout — see the note above |

### Uninstall

```bash
systemctl --user disable --now kaseta.service
rm -f ~/.local/bin/kasetad ~/.local/bin/kaseta-open ~/.local/bin/kaseta-tray \
      ~/.local/share/applications/kaseta.desktop \
      ~/.local/share/applications/kaseta-record.desktop \
      ~/.local/share/icons/hicolor/scalable/apps/kaseta*.svg \
      ~/.config/systemd/user/kaseta.service
rm -rf ~/.config/kaseta
```

Recordings in `~/.local/share/kaseta/` are left alone; delete that directory too
if you want them gone. The worker's virtual environment lives in this checkout,
so removing the checkout removes it.

## Command line

Everything the application does is also available directly, which is useful for
diagnosing a machine that cannot record.

```bash
kasetad doctor        # can this machine record, and import?
kasetad devices       # what can it record from?
kasetad record 60     # record both sides for 60 seconds
kasetad import FILE   # turn an audio or video file into a recording
kasetad transcribe    # transcribe the most recent recording
kasetad export        # write mixed and per-track audio files
kasetad serve         # run the daemon and its interface
cargo test            # no audio hardware needed
```

`doctor` reports whether both a microphone and a playback monitor are present —
the two devices a meeting recording needs — and explains what is missing if not.
It also reports where ffmpeg, ffprobe and bubblewrap were found, their versions,
and whether importing works, or what to install if it does not.

`import` takes `--title TEXT` and `--keep-original`; see
[Importing a file](#importing-a-file).

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
| `KASETA_PORT` | `7777` | Port the interface is served on, and the one `kasetad import` sends to |
| `KASETA_WORKER_PYTHON` | `worker/.venv/bin/python` | Interpreter with the transcription worker |
| `KASETA_OPENROUTER_KEY` | — | Key for summaries; a key alone does not switch them on |
| `KASETA_OPENROUTER_MODEL` | `anthropic/claude-haiku-4.5` | Model used for summaries |

Most of these are better set in **Settings**. Installed setups read the
environment from `~/.config/kaseta/env`, which wins over anything saved in
Settings, so a key exported for a one-off run is not silently overridden.

## Layout

```
crates/
  kaseta-contracts/   Types crossing a process or storage boundary. No I/O.
  kasetad/            The daemon: capture, storage, jobs, local API, interface.
worker/               Transcription worker. Short-lived, spawned per job.
packaging/            Desktop entries, user service, icon.
install.sh            Build, install, enable.
```

## Not built yet

Roughly in the order they would be worth doing.

- **An index at the bucket root**, mapping object prefixes to titles and dates,
  so a bucket is navigable without opening every folder.
- **A packaged install** — an AUR package, so upgrading does not mean keeping a
  checkout around.
- **Reading audio back from the bucket**, so removing local copies can reclaim
  everything rather than keeping the mixed export for playback.
- **Video capture.**
- **Windows and macOS.** Most of the stack above capture is not inherently
  Linux-specific, but it has never been built elsewhere, so treat that as
  untested. Each platform would need its own two-track capture backend — WASAPI
  loopback on Windows, ScreenCaptureKit or a virtual device on macOS.

Nothing here is promised, and there is no schedule.

## Legal

**Recording other people is regulated, and the rules differ by country and by
state.** Some places require every participant to consent; some require only
one; some distinguish private conversations from business calls, or add
obligations when a recording contains personal data. This software does not know
where you are, who is on the call, or what applies to you, and it makes no
attempt to enforce any of it.

**Complying is your responsibility, not the author's.** You are responsible for
obtaining whatever consent is required, for telling participants they are being
recorded where that is required, and for how you store, transmit and dispose of
what you capture — including anything sent to a summary provider or uploaded to
your own cloud storage.

This is a plain description of how the software behaves, not legal advice. If
you are unsure whether you may record a particular conversation, ask someone
qualified before you do.

The licence disclaims warranties and limits liability to the extent the law
allows. Nothing in this file changes that.

## Licence

Apache License 2.0. See [LICENSE](LICENSE).

Permissive: use it, change it, ship it in something commercial, keep your
changes private. It comes with **no warranty of any kind**, and the author is
not liable for anything arising from its use.

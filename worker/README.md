# Kaseta transcription worker

Reads a job spec on stdin, writes a result on stdout, exits. The daemon spawns
one per job, so transcription costs nothing while idle.

## Install

```bash
cd worker
python -m venv .venv
.venv/bin/pip install -e .
```

Then point the daemon at it:

```bash
export KASETA_WORKER_PYTHON="$PWD/.venv/bin/python"
```

The daemon looks for `.venv/bin/python` beside this file if that variable is
unset, so the layout above needs no configuration.

## Model

Defaults to **`nemo-parakeet-tdt-0.6b-v3`, int8-quantised** — NVIDIA's Parakeet
TDT 0.6B v3, converted to ONNX. For English on a CPU-only machine it is both
more accurate and several times faster than Whisper large-v3: the difference
between transcription finishing in a fraction of the meeting's length and taking
longer than the meeting did.

Downloads on first use and is cached by the Hugging Face hub. Expect roughly
600 MB, plus a small **Silero** voice-activity model.

Voice activity detection is what splits audio into utterances with start and end
times. Without it the recogniser returns one block of text per file, which
cannot be placed on a timeline or interleaved with the other speaker. If it
fails to load, the worker falls back to grouping per-token timestamps at pauses.

Other models are reachable by setting `params.model` to any identifier
`onnx_asr.load_model` accepts — `whisper-base`, `nemo-parakeet-tdt-0.6b-v2`, and
the `gigaam` family among them. That matters for languages Parakeet does not
cover well.

## Contract

Input is `transcribe-tracks/v1`, output is `worker-result/v1`; both live in
`crates/kaseta-contracts/src/worker.rs`.

The worker is handed **merged per-track audio**, not chunks. Merging already
concatenated the chunks and padded every dropped-audio gap with silence of
exactly the missing duration, so the file is a linear timeline and the worker
can return plain offsets from its start. It knows nothing about the canonical
clock, chunks, or manifests — the daemon maps offsets onto the timeline, so
changing how time is tracked never requires changing this.

Audio arrives as **mono 16 kHz WAV**. The recogniser reads WAV through Python's
standard library and accepts mono only, while archives are stereo FLAC, so the
daemon converts — which also keeps this package free of any audio-decoding
dependency.

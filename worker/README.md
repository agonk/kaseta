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

Defaults to **Parakeet TDT 0.6B v3, int8-quantised**. For English on a CPU-only
machine it is both more accurate and several times faster than Whisper
large-v3 — the difference between transcription finishing in a fraction of the
meeting's length and taking longer than the meeting did.

The model downloads on first use and is cached by the Hugging Face hub. Expect
roughly 600 MB.

Whisper is reachable through the same interface by setting `params.model`, which
matters for languages Parakeet does not cover well.

## Contract

Input is `transcribe-tracks/v1`, output is `worker-result/v1`; both live in
`crates/kaseta-contracts/src/worker.rs`.

The worker is handed **merged per-track audio**, not chunks. Merging already
concatenated the chunks and padded every dropped-audio gap with silence of
exactly the missing duration, so the file is a linear timeline and the worker
can return plain offsets from its start. It knows nothing about the canonical
clock, chunks, or manifests — the daemon maps offsets onto the timeline, so
changing how time is tracked never requires changing this.

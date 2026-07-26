"""Kaseta transcription worker.

Reads a job spec on stdin, writes a result on stdout, exits. The daemon spawns
one per job, so transcription costs nothing while idle — the model is loaded,
used, and released with the process.

The worker is deliberately ignorant of almost everything: it does not know about
the canonical clock, chunks, gaps, or manifests. It is handed audio and returns
offsets relative to the start of that audio. Keeping the clock arithmetic in the
daemon means changing how time is tracked never requires a matching change here.
"""

from __future__ import annotations

import json
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path

RESULT_VERSION = "worker-result/v1"
SPEC_VERSION = "transcribe-tracks/v1"

NS_PER_SECOND = 1_000_000_000


@dataclass(frozen=True)
class Track:
    track_id: str
    speaker_hint: str
    audio_path: Path


def fail(code: str, message: str, *, job_id: str = "", retryable: bool = False) -> int:
    """Reports a failure the daemon can act on, and exits non-zero.

    The daemon distinguishes retryable from terminal, so a missing model (fix it
    and retry) is not treated like a corrupt file (retrying cannot help).
    """
    json.dump(
        {
            "contract_version": RESULT_VERSION,
            "job_id": job_id,
            "status": "error",
            "tracks": [],
            "error": {"code": code, "message": message, "retryable": retryable},
        },
        sys.stdout,
    )
    sys.stdout.flush()
    return 1


def resolve_storage(storage: dict) -> Path:
    """Turns the spec's storage descriptor into a local root directory."""
    kind = storage.get("kind")
    if kind != "local_fs":
        raise ValueError(f"unsupported storage kind: {kind!r}")
    root = storage.get("root")
    if not root:
        raise ValueError("local_fs storage has no root")
    return Path(root)


def parse_spec(raw: str) -> tuple[str, list[Track], dict]:
    spec = json.loads(raw)

    version = spec.get("contract_version")
    if version != SPEC_VERSION:
        # Guessing at an unknown spec risks transcribing the wrong audio or
        # misreporting timings; refusing is the safe response.
        raise ValueError(f"unsupported spec version: {version!r}")

    job_id = spec.get("job_id", "")
    root = resolve_storage(spec.get("storage", {}))

    tracks = []
    for t in spec.get("tracks", []):
        path = root / t["audio"]
        if not path.is_file():
            raise FileNotFoundError(f"audio for {t['track_id']} is missing: {t['audio']}")
        tracks.append(
            Track(
                track_id=t["track_id"],
                speaker_hint=t.get("speaker_hint", "unknown"),
                audio_path=path,
            )
        )

    if not tracks:
        raise ValueError("the spec lists no tracks")

    return job_id, tracks, spec.get("params", {})


def load_model(params: dict):
    """Loads the recogniser named by the spec.

    `onnx-asr` runs Parakeet through ONNX Runtime with no PyTorch, NeMo, or
    transformers dependency, which keeps both install size and peak memory far
    below the reference implementation. The model is quantised to int8: on a
    CPU-only machine that is the difference between transcription being
    practical and taking longer than the meeting did.
    """
    try:
        import onnx_asr
    except ImportError as e:
        raise RuntimeError(
            "onnx-asr is not installed; see worker/README.md for setup"
        ) from e

    model = params.get("model", "parakeet-tdt-0.6b-v3-int8")
    # onnx-asr names quantisation separately from the model.
    name, _, quantisation = model.partition("-int8")
    return onnx_asr.load_model(
        name if quantisation == "" else name,
        quantization="int8" if "int8" in model else None,
    )


def transcribe_track(model, track: Track, params: dict) -> dict:
    """Transcribes one track into segments with offsets from its own start."""
    result = model.recognize(
        str(track.audio_path),
        timestamps=params.get("word_timestamps", True),
    )

    segments = []
    for seg in getattr(result, "segments", None) or []:
        segments.append(
            {
                "start_ns": int(seg.start * NS_PER_SECOND),
                "end_ns": int(seg.end * NS_PER_SECOND),
                "text": seg.text.strip(),
                "speaker_hint": track.speaker_hint,
                "words": [
                    {
                        "start_ns": int(w.start * NS_PER_SECOND),
                        "end_ns": int(w.end * NS_PER_SECOND),
                        "text": w.text,
                    }
                    for w in (getattr(seg, "words", None) or [])
                ],
            }
        )

    # Some engines return only flat text when no timing was requested. A single
    # segment spanning the track is still usable, and is better than discarding
    # the transcript because its shape was unexpected.
    if not segments and getattr(result, "text", "").strip():
        segments = [
            {
                "start_ns": 0,
                "end_ns": 0,
                "text": result.text.strip(),
                "speaker_hint": track.speaker_hint,
                "words": [],
            }
        ]

    return {
        "track_id": track.track_id,
        "language": params.get("language") or None,
        "segments": segments,
    }


def main() -> int:
    raw = sys.stdin.read()
    if not raw.strip():
        return fail("empty_spec", "no job spec was provided on stdin")

    try:
        job_id, tracks, params = parse_spec(raw)
    except Exception as e:
        return fail("bad_spec", str(e))

    try:
        model = load_model(params)
    except Exception as e:
        # A missing or unloadable model is worth retrying once it is installed,
        # so it is reported as retryable rather than terminal.
        return fail("model_unavailable", str(e), job_id=job_id, retryable=True)

    transcripts = []
    warnings = []
    for track in tracks:
        try:
            transcripts.append(transcribe_track(model, track, params))
        except Exception as e:
            # One unreadable track must not discard the others: half a meeting
            # transcribed is far better than none.
            warnings.append(f"{track.track_id}: {e}")
            traceback.print_exc(file=sys.stderr)

    if not transcripts:
        return fail(
            "all_tracks_failed",
            "; ".join(warnings) or "no track could be transcribed",
            job_id=job_id,
        )

    json.dump(
        {
            "contract_version": RESULT_VERSION,
            "job_id": job_id,
            "status": "partial" if warnings else "ok",
            "engine": {
                "name": "onnx-asr",
                "version": getattr(model, "__version__", "unknown"),
                "model": params.get("model", "parakeet-tdt-0.6b-v3-int8"),
            },
            "warnings": warnings,
            "tracks": transcripts,
        },
        sys.stdout,
    )
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())

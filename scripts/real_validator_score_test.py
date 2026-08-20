#!/usr/bin/env python3
"""Score our own compressed outputs using the REAL validator-side scoring
code (services/scoring/server.py's score_compression_synthetics), not a
hand-ported estimate. This repo already contains that code -- it's not
exclusive to validator infrastructure -- so we can call the literal
function directly instead of guessing whether our own port matches it.

This catches things a hand-ported formula never could: the real pipeline
also runs encoding-settings validation (codec profile, pixel format, SAR,
container, color tags) and frame-count/color/chroma checks *before* VMAF is
even computed. Any of those failing zeroes the score regardless of how good
the compression/VMAF tradeoff is -- and we've never verified locally that
our own outputs pass them.

`pieapp_metric` (only used for upscaling scoring) isn't installed in this
environment, so a stub is put on sys.path -- server.py imports it
unconditionally at module load even though compression scoring never calls
it.

Usage:
    python3 scripts/real_validator_score_test.py --limit 3
"""
import argparse
import asyncio
import json
import subprocess
import sys
import time
from pathlib import Path

REPO = Path("/root/vidaio-subnet")
SHIM_DIR = Path("/tmp/vidaio-real-scoring-shim")
LIBRARY_PATH = Path("/root/vidaio-real-content-library")

sys.path.insert(0, str(SHIM_DIR))
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "services" / "scoring"))

from services.scoring.server import (  # noqa: E402
    CompressionScoringRequest,
    score_compression_synthetics,
)


def compress_via_service(
    service_url: str, container_mp4_path: str, meta: dict
) -> dict | None:
    """Runs the clip through the real production /compress path (adaptive
    CQ search included -- no explicit cq override), exactly like a live
    validator request would."""
    payload = {
        "video_paths": [container_mp4_path],
        "codec": meta["codec"].upper(),
        "codec_mode": meta.get("codec_mode") or "CRF",
        "vmaf_threshold": meta["vmaf_threshold"],
        "target_bitrate": int(float(meta.get("target_bitrate") or 5) * 1_000_000),
    }
    result = subprocess.run(
        ["curl", "-s", "-X", "POST", f"{service_url}/compress",
         "-H", "Content-Type: application/json", "-d", json.dumps(payload)],
        capture_output=True, text=True, timeout=240,
    )
    try:
        resp = json.loads(result.stdout)
    except json.JSONDecodeError:
        print(f"    bad response: {result.stdout[:200]}")
        return None
    if not resp.get("success"):
        print(f"    compress failed: {resp.get('errors') or resp.get('detail')}")
        return None
    return resp


async def score_one(
    host_output_dir: Path,
    container_output_prefix: str,
    mp4_path: Path,
    meta: dict,
    resp: dict,
) -> None:
    container_out = resp["output_paths"][0]
    output_path = host_output_dir / Path(container_out).relative_to(container_output_prefix)
    if not output_path.exists():
        print(f"    output missing on host: {output_path}")
        return

    request = CompressionScoringRequest(
        distorted_file_paths=[str(output_path)],
        reference_paths=[str(mp4_path)],
        uids=[21],
        video_ids=[meta["task_id"]],
        uploaded_object_names=[""],
        vmaf_thresholds=[meta["vmaf_threshold"]],
        target_codec=meta["codec"],
        codec_mode=meta.get("codec_mode") or "CRF",
        target_bitrate=float(meta.get("target_bitrate") or 5),
    )

    result = await score_compression_synthetics(request)
    r = result.model_dump() if hasattr(result, "model_dump") else result.dict()
    print(
        f"  REAL SCORE  vmaf={r['vmaf_scores'][0]:.2f}  "
        f"ratio={r['compression_rates'][0]:.4f}  "
        f"score={r['final_scores'][0]:.4f}  "
        f"reason={r['reasons'][0]}"
    )


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-url", default="http://localhost:8004")
    parser.add_argument("--container-output-prefix", default="/tmp/organic-proxy")
    parser.add_argument("--host-output-dir", default="/tmp/vidaio-miner-video-tmp")
    parser.add_argument("--container-library-path", default="/tmp/organic-proxy/calib-library")
    parser.add_argument("--library-path", default=str(LIBRARY_PATH))
    parser.add_argument("--limit", type=int, default=3)
    args = parser.parse_args()

    clips = []
    for mp4_path in sorted(Path(args.library_path).glob("*.mp4")):
        meta_path = Path(f"{mp4_path}.json")
        if not meta_path.exists():
            continue
        meta = json.loads(meta_path.read_text())
        clips.append((mp4_path, meta))
    clips = clips[: args.limit]

    host_output_dir = Path(args.host_output_dir)

    for mp4_path, meta in clips:
        print(f"[{meta['task_id']}] codec={meta['codec']} thr={meta['vmaf_threshold']}")
        container_mp4_path = f"{args.container_library_path}/{mp4_path.name}"
        resp = compress_via_service(args.service_url, container_mp4_path, meta)
        if resp is None:
            continue
        await score_one(
            host_output_dir, args.container_output_prefix, mp4_path, meta, resp
        )

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

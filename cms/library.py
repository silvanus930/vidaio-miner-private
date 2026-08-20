"""Browses the real-content-library and compressed-sample-library: coverage
grid (which codec/threshold categories are represented, and how thin) plus
on-demand thumbnails so the CMS doesn't need to store images separately.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

REFERENCE_LIBRARY_PATH = Path("/root/vidaio-real-content-library")
COMPRESSED_LIBRARY_PATH = Path("/root/vidaio-compressed-sample-library")
THUMBNAIL_CACHE_DIR = Path("/root/vidaio-cms/thumbnails")
MIN_PER_CATEGORY = 3  # mirrors neurons/miner.py's REAL_CONTENT_LIBRARY_MIN_PER_CATEGORY


def _clip_meta(mp4_path: Path) -> dict:
    try:
        meta = json.loads(Path(f"{mp4_path}.json").read_text())
    except Exception:
        meta = {}
    meta["task_id"] = meta.get("task_id", mp4_path.stem.split("_")[0])
    meta["filename"] = mp4_path.name
    meta["size_bytes"] = mp4_path.stat().st_size
    meta["mtime"] = mp4_path.stat().st_mtime
    return meta


def list_reference_clips() -> list[dict]:
    if not REFERENCE_LIBRARY_PATH.exists():
        return []
    clips = [_clip_meta(p) for p in sorted(REFERENCE_LIBRARY_PATH.glob("*.mp4"))]
    clips.sort(key=lambda c: c["mtime"], reverse=True)
    return clips


def coverage_grid() -> list[dict]:
    """Per-(codec, threshold) count in the reference library, flagged thin
    if at or below the eviction reserve -- the same signal that protects a
    category from FIFO eviction (see neurons/miner.py:_pick_eviction_victim)
    also tells you here which categories are thin *right now*.
    """
    counts: dict[tuple, int] = {}
    for clip in list_reference_clips():
        key = (str(clip.get("codec", "unknown")), clip.get("vmaf_threshold", "unknown"))
        counts[key] = counts.get(key, 0) + 1
    rows = [
        {"codec": codec, "vmaf_threshold": threshold, "count": n, "thin": n <= MIN_PER_CATEGORY}
        for (codec, threshold), n in sorted(counts.items())
    ]
    return rows


def get_thumbnail(task_id: str) -> Path | None:
    THUMBNAIL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cached = THUMBNAIL_CACHE_DIR / f"{task_id}.jpg"
    if cached.exists():
        return cached

    matches = list(REFERENCE_LIBRARY_PATH.glob(f"{task_id}_*.mp4"))
    if not matches:
        return None
    src = matches[0]
    try:
        subprocess.run(
            [
                "ffmpeg", "-y", "-ss", "1", "-i", str(src),
                "-frames:v", "1", "-vf", "scale=320:-1",
                str(cached),
            ],
            capture_output=True, timeout=30, check=True,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        return None
    return cached if cached.exists() else None

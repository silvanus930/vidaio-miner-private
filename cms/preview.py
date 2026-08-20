"""Produces a browser-playable preview copy of a retained sample, cached on
first request.

Two independent problems, confirmed directly on real files:
1. Reference clips arrive from the validator with the moov atom at the end
   of the file (not faststart) -- a 265MB file needs its tail fetched and
   parsed before a browser can even start playback.
2. Compressed outputs are frequently HEVC, which Chrome/Firefox cannot
   decode in <video> on most platforms (licensing) -- codec-incompatible,
   not a streaming problem, and looks identical to (1) from the UI (an
   endless spinner).

Preview copies are always H.264 + faststart, scaled down (this is a visual
check, not a quality measurement -- the real score already covers that),
and cached so only the first view per item pays the transcode cost.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

PREVIEW_CACHE_DIR = Path("/root/vidaio-cms/web-preview")
PREVIEW_MAX_WIDTH = 1280
TRANSCODE_TIMEOUT_SECONDS = 90


def get_preview(source_path: Path, cache_key: str) -> Path | None:
    PREVIEW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cached = PREVIEW_CACHE_DIR / f"{cache_key}.mp4"
    if cached.exists() and cached.stat().st_mtime >= source_path.stat().st_mtime:
        return cached

    tmp = PREVIEW_CACHE_DIR / f"{cache_key}.tmp.mp4"
    try:
        subprocess.run(
            [
                "ffmpeg", "-y", "-i", str(source_path),
                "-map", "0:v:0",
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "26",
                "-vf", f"scale='min({PREVIEW_MAX_WIDTH},iw)':-2",
                "-movflags", "+faststart",
                "-an",
                str(tmp),
            ],
            capture_output=True, timeout=TRANSCODE_TIMEOUT_SECONDS, check=True,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        tmp.unlink(missing_ok=True)
        return None

    tmp.rename(cached)
    return cached

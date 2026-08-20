"""
Compression microservice — wraps the ffmpeg binary installed in this container.

Accepts a video path (on shared volume) OR a URL, codec, and quality settings,
runs GPU-accelerated ffmpeg compression, returns the output path or S3 URL.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import shutil
import time
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal, Optional

import boto3
import httpx
from botocore.config import Config
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field, model_validator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("compression")


@asynccontextmanager
async def lifespan(app: FastAPI):
    cleanup_task = asyncio.create_task(_cleanup_worker())
    try:
        yield
    finally:
        cleanup_task.cancel()
        with suppress(asyncio.CancelledError):
            await cleanup_task


app = FastAPI(title="Video Compression Service", lifespan=lifespan)

SHARED_VOLUME_PATH = os.getenv("SHARED_VOLUME_PATH", "/tmp/organic-proxy")
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_COMPRESSION", "5"))
MAX_QUEUE_SIZE = int(
    os.getenv("MAX_QUEUE_SIZE_COMPRESSION") or os.getenv("MAX_QUEUE_SIZE", "5")
)
DISABLE_REMOTE_IO = os.getenv("DISABLE_REMOTE_IO", "false").lower() in (
    "1",
    "true",
    "yes",
)
COMPETITION_INPUT_ROOT = os.getenv("COMPETITION_INPUT_ROOT", "/evaluation-inputs")
COMPETITION_OUTPUT_ROOT = os.getenv("COMPETITION_OUTPUT_ROOT", "/output")
DEFAULT_COMPRESSION_CQ = 35
# HEVC Medium (threshold 89): the same calibration methodology used to fix
# AV1's tables (explicit cq bypassing search, real score via the production
# formula, real captured clips) found the same class of bug here -- cq=35
# was landing 2 of 4 real clips at VMAF 84.6-85.0, right at the hard-cutoff
# (84), scoring 0.0011-0.0027 (essentially zero). cq=30 kept every clip
# safely in the full-credit zone (min real score 0.2783 vs cq=35's 0.0011;
# mean 0.3255 vs 0.1790).
# HEVC Low (threshold 85): the old default (40) was worse than Medium's bug --
# real VMAF landed at 75-76 on every tested clip, below the hard-cutoff (80),
# scoring 0.0000 across the board with zero exceptions. cq=35 fixed it (mean
# 0.289, min 0.076 across 4 real clips). Also tested going more conservative
# still (cq=30/32/33) in case there was headroom below 35 the way AV1's Low
# tier had -- there wasn't: on the 2 clips tested at all four values, score
# fell monotonically as cq decreased below 35 (clip1: 0.292/0.315/0.329/0.360
# for cq=30/32/33/35; clip2: 0.293/0.315/0.328/0.359), because the formula's
# 70% compression weight punishes the extra ratio cost of over-shooting the
# threshold faster than the quality bonus repays it. cq=35 is a real optimum
# here, not just "safe enough."
# Both fixes only apply to CRF-mode requests -- real HEVC traffic observed
# this session has mostly been VBR (which ignores this table entirely, see
# the VBR branch in _build_ffmpeg_args), but the CRF path exists in the code
# and would hit these same failures the moment a validator sends
# codec_mode=CRF for HEVC.
COMPRESSION_CQ_BY_TYPE = {
    "Low": 35,
    "Medium": 30,
    "High": 30,
}
# AV1 (av1_nvenc) and HEVC (hevc_nvenc) do not share the same CQ-to-VMAF
# curve at the same numeric CQ. These defaults are a ratchet: each value is
# only ever promoted here after a real (non-synthetic) production search
# probe confirms it's safe at that threshold, never guessed from local
# testing (synthetic content's rate-distortion curve doesn't transfer --
# confirmed repeatedly, e.g. a same-CQ probe landing ~90 VMAF on synthetic
# content vs delivering ~9x the bitrate real content needed for a similar
# VMAF). Round 1: HEVC cq=41 hard-fails at threshold 89 (VMAF ~84.6) while
# AV1 cq=41 is comfortably safe at the same threshold (VMAF ~91.4-92.2) --
# gave AV1 its own, higher table. Round 2: real search results confirmed
# cq=41 safe at threshold 89 (promoted from the round-1 guess of 38) and
# cq=46 safe at threshold 85 (promoted from 43).
# Round 3 ("High", threshold>=93): a systematic calibration sweep (explicit
# cq bypassing search, real score computed via the exact production formula)
# across 5 real captured threshold-93 clips found cq=33 was actively wrong,
# not just suboptimal -- it pushed VMAF below 93 on 2/5 clips, landing them
# in the soft-zone where the score formula craters even 1 point under target
# (min real score 0.0042 vs cq=30's 0.2063; mean 0.1592 vs cq=30's 0.2333).
# cq=30 kept every one of the 5 clips at or above the full-credit VMAF>=93
# line while still compressing to 0.44-0.71 ratio. cq=27 was tested too and
# is worse in the other direction: 4/5 clips hit ratio>=0.80, the automatic
# compression-side hard-fail -- confirming compression that's too gentle
# zeroes the score exactly like VMAF that's too low does.
# Round 4 ("Medium", threshold 89): the same calibration methodology found
# the round-2 "confirmation" of cq=41 was wrong -- and wrong in a way that
# reveals why: round 2 trusted the *adaptive search's own* subsampled,
# margin-adjusted probe (n_subsample=6, search aiming for 89+margin rather
# than 89 itself), which overestimated true VMAF enough to call cq=41 safe.
# Full-precision recalibration (n_subsample=1, real score formula) across 3
# real threshold-89 clips instead put cq=41 at real VMAF 83.5-86.0 -- at or
# below the hard-cutoff (84) on every single one, scoring 0.0000-0.0095 (i.e.
# essentially zero). cq=36 measured 88.3-90.1 VMAF on the same 3 clips,
# scoring 0.02-0.27 -- a 20-30x real improvement, because it sits close
# enough to the 89 target that most clips land at/above it instead of
# camping in the hard-cutoff's shadow. This is now the more-trusted number
# precisely because it came from the same ground-truth method that also
# fixed "High" above, rather than from the search's own noisier signal.
# Round 5 ("Low", threshold 85): calibration across 4 real clips revealed a
# real content-difficulty split at this tier -- 2 clips stayed safely above
# threshold even at cq=50 (the most aggressive tested), while the other 2
# hard-cutoff-failed outright by cq=50 (VMAF 77.67/79.81, below the 80
# floor). Across that split, cq=43 was strictly better than the round-2
# default (46) on both worst-case AND average real score: min 0.4069 vs
# 0.0838, mean 0.4381 vs 0.4042 -- 46 wasn't a middling choice, it was
# dominated on every axis by the more conservative value. Untested below 43;
# there may be further headroom, but 43 is already a clean win with no
# tail risk in the tested range.
# HEVC is untouched throughout -- no real evidence yet of what it can do
# above cq=35, so it keeps discovering that incrementally via search.
AV1_COMPRESSION_CQ_BY_TYPE = {
    "Low": 43,
    "Medium": 36,
    "High": 30,
}
# SVT-AV1's CRF scale is numerically similar to NVENC's CQ scale but not the
# same curve -- confirmed on real hard content that the NVENC-tuned "High"
# center (33) explores a range (27-36) that never gets close to what SVT can
# actually do: a manual test at crf=18 (well outside that range) reached
# VMAF 94.10 vs the search's own best of 91.59 within the NVENC-calibrated
# range. Only reachable via encoder_mode="svt" (organic job path).
SVT_AV1_COMPRESSION_CQ_BY_TYPE = {
    "Low": 30,
    "Medium": 24,
    "High": 18,
}

# Adaptive CQ search: probe a handful of CQ values against real VMAF instead
# of trusting the static table above, so the encode lands just above the
# requested threshold (maximizing compression credit) instead of guessing.
# The real validator scores with the VMAF NEG model (services/scoring/vmaf_metric.py:
# model='version=vmaf_v0.6.1neg'), not the standard model -- measured ~2.2-2.5
# points lower than standard on identical output for both encoders tested. The
# search must optimize against the same metric that will actually grade it, or
# it silently overestimates its safety margin against the hard-fail cutoff.
VMAF_MODEL_PATH = os.getenv(
    "VMAF_MODEL_PATH", "/usr/local/share/vmaf/model/vmaf_v0.6.1neg.json"
)
# libvmaf defaults to n_threads=0 (effectively single-threaded), measured at
# ~12s for a 10s clip on this host vs ~4.4s at n_threads=4 -- identical VMAF
# output, no accuracy cost, just a config default nobody had set. 4 is a mild
# oversubscription at MAX_CONCURRENT=5 on a 14-core host and was the tested
# value; override via env if the deployment's core count differs a lot.
VMAF_N_THREADS = int(os.getenv("VMAF_N_THREADS", "4"))
CQ_SEARCH_ENABLED = os.getenv("COMPRESSION_CQ_SEARCH_ENABLED", "true").lower() in (
    "1",
    "true",
    "yes",
)
CQ_SEARCH_MAX_ITERS = int(os.getenv("COMPRESSION_CQ_SEARCH_MAX_ITERS", "6"))
CQ_SEARCH_MAX_SECONDS = float(os.getenv("COMPRESSION_CQ_SEARCH_MAX_SECONDS", "90"))
# Search probes measure VMAF with n_subsample > 1 at higher resolutions (see
# _vmaf_subsample_for_resolution) to stay inside the timeout budget. That's a
# noisier estimate than the validator's own check, and the search otherwise
# targets "just barely above threshold" -- which then lands just barely
# *below* the real threshold about as often as above it. Search against a
# nudged-up threshold so the chosen CQ carries a margin proportional to how
# much subsampling noise it was measured under.
# Real production data (threshold-85 batch, n_subsample=6) showed the
# original margin (0.5 + 0.2*6 = 1.7) still wasn't enough on hard/high-
# motion content: probe estimates of ~81-82 delivered real VMAF of
# ~79.7-82.5, a systematic ~1-2 point undershoot on 4 of 5 items, not just
# random noise. Widened base/per-subsample so n_subsample=6 carries ~2.8
# points of cushion instead of 1.7 -- still comfortably below the margins
# seen on confirmed real wins (2.4+ points), so this shouldn't cost the
# compression gains already validated in production.
CQ_SEARCH_VMAF_MARGIN_BASE = float(
    os.getenv("COMPRESSION_CQ_SEARCH_VMAF_MARGIN_BASE", "1.0")
)
CQ_SEARCH_VMAF_MARGIN_PER_SUBSAMPLE = float(
    os.getenv("COMPRESSION_CQ_SEARCH_VMAF_MARGIN_PER_SUBSAMPLE", "0.3")
)
# The validator's dendrite call to us has a hard 180s timeout (neurons/
# validator.py, call_miner_batch(..., timeout=180)) measured end-to-end,
# including our miner's input download and output upload around this
# service's own queueing+search+encode work. A response that arrives even a
# few seconds late isn't scored low -- it's discarded entirely (empty URL ->
# "invalid or missing distorted video file", final_score 0).
# CQ_SEARCH_MAX_SECONDS alone doesn't protect against this: it bounds the
# search loop but ignores time already burned queueing behind other
# concurrent items, and the final encode after the search has no timeout at
# all. Give _compress_one an overall deadline for its own queueing+search+
# encode work and have both phases shrink to fit whatever actually remains,
# instead of each independently assuming a full budget.
#
# Originally sized around a ~40s download / ~15s upload baseline (giving
# 120s here). Real production data since then has shown download alone
# regularly taking 60-80s under real network conditions (once measured at
# 71s in a batch that totaled 196s and almost certainly missed the 180s
# window) -- that baseline was too optimistic. Tightened to leave real
# margin against that observed variance rather than the best case.
COMPRESSION_OVERALL_DEADLINE_SECONDS = float(
    os.getenv("COMPRESSION_OVERALL_DEADLINE_SECONDS", "90")
)
COMPRESSION_FINAL_ENCODE_RESERVE_SECONDS = float(
    os.getenv("COMPRESSION_FINAL_ENCODE_RESERVE_SECONDS", "20")
)
# Hard per-subprocess ceiling so one hung ffmpeg/vmaf call (GPU/driver glitch)
# can't block the whole request indefinitely -- CQ_SEARCH_MAX_SECONDS is only
# checked *between* probes, so without this a single stuck call defeats it.
# 60s (was 45s) is extra margin on top of the VMAF n_subsample fix below --
# defense in depth, not the primary fix. Real production failure: a 4K probe
# measured at 25.7s for a 10s clip with n_subsample=1 (0.41x realtime), which
# scales past 45s for anything beyond ~18s of 4K footage.
CQ_SEARCH_PROBE_TIMEOUT_SECONDS = float(
    os.getenv("COMPRESSION_CQ_SEARCH_PROBE_TIMEOUT_SECONDS", "60")
)
CQ_SEARCH_MAX_DURATION_SECONDS = float(
    os.getenv("COMPRESSION_CQ_SEARCH_MAX_DURATION_SECONDS", "120")
)
CQ_SEARCH_MIN = 18
CQ_SEARCH_MAX = 51
# Empirical |dVMAF/dcq| across every real (cq, vmaf) pair collected this
# session, pooled over both codecs (n=48, mean 1.689, median 1.773, range
# 0.44-2.68). Used only as the first-jump prior in the search below; the
# second jump uses the clip's own measured local slope instead, which is
# more accurate since real content varies by ~6x in how steep this curve is.
CQ_SEARCH_SLOPE_PRIOR = 1.7

# Every real probe (encode + VMAF measure) run by the search below is
# training data for a future learned CQ predictor -- persisted on the host
# bind mount (survives container rebuilds) so it accumulates across
# redeploys instead of resetting with the container's own log buffer.
# Lives under a reserved subdirectory (see PERSISTENT_DATA_DIR) that the
# cleanup worker explicitly skips -- a one-off calibration log written
# directly under SHARED_VOLUME_PATH was silently deleted by the cleanup
# worker's TTL sweep (it walks the whole tree with no notion of "this file
# isn't scratch data"), losing real calibration output before it could be
# reused. This log file only survived that same sweep because its frequent
# appends kept refreshing its mtime past the TTL window -- pure luck, not a
# guarantee.
PERSISTENT_DATA_DIR = os.path.join(SHARED_VOLUME_PATH, "_persistent")
CQ_SEARCH_LOG_PATH = os.getenv(
    "COMPRESSION_CQ_SEARCH_LOG_PATH",
    os.path.join(PERSISTENT_DATA_DIR, "cq_search_log.jsonl"),
)


def _log_cq_search_sample(
    *,
    task_label: str,
    codec: str,
    encoder_mode: str | None,
    compression_type: str | None,
    vmaf_threshold: float | None,
    resolution: tuple[int, int] | None,
    n_subsample: int,
    cq: int,
    vmaf: float,
    score_est: float,
    original_size: int,
    compressed_size: int,
) -> None:
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "task": task_label,
        "codec": codec,
        "encoder_mode": encoder_mode,
        "compression_type": compression_type,
        "vmaf_threshold": vmaf_threshold,
        "width": resolution[0] if resolution else None,
        "height": resolution[1] if resolution else None,
        "n_subsample": n_subsample,
        "cq": cq,
        "vmaf": vmaf,
        "score_est": score_est,
        "ratio": (compressed_size / original_size) if original_size else None,
    }
    try:
        os.makedirs(PERSISTENT_DATA_DIR, exist_ok=True)
        with open(CQ_SEARCH_LOG_PATH, "a") as f:
            f.write(json.dumps(record) + "\n")
    except OSError as exc:
        log.warning(f"[{task_label}] failed to write cq search log sample: {exc}")

# Storage provider label only. Uploads use one S3-compatible code path.
STORAGE_PROVIDER = os.getenv("MINER_STORAGE_PROVIDER", "s3").lower()
S3_REGION = os.getenv("MINER_STORAGE_S3_REGION", "us-east-1").strip() or "us-east-1"
S3_BUCKET = os.getenv("MINER_STORAGE_S3_BUCKET_NAME", "").strip()
S3_ACCESS_KEY_ID = os.getenv("MINER_STORAGE_S3_ACCESS_KEY_ID", "").strip()
S3_SECRET_ACCESS_KEY = os.getenv("MINER_STORAGE_S3_SECRET_ACCESS_KEY", "").strip()
S3_ENDPOINT_URL = (
    os.getenv("MINER_STORAGE_S3_ENDPOINT_URL")
    or os.getenv("MINER_STORAGE_S3_ENDPOINT")
    or ""
).strip()
S3_PRESIGNED_EXPIRY = int(
    os.getenv("MINER_STORAGE_S3_PRESIGNED_EXPIRY")
    or os.getenv("S3_PRESIGNED_EXPIRY")
    or "3600"
)
PRESIGNED_URL_CLEANUP_GRACE_SECONDS = int(
    os.getenv("PRESIGNED_URL_CLEANUP_GRACE_SECONDS", "600")
)
TEMP_FILE_TTL_SECONDS = int(
    os.getenv("MINER_TEMP_FILE_TTL_SECONDS")
    or os.getenv("TEMP_FILE_TTL_SECONDS")
    or str(min(S3_PRESIGNED_EXPIRY, 604800) + PRESIGNED_URL_CLEANUP_GRACE_SECONDS)
)
CLEANUP_INTERVAL_SECONDS = int(
    os.getenv("MINER_CLEANUP_INTERVAL_SECONDS")
    or os.getenv("CLEANUP_INTERVAL_SECONDS")
    or "300"
)
CLEANUP_MAX_VOLUME_BYTES = int(
    os.getenv("MINER_CLEANUP_MAX_VOLUME_BYTES")
    or os.getenv("CLEANUP_MAX_VOLUME_BYTES")
    or "9000000000"
)
CLEANUP_MIN_FILE_AGE_SECONDS = int(
    os.getenv("MINER_CLEANUP_MIN_FILE_AGE_SECONDS")
    or os.getenv("CLEANUP_MIN_FILE_AGE_SECONDS")
    or "60"
)
CLEANUP_ENABLED = os.getenv(
    "MINER_CLEANUP_ENABLED", os.getenv("CLEANUP_ENABLED", "true")
).lower() in (
    "1",
    "true",
    "yes",
)
STORAGE_CLEANUP_ENABLED = os.getenv(
    "MINER_STORAGE_CLEANUP_ENABLED", os.getenv("STORAGE_CLEANUP_ENABLED", "true")
).lower() in ("1", "true", "yes")
STORAGE_CLEANUP_PREFIXES = [
    prefix.strip()
    for prefix in os.getenv(
        "MINER_STORAGE_CLEANUP_PREFIXES", "processing/,upscaling/"
    ).split(",")
    if prefix.strip()
]
STORAGE_OBJECT_TTL_SECONDS = int(
    os.getenv("MINER_STORAGE_OBJECT_TTL_SECONDS")
    or os.getenv("STORAGE_OBJECT_TTL_SECONDS")
    or str(min(S3_PRESIGNED_EXPIRY, 604800) + PRESIGNED_URL_CLEANUP_GRACE_SECONDS)
)
COMPRESSION_CHUNKING_ENABLED = os.getenv(
    "COMPRESSION_CHUNKING_ENABLED", "true"
).lower() in (
    "1",
    "true",
    "yes",
)
COMPRESSION_CHUNK_MIN_DURATION_SECONDS = int(
    os.getenv("COMPRESSION_CHUNK_MIN_DURATION_SECONDS", "1200")
)
COMPRESSION_CHUNK_TARGET_SECONDS = int(
    os.getenv("COMPRESSION_CHUNK_TARGET_SECONDS", "600")
)
COMPRESSION_CHUNK_PARALLELISM = max(
    1, int(os.getenv("COMPRESSION_CHUNK_PARALLELISM", "2"))
)
FFPROBE_BIN = os.getenv("FFPROBE_BIN", "ffprobe")
FFMPEG_BIN = os.getenv("FFMPEG_BIN", "ffmpeg")


def resolve_compression_cq(
    *,
    explicit_cq: int | None = None,
    compression_type: Literal["Low", "Medium", "High"] | None = None,
    vmaf_threshold: float | None = None,
    codec: str | None = None,
    encoder_mode: str | None = None,
) -> int:
    """Resolve CQ once for inference and competition requests.

    Explicit legacy overrides retain precedence. Otherwise an explicit quality
    tier wins, followed by the VMAF target and the historical medium default.
    """

    is_av1 = codec is not None and codec.upper() == "AV1"
    if is_av1 and encoder_mode == "svt":
        cq_by_type = SVT_AV1_COMPRESSION_CQ_BY_TYPE
    elif is_av1:
        cq_by_type = AV1_COMPRESSION_CQ_BY_TYPE
    else:
        cq_by_type = COMPRESSION_CQ_BY_TYPE
    if explicit_cq is not None:
        return explicit_cq
    if compression_type is not None:
        return cq_by_type[compression_type]
    if vmaf_threshold is not None:
        if vmaf_threshold >= 93:
            return cq_by_type["High"]
        if vmaf_threshold >= 89:
            return cq_by_type["Medium"]
        return cq_by_type["Low"]
    return DEFAULT_COMPRESSION_CQ


_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
_queue_size = 0
_active_count = 0
_active_file_paths: set[str] = set()
_lock = asyncio.Lock()

# Codec name → nvenc/software encoder mapping
CODEC_MAP = {
    "AV1": "av1_nvenc",
    "H264": "h264_nvenc",
    "H.264": "h264_nvenc",
    "HEVC": "hevc_nvenc",
    "H265": "hevc_nvenc",
    "H.265": "hevc_nvenc",
    "VP9": "libvpx-vp9",
}

# Software SVT-AV1 vs NVENC on the synchronous (scored, 180s-budgeted) path:
# marginal and inconsistent under fast presets, not worth the latency --
# stays "nvenc" here, and only the organic job/poll path (minutes-scale
# budget, CompressRequest.encoder_mode="svt", not scored synthetic traffic)
# ever opts into "svt". On that path the tradeoff is different: a real
# production hard clip measured SVT beating NVENC on both axes at once
# (smaller file *and* higher VMAF at the same bitrate) once given a genuinely
# slow preset -- preset 8 (SVT's -2..13 scale, lower=slower/better) is
# actually a *fast*, low-quality setting despite the number looking
# conservative; preset 4 is what produced the real win.
AV1_ENCODER_MODE = os.getenv("COMPRESSION_AV1_ENCODER", "nvenc").lower()
SVT_AV1_PRESET = int(os.getenv("SVT_AV1_PRESET", "4"))


def _is_url(path: str) -> bool:
    return path.startswith("http://") or path.startswith("https://")


def _get_s3_client():
    client_kwargs = {
        "region_name": S3_REGION,
        "aws_access_key_id": S3_ACCESS_KEY_ID or None,
        "aws_secret_access_key": S3_SECRET_ACCESS_KEY or None,
        "config": Config(signature_version="s3v4"),
    }
    if S3_ENDPOINT_URL:
        client_kwargs["endpoint_url"] = S3_ENDPOINT_URL
    return boto3.client("s3", **client_kwargs)


def _storage_config_status() -> dict[str, object]:
    return {
        "provider": STORAGE_PROVIDER,
        "region": S3_REGION,
        "bucket_configured": bool(S3_BUCKET),
        "access_key_configured": bool(S3_ACCESS_KEY_ID),
        "secret_key_configured": bool(S3_SECRET_ACCESS_KEY),
        "endpoint_configured": bool(S3_ENDPOINT_URL),
    }


def _compression_config_status() -> dict[str, object]:
    return {
        "chunking_enabled": COMPRESSION_CHUNKING_ENABLED,
        "chunk_min_duration_seconds": COMPRESSION_CHUNK_MIN_DURATION_SECONDS,
        "chunk_target_seconds": COMPRESSION_CHUNK_TARGET_SECONDS,
        "chunk_parallelism": COMPRESSION_CHUNK_PARALLELISM,
    }


def _validate_s3_config():
    missing = []
    if not S3_BUCKET:
        missing.append("MINER_STORAGE_S3_BUCKET_NAME")
    if not S3_ACCESS_KEY_ID:
        missing.append("MINER_STORAGE_S3_ACCESS_KEY_ID")
    if not S3_SECRET_ACCESS_KEY:
        missing.append("MINER_STORAGE_S3_SECRET_ACCESS_KEY")

    if missing:
        raise RuntimeError(f"Missing storage configuration: {', '.join(missing)}")


async def _download_url(url: str, dest: str):
    log.info(f"Downloading {url[:80]}... → {dest}")
    async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=8192):
                    f.write(chunk)
    log.info(f"Downloaded {os.path.getsize(dest) / (1024 * 1024):.1f} MB → {dest}")


def _upload_to_s3(local_path: str, key: str) -> str:
    _validate_s3_config()
    client = _get_s3_client()
    client.upload_file(local_path, S3_BUCKET, key)
    url = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=min(S3_PRESIGNED_EXPIRY, 604800),
    )
    log.info(f"Uploaded to {STORAGE_PROVIDER} storage: s3://{S3_BUCKET}/{key}")
    return url


def _cleanup(*paths: str):
    for p in paths:
        if p and os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


def _cleanup_tree(path: str):
    if path and os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)


def _format_process_output(stdout: bytes, stderr: bytes) -> str:
    parts = []
    stdout_msg = stdout.decode(errors="replace").strip()
    stderr_msg = stderr.decode(errors="replace").strip()
    if stdout_msg:
        parts.append(f"stdout:\n{stdout_msg}")
    if stderr_msg:
        parts.append(f"stderr:\n{stderr_msg}")
    return "\n\n".join(parts)


async def _run_process(
    cmd: list[str], task_label: str, step: str, timeout: float | None = None
) -> tuple[int | None, bytes, bytes, str]:
    try:
        log.info(f"[{task_label}] {step}: {' '.join(cmd)}")
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError:
            with suppress(ProcessLookupError):
                proc.kill()
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(proc.communicate(), timeout=5)
            return None, b"", b"", f"{step} timed out after {timeout}s"
        return proc.returncode, stdout, stderr, ""
    except FileNotFoundError:
        return None, b"", b"", f"{cmd[0]} binary not found"
    except OSError as e:
        return None, b"", b"", f"Failed to start {cmd[0]}: {e}"


async def _probe_duration_seconds(path: str, task_label: str) -> float | None:
    cmd = [
        FFPROBE_BIN,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        path,
    ]
    returncode, stdout, stderr, run_error = await _run_process(
        cmd, task_label, "ffprobe duration"
    )
    if returncode != 0 or run_error:
        detail = run_error or stderr.decode(errors="replace").strip()
        log.warning(f"[{task_label}] Failed to probe duration: {detail}")
        return None
    try:
        return float(stdout.decode().strip())
    except ValueError:
        log.warning(f"[{task_label}] ffprobe returned invalid duration: {stdout!r}")
        return None


async def _probe_resolution(path: str, task_label: str) -> tuple[int, int] | None:
    cmd = [
        FFPROBE_BIN,
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=width,height",
        "-of",
        "csv=p=0",
        path,
    ]
    returncode, stdout, stderr, run_error = await _run_process(
        cmd, task_label, "ffprobe resolution"
    )
    if returncode != 0 or run_error:
        return None
    try:
        w_str, h_str = stdout.decode().strip().split(",")[:2]
        return int(w_str), int(h_str)
    except (ValueError, IndexError):
        return None


def _vmaf_subsample_for_resolution(
    width: int, height: int, available_seconds: float | None = None
) -> int:
    """VMAF (CPU-bound) measured at ~0.41x realtime on 4K vs generously
    faster than realtime at <=1080p on this host -- a single probe on a
    full-length clip can exceed the per-probe timeout outright, and did in
    production (see the 2026-08-17 10:48 UTC batch: 0 successful probes
    logged, silent fallback to the static table, 4/5 items landed below the
    VMAF threshold as a direct result). Subsampling frames during the SEARCH
    only (never for anything already scored/returned) cuts compute
    proportionally with negligible score deltas (measured: 99.96 -> 99.83 at
    5x on a real 4K clip) since VMAF's harmonic mean is stable under
    sparse-but-regular sampling.

    The exact same failure mode recurred on 2026-08-19/20: a real batch
    where the download phase alone consumed ~100s of the validator's 180s
    wall left too little of the remaining budget for even one probe to
    finish at the fixed subsample level under 5-way concurrent load -- 0/5
    items got any search refinement, all fell back to the static table,
    leaving real, measured VMAF headroom (8-13 points above threshold) on
    the table purely because nothing had time to probe for it. Scaling
    n_subsample coarser as available_seconds shrinks trades measurement
    precision for a real chance to complete at least one probe, rather than
    a guaranteed zero -- reusing the existing margin mechanism
    (CQ_SEARCH_VMAF_MARGIN_PER_SUBSAMPLE already widens the safety margin
    proportionally to n_subsample) instead of adding a new one.
    """
    pixels = width * height
    if pixels > 1920 * 1080:
        base = 6
        if available_seconds is not None:
            if available_seconds < 40:
                base = 12
            elif available_seconds < 70:
                base = 9
        return base
    if pixels > 1280 * 720:
        base = 2
        if available_seconds is not None and available_seconds < 40:
            base = 4
        return base
    return 1


def _size_based_cq_bonus(
    vmaf_threshold: float | None,
    codec: str | None,
    resolution: tuple[int, int] | None,
    original_size: int,
) -> int:
    """Free per-clip difficulty signal -- the reference file's own size is
    already known before any computation runs, and correlates strongly
    (r=0.755, measured on 7 real 4K clips at the Low tier) with how much
    VMAF degrades under more aggressive compression: larger/denser source
    files are harder content. Clips under ~160MB (30s @ native 4K) stayed
    safely above threshold even pushed 5 CQ steps more aggressive than the
    tier default (VMAF 89.1-89.4 vs an 85 threshold), while clips over
    ~200MB were already at the edge at that same push (VMAF 85.1-85.9).
    Matters most exactly when the search can't refine at all -- a real
    batch was observed completing zero probes under contention, meaning
    this fallback value is what actually gets used, not just a starting
    point for exploration.

    Deliberately narrow: only applies where it's been measured (AV1, the
    Low tier, native 4K). No data yet for other codecs, tiers, or
    resolutions, so it makes no adjustment there rather than extrapolate.
    """
    if not codec or codec.upper() != "AV1":
        return 0
    if vmaf_threshold is None or vmaf_threshold >= 89:
        return 0
    if not resolution or resolution[0] * resolution[1] < 3840 * 2160 * 0.9:
        return 0
    if original_size / 1_000_000 < 160:
        return 5
    return 0


async def _probe_segment_duration_seconds(path: str, task_label: str) -> float:
    duration = await _probe_duration_seconds(path, task_label)
    if duration is None:
        raise RuntimeError(f"Unable to probe segment duration: {path}")
    return duration


def _format_timestamp(seconds: float) -> str:
    milliseconds = int(round(seconds * 1000))
    hours, remainder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    whole_seconds, milliseconds = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{whole_seconds:02d}.{milliseconds:03d}"


async def _log_chunk_seams(segments: list[str], task_label: str) -> list[float]:
    durations = [
        await _probe_segment_duration_seconds(segment, task_label)
        for segment in segments
    ]
    seams: list[float] = []
    elapsed = 0.0
    for duration in durations[:-1]:
        elapsed += duration
        seams.append(elapsed)

    log.info(
        f"[{task_label}] chunk_seams "
        + json.dumps(
            {
                "seam_seconds": [round(seam, 3) for seam in seams],
                "seam_timestamps": [_format_timestamp(seam) for seam in seams],
                "segment_durations_seconds": [
                    round(duration, 3) for duration in durations
                ],
            }
        )
    )
    return seams


def _build_ffmpeg_args(
    local_input: str, output_path: str, req: "CompressRequest", encoder: str
) -> list[str]:
    resolved_cq = resolve_compression_cq(
        explicit_cq=req.cq,
        compression_type=req.compression_type,
        vmaf_threshold=req.vmaf_threshold,
        codec=req.codec,
        encoder_mode=req.encoder_mode,
    )
    ffmpeg_args = [
        FFMPEG_BIN,
        "-y",
        "-hwaccel",
        "cuda",
        "-i",
        local_input,
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c:v",
        encoder,
    ]

    is_svt = encoder == "libsvtav1"

    if req.codec_mode == "VBR" and req.target_bitrate:
        ffmpeg_args.extend(["-b:v", str(req.target_bitrate)])
        # Bitrate is fixed in VBR mode, so the only quality lever left is
        # encoder effort. Default to a slower/higher-quality preset instead
        # of the CRF-tuned default, unless the caller overrode it.
        preset = str(SVT_AV1_PRESET) if is_svt else ("p6" if req.preset == "p4" else req.preset)
        # NVENC advanced rate-control flags. VBR mode has no CQ to tune (the
        # bitrate is fixed by the validator's request), so encoder effort is
        # the only real lever -- A/B tested via explicit per-request
        # overrides against 3 real HEVC/threshold-89 clips at their real
        # 8Mbps target, scored with the production formula (full-video VMAF,
        # not the noisy random-10-frame sample the real scoring fallback
        # uses, so relative ranking between combos is trustworthy even
        # though absolute VMAF differs from that path). Plain preset p7 gave
        # essentially nothing over p6 (mean score 0.1841 vs 0.1838) despite
        # costing ~12% more encode time. Adding multipass + lookahead +
        # spatial/temporal AQ on top of p6 did help (mean 0.1886, no
        # regression on any tested clip): +0.016 score on the one clip with
        # real bitrate headroom to use it. Two of three clips were
        # genuinely bitrate-starved at 8Mbps regardless of settings (VMAF
        # 83-85, structurally below threshold) -- these flags help on the
        # margin, they can't rescue a bitrate too low for the content.
        if not is_svt:
            multipass = req.nvenc_multipass or "fullres"
            rc_lookahead = req.nvenc_rc_lookahead if req.nvenc_rc_lookahead is not None else 32
            spatial_aq = req.nvenc_spatial_aq if req.nvenc_spatial_aq is not None else True
            temporal_aq = req.nvenc_temporal_aq if req.nvenc_temporal_aq is not None else True
            aq_strength = req.nvenc_aq_strength if req.nvenc_aq_strength is not None else 8
            ffmpeg_args.extend(["-multipass", multipass])
            ffmpeg_args.extend(["-rc-lookahead", str(rc_lookahead)])
            ffmpeg_args.extend(["-spatial-aq", "1" if spatial_aq else "0"])
            ffmpeg_args.extend(["-temporal-aq", "1" if temporal_aq else "0"])
            ffmpeg_args.extend(["-aq-strength", str(aq_strength)])
    else:
        # SVT-AV1 uses -crf (0-63, same direction as NVENC's -cq: lower =
        # higher quality) instead of NVENC's -cq flag.
        ffmpeg_args.extend(["-crf" if is_svt else "-cq", str(resolved_cq)])
        preset = str(SVT_AV1_PRESET) if is_svt else req.preset

    ffmpeg_args.extend(["-preset", preset])

    video_filters = []
    if req.target_width and req.target_height:
        w = req.target_width if req.target_width % 2 == 0 else req.target_width - 1
        h = req.target_height if req.target_height % 2 == 0 else req.target_height - 1
        video_filters.append(f"scale={w}:{h}")

    video_filters.append("setsar=1")
    ffmpeg_args.extend(["-vf", ",".join(video_filters)])

    ffmpeg_args.extend(
        ["-c:a", "copy", "-sn", "-dn", "-movflags", "+faststart", output_path]
    )
    return ffmpeg_args


_VMAF_SCORE_RE = re.compile(r"VMAF score:\s*([\d.]+)")


async def _measure_vmaf(
    reference_path: str,
    distorted_path: str,
    task_label: str,
    n_subsample: int = 1,
    timeout: float = CQ_SEARCH_PROBE_TIMEOUT_SECONDS,
) -> float | None:
    """Real VMAF of distorted_path against reference_path, or None on failure.

    n_subsample > 1 scores every Nth frame instead of every frame -- only
    safe to use for search-time probes (see _vmaf_subsample_for_resolution),
    never for a value that gets returned or scored as final.
    """
    model = f"model=path={VMAF_MODEL_PATH}"
    if n_subsample > 1:
        model += f":n_subsample={n_subsample}"
    cmd = [
        FFMPEG_BIN,
        "-hide_banner",
        "-y",
        "-i",
        distorted_path,
        "-i",
        reference_path,
        "-lavfi",
        f"[0:v][1:v]libvmaf={model}:n_threads={VMAF_N_THREADS}",
        "-f",
        "null",
        "-",
    ]
    returncode, stdout, stderr, run_error = await _run_process(
        cmd, task_label, "vmaf probe", timeout=timeout
    )
    if returncode != 0 or run_error:
        return None
    match = _VMAF_SCORE_RE.search(stderr.decode(errors="replace"))
    return float(match.group(1)) if match else None


async def _encode_cq_probe(
    local_input: str,
    cq: int,
    req: "CompressRequest",
    encoder: str,
    task_label: str,
    tmp_dir: str,
    timeout: float = CQ_SEARCH_PROBE_TIMEOUT_SECONDS,
) -> str | None:
    probe_path = os.path.join(tmp_dir, f"probe_cq{cq}.mp4")
    probe_req = req.model_copy(update={"cq": cq})
    cmd = _build_ffmpeg_args(local_input, probe_path, probe_req, encoder)
    returncode, stdout, stderr, run_error = await _run_process(
        cmd, task_label, f"cq probe cq={cq}", timeout=timeout
    )
    if returncode != 0 or run_error or not os.path.exists(probe_path):
        return None
    return probe_path


def _estimate_compression_score(
    original_size: int, compressed_size: int, vmaf: float, vmaf_threshold: float
) -> float:
    """Mirrors the validator's compression scoring formula (docs/incentive_mechanism.md)
    so the search can optimize the real objective instead of a VMAF proxy.
    Maximizing "VMAF just above threshold" is NOT the same as maximizing this
    score everywhere in the curve: at low compression ratios the quality
    component (weight 0.3, linear in VMAF) can outweigh the marginal
    compression gain (weight 0.7, but sub-linear via the exponent) from
    pushing VMAF down toward the threshold. Only a direct score search gets
    this right in every regime.
    """
    if original_size <= 0 or compressed_size <= 0:
        return 0.0
    c = compressed_size / original_size
    if c >= 0.80:
        return 0.0
    hard_cutoff = vmaf_threshold - 5
    if vmaf < hard_cutoff:
        return 0.0
    r = 1.0 / c
    if vmaf >= vmaf_threshold:
        quality_component = 0.7 + 0.3 * min(
            1.0, (vmaf - vmaf_threshold) / (100 - vmaf_threshold)
        )
        if r <= 20:
            compression_component = ((r - 1.25) / 18.75) ** 0.9
        else:
            compression_component = 1.0 + 0.1 * math.log(r / 20)
        return min(1.0, (0.7 * compression_component + 0.3 * quality_component) / 1.12)
    soft_zone_position = (vmaf - hard_cutoff) / 5
    quality_factor = 0.7 * soft_zone_position**2
    if r <= 20:
        compression_component = ((r - 1) / 19) ** 1.5
    else:
        compression_component = 1.0 + 0.3 * math.log(r / 20)
    return min(1.0, compression_component * quality_factor / 1.12)


async def _search_cq_for_max_score(
    local_input: str,
    req: "CompressRequest",
    encoder: str,
    task_label: str,
    deadline: float | None = None,
) -> int:
    """Local search over AV1 CQ that directly maximizes the real compression
    score (probe-encode, measure real size + VMAF, score it), instead of
    trusting a fixed per-band guess. Starts near the static table's value and
    does coordinate-ascent with a shrinking step. Falls back to the static
    table on any probe failure, timeout, or if nothing scores above zero.

    ``deadline`` is an absolute time.monotonic() value for the overall
    request (see COMPRESSION_OVERALL_DEADLINE_SECONDS); the search stops
    early against whichever of CQ_SEARCH_MAX_SECONDS or the remaining
    request budget is tighter, so a slow queue wait doesn't leave the final
    encode without enough time to finish before the validator's own timeout.
    """
    fallback_cq = resolve_compression_cq(
        explicit_cq=None,
        compression_type=req.compression_type,
        vmaf_threshold=req.vmaf_threshold,
        codec=req.codec,
        encoder_mode=req.encoder_mode,
    )
    if not CQ_SEARCH_ENABLED or req.vmaf_threshold is None:
        return fallback_cq

    try:
        original_size = os.path.getsize(local_input)
    except OSError:
        return fallback_cq
    if original_size <= 0:
        return fallback_cq

    resolution = await _probe_resolution(local_input, task_label)
    size_bonus = _size_based_cq_bonus(req.vmaf_threshold, req.codec, resolution, original_size)
    if size_bonus:
        adjusted_fallback_cq = min(max(fallback_cq + size_bonus, CQ_SEARCH_MIN), CQ_SEARCH_MAX)
        log.info(
            f"[{task_label}] {original_size / 1_000_000:.1f}MB reference, size-based "
            f"cq bonus +{size_bonus} (fallback {fallback_cq} -> {adjusted_fallback_cq})"
        )
        fallback_cq = adjusted_fallback_cq

    start = time.monotonic()
    max_seconds = req.search_max_seconds or CQ_SEARCH_MAX_SECONDS
    if deadline is not None:
        max_seconds = min(max_seconds, deadline - start)
    if max_seconds <= 0:
        log.warning(
            f"[{task_label}] no time budget left for cq search, using fallback cq={fallback_cq}"
        )
        return fallback_cq

    n_subsample = req.vmaf_n_subsample or (
        _vmaf_subsample_for_resolution(*resolution, available_seconds=max_seconds)
        if resolution
        else 1
    )
    search_vmaf_margin = (
        CQ_SEARCH_VMAF_MARGIN_BASE + CQ_SEARCH_VMAF_MARGIN_PER_SUBSAMPLE * n_subsample
    )
    search_threshold = req.vmaf_threshold + search_vmaf_margin
    if n_subsample > 1:
        log.info(
            f"[{task_label}] {resolution[0]}x{resolution[1]} input, using "
            f"VMAF n_subsample={n_subsample} for search probes "
            f"(target={search_threshold:.2f}, margin={search_vmaf_margin:.2f}, "
            f"budget={max_seconds:.1f}s)"
        )

    tmp_dir = os.path.join(SHARED_VOLUME_PATH, f"{task_label}_cqsearch")
    os.makedirs(tmp_dir, exist_ok=True)
    tried: dict[int, float] = {}
    probed_vmaf: dict[int, float] = {}
    # Under heavy concurrent load a single probe (encode + VMAF) has been
    # observed taking 45-60s -- attempting a second one with too little of
    # the budget left doesn't fail fast, it fails *slow* (runs right up to
    # its shrinking timeout for zero information gained), which just burns
    # time that would otherwise go to the final encode or simply finishing
    # sooner. Track how long completed attempts actually took and skip
    # starting another once the remaining budget can't plausibly cover one.
    probe_durations: list[float] = []

    def _time_for_another_probe() -> bool:
        remaining = max_seconds - (time.monotonic() - start)
        if remaining <= 2:
            return False
        if probe_durations:
            expected = sum(probe_durations) / len(probe_durations)
            if remaining < expected * 0.8:
                return False
        return True

    async def probe_score(cq: int) -> None:
        if cq in tried:
            return
        remaining = max_seconds - (time.monotonic() - start)
        attempt_start = time.monotonic()
        try:
            probe_path = await _encode_cq_probe(
                local_input, cq, req, encoder, task_label, tmp_dir, timeout=remaining
            )
            if probe_path is None:
                tried[cq] = 0.0
                return
            try:
                compressed_size = os.path.getsize(probe_path)
                remaining = max_seconds - (time.monotonic() - start)
                if remaining <= 2:
                    tried[cq] = 0.0
                    return
                vmaf = await _measure_vmaf(
                    local_input,
                    probe_path,
                    task_label,
                    n_subsample=n_subsample,
                    timeout=remaining,
                )
            finally:
                _cleanup(probe_path)
            if vmaf is None:
                tried[cq] = 0.0
                return
            s = _estimate_compression_score(
                original_size, compressed_size, vmaf, search_threshold
            )
            tried[cq] = s
            probed_vmaf[cq] = vmaf
            log.info(
                f"[{task_label}] cq search probe cq={cq} vmaf={vmaf:.2f} "
                f"score_est={s:.4f}"
            )
            _log_cq_search_sample(
                task_label=task_label,
                codec=req.codec,
                encoder_mode=req.encoder_mode,
                compression_type=req.compression_type,
                vmaf_threshold=req.vmaf_threshold,
                resolution=resolution,
                n_subsample=n_subsample,
                cq=cq,
                vmaf=vmaf,
                score_est=s,
                original_size=original_size,
                compressed_size=compressed_size,
            )
        finally:
            probe_durations.append(time.monotonic() - attempt_start)

    try:
        center = min(max(fallback_cq, CQ_SEARCH_MIN), CQ_SEARCH_MAX)
        step = 3
        # Under heavy concurrent load, probe_score's shrinking per-call
        # timeout means often only one of these initial probes actually
        # completes before the deadline (observed in production: a step of
        # 6 landed the sole surviving probe on center+6, which hard-failed,
        # wasting the whole budget and forcing fallback to the untouched
        # static cq every time). Use a smaller step so a single probe is
        # less likely to overshoot into hard-fail territory.
        # center is not just a fallback guess -- a systematic calibration
        # sweep (explicit cq bypassing search, real score via the production
        # formula, real captured clips) put it at or near the true optimum
        # at every tier tested this session. So center always goes first,
        # guaranteeing a real score even if only one probe fits in the time
        # budget.
        #
        # Past that, this replaces the old blind +/-3 neighbor-probe
        # coordinate-ascent with slope-informed jumps that aim directly at
        # search_threshold instead of hoping a fixed step happens to land
        # close. A pooled analysis of every real (cq, vmaf) pair collected
        # this session across both codecs put the empirical dVMAF/dcq slope
        # at a consistent -1.7 (n=48, range -0.44 to -2.68) -- steep enough
        # that blind +/-3 steps routinely overshoot past the hard-cutoff
        # (exactly what several real calibration runs this session showed:
        # a clip safely above threshold at one cq scoring zero just 2-3 cq
        # later). Use that as the first jump's slope prior, then refine with
        # the *actual* measured local slope for this specific clip on the
        # second jump -- real content varies 6x in how steep this curve is,
        # so the clip's own two data points beat any fixed prior. Whatever
        # time budget remains after these targeted jumps still falls through
        # to the coordinate-ascent loop below for further refinement.
        await probe_score(center)
        if center in probed_vmaf and _time_for_another_probe():
            vmaf0 = probed_vmaf[center]
            delta = (vmaf0 - search_threshold) / CQ_SEARCH_SLOPE_PRIOR
            jump1 = min(CQ_SEARCH_MAX, max(CQ_SEARCH_MIN, round(center + delta)))
            if jump1 == center:
                jump1 = min(CQ_SEARCH_MAX, max(CQ_SEARCH_MIN,
                    center + (1 if vmaf0 > search_threshold else -1)))
            await probe_score(jump1)
            if (
                jump1 in probed_vmaf
                and jump1 != center
                and _time_for_another_probe()
            ):
                vmaf1 = probed_vmaf[jump1]
                cq_gap = jump1 - center
                slope_actual = (vmaf1 - vmaf0) / cq_gap
                # A 1-cq baseline amplifies ordinary measurement noise into a
                # wild slope estimate (observed live: a tiny "slope" off a
                # 1-cq gap extrapolated to a jump 10 cq away, wasting the
                # probe). Only trust the measured slope with a >=2-cq
                # baseline; otherwise fall back to the pooled prior, which is
                # noisier per-clip but bounded.
                #
                # A content-adaptive version of the margin below (shrinking
                # it on demonstrably gentle-slope clips, using paired
                # subsampled-vs-full-precision measurements that showed real
                # noise is content-dependent) was tried and reverted: the
                # *first* jump's direction is decided against the inflated
                # margin-threshold, so on easy content it's already biased
                # toward more-conservative before there's any evidence the
                # content is easy, which undermines the adaptation before it
                # can help. Fixing that needs the first jump's direction
                # logic reworked too -- left as a scoped future project
                # rather than iterating further on live search behavior.
                slope_for_aim = (
                    slope_actual if abs(cq_gap) >= 2 else -CQ_SEARCH_SLOPE_PRIOR
                )
                if slope_for_aim < -0.1:
                    delta2 = (vmaf1 - search_threshold) / (-slope_for_aim)
                    delta2 = max(-8.0, min(8.0, delta2))
                    jump2 = min(CQ_SEARCH_MAX, max(CQ_SEARCH_MIN, round(jump1 + delta2)))
                    if jump2 not in tried:
                        await probe_score(jump2)

        while len(tried) < CQ_SEARCH_MAX_ITERS:
            if not tried or not _time_for_another_probe():
                break
            best_cq = max(tried, key=tried.get)
            neighbors = [
                c
                for c in (best_cq - step, best_cq + step)
                if CQ_SEARCH_MIN <= c <= CQ_SEARCH_MAX and c not in tried
            ]
            if not neighbors:
                if step == 1:
                    break
                step = max(1, step // 2)
                neighbors = [
                    c
                    for c in (best_cq - step, best_cq + step)
                    if CQ_SEARCH_MIN <= c <= CQ_SEARCH_MAX and c not in tried
                ]
                if not neighbors:
                    break
            for c in neighbors:
                if not _time_for_another_probe():
                    break
                await probe_score(c)
            step = max(1, step // 2)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    if not tried:
        return fallback_cq
    best_cq = max(tried, key=tried.get)
    if tried[best_cq] <= 0.0:
        return fallback_cq
    return best_cq


def _should_chunk(req: "CompressRequest", duration_seconds: float | None) -> bool:
    if req.chunked is not None:
        return req.chunked
    return (
        COMPRESSION_CHUNKING_ENABLED
        and duration_seconds is not None
        and duration_seconds >= COMPRESSION_CHUNK_MIN_DURATION_SECONDS
    )


def _concat_file_line(path: str) -> str:
    escaped = path.replace("'", "'\\''")
    return f"file '{escaped}'\n"


async def _split_at_keyframes(
    local_input: str,
    segments_dir: str,
    task_label: str,
    chunk_duration_seconds: int,
) -> list[str]:
    os.makedirs(segments_dir, exist_ok=True)
    segment_pattern = os.path.join(segments_dir, "input_%05d.mp4")
    cmd = [
        FFMPEG_BIN,
        "-hide_banner",
        "-y",
        "-i",
        local_input,
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c",
        "copy",
        "-sn",
        "-dn",
        "-f",
        "segment",
        "-segment_time",
        str(chunk_duration_seconds),
        "-reset_timestamps",
        "1",
        "-segment_format",
        "mp4",
        segment_pattern,
    ]
    returncode, stdout, stderr, run_error = await _run_process(
        cmd, task_label, "split segments"
    )
    if returncode != 0 or run_error:
        detail = (
            run_error
            or _format_process_output(stdout, stderr)
            or "segment split failed"
        )
        raise RuntimeError(detail)

    segments = sorted(
        os.path.join(segments_dir, filename)
        for filename in os.listdir(segments_dir)
        if filename.startswith("input_") and filename.endswith(".mp4")
    )
    if not segments:
        raise RuntimeError("segment split produced no files")
    return segments


async def _compress_chunked(
    local_input: str,
    output_path: str,
    req: "CompressRequest",
    encoder: str,
    task_label: str,
) -> None:
    chunk_duration_seconds = (
        req.chunk_duration_seconds or COMPRESSION_CHUNK_TARGET_SECONDS
    )
    parallelism = max(1, req.chunk_parallelism or COMPRESSION_CHUNK_PARALLELISM)
    work_dir = os.path.join(SHARED_VOLUME_PATH, f"{task_label}_chunks")
    encoded_dir = os.path.join(work_dir, "encoded")
    tracked_paths: list[str] = []

    try:
        input_segments = await _split_at_keyframes(
            local_input, work_dir, task_label, chunk_duration_seconds
        )
        if len(input_segments) < 2:
            raise RuntimeError(
                "segment split produced one chunk; falling back to single-pass compression"
            )
        await _log_chunk_seams(input_segments, task_label)

        os.makedirs(encoded_dir, exist_ok=True)
        encoded_segments = [
            os.path.join(encoded_dir, f"encoded_{index:05d}.mp4")
            for index, _ in enumerate(input_segments)
        ]
        tracked_paths = [*input_segments, *encoded_segments]
        await _track_temp_files(*tracked_paths)

        log.info(
            f"[{task_label}] Compressing {len(input_segments)} chunks "
            f"(target={chunk_duration_seconds}s, parallelism={parallelism})"
        )
        semaphore = asyncio.Semaphore(parallelism)

        async def _compress_one(index: int, segment_input: str, segment_output: str):
            async with semaphore:
                cmd = _build_ffmpeg_args(segment_input, segment_output, req, encoder)
                returncode, stdout, stderr, run_error = await _run_process(
                    cmd,
                    task_label,
                    f"compress chunk {index + 1}/{len(input_segments)}",
                )
                if returncode != 0 or run_error:
                    detail = (
                        run_error
                        or _format_process_output(stdout, stderr)
                        or "chunk compression failed"
                    )
                    raise RuntimeError(f"chunk {index + 1} failed: {detail}")
                if not os.path.exists(segment_output):
                    raise RuntimeError(
                        f"chunk {index + 1} output missing: {segment_output}"
                    )

        chunk_results = await asyncio.gather(
            *[
                _compress_one(index, segment_input, encoded_segments[index])
                for index, segment_input in enumerate(input_segments)
            ],
            return_exceptions=True,
        )
        chunk_errors = [
            result for result in chunk_results if isinstance(result, Exception)
        ]
        if chunk_errors:
            raise RuntimeError(str(chunk_errors[0]))

        concat_list = os.path.join(work_dir, "concat.txt")
        with open(concat_list, "w", encoding="utf-8") as file:
            for segment in encoded_segments:
                file.write(_concat_file_line(segment))

        cmd = [
            FFMPEG_BIN,
            "-hide_banner",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            concat_list,
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            output_path,
        ]
        returncode, stdout, stderr, run_error = await _run_process(
            cmd, task_label, "merge chunks"
        )
        if returncode != 0 or run_error:
            detail = (
                run_error
                or _format_process_output(stdout, stderr)
                or "chunk merge failed"
            )
            raise RuntimeError(detail)
    finally:
        await _untrack_temp_files(*tracked_paths)
        _cleanup_tree(work_dir)


def _cleanup_config_status() -> dict[str, object]:
    return {
        "enabled": CLEANUP_ENABLED,
        "interval_seconds": CLEANUP_INTERVAL_SECONDS,
        "temp_file_ttl_seconds": TEMP_FILE_TTL_SECONDS,
        "max_volume_bytes": CLEANUP_MAX_VOLUME_BYTES,
        "min_file_age_seconds": CLEANUP_MIN_FILE_AGE_SECONDS,
        "presigned_url_expiry_seconds": min(S3_PRESIGNED_EXPIRY, 604800),
        "presigned_url_cleanup_grace_seconds": PRESIGNED_URL_CLEANUP_GRACE_SECONDS,
        "storage_cleanup_enabled": STORAGE_CLEANUP_ENABLED,
        "storage_object_ttl_seconds": STORAGE_OBJECT_TTL_SECONDS,
        "storage_cleanup_prefixes": STORAGE_CLEANUP_PREFIXES,
    }


def _shared_root() -> str:
    return os.path.abspath(SHARED_VOLUME_PATH)


def _normalize_path(path: str) -> str:
    return os.path.abspath(path)


def _is_shared_path(path: str) -> bool:
    try:
        return (
            os.path.commonpath([_shared_root(), _normalize_path(path)])
            == _shared_root()
        )
    except ValueError:
        return False


async def _track_temp_files(*paths: str):
    async with _lock:
        for path in paths:
            if path and _is_shared_path(path):
                _active_file_paths.add(_normalize_path(path))


async def _untrack_temp_files(*paths: str):
    async with _lock:
        for path in paths:
            if path:
                _active_file_paths.discard(_normalize_path(path))


async def _protected_paths_snapshot() -> set[str]:
    async with _lock:
        return set(_active_file_paths)


def _remove_stale_file(path: str, reason: str) -> int:
    try:
        size = os.path.getsize(path)
        os.remove(path)
        log.info(f"Removed {reason} temp file: {path} ({size} bytes)")
        return size
    except FileNotFoundError:
        return 0
    except OSError as e:
        log.warning(f"Failed to remove temp file {path}: {e}")
        return 0


async def _cleanup_shared_volume_once():
    if not CLEANUP_ENABLED or not os.path.isdir(SHARED_VOLUME_PATH):
        return

    now = time.time()
    protected_paths = await _protected_paths_snapshot()
    total_bytes = 0
    candidates: list[tuple[float, str, int]] = []

    for root, dirs, files in os.walk(SHARED_VOLUME_PATH):
        # PERSISTENT_DATA_DIR holds long-lived data (e.g. the CQ search
        # log) that this TTL/quota sweep must never treat as scratch --
        # pruning it from os.walk's traversal (not just skipping matched
        # files) also protects anything nested inside it in the future.
        dirs[:] = [d for d in dirs if os.path.join(root, d) != PERSISTENT_DATA_DIR]
        for filename in files:
            path = os.path.abspath(os.path.join(root, filename))
            try:
                stat = os.stat(path)
            except FileNotFoundError:
                continue

            total_bytes += stat.st_size
            if path in protected_paths:
                continue

            candidates.append((stat.st_mtime, path, stat.st_size))
            if now - stat.st_mtime >= TEMP_FILE_TTL_SECONDS:
                total_bytes -= _remove_stale_file(path, "expired")

    if CLEANUP_MAX_VOLUME_BYTES <= 0 or total_bytes <= CLEANUP_MAX_VOLUME_BYTES:
        return

    for _, path, size in sorted(candidates):
        if total_bytes <= CLEANUP_MAX_VOLUME_BYTES:
            break
        if path in protected_paths or not os.path.exists(path):
            continue
        try:
            file_age = now - os.path.getmtime(path)
        except FileNotFoundError:
            continue
        if file_age < CLEANUP_MIN_FILE_AGE_SECONDS:
            continue
        total_bytes -= _remove_stale_file(path, "over-quota")


def _storage_cleanup_ready() -> bool:
    return (
        STORAGE_CLEANUP_ENABLED
        and STORAGE_OBJECT_TTL_SECONDS > 0
        and bool(STORAGE_CLEANUP_PREFIXES)
        and bool(S3_BUCKET)
        and bool(S3_ACCESS_KEY_ID)
        and bool(S3_SECRET_ACCESS_KEY)
    )


def _cleanup_expired_storage_objects_once() -> int:
    if not _storage_cleanup_ready():
        return 0

    cutoff = datetime.now(timezone.utc) - timedelta(seconds=STORAGE_OBJECT_TTL_SECONDS)
    client = _get_s3_client()
    deleted = 0

    for prefix in STORAGE_CLEANUP_PREFIXES:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            expired_objects = []
            for obj in page.get("Contents", []):
                last_modified = obj.get("LastModified")
                if last_modified is None:
                    continue
                if last_modified.tzinfo is None:
                    last_modified = last_modified.replace(tzinfo=timezone.utc)
                if last_modified < cutoff:
                    expired_objects.append({"Key": obj["Key"]})

            for index in range(0, len(expired_objects), 1000):
                batch = expired_objects[index : index + 1000]
                if not batch:
                    continue
                response = client.delete_objects(
                    Bucket=S3_BUCKET,
                    Delete={"Objects": batch, "Quiet": True},
                )
                deleted += len(batch) - len(response.get("Errors", []))

    if deleted:
        log.info(f"Deleted {deleted} expired {STORAGE_PROVIDER} object(s)")
    return deleted


async def _cleanup_worker():
    if not CLEANUP_ENABLED and not _storage_cleanup_ready():
        log.info("Cleanup worker disabled")
        return

    log.info(
        "Starting cleanup worker "
        f"(path={SHARED_VOLUME_PATH}, ttl={TEMP_FILE_TTL_SECONDS}s, "
        f"interval={CLEANUP_INTERVAL_SECONDS}s, max_bytes={CLEANUP_MAX_VOLUME_BYTES}, "
        f"storage_ttl={STORAGE_OBJECT_TTL_SECONDS}s)"
    )

    while True:
        try:
            if CLEANUP_ENABLED:
                await _cleanup_shared_volume_once()
            if _storage_cleanup_ready():
                await asyncio.to_thread(_cleanup_expired_storage_objects_once)
        except Exception as e:
            log.warning(f"Cleanup pass failed: {e}")
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)


def _queue_capacity() -> int:
    return MAX_CONCURRENT + MAX_QUEUE_SIZE


def _queue_snapshot_locked() -> dict[str, int]:
    queued_tasks = max(0, _queue_size - _active_count)
    return {
        "max_concurrent": MAX_CONCURRENT,
        "max_queue_size": MAX_QUEUE_SIZE,
        "active_tasks": _active_count,
        "queued_tasks": queued_tasks,
        "total_pending": _queue_size,
        "queue_capacity_remaining": max(0, _queue_capacity() - _queue_size),
    }


async def _queue_snapshot() -> dict[str, int]:
    async with _lock:
        return _queue_snapshot_locked()


@asynccontextmanager
async def _queued_task(task_label: str):
    global _queue_size

    async with _lock:
        if _queue_size >= _queue_capacity():
            snapshot = _queue_snapshot_locked()
            detail = (
                f"Compression queue full: {snapshot['queued_tasks']}/{MAX_QUEUE_SIZE} queued, "
                f"{snapshot['active_tasks']}/{MAX_CONCURRENT} active"
            )
            log.warning(f"[{task_label}] {detail}")
            raise HTTPException(status_code=429, detail=detail)

        _queue_size += 1
        queue_position = max(0, _queue_size - MAX_CONCURRENT)
        snapshot = _queue_snapshot_locked()

    try:
        yield queue_position, snapshot
    finally:
        async with _lock:
            _queue_size = max(0, _queue_size - 1)


@asynccontextmanager
async def _running_task():
    global _active_count

    async with _semaphore:
        async with _lock:
            _active_count += 1
            snapshot = _queue_snapshot_locked()

        try:
            yield snapshot
        finally:
            async with _lock:
                _active_count = max(0, _active_count - 1)


class CompressRequest(BaseModel):
    video_paths: list[str] = Field(
        ...,
        min_length=1,
        max_length=5,
        description="Input video path or URL list, up to 5 items",
    )
    output_paths: list[str] = Field(
        default_factory=list,
        max_length=5,
        description="Optional caller-owned local output paths",
    )
    task_id: str = Field("", description="Task ID for logging")
    codec: str = Field("AV1", description="Target codec: AV1, H264, HEVC, VP9")
    codec_mode: str = Field("CRF", description="Rate control mode: CRF or VBR")
    cq: Optional[int] = Field(
        None,
        ge=0,
        le=63,
        description="Optional explicit CQ override (lower = higher quality)",
    )
    compression_type: Optional[Literal["Low", "Medium", "High"]] = Field(
        None,
        description="Optional inference quality tier used when cq is omitted",
    )
    preset: str = Field("p4", description="Encoder preset")
    target_bitrate: Optional[int] = Field(
        None, description="Target bitrate in bps (for VBR mode)"
    )
    nvenc_multipass: Optional[str] = Field(
        None, description="Per-request NVENC -multipass override (e.g. 'fullres', 'qres'). VBR-mode experiment only."
    )
    nvenc_rc_lookahead: Optional[int] = Field(
        None, description="Per-request NVENC -rc-lookahead override (frames). VBR-mode experiment only."
    )
    nvenc_spatial_aq: Optional[bool] = Field(
        None, description="Per-request NVENC -spatial-aq override. VBR-mode experiment only."
    )
    nvenc_temporal_aq: Optional[bool] = Field(
        None, description="Per-request NVENC -temporal-aq override. VBR-mode experiment only."
    )
    nvenc_aq_strength: Optional[int] = Field(
        None, description="Per-request NVENC -aq-strength override (1-15). VBR-mode experiment only."
    )
    target_width: Optional[int] = Field(
        None, description="Target width for downscaling"
    )
    target_height: Optional[int] = Field(
        None, description="Target height for downscaling"
    )
    chunked: Optional[bool] = Field(
        None, description="Override automatic long-video chunking"
    )
    chunk_duration_seconds: Optional[int] = Field(
        None, description="Target chunk duration"
    )
    chunk_parallelism: Optional[int] = Field(
        None, description="Parallel chunk encodes per request"
    )
    vmaf_threshold: Optional[float] = Field(
        None,
        ge=0,
        le=100,
        description="Competition quality floor communicated to customized solutions",
    )
    encoder_mode: Optional[str] = Field(
        None,
        description="Per-request override of AV1_ENCODER_MODE ('nvenc' or 'svt'). "
        "Only the organic job/poll path (minutes-scale budget, not scored "
        "synthetic traffic) should ever set this to 'svt' -- slow SVT-AV1 is "
        "more bit-efficient than NVENC but 10-15x slower.",
    )
    deadline_seconds: Optional[float] = Field(
        None,
        gt=0,
        description="Per-request override of COMPRESSION_OVERALL_DEADLINE_SECONDS, "
        "for callers with a longer budget than the default synchronous path.",
    )
    search_max_seconds: Optional[float] = Field(
        None,
        gt=0,
        description="Per-request override of CQ_SEARCH_MAX_SECONDS. Needed alongside "
        "deadline_seconds for slow encoders (e.g. SVT-AV1 at a real quality preset) "
        "where a single probe can itself exceed the default 90s search cap, which "
        "would otherwise silently disable the search regardless of how generous "
        "deadline_seconds is.",
    )
    vmaf_n_subsample: Optional[int] = Field(
        None,
        ge=1,
        description="Per-request override of the resolution-based VMAF n_subsample "
        "search probes normally use. n_subsample>1 trades measurement accuracy for "
        "speed under the tight synchronous budget; with a generous deadline (e.g. "
        "the organic job path) forcing 1 gets an accurate signal instead of a noisy "
        "one, and the safety margin (which scales with n_subsample) shrinks to match "
        "automatically -- so the search can tell 'really passes' from 'really fails' "
        "instead of everything looking like a marginal soft-zone case.",
    )

    @model_validator(mode="after")
    def validate_output_paths(self):
        if self.output_paths and len(self.output_paths) != len(self.video_paths):
            raise ValueError("output_paths must have the same length as video_paths")
        if len(self.output_paths) != len(set(self.output_paths)):
            raise ValueError("output_paths values must be unique")
        return self


class CompressResponse(BaseModel):
    output_paths: list[str] = Field(
        default_factory=list, description="Per-input local output paths"
    )
    output_urls: list[str] = Field(
        default_factory=list, description="Per-input S3 presigned URLs"
    )
    errors: list[Optional[str]] = Field(
        default_factory=list, description="Per-input errors"
    )
    success: bool
    active_tasks: Optional[int] = None
    queued_tasks: Optional[int] = None


class CompetitionCompressionItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evaluation_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._-]+$"
    )
    input_path: str
    output_path: str
    codec: Literal["AV1"] = "AV1"
    codec_mode: Literal["CRF", "VBR"] = "CRF"
    target_bitrate: Literal[5_000_000, 8_000_000, 10_000_000] | None = None
    vmaf_threshold: float = Field(ge=0, le=100)

    @model_validator(mode="after")
    def validate_rate_control(self):
        if self.codec_mode == "VBR" and self.target_bitrate is None:
            raise ValueError("VBR competition item requires target_bitrate")
        if self.codec_mode == "CRF" and self.target_bitrate is not None:
            raise ValueError("CRF competition item cannot set target_bitrate")
        return self


class CompetitionCompressionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    competition_id: str = Field(min_length=1, max_length=64)
    hotkey: str = Field(min_length=1, max_length=128)
    batch_id: str = Field(min_length=1, max_length=128)
    items: list[CompetitionCompressionItem] = Field(min_length=1, max_length=5)

    @model_validator(mode="after")
    def validate_unique_values(self):
        if len({item.evaluation_id for item in self.items}) != len(self.items):
            raise ValueError("evaluation_id values must be unique")
        if len({item.output_path for item in self.items}) != len(self.items):
            raise ValueError("output_path values must be unique")
        return self


class CompetitionCompressionResult(BaseModel):
    output_path: str | None = None


class CompetitionCompressionResponse(BaseModel):
    results: list[CompetitionCompressionResult]


@app.get("/health")
async def health():
    snapshot = await _queue_snapshot()
    return {
        "status": "ok",
        **snapshot,
        "storage": _storage_config_status(),
        "cleanup": _cleanup_config_status(),
        "compression": _compression_config_status(),
        "competition_local_io": {
            "remote_io_disabled": DISABLE_REMOTE_IO,
            "input_root": COMPETITION_INPUT_ROOT,
            "output_root": COMPETITION_OUTPUT_ROOT,
        },
    }


@app.get("/queue")
async def queue_status():
    return await _queue_snapshot()


async def _compress_one(
    req: CompressRequest,
    input_video: str,
    task_label: str,
    *,
    requested_output_path: str | None = None,
) -> CompressResponse:
    item_start = time.monotonic()
    overall_deadline = item_start + (req.deadline_seconds or COMPRESSION_OVERALL_DEADLINE_SECONDS)
    remote_mode = _is_url(input_video)
    encoder = CODEC_MAP.get(req.codec.upper(), "av1_nvenc")
    effective_av1_mode = req.encoder_mode or AV1_ENCODER_MODE
    if req.codec.upper() == "AV1" and effective_av1_mode == "svt":
        encoder = "libsvtav1"
    resolved_cq = resolve_compression_cq(
        explicit_cq=req.cq,
        compression_type=req.compression_type,
        vmaf_threshold=req.vmaf_threshold,
        codec=req.codec,
        encoder_mode=effective_av1_mode,
    )

    if remote_mode and DISABLE_REMOTE_IO:
        return CompressResponse(
            success=False,
            errors=["Remote URL input is disabled for this local-only service"],
        )

    local_input = ""
    output_path = ""
    output_filename = ""
    returncode: int | None = None
    stdout = b""
    stderr = b""
    run_error = ""

    try:
        async with _queued_task(task_label) as (queue_position, snapshot):
            log.info(
                f"[{task_label}] Queued compression "
                f"(codec={encoder}, cq={resolved_cq}, position={queue_position}, "
                f"waiting={snapshot['queued_tasks']}, remote={remote_mode})"
            )

            os.makedirs(SHARED_VOLUME_PATH, exist_ok=True)

            # --- Resolve input to local path ---
            if remote_mode:
                local_input = os.path.join(
                    SHARED_VOLUME_PATH, f"{task_label}_input.mp4"
                )
                await _track_temp_files(local_input)
                try:
                    await _download_url(input_video, local_input)
                except Exception as e:
                    _cleanup(local_input)
                    return CompressResponse(
                        success=False, errors=[f"Failed to download input: {e}"]
                    )
            else:
                local_input = input_video
                if not os.path.exists(local_input):
                    raise HTTPException(
                        status_code=400, detail=f"Input file not found: {local_input}"
                    )
                await _track_temp_files(local_input)

            basename = os.path.splitext(os.path.basename(local_input))[0]
            output_filename = f"{basename}_compressed.mp4"
            output_path = requested_output_path or os.path.join(
                SHARED_VOLUME_PATH, output_filename
            )
            await _track_temp_files(output_path)

            duration_seconds = await _probe_duration_seconds(local_input, task_label)
            use_chunked = _should_chunk(req, duration_seconds)

            if (
                CQ_SEARCH_ENABLED
                and req.codec_mode != "VBR"
                and req.vmaf_threshold is not None
                and req.cq is None
                and not use_chunked
                and (
                    duration_seconds is None
                    or duration_seconds <= CQ_SEARCH_MAX_DURATION_SECONDS
                )
            ):
                searched_cq = await _search_cq_for_max_score(
                    local_input,
                    req,
                    encoder,
                    task_label,
                    deadline=overall_deadline - COMPRESSION_FINAL_ENCODE_RESERVE_SECONDS,
                )
                log.info(f"[{task_label}] adaptive cq search selected cq={searched_cq}")
                req.cq = searched_cq
                resolved_cq = searched_cq

            async with _running_task() as running_snapshot:
                log.info(
                    f"[{task_label}] Starting compression "
                    f"(active={running_snapshot['active_tasks']}/{MAX_CONCURRENT}, "
                    f"queued={running_snapshot['queued_tasks']}, "
                    f"duration={duration_seconds}, chunked={use_chunked})"
                )

                if use_chunked:
                    try:
                        await _compress_chunked(
                            local_input, output_path, req, encoder, task_label
                        )
                        returncode = 0
                    except RuntimeError as e:
                        if "falling back to single-pass compression" in str(e):
                            log.warning(f"[{task_label}] {e}")
                            cmd = _build_ffmpeg_args(
                                local_input, output_path, req, encoder
                            )
                            returncode, stdout, stderr, run_error = await _run_process(
                                cmd,
                                task_label,
                                "single-pass compression fallback",
                                timeout=max(15.0, overall_deadline - time.monotonic()),
                            )
                        else:
                            run_error = str(e)
                            log.error(
                                f"[{task_label}] Chunked compression failed: {run_error}"
                            )
                else:
                    cmd = _build_ffmpeg_args(local_input, output_path, req, encoder)
                    returncode, stdout, stderr, run_error = await _run_process(
                        cmd,
                        task_label,
                        "single-pass compression",
                        timeout=max(15.0, overall_deadline - time.monotonic()),
                    )

        snapshot = await _queue_snapshot()
        stats = dict(
            active_tasks=snapshot["active_tasks"], queued_tasks=snapshot["queued_tasks"]
        )

        if returncode is None:
            _cleanup(output_path)
            if remote_mode:
                _cleanup(local_input)
            return CompressResponse(
                success=False,
                errors=[run_error or "compression did not start"],
                **stats,
            )

        if returncode != 0:
            err_msg = (
                run_error
                or _format_process_output(stdout, stderr)
                or "compression failed without output"
            )
            log.error(f"[{task_label}] ffmpeg failed (rc={returncode}): {err_msg}")
            _cleanup(output_path)
            if remote_mode:
                _cleanup(local_input)
            return CompressResponse(success=False, errors=[err_msg], **stats)

        if not os.path.exists(output_path):
            log.error(f"[{task_label}] Output file not found: {output_path}")
            if remote_mode:
                _cleanup(local_input)
            return CompressResponse(
                success=False, errors=["Output file not created"], **stats
            )

        # --- Remote mode: upload result to S3, return URL ---
        if remote_mode:
            try:
                s3_key = f"processing/{task_label}/{output_filename}"
                output_url = _upload_to_s3(output_path, s3_key)
                log.info(
                    f"[{task_label}] Compression complete (remote): {output_url[:80]}..."
                )
                return CompressResponse(
                    output_urls=[output_url], errors=[None], success=True, **stats
                )
            except Exception as e:
                log.error(f"[{task_label}] S3 upload failed: {e}")
                return CompressResponse(
                    success=False, errors=[f"S3 upload failed: {e}"], **stats
                )
            finally:
                _cleanup(local_input, output_path)

        log.info(f"[{task_label}] Compression complete (local): {output_path}")
        return CompressResponse(
            output_paths=[output_path], errors=[None], success=True, **stats
        )
    finally:
        await _untrack_temp_files(local_input, output_path)


def _combine_compress_responses(responses: list[CompressResponse]) -> CompressResponse:
    output_paths = [
        response.output_paths[0] if response.output_paths else ""
        for response in responses
    ]
    output_urls = [
        response.output_urls[0] if response.output_urls else ""
        for response in responses
    ]
    errors = [response.errors[0] if response.errors else None for response in responses]
    success = all(response.success for response in responses)
    latest = responses[-1] if responses else None

    return CompressResponse(
        output_paths=output_paths,
        output_urls=output_urls,
        errors=errors,
        success=success,
        active_tasks=latest.active_tasks if latest else None,
        queued_tasks=latest.queued_tasks if latest else None,
    )


def _competition_input_path(raw_path: str) -> Path:
    if _is_url(raw_path) or not os.path.isabs(raw_path):
        raise ValueError("competition input must be an absolute local path")
    root = Path(COMPETITION_INPUT_ROOT).resolve(strict=True)
    path = Path(raw_path).resolve(strict=True)
    if path == root or not path.is_relative_to(root) or not path.is_file():
        raise ValueError("competition input must be a file below /evaluation-inputs")
    return path


def _competition_output_path(raw_path: str) -> Path:
    if (
        _is_url(raw_path)
        or not os.path.isabs(raw_path)
        or not raw_path.lower().endswith(".mp4")
    ):
        raise ValueError("competition output must be an absolute local MP4 path")
    root = Path(COMPETITION_OUTPUT_ROOT).resolve(strict=True)
    path = Path(raw_path)
    if ".." in path.parts:
        raise ValueError("competition output cannot contain traversal")
    lexical_root = Path(COMPETITION_OUTPUT_ROOT).absolute()
    if path == lexical_root or not path.is_relative_to(lexical_root):
        raise ValueError("competition output must be below /output")
    parent = path.parent
    if not parent.is_dir():
        raise ValueError("competition output parent must already exist")
    resolved = path.resolve(strict=False)
    if resolved == root or not resolved.is_relative_to(root):
        raise ValueError("competition output must be below /output")
    current = root
    for part in resolved.relative_to(root).parts[:-1]:
        current = current / part
        if current.is_symlink():
            raise ValueError("competition output cannot traverse symlinks")
    if path.exists() or path.is_symlink():
        raise ValueError("competition output must not overwrite an existing path")
    return resolved


async def _compress_competition(
    req: CompetitionCompressionRequest,
) -> CompetitionCompressionResponse:
    if not DISABLE_REMOTE_IO:
        raise HTTPException(
            status_code=503, detail="competition route requires DISABLE_REMOTE_IO=true"
        )
    prepared: list[tuple[CompetitionCompressionItem, Path, Path]] = []
    try:
        for item in req.items:
            prepared.append(
                (
                    item,
                    _competition_input_path(item.input_path),
                    _competition_output_path(item.output_path),
                )
            )
    except (OSError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    log.info(
        "[competition:%s] Batch received (hotkey=%s, items=%s)",
        req.batch_id,
        req.hotkey,
        [item.evaluation_id for item, _, _ in prepared],
    )

    async def run(
        item: CompetitionCompressionItem, input_path: Path, output_path: Path
    ) -> CompetitionCompressionResult:
        legacy = CompressRequest(
            video_paths=[str(input_path)],
            task_id=item.evaluation_id,
            codec=item.codec,
            codec_mode=item.codec_mode,
            target_bitrate=item.target_bitrate,
            vmaf_threshold=item.vmaf_threshold,
        )
        response = await _compress_one(
            legacy,
            str(input_path),
            item.evaluation_id,
            requested_output_path=str(output_path),
        )
        if not response.success:
            log.error(
                "[%s] Competition compression failed: %s",
                item.evaluation_id,
                response.errors[0] if response.errors else "compression failed",
            )
            return CompetitionCompressionResult(output_path=None)
        return CompetitionCompressionResult(output_path=item.output_path)

    results = await asyncio.gather(*(run(*item) for item in prepared))
    log.info(
        "[competition:%s] Batch complete (scored_outputs=%s, failed_outputs=%s)",
        req.batch_id,
        sum(result.output_path is not None for result in results),
        sum(result.output_path is None for result in results),
    )
    return CompetitionCompressionResponse(results=list(results))


@app.post("/compress", response_model=CompressResponse | CompetitionCompressionResponse)
async def compress(req: CompressRequest | CompetitionCompressionRequest):
    if isinstance(req, CompetitionCompressionRequest):
        return await _compress_competition(req)

    input_videos = [video_path.strip() for video_path in req.video_paths]
    if any(not video_path for video_path in input_videos):
        raise HTTPException(status_code=400, detail="video_paths entries are required")

    base_task_id = req.task_id or uuid.uuid4().hex[:8]
    requested_outputs: list[str | None] = [None] * len(input_videos)
    if req.output_paths:
        if not DISABLE_REMOTE_IO:
            raise HTTPException(
                status_code=400,
                detail="caller-owned output_paths require DISABLE_REMOTE_IO=true",
            )
        try:
            requested_outputs = [
                str(_competition_output_path(path)) for path in req.output_paths
            ]
        except (OSError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    responses = await asyncio.gather(
        *[
            _compress_one(
                req,
                input_video,
                base_task_id
                if len(input_videos) == 1
                else f"{base_task_id}-{index + 1}",
                requested_output_path=requested_outputs[index],
            )
            for index, input_video in enumerate(input_videos)
        ]
    )

    return (
        responses[0] if len(responses) == 1 else _combine_compress_responses(responses)
    )

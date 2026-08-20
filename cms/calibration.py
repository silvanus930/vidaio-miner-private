"""Browse the calibration log and trigger new calibration runs from the CMS
instead of hand-launching scripts/calibrate_cq_curve.py on the command line.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import db

REPO_ROOT = Path("/root/vidaio-subnet")
CALIBRATION_LOG_PATH = Path(
    "/tmp/vidaio-miner-video-tmp/_persistent/cq_calibration_log.jsonl"
)
CALIBRATION_LIBRARY_PATH = Path("/tmp/vidaio-miner-video-tmp/calib-library")
REAL_CONTENT_LIBRARY_PATH = Path("/root/vidaio-real-content-library")


def read_calibration_log(codec: str | None = None, vmaf_threshold: float | None = None, limit: int = 200) -> list[dict]:
    if not CALIBRATION_LOG_PATH.exists():
        return []
    rows = []
    with open(CALIBRATION_LOG_PATH) as f:
        for line in f:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if codec and str(row.get("codec", "")).lower() != codec.lower():
                continue
            if vmaf_threshold is not None and row.get("vmaf_threshold") != vmaf_threshold:
                continue
            rows.append(row)
    rows.sort(key=lambda r: r.get("ts", 0), reverse=True)
    return rows[:limit]


def start_calibration(codec: str, vmaf_threshold: float, cq_grid: str, limit_clips: int) -> int:
    """Launches calibrate_cq_curve.py as a detached background process and
    tracks it in the calibration_runs table. Only ever reads/copies from
    the real-content-library and writes to the same persistent calibration
    log every other calibration this session used -- never touches live
    traffic or the deployed CQ tables.
    """
    CALIBRATION_LIBRARY_PATH.mkdir(parents=True, exist_ok=True)
    codec_lower = codec.lower()
    for mp4 in REAL_CONTENT_LIBRARY_PATH.glob(f"*_{codec_lower}_thr{vmaf_threshold}.mp4"):
        dest = CALIBRATION_LIBRARY_PATH / mp4.name
        if not dest.exists():
            dest.write_bytes(mp4.read_bytes())
            json_src = Path(f"{mp4}.json")
            if json_src.exists():
                Path(f"{dest}.json").write_text(json_src.read_text())

    started_at = datetime.now(timezone.utc).isoformat()
    log_path = REPO_ROOT / "cms" / "_calibration_run_output.log"
    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "calibrate_cq_curve.py"),
        "--codec", codec_lower,
        "--vmaf-threshold", str(vmaf_threshold),
        "--cq-grid", cq_grid,
        "--limit-clips", str(limit_clips),
        "--library-path", str(CALIBRATION_LIBRARY_PATH),
        "--container-library-path", "/tmp/organic-proxy/calib-library",
        "--out", str(CALIBRATION_LOG_PATH),
    ]
    with open(log_path, "a") as logf:
        logf.write(f"\n=== run started {started_at} ===\n")
        proc = subprocess.Popen(
            cmd, cwd=str(REPO_ROOT), stdout=logf, stderr=subprocess.STDOUT,
        )
    run_id = db.start_calibration_run(codec_lower, vmaf_threshold, cq_grid, limit_clips, proc.pid, started_at)
    return run_id


def poll_calibration_run(run_id: int) -> dict | None:
    run = db.get_calibration_run(run_id)
    if run is None:
        return None
    log_path = REPO_ROOT / "cms" / "_calibration_run_output.log"
    tail = ""
    if log_path.exists():
        lines = log_path.read_text(errors="replace").splitlines()
        tail = "\n".join(lines[-30:])

    still_running = False
    if run["pid"]:
        try:
            import os
            os.kill(run["pid"], 0)
            still_running = True
        except (ProcessLookupError, PermissionError):
            still_running = False

    if run["status"] == "running" and not still_running:
        finished_at = datetime.now(timezone.utc).isoformat()
        db.update_calibration_run(run_id, "done", tail, finished_at)
        run = db.get_calibration_run(run_id)
    elif tail != run.get("log_tail"):
        db.update_calibration_run(run_id, run["status"], tail)
        run = db.get_calibration_run(run_id)
    return run

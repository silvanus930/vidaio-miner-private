"""Turns conditions already visible in the tracker log and the items table
into alert rows instead of requiring someone to notice them by eye. Called
from the ingest loop (see ingest.py) every cycle -- cheap checks only, no
new polling of its own.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import db

TRACKER_LOG_PATH = Path("/tmp/vidaio-tracker.log")
DEADLINE_WARN_SECONDS = 165  # validator's hard cutoff is 180s
DEADLINE_BREACH_SECONDS = 180


def _latest_tracker_snapshot() -> dict | None:
    if not TRACKER_LOG_PATH.exists():
        return None
    try:
        with open(TRACKER_LOG_PATH, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 4096))
            lines = f.read().decode(errors="replace").strip().splitlines()
        if not lines:
            return None
        return json.loads(lines[-1])
    except (OSError, json.JSONDecodeError):
        return None


def check_alerts(new_items: list[dict]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    count = 0

    for item in new_items:
        elapsed = item.get("elapsed_seconds")
        if elapsed is None:
            continue
        if elapsed >= DEADLINE_BREACH_SECONDS:
            db.add_alert(
                "deadline_breach", "critical",
                f"Item {item['task_id']} took {elapsed:.1f}s, over the 180s validator deadline",
                dedupe_key=f"deadline_breach:{item['task_id']}", ts=now,
            )
            count += 1
        elif elapsed >= DEADLINE_WARN_SECONDS:
            db.add_alert(
                "deadline_warn", "warning",
                f"Item {item['task_id']} took {elapsed:.1f}s, close to the 180s validator deadline",
                dedupe_key=f"deadline_warn:{item['task_id']}", ts=now,
            )
            count += 1
        if item.get("score") == 0.0:
            db.add_alert(
                "zero_score", "warning",
                f"Item {item['task_id']} ({item.get('codec')} thr{item.get('vmaf_threshold')}) scored 0 "
                f"(vmaf={item.get('vmaf')}, ratio={item.get('ratio')})",
                dedupe_key=f"zero_score:{item['task_id']}", ts=now,
            )
            count += 1

    snap = _latest_tracker_snapshot()
    if snap:
        snap_ts = snap.get("ts", now)
        if not snap.get("compression_healthy", True):
            db.add_alert(
                "service_unhealthy", "critical", "Compression service health check failing",
                dedupe_key=f"service_unhealthy:{snap_ts}", ts=now,
            )
            count += 1
        if not snap.get("axon_process_running", True):
            db.add_alert(
                "axon_down", "critical", "Miner axon process not running",
                dedupe_key=f"axon_down:{snap_ts}", ts=now,
            )
            count += 1
        restarts = snap.get("compression_restarts", 0) or 0
        if restarts > 0:
            db.add_alert(
                "restarts", "warning", f"Compression container has restarted {restarts} time(s)",
                dedupe_key=f"restarts:{restarts}", ts=now,
            )
            count += 1
    return count

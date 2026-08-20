"""Tails compression_outcomes.jsonl and populates the CMS database.

Runs as its own long-lived loop (see cms/run_ingest_loop.sh), independent
of the compression service and the miner process -- it only reads a log
file and two sample directories on the shared host filesystem, so it can
be restarted or iterated on without touching production traffic at all.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import alerting
import db

OUTCOME_LOG_PATH = Path(
    "/tmp/vidaio-miner-video-tmp/_persistent/compression_outcomes.jsonl"
)
REFERENCE_LIBRARY_PATH = Path("/root/vidaio-real-content-library")
COMPRESSED_LIBRARY_PATH = Path("/root/vidaio-compressed-sample-library")

SOURCE_NAME = "compression_outcomes"


def _sample_exists(library_path: Path, task_id: str) -> bool:
    return any(library_path.glob(f"{task_id}_*.mp4"))


def ingest_once() -> int:
    if not OUTCOME_LOG_PATH.exists():
        return 0

    offset = db.get_ingest_offset(SOURCE_NAME)
    file_size = OUTCOME_LOG_PATH.stat().st_size
    if file_size < offset:
        # Log rotated/truncated -- restart from the beginning rather than
        # seeking past the end of a shorter file.
        offset = 0

    new_records = 0
    ingested: list[dict] = []
    with open(OUTCOME_LOG_PATH, "r") as f:
        f.seek(offset)
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            task_id = record.get("task_id")
            if not task_id:
                continue
            has_ref = _sample_exists(REFERENCE_LIBRARY_PATH, task_id)
            has_comp = _sample_exists(COMPRESSED_LIBRARY_PATH, task_id)
            db.upsert_item(record, has_ref, has_comp)
            ingested.append(db.get_item(task_id))
            new_records += 1
        new_offset = f.tell()

    db.set_ingest_offset(SOURCE_NAME, new_offset)
    if ingested:
        alerting.check_alerts(ingested)
    return new_records


def main() -> None:
    db.init_db()
    print(f"CMS ingest loop started, watching {OUTCOME_LOG_PATH}", flush=True)
    while True:
        try:
            n = ingest_once()
            if n:
                print(f"ingested {n} new item(s)", flush=True)
            alerting.check_alerts([])  # cheap health-only pass every cycle
        except Exception as e:
            print(f"ingest error: {e}", flush=True)
        time.sleep(15)


if __name__ == "__main__":
    main()

"""SQLite schema and access helpers for the compression CMS.

Single source of truth for every processed item's outcome. Populated by
ingest.py tailing the compression service's compression_outcomes.jsonl
(see miner/compression/app.py:_log_compression_outcome) -- the CMS never
talks to the compression service directly, it only reads what it already
logs.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path("/root/vidaio-cms/cms.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    task_id TEXT PRIMARY KEY,
    ts TEXT NOT NULL,
    codec TEXT,
    codec_mode TEXT,
    vmaf_threshold REAL,
    target_bitrate REAL,
    compression_type TEXT,
    cq INTEGER,
    vmaf REAL,
    original_size INTEGER,
    compressed_size INTEGER,
    ratio REAL,
    elapsed_seconds REAL,
    searched INTEGER,
    has_reference_sample INTEGER DEFAULT 0,
    has_compressed_sample INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_items_ts ON items(ts);
CREATE INDEX IF NOT EXISTS idx_items_codec_thr ON items(codec, vmaf_threshold);

CREATE TABLE IF NOT EXISTS ingest_state (
    source TEXT PRIMARY KEY,
    byte_offset INTEGER NOT NULL DEFAULT 0
);
"""


@contextmanager
def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with connect() as conn:
        conn.executescript(SCHEMA)


def get_ingest_offset(source: str) -> int:
    with connect() as conn:
        row = conn.execute(
            "SELECT byte_offset FROM ingest_state WHERE source = ?", (source,)
        ).fetchone()
        return row["byte_offset"] if row else 0


def set_ingest_offset(source: str, offset: int) -> None:
    with connect() as conn:
        conn.execute(
            "INSERT INTO ingest_state (source, byte_offset) VALUES (?, ?) "
            "ON CONFLICT(source) DO UPDATE SET byte_offset = excluded.byte_offset",
            (source, offset),
        )


def upsert_item(record: dict, has_reference: bool, has_compressed: bool) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO items (
                task_id, ts, codec, codec_mode, vmaf_threshold, target_bitrate,
                compression_type, cq, vmaf, original_size, compressed_size, ratio,
                elapsed_seconds, searched, has_reference_sample, has_compressed_sample
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                ts=excluded.ts, codec=excluded.codec, codec_mode=excluded.codec_mode,
                vmaf_threshold=excluded.vmaf_threshold, target_bitrate=excluded.target_bitrate,
                compression_type=excluded.compression_type, cq=excluded.cq, vmaf=excluded.vmaf,
                original_size=excluded.original_size, compressed_size=excluded.compressed_size,
                ratio=excluded.ratio, elapsed_seconds=excluded.elapsed_seconds,
                searched=excluded.searched,
                has_reference_sample=excluded.has_reference_sample,
                has_compressed_sample=excluded.has_compressed_sample
            """,
            (
                record["task_id"], record["ts"], record.get("codec"),
                record.get("codec_mode"), record.get("vmaf_threshold"),
                record.get("target_bitrate"), record.get("compression_type"),
                record.get("cq"), record.get("vmaf"), record.get("original_size"),
                record.get("compressed_size"), record.get("ratio"),
                record.get("elapsed_seconds"), int(bool(record.get("searched"))),
                int(has_reference), int(has_compressed),
            ),
        )


def list_items(
    codec: str | None = None,
    vmaf_threshold: float | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    query = "SELECT * FROM items WHERE 1=1"
    params: list = []
    if codec:
        query += " AND codec = ?"
        params.append(codec)
    if vmaf_threshold is not None:
        query += " AND vmaf_threshold = ?"
        params.append(vmaf_threshold)
    query += " ORDER BY ts DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    with connect() as conn:
        return [dict(r) for r in conn.execute(query, params).fetchall()]


def get_item(task_id: str) -> dict | None:
    with connect() as conn:
        row = conn.execute("SELECT * FROM items WHERE task_id = ?", (task_id,)).fetchone()
        return dict(row) if row else None


def stats_summary() -> dict:
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) c FROM items").fetchone()["c"]
        by_category = conn.execute(
            """
            SELECT codec, vmaf_threshold, COUNT(*) n,
                   AVG(vmaf) avg_vmaf, AVG(ratio) avg_ratio,
                   MIN(ts) oldest, MAX(ts) newest
            FROM items GROUP BY codec, vmaf_threshold ORDER BY codec, vmaf_threshold
            """
        ).fetchall()
        return {
            "total_items": total,
            "by_category": [dict(r) for r in by_category],
        }

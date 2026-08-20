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

from scoring import compression_score

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
    score REAL,
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

CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    dedupe_key TEXT
);
CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts);
CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_dedupe ON alerts(dedupe_key);

CREATE TABLE IF NOT EXISTS calibration_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    codec TEXT NOT NULL,
    vmaf_threshold REAL NOT NULL,
    cq_grid TEXT NOT NULL,
    limit_clips INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    pid INTEGER,
    log_tail TEXT
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
        # CREATE TABLE IF NOT EXISTS doesn't alter an already-existing table,
        # so columns added after the first deploy need an explicit
        # migration -- idempotent via the duplicate-column error, not a
        # tracked migration system, since this schema is still young enough
        # that isn't worth the overhead yet.
        existing_cols = {row["name"] for row in conn.execute("PRAGMA table_info(items)")}
        if "score" not in existing_cols:
            conn.execute("ALTER TABLE items ADD COLUMN score REAL")


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
    score = compression_score(record.get("ratio"), record.get("vmaf"), record.get("vmaf_threshold"))
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO items (
                task_id, ts, codec, codec_mode, vmaf_threshold, target_bitrate,
                compression_type, cq, vmaf, original_size, compressed_size, ratio, score,
                elapsed_seconds, searched, has_reference_sample, has_compressed_sample
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                ts=excluded.ts, codec=excluded.codec, codec_mode=excluded.codec_mode,
                vmaf_threshold=excluded.vmaf_threshold, target_bitrate=excluded.target_bitrate,
                compression_type=excluded.compression_type, cq=excluded.cq, vmaf=excluded.vmaf,
                original_size=excluded.original_size, compressed_size=excluded.compressed_size,
                ratio=excluded.ratio, score=excluded.score, elapsed_seconds=excluded.elapsed_seconds,
                searched=excluded.searched,
                has_reference_sample=excluded.has_reference_sample,
                has_compressed_sample=excluded.has_compressed_sample
            """,
            (
                record["task_id"], record["ts"], record.get("codec"),
                record.get("codec_mode"), record.get("vmaf_threshold"),
                record.get("target_bitrate"), record.get("compression_type"),
                record.get("cq"), record.get("vmaf"), record.get("original_size"),
                record.get("compressed_size"), record.get("ratio"), score,
                record.get("elapsed_seconds"), int(bool(record.get("searched"))),
                int(has_reference), int(has_compressed),
            ),
        )


_SORT_COLUMNS = {"ts", "score", "vmaf", "ratio", "cq", "elapsed_seconds"}


def list_items(
    codec: str | None = None,
    vmaf_threshold: float | None = None,
    limit: int = 100,
    offset: int = 0,
    sort: str = "ts",
    order: str = "desc",
) -> list[dict]:
    sort_col = sort if sort in _SORT_COLUMNS else "ts"
    order_sql = "ASC" if order.lower() == "asc" else "DESC"
    query = "SELECT * FROM items WHERE 1=1"
    params: list = []
    if codec:
        query += " AND codec = ?"
        params.append(codec)
    if vmaf_threshold is not None:
        query += " AND vmaf_threshold = ?"
        params.append(vmaf_threshold)
    query += f" ORDER BY {sort_col} {order_sql} LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    with connect() as conn:
        return [dict(r) for r in conn.execute(query, params).fetchall()]


def get_item(task_id: str) -> dict | None:
    with connect() as conn:
        row = conn.execute("SELECT * FROM items WHERE task_id = ?", (task_id,)).fetchone()
        return dict(row) if row else None


def distinct_categories() -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT codec, vmaf_threshold FROM items ORDER BY codec, vmaf_threshold"
        ).fetchall()
        return [dict(r) for r in rows]


def stats_summary() -> dict:
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) c FROM items").fetchone()["c"]
        by_category = conn.execute(
            """
            SELECT codec, vmaf_threshold, COUNT(*) n,
                   AVG(vmaf) avg_vmaf, AVG(ratio) avg_ratio, AVG(score) avg_score,
                   MIN(ts) oldest, MAX(ts) newest
            FROM items GROUP BY codec, vmaf_threshold ORDER BY codec, vmaf_threshold
            """
        ).fetchall()
        return {
            "total_items": total,
            "by_category": [dict(r) for r in by_category],
        }


def score_trend(days: int = 14) -> list[dict]:
    """Daily mean/median-ish (avg + min) score, most recent `days` days."""
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT substr(ts, 1, 10) as day, COUNT(*) n,
                   AVG(score) avg_score, MIN(score) min_score, MAX(score) max_score
            FROM items
            WHERE score IS NOT NULL
            GROUP BY day
            ORDER BY day DESC
            LIMIT ?
            """,
            (days,),
        ).fetchall()
        return list(reversed([dict(r) for r in rows]))


def rate_distortion_points(limit: int = 500) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT task_id, codec, vmaf_threshold, vmaf, ratio, score
            FROM items
            WHERE vmaf IS NOT NULL AND ratio IS NOT NULL
            ORDER BY ts DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


# --- Alerts ---------------------------------------------------------------

def add_alert(kind: str, severity: str, message: str, dedupe_key: str, ts: str) -> None:
    """dedupe_key makes repeated identical conditions (e.g. the same stalled
    restart count) collapse to one row instead of spamming -- INSERT OR
    IGNORE against the unique index on dedupe_key.
    """
    with connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO alerts (ts, kind, severity, message, dedupe_key) VALUES (?, ?, ?, ?, ?)",
            (ts, kind, severity, message, dedupe_key),
        )


def recent_alerts(limit: int = 50) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM alerts ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


# --- Calibration runs -------------------------------------------------------

def start_calibration_run(codec: str, vmaf_threshold: float, cq_grid: str, limit_clips: int, pid: int, started_at: str) -> int:
    with connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO calibration_runs (started_at, codec, vmaf_threshold, cq_grid, limit_clips, status, pid)
            VALUES (?, ?, ?, ?, ?, 'running', ?)
            """,
            (started_at, codec, vmaf_threshold, cq_grid, limit_clips, pid),
        )
        return cur.lastrowid


def update_calibration_run(run_id: int, status: str, log_tail: str, finished_at: str | None = None) -> None:
    with connect() as conn:
        conn.execute(
            "UPDATE calibration_runs SET status=?, log_tail=?, finished_at=COALESCE(?, finished_at) WHERE id=?",
            (status, log_tail, finished_at, run_id),
        )


def list_calibration_runs(limit: int = 20) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM calibration_runs ORDER BY started_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def get_calibration_run(run_id: int) -> dict | None:
    with connect() as conn:
        row = conn.execute("SELECT * FROM calibration_runs WHERE id = ?", (run_id,)).fetchone()
        return dict(row) if row else None

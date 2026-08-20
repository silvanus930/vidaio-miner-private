"""CMS API + browsing UI for the compression miner.

Reads the SQLite DB (populated by ingest.py), the calibration log, the CQ
tables' source, and the two sample libraries. Runs as its own process,
independent of the compression service and the miner -- see run_server.sh.
"""
from __future__ import annotations

import re
from pathlib import Path

import calibration
import cq_health
import db
import library
from auth import require_auth
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse

REFERENCE_LIBRARY_PATH = Path("/root/vidaio-real-content-library")
COMPRESSED_LIBRARY_PATH = Path("/root/vidaio-compressed-sample-library")
_RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)")

app = FastAPI(title="Vidaio Miner CMS")
Auth = Depends(require_auth)


def _find_sample(library_path: Path, task_id: str) -> Path | None:
    matches = list(library_path.glob(f"{task_id}_*.mp4"))
    return matches[0] if matches else None


_STREAM_CHUNK = 1024 * 1024


def _stream_video(path: Path, request: Request) -> StreamingResponse:
    """Plain FileResponse ignores the Range header and always returns 200
    with the entire file -- for a 200MB+ 4K reference clip, that means the
    browser has to download the whole thing before it can play anything,
    which looks exactly like the "spinning, never plays" symptom this was
    built to fix. Real 206 Partial Content support so the <video> element
    can seek and start playing immediately.
    """
    file_size = path.stat().st_size
    range_header = request.headers.get("range")
    start, end = 0, file_size - 1
    status_code = 200
    if range_header:
        match = _RANGE_RE.match(range_header)
        if match:
            if match.group(1):
                start = int(match.group(1))
            if match.group(2):
                end = int(match.group(2))
            end = min(end, file_size - 1)
            status_code = 206

    def iterfile():
        with open(path, "rb") as f:
            f.seek(start)
            remaining = end - start + 1
            while remaining > 0:
                chunk = f.read(min(_STREAM_CHUNK, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(end - start + 1),
    }
    return StreamingResponse(iterfile(), status_code=status_code, media_type="video/mp4", headers=headers)


@app.on_event("startup")
def _startup() -> None:
    db.init_db()
    from auth import get_credentials
    user, pw = get_credentials()
    print(f"CMS auth -- username: {user}  password: {pw}", flush=True)
    print(f"(persisted at {__import__('auth').CREDENTIALS_FILE})", flush=True)


# --- Items ------------------------------------------------------------

@app.get("/api/items")
def api_list_items(codec: str | None = None, vmaf_threshold: float | None = None,
                    limit: int = 100, offset: int = 0, sort: str = "ts", order: str = "desc",
                    user: str = Auth):
    return db.list_items(codec=codec, vmaf_threshold=vmaf_threshold, limit=limit,
                          offset=offset, sort=sort, order=order)


@app.get("/api/items/{task_id}")
def api_get_item(task_id: str, user: str = Auth):
    item = db.get_item(task_id)
    if item is None:
        raise HTTPException(status_code=404, detail="item not found")
    # The DB's has_*_sample flags are set once at ingest time, but the
    # compressed-sample capture is a fire-and-forget background task in a
    # separate process (neurons/miner.py) with no ordering guarantee
    # against the outcome-log write this row was ingested from -- ingest
    # can easily run before the file finishes copying, permanently
    # freezing the flag at False even though the file shows up moments
    # later. Check the filesystem live instead of trusting that snapshot.
    item["has_reference_sample"] = int(_find_sample(REFERENCE_LIBRARY_PATH, task_id) is not None)
    item["has_compressed_sample"] = int(_find_sample(COMPRESSED_LIBRARY_PATH, task_id) is not None)
    return item


@app.get("/api/stats")
def api_stats(user: str = Auth):
    return db.stats_summary()


@app.get("/api/score-trend")
def api_score_trend(days: int = 14, user: str = Auth):
    return db.score_trend(days=days)


@app.get("/api/rate-distortion")
def api_rate_distortion(limit: int = 500, user: str = Auth):
    return db.rate_distortion_points(limit=limit)


@app.get("/video/{task_id}/reference")
def video_reference(task_id: str, request: Request, user: str = Auth):
    path = _find_sample(REFERENCE_LIBRARY_PATH, task_id)
    if path is None:
        raise HTTPException(status_code=404, detail="reference sample not retained for this item")
    return _stream_video(path, request)


@app.get("/video/{task_id}/compressed")
def video_compressed(task_id: str, request: Request, user: str = Auth):
    path = _find_sample(COMPRESSED_LIBRARY_PATH, task_id)
    if path is None:
        raise HTTPException(status_code=404, detail="compressed sample not retained for this item")
    return _stream_video(path, request)


# --- CQ table health ----------------------------------------------------

@app.get("/api/cq-health")
def api_cq_health(user: str = Auth):
    return cq_health.cq_table_health()


# --- Calibration ----------------------------------------------------------

@app.get("/api/calibration/log")
def api_calibration_log(codec: str | None = None, vmaf_threshold: float | None = None,
                         limit: int = 200, user: str = Auth):
    return calibration.read_calibration_log(codec=codec, vmaf_threshold=vmaf_threshold, limit=limit)


@app.post("/api/calibration/start")
def api_calibration_start(codec: str, vmaf_threshold: float, cq_grid: str = "30,33,35,38,40",
                           limit_clips: int = 6, user: str = Auth):
    run_id = calibration.start_calibration(codec, vmaf_threshold, cq_grid, limit_clips)
    return {"run_id": run_id}


@app.get("/api/calibration/runs")
def api_calibration_runs(user: str = Auth):
    return db.list_calibration_runs()


@app.get("/api/calibration/runs/{run_id}")
def api_calibration_run(run_id: int, user: str = Auth):
    run = calibration.poll_calibration_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    return run


# --- Content library --------------------------------------------------

@app.get("/api/library/coverage")
def api_library_coverage(user: str = Auth):
    return library.coverage_grid()


@app.get("/api/library/clips")
def api_library_clips(user: str = Auth):
    return library.list_reference_clips()


@app.get("/thumbnail/{task_id}")
def get_thumbnail(task_id: str, user: str = Auth):
    path = library.get_thumbnail(task_id)
    if path is None:
        raise HTTPException(status_code=404, detail="no thumbnail available")
    return FileResponse(path, media_type="image/jpeg")


# --- Alerts -----------------------------------------------------------

@app.get("/api/alerts")
def api_alerts(limit: int = 50, user: str = Auth):
    return db.recent_alerts(limit=limit)


# --- UI -----------------------------------------------------------------

_PAGE_PATH = Path(__file__).parent / "page.html"


@app.get("/", response_class=HTMLResponse)
def index(user: str = Auth):
    return _PAGE_PATH.read_text()

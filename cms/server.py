"""CMS API + minimal browsing UI for the compression miner.

Read-only over the SQLite DB that ingest.py populates, plus streams video
files directly from the two sample libraries for the before/after preview.
Runs as its own process, independent of the compression service and the
miner -- see cms/run_server.sh.
"""
from __future__ import annotations

from pathlib import Path

import db
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse

REFERENCE_LIBRARY_PATH = Path("/root/vidaio-real-content-library")
COMPRESSED_LIBRARY_PATH = Path("/root/vidaio-compressed-sample-library")

app = FastAPI(title="Vidaio Miner CMS")


def _find_sample(library_path: Path, task_id: str) -> Path | None:
    matches = list(library_path.glob(f"{task_id}_*.mp4"))
    return matches[0] if matches else None


@app.on_event("startup")
def _startup() -> None:
    db.init_db()


@app.get("/api/items")
def api_list_items(codec: str | None = None, vmaf_threshold: float | None = None,
                    limit: int = 100, offset: int = 0):
    return db.list_items(codec=codec, vmaf_threshold=vmaf_threshold, limit=limit, offset=offset)


@app.get("/api/items/{task_id}")
def api_get_item(task_id: str):
    item = db.get_item(task_id)
    if item is None:
        raise HTTPException(status_code=404, detail="item not found")
    return item


@app.get("/api/stats")
def api_stats():
    return db.stats_summary()


@app.get("/video/{task_id}/reference")
def video_reference(task_id: str):
    path = _find_sample(REFERENCE_LIBRARY_PATH, task_id)
    if path is None:
        raise HTTPException(status_code=404, detail="reference sample not retained for this item")
    return FileResponse(path, media_type="video/mp4")


@app.get("/video/{task_id}/compressed")
def video_compressed(task_id: str):
    path = _find_sample(COMPRESSED_LIBRARY_PATH, task_id)
    if path is None:
        raise HTTPException(status_code=404, detail="compressed sample not retained for this item")
    return FileResponse(path, media_type="video/mp4")


_PAGE = """<!doctype html>
<html><head>
<meta charset="utf-8">
<title>Vidaio Miner CMS</title>
<style>
  :root { color-scheme: dark; }
  body { font-family: -apple-system, sans-serif; background: #0f1216; color: #e6e8eb; margin: 0; padding: 24px; }
  h1 { font-size: 18px; font-weight: 600; margin: 0 0 16px; }
  .stats { display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; }
  .stat-card { background: #1a1f26; border: 1px solid #2a313b; border-radius: 8px; padding: 10px 14px; font-size: 13px; }
  .stat-card b { display: block; font-size: 16px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid #2a313b; }
  th { color: #8b96a5; font-weight: 500; }
  tr:hover { background: #1a1f26; cursor: pointer; }
  #detail { margin-top: 20px; padding: 16px; background: #1a1f26; border-radius: 8px; display: none; }
  .videos { display: flex; gap: 16px; margin-top: 12px; flex-wrap: wrap; }
  video { width: 380px; max-width: 100%; background: #000; border-radius: 6px; }
  .meta { font-size: 12px; color: #8b96a5; margin-top: 4px; }
  .missing { color: #6b7480; font-style: italic; padding: 40px; text-align: center; }
</style>
</head>
<body>
<h1>Vidaio Miner &mdash; Compression CMS</h1>
<div class="stats" id="stats"></div>
<table>
  <thead><tr><th>Time</th><th>Task</th><th>Codec</th><th>Thr</th><th>CQ</th><th>VMAF</th><th>Ratio</th><th>Elapsed</th></tr></thead>
  <tbody id="rows"></tbody>
</table>
<div id="detail"></div>
<script>
async function loadStats() {
  const s = await (await fetch('/api/stats')).json();
  document.getElementById('stats').innerHTML = s.by_category.map(c =>
    `<div class="stat-card"><b>${c.n}</b>${c.codec} / thr${c.vmaf_threshold}<br>vmaf ${c.avg_vmaf?.toFixed(1) ?? '-'} &middot; ratio ${c.avg_ratio?.toFixed(3) ?? '-'}</div>`
  ).join('') + `<div class="stat-card"><b>${s.total_items}</b>total items</div>`;
}
async function loadItems() {
  const items = await (await fetch('/api/items?limit=200')).json();
  document.getElementById('rows').innerHTML = items.map(it => `
    <tr onclick="showDetail('${it.task_id}')">
      <td>${(it.ts||'').replace('T',' ').slice(0,19)}</td>
      <td>${it.task_id}</td>
      <td>${it.codec||''}</td>
      <td>${it.vmaf_threshold ?? ''}</td>
      <td>${it.cq ?? ''}</td>
      <td>${it.vmaf?.toFixed(2) ?? ''}</td>
      <td>${it.ratio?.toFixed(3) ?? ''}</td>
      <td>${it.elapsed_seconds?.toFixed(1) ?? ''}s</td>
    </tr>`).join('');
}
async function showDetail(taskId) {
  const it = await (await fetch(`/api/items/${taskId}`)).json();
  const det = document.getElementById('detail');
  det.style.display = 'block';
  const refVideo = it.has_reference_sample
    ? `<div><video controls src="/video/${taskId}/reference"></video><div class="meta">reference</div></div>` : '';
  const compVideo = it.has_compressed_sample
    ? `<div><video controls src="/video/${taskId}/compressed"></video><div class="meta">compressed</div></div>` : '';
  const noVideo = (!it.has_reference_sample && !it.has_compressed_sample)
    ? '<div class="missing">no video sample retained for this item</div>' : '';
  det.innerHTML = `<b>${taskId}</b> &mdash; ${it.codec} ${it.codec_mode}, threshold ${it.vmaf_threshold}, cq=${it.cq}
    <div class="meta">vmaf=${it.vmaf?.toFixed(2) ?? '-'} ratio=${it.ratio?.toFixed(4) ?? '-'}
    (${it.original_size} &rarr; ${it.compressed_size} bytes) elapsed=${it.elapsed_seconds?.toFixed(1)}s
    searched=${it.searched ? 'yes' : 'no (static table)'}</div>
    <div class="videos">${refVideo}${compVideo}${noVideo}</div>`;
}
loadStats(); loadItems();
setInterval(() => { loadStats(); loadItems(); }, 20000);
</script>
</body></html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return _PAGE

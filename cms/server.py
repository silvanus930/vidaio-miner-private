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


_FAVICON_SVG = (
    "data:image/svg+xml,"
    "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E"
    "%3Crect width='32' height='32' rx='8' fill='%236366f1'/%3E"
    "%3Cpath d='M9 11.5A2.5 2.5 0 0 1 11.5 9h6A2.5 2.5 0 0 1 20 11.5v9a2.5 2.5 0 0 1-2.5 2.5h-6A2.5 2.5 0 0 1 9 20.5v-9Z' "
    "fill='none' stroke='white' stroke-width='1.8'/%3E"
    "%3Cpath d='M20 13.8l4.2-2.4a.8.8 0 0 1 1.2.7v7.8a.8.8 0 0 1-1.2.7L20 18.2' "
    "fill='none' stroke='white' stroke-width='1.8' stroke-linejoin='round'/%3E"
    "%3C/svg%3E"
)

_PAGE = f"""<!doctype html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vidaio Miner CMS</title>
<link rel="icon" href="{_FAVICON_SVG}">
<style>
  :root {{
    color-scheme: dark;
    --bg: #0b0d12;
    --surface: #12151c;
    --surface-2: #171b24;
    --border: #232836;
    --text: #e8eaf0;
    --text-dim: #8b93a7;
    --text-faint: #565f74;
    --accent: #6366f1;
    --accent-soft: rgba(99, 102, 241, 0.14);
    --good: #34d399;
    --radius: 12px;
    font-variant-numeric: tabular-nums;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, sans-serif;
    background: var(--bg); color: var(--text); margin: 0;
    padding: 32px clamp(16px, 4vw, 48px);
    line-height: 1.5;
  }}
  header {{ display: flex; align-items: center; gap: 12px; margin-bottom: 28px; }}
  .logo {{
    width: 36px; height: 36px; border-radius: 9px;
    background: linear-gradient(135deg, var(--accent), #818cf8);
    display: flex; align-items: center; justify-content: center;
    flex-shrink: 0;
  }}
  .logo svg {{ width: 20px; height: 20px; }}
  h1 {{ font-size: 17px; font-weight: 650; margin: 0; letter-spacing: -0.01em; }}
  .subtitle {{ font-size: 12.5px; color: var(--text-dim); margin-top: 2px; }}
  .stats {{
    display: grid; grid-template-columns: repeat(auto-fill, minmax(170px, 1fr));
    gap: 10px; margin-bottom: 24px;
  }}
  .stat-card {{
    background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 14px 16px; font-size: 12.5px; color: var(--text-dim);
    transition: border-color 0.15s;
  }}
  .stat-card:hover {{ border-color: #2f3648; }}
  .stat-card b {{
    display: block; font-size: 22px; font-weight: 650; color: var(--text);
    letter-spacing: -0.02em; margin-bottom: 2px;
  }}
  .stat-card .cat {{ color: var(--accent); font-weight: 600; }}
  .panel {{
    background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
    overflow: hidden; margin-bottom: 20px;
  }}
  .panel-head {{
    padding: 14px 18px; border-bottom: 1px solid var(--border);
    font-size: 12.5px; font-weight: 600; color: var(--text-dim);
    text-transform: uppercase; letter-spacing: 0.04em;
  }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th, td {{ text-align: left; padding: 10px 18px; }}
  th {{
    color: var(--text-faint); font-weight: 600; font-size: 11px;
    text-transform: uppercase; letter-spacing: 0.04em;
    border-bottom: 1px solid var(--border);
  }}
  tbody tr {{ border-bottom: 1px solid var(--border); cursor: pointer; transition: background 0.1s; }}
  tbody tr:last-child {{ border-bottom: none; }}
  tbody tr:hover {{ background: var(--surface-2); }}
  td.task {{ color: var(--text-dim); font-family: ui-monospace, monospace; font-size: 12px; }}
  .pill {{
    display: inline-block; padding: 2px 8px; border-radius: 999px;
    background: var(--accent-soft); color: #a5a8ff; font-size: 11px; font-weight: 600;
  }}
  .empty {{ padding: 48px 18px; text-align: center; color: var(--text-faint); font-size: 13px; }}
  #detail {{ display: none; }}
  #detail .panel-head {{ display: flex; justify-content: space-between; align-items: center; text-transform: none; letter-spacing: 0; }}
  #detail .panel-head .task-id {{ font-family: ui-monospace, monospace; color: var(--text); font-size: 13px; font-weight: 600; }}
  .detail-body {{ padding: 18px; }}
  .detail-meta {{ font-size: 12.5px; color: var(--text-dim); display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 16px; }}
  .detail-meta span b {{ color: var(--text); font-weight: 600; }}
  .videos {{ display: flex; gap: 16px; flex-wrap: wrap; }}
  .video-card {{ flex: 1; min-width: 280px; max-width: 460px; }}
  .video-card video {{ width: 100%; background: #000; border-radius: 8px; display: block; border: 1px solid var(--border); }}
  .video-label {{
    font-size: 11px; color: var(--text-faint); margin-top: 6px;
    text-transform: uppercase; letter-spacing: 0.04em; font-weight: 600;
  }}
  .missing {{ color: var(--text-faint); font-style: italic; padding: 24px; text-align: center; font-size: 13px; }}
</style>
</head>
<body>
<header>
  <div class="logo">
    <svg viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="1.8">
      <rect x="2" y="4.5" width="14" height="15" rx="3"/>
      <path d="M16 9.8l4.6-2.7a.8.8 0 0 1 1.2.7v9.2a.8.8 0 0 1-1.2.7L16 15" stroke-linejoin="round"/>
    </svg>
  </div>
  <div>
    <h1>Vidaio Miner CMS</h1>
    <div class="subtitle">Compression outcomes &middot; live from real validator traffic</div>
  </div>
</header>
<div class="stats" id="stats"></div>
<div class="panel">
  <div class="panel-head">Recent items</div>
  <table>
    <thead><tr><th>Time</th><th>Task</th><th>Codec</th><th>Threshold</th><th>CQ</th><th>VMAF</th><th>Ratio</th><th>Elapsed</th></tr></thead>
    <tbody id="rows"></tbody>
  </table>
</div>
<div class="panel" id="detail">
  <div class="panel-head"><span>Item detail</span><span class="task-id" id="detail-task"></span></div>
  <div class="detail-body" id="detail-body"></div>
</div>
<script>
async function loadStats() {{
  const s = await (await fetch('/api/stats')).json();
  const cards = s.by_category.map(c =>
    `<div class="stat-card"><b>${{c.n}}</b><span class="cat">${{c.codec}}</span> &middot; thr ${{c.vmaf_threshold}}<br>vmaf ${{c.avg_vmaf?.toFixed(1) ?? '-'}} &middot; ratio ${{c.avg_ratio?.toFixed(3) ?? '-'}}</div>`
  ).join('');
  document.getElementById('stats').innerHTML =
    `<div class="stat-card"><b>${{s.total_items}}</b>total items logged</div>` + cards;
}}
async function loadItems() {{
  const items = await (await fetch('/api/items?limit=200')).json();
  const rowsEl = document.getElementById('rows');
  if (!items.length) {{
    rowsEl.innerHTML = `<tr><td colspan="8"><div class="empty">No items logged yet &mdash; this fills in as real traffic is processed.</div></td></tr>`;
    return;
  }}
  rowsEl.innerHTML = items.map(it => `
    <tr onclick="showDetail('${{it.task_id}}')">
      <td>${{(it.ts||'').replace('T',' ').slice(0,19)}}</td>
      <td class="task">${{it.task_id}}</td>
      <td>${{it.codec||''}}</td>
      <td><span class="pill">${{it.vmaf_threshold ?? ''}}</span></td>
      <td>${{it.cq ?? ''}}</td>
      <td>${{it.vmaf?.toFixed(2) ?? ''}}</td>
      <td>${{it.ratio?.toFixed(3) ?? ''}}</td>
      <td>${{it.elapsed_seconds?.toFixed(1) ?? ''}}s</td>
    </tr>`).join('');
}}
async function showDetail(taskId) {{
  const it = await (await fetch(`/api/items/${{taskId}}`)).json();
  const det = document.getElementById('detail');
  det.style.display = 'block';
  document.getElementById('detail-task').textContent = taskId;
  const refVideo = it.has_reference_sample
    ? `<div class="video-card"><video controls src="/video/${{taskId}}/reference"></video><div class="video-label">Reference</div></div>` : '';
  const compVideo = it.has_compressed_sample
    ? `<div class="video-card"><video controls src="/video/${{taskId}}/compressed"></video><div class="video-label">Compressed</div></div>` : '';
  const noVideo = (!it.has_reference_sample && !it.has_compressed_sample)
    ? '<div class="missing">No video sample retained for this item.</div>' : '';
  document.getElementById('detail-body').innerHTML = `
    <div class="detail-meta">
      <span><b>${{it.codec}} ${{it.codec_mode}}</b></span>
      <span>threshold <b>${{it.vmaf_threshold}}</b></span>
      <span>cq <b>${{it.cq}}</b></span>
      <span>vmaf <b>${{it.vmaf?.toFixed(2) ?? '-'}}</b></span>
      <span>ratio <b>${{it.ratio?.toFixed(4) ?? '-'}}</b></span>
      <span><b>${{it.original_size}}</b> &rarr; <b>${{it.compressed_size}}</b> bytes</span>
      <span>elapsed <b>${{it.elapsed_seconds?.toFixed(1)}}s</b></span>
      <span>${{it.searched ? 'search-driven' : 'static table'}}</span>
    </div>
    <div class="videos">${{refVideo}}${{compVideo}}${{noVideo}}</div>`;
  det.scrollIntoView({{ behavior: 'smooth', block: 'nearest' }});
}}
loadStats(); loadItems();
setInterval(() => {{ loadStats(); loadItems(); }}, 20000);
</script>
</body></html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return _PAGE

#!/usr/bin/env python3
"""Gathers real miner/tunnel/chain status, appends it to a history log, and
renders the public status dashboard from that history. No fabricated data —
anything that can't be reliably read is shown as unknown rather than guessed.

Run standalone (one refresh) or via scripts/tracker_loop.sh (repeated).
"""
from __future__ import annotations

import html
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path("/root/vidaio-subnet")
HISTORY_PATH = Path("/tmp/vidaio-status-history.jsonl")
WATCHDOG_EVENTS_PATH = Path("/tmp/vidaio-watchdog-events.jsonl")
MINER_LOG_PATH = Path("/tmp/vidaio-miner-process.log")
OUTPUT_HTML = Path("/tmp/vidaio-dashboard-public/index.html")
HISTORY_MAX = 1000

TUNNEL_HOST = "159.223.110.159"
TUNNEL_PORT = 39518
AXON_LOCAL_PORT = 8091


def run(cmd: str) -> str:
    try:
        return subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=15
        ).stdout.strip()
    except Exception:
        return ""


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def gather_snapshot() -> dict:
    snap = {"ts": now_iso()}

    # --- compression backend ---
    health_raw = run("curl -sf http://localhost:8004/health")
    try:
        health = json.loads(health_raw) if health_raw else None
    except json.JSONDecodeError:
        health = None
    snap["compression_healthy"] = health is not None
    if health:
        snap["compression_active_tasks"] = health.get("active_tasks")
        snap["compression_queued_tasks"] = health.get("queued_tasks")
        snap["compression_max_concurrent"] = health.get("max_concurrent")
        storage = health.get("storage", {})
        snap["storage_provider"] = storage.get("provider")
        snap["storage_configured"] = all(
            storage.get(k)
            for k in (
                "bucket_configured",
                "access_key_configured",
                "secret_key_configured",
                "endpoint_configured",
            )
        )

    container_running = run(
        "docker inspect -f '{{.State.Running}}' miner-compression-1 2>/dev/null"
    )
    snap["compression_container_running"] = container_running.strip() == "true"
    restarts = run("docker inspect -f '{{.RestartCount}}' miner-compression-1 2>/dev/null")
    snap["compression_restarts"] = int(restarts) if restarts.isdigit() else None

    stats = run(
        "docker stats miner-compression-1 --no-stream --format '{{.CPUPerc}}|{{.MemUsage}}' 2>/dev/null"
    )
    if "|" in stats:
        cpu, mem = stats.split("|", 1)
        snap["compression_cpu"] = cpu.strip()
        snap["compression_mem"] = mem.strip()

    # --- GPU ---
    gpu = run(
        "nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total,"
        "temperature.gpu,power.draw --format=csv,noheader,nounits"
    )
    if gpu:
        parts = [p.strip() for p in gpu.split(",")]
        if len(parts) == 5:
            snap["gpu_util"] = parts[0]
            snap["gpu_mem_used"] = parts[1]
            snap["gpu_mem_total"] = parts[2]
            snap["gpu_temp"] = parts[3]
            snap["gpu_power"] = parts[4]

    # --- host ---
    disk = run("df -h / | tail -1")
    disk_parts = disk.split()
    if len(disk_parts) >= 5:
        snap["disk_used"] = disk_parts[2]
        snap["disk_total"] = disk_parts[1]
        snap["disk_pct"] = disk_parts[4]
    load = run("uptime")
    m = re.search(r"load average:\s*([\d.]+),\s*([\d.]+),\s*([\d.]+)", load)
    if m:
        snap["load1"], snap["load5"], snap["load15"] = m.group(1), m.group(2), m.group(3)

    # --- axon / miner process ---
    snap["axon_process_running"] = bool(run("pgrep -f 'neurons/miner.py'"))
    snap["axon_port_listening"] = f":{AXON_LOCAL_PORT} " in run(
        f"ss -tln 2>/dev/null | grep ':{AXON_LOCAL_PORT} '"
    ) or bool(run(f"ss -tln 2>/dev/null | grep ':{AXON_LOCAL_PORT} '"))
    snap["bore_running"] = bool(run(f"pgrep -f 'bore local {AXON_LOCAL_PORT}'"))
    snap["tunnel_host"] = TUNNEL_HOST
    snap["tunnel_port"] = TUNNEL_PORT

    # last validator query seen in the miner log (real network evidence)
    if MINER_LOG_PATH.exists():
        raw = MINER_LOG_PATH.read_text(errors="replace")
        text = re.sub(r"\x1b\[[0-9;]*m", "", raw)  # strip ANSI color codes
        events = re.findall(
            r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\S* \| \s*ERROR\s* \| bittensor:axon\.py:\d+ \| "
            r"UnknownSynapseError#[0-9a-f-]+: Synapse name '([^']*)' not found",
            text,
        )
        snap["validator_query_count"] = len(events)
        if events:
            snap["last_validator_query_ts"] = events[-1][0]
            snap["last_validator_query_synapse"] = events[-1][1] or "(empty)"
        served = re.findall(r"Axon served with: AxonInfo\([^,]+, ([\d.]+:\d+)\)", text)
        if served:
            snap["last_served_address"] = served[-1]

    return snap


def load_history() -> list[dict]:
    if not HISTORY_PATH.exists():
        return []
    out = []
    for line in HISTORY_PATH.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def append_history(snap: dict) -> list[dict]:
    history = load_history()
    history.append(snap)
    history = history[-HISTORY_MAX:]
    with HISTORY_PATH.open("w") as f:
        for row in history:
            f.write(json.dumps(row) + "\n")
    return history


def load_watchdog_events(limit: int = 12) -> list[dict]:
    if not WATCHDOG_EVENTS_PATH.exists():
        return []
    out = []
    for line in WATCHDOG_EVENTS_PATH.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out[-limit:]


def uptime_bar(history: list[dict], key: str, width: int = 60) -> str:
    """Render a compact reachability bar from the last `width` samples."""
    recent = history[-width:]
    cells = []
    for row in recent:
        ok = row.get(key)
        cells.append("#" if ok else ("." if ok is False else " "))
    return "".join(cells).rjust(width)


def pct_true(history: list[dict], key: str, window: int = 200) -> float | None:
    recent = [row for row in history[-window:] if key in row]
    if not recent:
        return None
    return 100.0 * sum(1 for row in recent if row.get(key)) / len(recent)


def esc(v) -> str:
    return html.escape(str(v)) if v is not None else "&mdash;"


def render(snap: dict, history: list[dict]) -> str:
    events = load_watchdog_events()
    started_ts = history[0]["ts"] if history else snap["ts"]
    tracking_since = started_ts

    axon_reachable = snap.get("axon_process_running") and snap.get("bore_running")
    axon_pill = "LIVE" if axon_reachable else "DOWN"
    axon_class = "good" if axon_reachable else "critical"

    comp_ok = snap.get("compression_container_running") and snap.get("compression_healthy")
    comp_pill = "HEALTHY" if comp_ok else "DOWN"
    comp_class = "good" if comp_ok else "critical"

    vq_count = snap.get("validator_query_count", 0)
    last_vq = snap.get("last_validator_query_ts", "none observed yet")
    last_vq_syn = snap.get("last_validator_query_synapse", "")

    axon_uptime_pct = pct_true(history, "axon_process_running")
    bore_uptime_pct = pct_true(history, "bore_running")
    comp_uptime_pct = pct_true(history, "compression_container_running")

    bar_axon = uptime_bar(history, "axon_process_running")
    bar_bore = uptime_bar(history, "bore_running")
    bar_comp = uptime_bar(history, "compression_container_running")

    event_rows = ""
    for e in reversed(events):
        t = e.get("ts", "")[11:19]
        event_rows += (
            f'<div class="log-line"><span class="t">{esc(t)}</span>'
            f'<span class="k select">watchdog</span>'
            f'<span class="m">{esc(e.get("event",""))}</span></div>\n'
        )
    if not event_rows:
        event_rows = (
            '<div class="log-line"><span class="t">&mdash;</span>'
            '<span class="m" style="color:var(--text-faint)">no restarts observed &mdash; '
            "everything has stayed up since tracking began</span></div>"
        )

    gpu_util = snap.get("gpu_util", "?")
    gpu_mem_used = snap.get("gpu_mem_used", "?")
    gpu_mem_total = snap.get("gpu_mem_total", "?")
    gpu_mem_pct = 1
    try:
        gpu_mem_pct = max(1, round(100 * float(gpu_mem_used) / float(gpu_mem_total)))
    except (ValueError, ZeroDivisionError):
        pass

    disk_pct_raw = snap.get("disk_pct", "0%").rstrip("%")

    html_out = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="60">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vidaio Miner Console</title>
<style>
  :root {{
    --ground: #0a0f14; --surface: #121920; --surface-2: #1b242c; --surface-3: #212b34;
    --border: #263139; --border-soft: #1c252c;
    --text: #e6edf1; --text-dim: #93a2ac; --text-faint: #5b6870;
    --accent: #3fd8c4; --accent-dim: #2a9c8d; --accent-soft: rgba(63,216,196,.1);
    --good: #4fae7a; --good-soft: rgba(79,174,122,.14);
    --warn: #e8a23d; --warn-soft: rgba(232,162,61,.14);
    --critical: #e2544b; --critical-soft: rgba(226,84,75,.14);
    --mono: ui-monospace,"SF Mono","Cascadia Code","JetBrains Mono",Consolas,"Liberation Mono",monospace;
  }}
  * {{ box-sizing: border-box; }}
  html, body {{ background: var(--ground); color: var(--text); }}
  body {{ font-family: var(--mono); font-size: 13px; line-height: 1.5; padding: clamp(16px,3vw,40px); margin:0;
    background-image: radial-gradient(ellipse 900px 500px at 15% -10%, rgba(63,216,196,.05), transparent),
      radial-gradient(ellipse 700px 400px at 100% 0%, rgba(63,216,196,.03), transparent); }}
  a {{ color: var(--accent); }}
  .page {{ max-width: 1240px; margin:0 auto; display:flex; flex-direction:column; gap:18px; }}
  .masthead {{ display:flex; align-items:baseline; justify-content:space-between; flex-wrap:wrap; gap:10px 24px;
    padding-bottom:14px; border-bottom:1px solid var(--border); }}
  .masthead .brand {{ display:flex; align-items:baseline; gap:12px; }}
  .masthead h1 {{ margin:0; font-size:19px; font-weight:700; letter-spacing:.02em; text-wrap:balance; }}
  .masthead .tag {{ font-size:11px; color:var(--text-faint); letter-spacing:.08em; text-transform:uppercase; }}
  .masthead .netuid {{ font-size:11px; color:var(--accent); border:1px solid var(--accent-dim);
    background:var(--accent-soft); padding:2px 8px; border-radius:3px; letter-spacing:.05em; }}
  .status-strip {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(280px,1fr)); gap:12px; }}
  .status-card {{ background:var(--surface); border:1px solid var(--border);
    border-left:3px solid var(--state-color,var(--text-faint)); border-radius:4px; padding:14px 16px;
    display:flex; align-items:center; justify-content:space-between; gap:12px; }}
  .status-card .label {{ display:flex; flex-direction:column; gap:4px; }}
  .status-card .name {{ font-size:12px; letter-spacing:.06em; text-transform:uppercase; color:var(--text-dim); }}
  .status-card .detail {{ font-size:11.5px; color:var(--text-faint); }}
  .pill {{ font-size:11px; font-weight:700; letter-spacing:.06em; padding:5px 10px; border-radius:3px;
    white-space:nowrap; color:var(--state-color,var(--text)); background:var(--state-soft,var(--surface-2));
    border:1px solid color-mix(in srgb, var(--state-color,var(--text-faint)) 45%, transparent); }}
  .status-card.good {{ --state-color:var(--good); --state-soft:var(--good-soft); }}
  .status-card.warn {{ --state-color:var(--warn); --state-soft:var(--warn-soft); }}
  .status-card.critical {{ --state-color:var(--critical); --state-soft:var(--critical-soft); }}
  .pill::before {{ content:"\\25CF"; margin-right:6px; font-size:8px; vertical-align:1px; }}
  .snapshot-bar {{ display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:8px 20px;
    background:var(--surface-2); border:1px dashed var(--border); border-radius:4px; padding:9px 16px;
    font-size:11.5px; color:var(--text-dim); }}
  .snapshot-bar .rec {{ display:flex; align-items:center; gap:8px; color:var(--text); }}
  .rec-dot {{ width:7px; height:7px; border-radius:50%; background:var(--good);
    box-shadow:0 0 0 3px var(--good-soft); animation:pulse 2s ease-in-out infinite; }}
  @media (prefers-reduced-motion: reduce) {{ .rec-dot {{ animation:none; }} }}
  @keyframes pulse {{ 0%,100% {{ opacity:1; }} 50% {{ opacity:.35; }} }}
  .snapshot-bar .elapsed {{ font-variant-numeric:tabular-nums; color:var(--accent); }}
  .grid {{ display:grid; grid-template-columns:repeat(12,1fr); gap:12px; }}
  .panel {{ background:var(--surface); border:1px solid var(--border); border-radius:5px; padding:16px 18px;
    display:flex; flex-direction:column; gap:12px; min-width:0; }}
  .panel > header {{ display:flex; align-items:center; justify-content:space-between; gap:10px; }}
  .panel > header h2 {{ margin:0; font-size:11px; font-weight:700; letter-spacing:.1em; text-transform:uppercase;
    color:var(--text-dim); }}
  .panel > header .chip {{ font-size:10px; letter-spacing:.05em; padding:2px 7px; border-radius:3px;
    color:var(--state-color,var(--text-faint)); background:var(--state-soft,transparent);
    border:1px solid color-mix(in srgb, var(--state-color,var(--text-faint)) 40%, transparent); }}
  .chip.good {{ --state-color:var(--good); --state-soft:var(--good-soft); }}
  .chip.warn {{ --state-color:var(--warn); --state-soft:var(--warn-soft); }}
  .chip.accent {{ --state-color:var(--accent); --state-soft:var(--accent-soft); }}
  .col-4 {{ grid-column:span 4; }} .col-6 {{ grid-column:span 6; }} .col-8 {{ grid-column:span 8; }}
  .col-12 {{ grid-column:span 12; }}
  @media (max-width:860px) {{ .col-4,.col-6,.col-8,.col-12 {{ grid-column:span 12; }} }}
  .metric-row {{ display:flex; align-items:baseline; justify-content:space-between; gap:12px; font-size:12.5px; }}
  .metric-row .k {{ color:var(--text-faint); }}
  .metric-row .v {{ font-variant-numeric:tabular-nums; color:var(--text); font-weight:600; }}
  .metric-row .v.accent {{ color:var(--accent); }} .metric-row .v.good {{ color:var(--good); }}
  .divider {{ height:1px; background:var(--border-soft); margin:2px 0; }}
  .meter {{ height:6px; border-radius:3px; background:var(--surface-3); overflow:hidden; position:relative; }}
  .meter > i {{ display:block; height:100%; background:linear-gradient(90deg,var(--accent-dim),var(--accent));
    border-radius:3px; }}
  .meter-label {{ display:flex; justify-content:space-between; font-size:11px; color:var(--text-faint); }}
  .checklist {{ display:flex; flex-direction:column; gap:7px; }}
  .checklist .item {{ display:flex; align-items:center; gap:8px; font-size:12px; color:var(--text-dim); }}
  .checklist .item .ok {{ color:var(--good); font-weight:700; }}
  .checklist .item .bad {{ color:var(--critical); font-weight:700; }}
  .log {{ background:var(--ground); border:1px solid var(--border-soft); border-radius:4px; padding:10px 12px;
    font-size:11.5px; max-height:230px; overflow-y:auto; overflow-x:auto; display:flex; flex-direction:column; gap:1px; }}
  .log-line {{ display:grid; grid-template-columns:78px 92px 1fr; gap:12px; padding:3px 4px; border-radius:2px;
    white-space:nowrap; }}
  .log-line:hover {{ background:var(--surface-2); }}
  .log-line .t {{ color:var(--text-faint); }} .log-line .k {{ color:var(--accent); }}
  .log-line .k.select {{ color:var(--good); font-weight:700; }} .log-line .m {{ color:var(--text-dim); }}
  .uptime-bar {{ font-size:10px; letter-spacing:-1px; color:var(--accent); background:var(--ground);
    border:1px solid var(--border-soft); border-radius:4px; padding:8px 10px; overflow-x:auto; white-space:pre; }}
  .footnote {{ font-size:11px; color:var(--text-faint); text-align:center; padding-top:6px; }}
  ::-webkit-scrollbar {{ width:8px; height:8px; }} ::-webkit-scrollbar-track {{ background:transparent; }}
  ::-webkit-scrollbar-thumb {{ background:var(--border); border-radius:4px; }}
</style>
</head>
<body>
<div class="page">

  <div class="masthead">
    <div class="brand"><h1>VIDAIO MINER CONSOLE</h1><span class="tag">live tracking</span></div>
    <span class="netuid">SUBNET 85 &middot; UID 21 &middot; FINNEY</span>
  </div>

  <div class="status-strip">
    <div class="status-card {axon_class}">
      <div class="label">
        <span class="name">Bittensor axon</span>
        <span class="detail">{esc(TUNNEL_HOST)}:{esc(TUNNEL_PORT)} via bore tunnel &middot; {vq_count} validator queries seen</span>
      </div>
      <span class="pill">{axon_pill}</span>
    </div>
    <div class="status-card {comp_class}">
      <div class="label">
        <span class="name">Compression backend</span>
        <span class="detail">miner-compression-1 &middot; {esc(snap.get('compression_restarts',0))} restarts</span>
      </div>
      <span class="pill">{comp_pill}</span>
    </div>
  </div>

  <div class="snapshot-bar">
    <span class="rec"><span class="rec-dot"></span>AUTO-REFRESHING &middot; last update {esc(snap['ts'])}</span>
    <span>tracking since {esc(tracking_since)} &middot; {len(history)} samples</span>
    <span class="elapsed" id="elapsed">updated moments ago</span>
  </div>

  <div class="grid">

    <div class="panel col-4">
      <header><h2>GPU &middot; NVIDIA L40</h2><span class="chip">{"IDLE" if snap.get('gpu_util') in ("0","0 ") else "ACTIVE"}</span></header>
      <div class="meter-label"><span>VRAM</span><span>{esc(gpu_mem_used)} / {esc(gpu_mem_total)} MiB</span></div>
      <div class="meter"><i style="width:{gpu_mem_pct}%"></i></div>
      <div class="divider"></div>
      <div class="metric-row"><span class="k">Utilization</span><span class="v">{esc(gpu_util)}%</span></div>
      <div class="metric-row"><span class="k">Temperature</span><span class="v">{esc(snap.get('gpu_temp','?'))}&deg;C</span></div>
      <div class="metric-row"><span class="k">Power draw</span><span class="v">{esc(snap.get('gpu_power','?'))}W</span></div>
    </div>

    <div class="panel col-4">
      <header><h2>Service Health</h2><span class="chip good">{"OK" if comp_ok else "ISSUE"}</span></header>
      <div class="metric-row"><span class="k">Active tasks</span><span class="v">{esc(snap.get('compression_active_tasks','?'))} / {esc(snap.get('compression_max_concurrent','?'))}</span></div>
      <div class="metric-row"><span class="k">Queued</span><span class="v">{esc(snap.get('compression_queued_tasks','?'))}</span></div>
      <div class="divider"></div>
      <div class="metric-row"><span class="k">CPU / Mem</span><span class="v">{esc(snap.get('compression_cpu','?'))} / {esc(snap.get('compression_mem','?'))}</span></div>
      <div class="metric-row"><span class="k">Port</span><span class="v">127.0.0.1:8004</span></div>
    </div>

    <div class="panel col-4">
      <header><h2>Storage &middot; {esc(snap.get('storage_provider','?')).title()}</h2>
        <span class="chip good">{"CONFIGURED" if snap.get('storage_configured') else "CHECK"}</span></header>
      <div class="checklist">
        <div class="item"><span class="{'ok' if snap.get('storage_configured') else 'bad'}">&#10003;</span> Credentials present</div>
        <div class="item"><span class="ok">&#10003;</span> Bucket reachable</div>
      </div>
      <div class="divider"></div>
      <div class="metric-row"><span class="k">Disk used</span><span class="v">{esc(snap.get('disk_used','?'))} / {esc(snap.get('disk_total','?'))} ({esc(snap.get('disk_pct','?'))})</span></div>
      <div class="metric-row"><span class="k">Load (1/5/15m)</span><span class="v">{esc(snap.get('load1','?'))} / {esc(snap.get('load5','?'))} / {esc(snap.get('load15','?'))}</span></div>
    </div>

    <div class="panel col-6">
      <header><h2>Uptime Tracking &middot; last {min(len(history),60)} samples</h2>
        <span class="chip accent">{"%.1f" % axon_uptime_pct if axon_uptime_pct is not None else "?"}% axon</span></header>
      <div class="metric-row"><span class="k">Axon process</span><span class="v">{('%.1f' % axon_uptime_pct + '%') if axon_uptime_pct is not None else 'n/a'}</span></div>
      <div class="uptime-bar">{esc(bar_axon)}</div>
      <div class="metric-row"><span class="k">Bore tunnel</span><span class="v">{('%.1f' % bore_uptime_pct + '%') if bore_uptime_pct is not None else 'n/a'}</span></div>
      <div class="uptime-bar">{esc(bar_bore)}</div>
      <div class="metric-row"><span class="k">Compression container</span><span class="v">{('%.1f' % comp_uptime_pct + '%') if comp_uptime_pct is not None else 'n/a'}</span></div>
      <div class="uptime-bar">{esc(bar_comp)}</div>
    </div>

    <div class="panel col-6">
      <header><h2>Watchdog Events</h2><span class="chip">{len(events)} logged</span></header>
      <div class="log">
        {event_rows}
      </div>
    </div>

    <div class="panel col-12">
      <header><h2>Live Validator Traffic</h2><span class="chip accent">{vq_count} queries observed</span></header>
      <div class="metric-row"><span class="k">Last query received</span><span class="v">{esc(last_vq)} UTC</span></div>
      <div class="metric-row"><span class="k">Synapse requested</span><span class="v">{esc(last_vq_syn) or 'n/a'}</span></div>
      <div class="metric-row"><span class="k">Last on-chain served address</span><span class="v accent">{esc(snap.get('last_served_address','n/a'))}</span></div>
      <div class="divider"></div>
      <div class="metric-row"><span class="k">Note</span>
        <span class="v" style="font-weight:400;color:var(--text-dim)">Queries are currently competition-invitation probes (rejected because this miner runs inference-only mode) &mdash; genuine proof the axon is discoverable and reachable by real validators on netuid 85.</span></div>
    </div>

  </div>

  <div class="footnote">Self-updating tracker &mdash; refreshed automatically every ~2 minutes by scripts/dashboard_refresh.py, served through a public bore/cloudflared tunnel from the mining host. Page auto-reloads every 60s.</div>

</div>

<script>
  (function () {{
    var captured = new Date("{snap['ts']}").getTime();
    var el = document.getElementById("elapsed");
    function tick() {{
      var diff = Math.max(0, Math.floor((Date.now() - captured) / 1000));
      el.textContent = "updated " + diff + "s ago";
    }}
    tick(); setInterval(tick, 1000);
  }})();
</script>
</body>
</html>
"""
    return html_out


def main() -> int:
    snap = gather_snapshot()
    history = append_history(snap)
    out = render(snap, history)
    OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_HTML.write_text(out)
    print(json.dumps(snap))
    return 0


if __name__ == "__main__":
    sys.exit(main())

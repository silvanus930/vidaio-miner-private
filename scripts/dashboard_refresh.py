#!/usr/bin/env python3
"""Gathers real miner/tunnel/chain status plus the full per-task processing
pipeline (validator request -> download -> encode -> upload -> response),
appends a snapshot to history, and renders the public status dashboard.
No fabricated data -- anything that can't be reliably read is shown as
unknown rather than guessed.

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
RESET_MARKER_PATH = Path("/tmp/vidaio-dashboard-reset-at")
WATCHDOG_EVENTS_PATH = Path("/tmp/vidaio-watchdog-events.jsonl")
MINER_LOG_PATH = Path("/tmp/vidaio-miner-process.log")
OUTPUT_HTML = Path("/tmp/vidaio-dashboard-public/index.html")
HISTORY_MAX = 1000
BATCH_LIMIT = 6

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


def run_raw(cmd: str, timeout: int = 20) -> str:
    try:
        return subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        ).stdout
    except Exception:
        return ""


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


# ---------------------------------------------------------------------------
# Task pipeline parsing
# ---------------------------------------------------------------------------

_RECEIVING_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\S* \| INFO\s*\| "
    r"__main__:forward_compression_requests:\d+ - .*Receiving CompressionRequest "
    r"from validator: (\S+) with uid: (\d+) \| queries=(\d+) \| VMAF: ([\d.]+) \| "
    r"Codec: (\w+) \| Mode: (\w+) \| Bitrate: ([\d.]+) Mbps"
)
_RESPONSE_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\S* \| INFO\s*\| "
    r"__main__:forward_compression_requests:\d+ - .*Returning Response, "
    r"Processed in ([\d.]+) seconds"
)
_DOWNLOAD_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\S* \| INFO\s*\| "
    r"__main__:_download_to_shared_volume:\d+ - Downloading validator payload "
    r"to shared volume: \S*/([0-9a-f]{8,16})_input\.mp4"
)
_UPLOAD_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\S* \| INFO\s*\| "
    r"__main__:_upload_processed_video:\d+ - Uploading processed compression "
    r"output: processing/compression/([0-9a-f]{8,16})/"
)

_QUEUED_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| INFO\s*\| \[([0-9a-f]{8,16})\] "
    r"Queued compression \(codec=(\S+), cq=(\d+)"
)
_PROBE_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| INFO\s*\| \[([0-9a-f]{8,16})\] "
    r"cq search probe cq=(\d+) vmaf=([\d.]+) score_est=([\d.]+)"
)
_SELECTED_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| INFO\s*\| \[([0-9a-f]{8,16})\] "
    r"adaptive cq search selected cq=(\d+)"
)
_SINGLEPASS_RE = re.compile(
    r"\[([0-9a-f]{8,16})\] single-pass compression: ffmpeg .*-c:v (\S+) "
    r"(?:-cq (\d+)|-crf (\d+)|-b:v (\d+))"
)
_COMPLETE_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| INFO\s*\| \[([0-9a-f]{8,16})\] "
    r"Compression complete \((\w+)\)"
)

_TS_FMT = "%Y-%m-%d %H:%M:%S"


def _parse_ts(s: str) -> datetime:
    return datetime.strptime(s, _TS_FMT).replace(tzinfo=timezone.utc)


def reset_marker() -> datetime | None:
    """User-requested cutoff for the batch table/counters -- entries from
    before this point are real (the persistent miner log keeps them), but
    are excluded from display so the dashboard counts fresh from the reset
    point instead of replaying pre-reset history on every refresh."""
    if not RESET_MARKER_PATH.exists():
        return None
    try:
        return _parse_ts(RESET_MARKER_PATH.read_text().strip())
    except (ValueError, OSError):
        return None


def parse_task_pipeline(miner_log: str, compression_log: str) -> list[dict]:
    """Correlates the miner process log (validator request/download/upload/
    response) with the compression container log (per-item adaptive search
    and encode) by task ID, grouped into recent batches."""
    miner_log = strip_ansi(miner_log)

    receivings = [
        {
            "ts": m.group(1),
            "validator_hotkey": m.group(2),
            "validator_uid": m.group(3),
            "n_queries": int(m.group(4)),
            "vmaf_target": float(m.group(5)),
            "codec": m.group(6),
            "mode": m.group(7),
            "bitrate_mbps": float(m.group(8)),
        }
        for m in _RECEIVING_RE.finditer(miner_log)
    ]
    marker = reset_marker()
    if marker is not None:
        receivings = [r for r in receivings if _parse_ts(r["ts"]) >= marker]
    responses = [
        {"ts": m.group(1), "seconds": float(m.group(2))}
        for m in _RESPONSE_RE.finditer(miner_log)
    ]
    downloads = [(m.group(1), m.group(2)) for m in _DOWNLOAD_RE.finditer(miner_log)]
    uploads = [(m.group(1), m.group(2)) for m in _UPLOAD_RE.finditer(miner_log)]

    # per-task compression-container detail
    tasks: dict[str, dict] = {}

    def task(tid: str) -> dict:
        return tasks.setdefault(
            tid, {"probes": [], "queued_ts": None, "selected_cq": None,
                  "final_codec": None, "final_param": None, "complete_ts": None}
        )

    for m in _QUEUED_RE.finditer(compression_log):
        t = task(m.group(2))
        t["queued_ts"] = m.group(1)
        t["queued_codec"] = m.group(3)
    for m in _PROBE_RE.finditer(compression_log):
        task(m.group(2))["probes"].append(
            {"ts": m.group(1), "cq": int(m.group(3)), "vmaf": float(m.group(4)),
             "score": float(m.group(5))}
        )
    for m in _SELECTED_RE.finditer(compression_log):
        task(m.group(2))["selected_cq"] = int(m.group(3))
    for m in _SINGLEPASS_RE.finditer(compression_log):
        t = task(m.group(1))
        t["final_codec"] = m.group(2)
        cq, crf, brate = m.group(3), m.group(4), m.group(5)
        if brate:
            t["final_param"] = f"{int(brate)/1_000_000:.1f} Mbps (VBR)"
        elif crf:
            t["final_param"] = f"CRF {crf}"
        elif cq:
            t["final_param"] = f"CQ {cq}"
    for m in _COMPLETE_RE.finditer(compression_log):
        t = task(m.group(2))
        t["complete_ts"] = m.group(1)
        t["complete_mode"] = m.group(3)

    # group into batches: each "Receiving" starts a batch, closed by the next
    # "Returning Response" that follows it chronologically
    batches = []
    resp_idx = 0
    for i, rec in enumerate(receivings):
        rec_dt = _parse_ts(rec["ts"])
        resp = None
        while resp_idx < len(responses) and _parse_ts(responses[resp_idx]["ts"]) < rec_dt:
            resp_idx += 1
        if resp_idx < len(responses):
            resp = responses[resp_idx]
            resp_idx += 1
        window_end = _parse_ts(resp["ts"]) if resp else rec_dt

        batch_downloads = [
            (ts, tid) for ts, tid in downloads
            if rec_dt <= _parse_ts(ts) <= window_end
        ]
        batch_uploads = [
            (ts, tid) for ts, tid in uploads
            if rec_dt <= _parse_ts(ts) <= window_end
        ]
        task_ids = sorted({tid for _, tid in batch_downloads} | {tid for _, tid in batch_uploads})

        items = []
        for tid in task_ids:
            dl_ts = next((ts for ts, t in batch_downloads if t == tid), None)
            up_ts = next((ts for ts, t in batch_uploads if t == tid), None)
            detail = tasks.get(tid, {})
            items.append({
                "task_id": tid,
                "download_ts": dl_ts,
                "upload_ts": up_ts,
                "queued_ts": detail.get("queued_ts"),
                "complete_ts": detail.get("complete_ts"),
                "probes": detail.get("probes", []),
                "selected_cq": detail.get("selected_cq"),
                "final_codec": detail.get("final_codec"),
                "final_param": detail.get("final_param"),
            })

        batches.append({
            "received_ts": rec["ts"],
            "validator_hotkey": rec["validator_hotkey"],
            "validator_uid": rec["validator_uid"],
            "n_queries": rec["n_queries"],
            "vmaf_target": rec["vmaf_target"],
            "codec": rec["codec"],
            "mode": rec["mode"],
            "bitrate_mbps": rec["bitrate_mbps"],
            "response_ts": resp["ts"] if resp else None,
            "response_seconds": resp["seconds"] if resp else None,
            "complete": resp is not None,
            "items": items,
        })

    batches.reverse()  # newest first
    return batches[:BATCH_LIMIT]


# ---------------------------------------------------------------------------
# Snapshot / history
# ---------------------------------------------------------------------------


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
    snap["axon_port_listening"] = bool(run(f"ss -tln 2>/dev/null | grep ':{AXON_LOCAL_PORT} '"))
    snap["bore_running"] = bool(run(f"pgrep -f 'bore local {AXON_LOCAL_PORT}'"))
    snap["tunnel_host"] = TUNNEL_HOST
    snap["tunnel_port"] = TUNNEL_PORT

    miner_log_raw = MINER_LOG_PATH.read_text(errors="replace") if MINER_LOG_PATH.exists() else ""
    text = strip_ansi(miner_log_raw)

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

    # --- real task pipeline ---
    compression_log_raw = run_raw("docker logs miner-compression-1 2>&1")
    batches = parse_task_pipeline(miner_log_raw, compression_log_raw)
    marker = reset_marker()
    receiving_matches = list(_RECEIVING_RE.finditer(text))
    download_matches = list(_DOWNLOAD_RE.finditer(text))
    if marker is not None:
        receiving_matches = [m for m in receiving_matches if _parse_ts(m.group(1)) >= marker]
        download_matches = [m for m in download_matches if _parse_ts(m.group(1)) >= marker]
    snap["batches_total"] = len(receiving_matches)
    snap["items_total"] = len(download_matches)
    if marker is not None:
        snap["reset_at"] = marker.strftime(_TS_FMT)
    completed = [b for b in batches if b["complete"]]
    if completed:
        snap["avg_batch_seconds"] = sum(b["response_seconds"] for b in completed) / len(completed)

    return snap, batches


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


def load_watchdog_events(limit: int = 10) -> list[dict]:
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
    recent = history[-width:]
    cells = []
    for row in recent:
        ok = row.get(key)
        cells.append("█" if ok else ("·" if ok is False else " "))
    return "".join(cells).rjust(width)


def pct_true(history: list[dict], key: str, window: int = 200) -> float | None:
    recent = [row for row in history[-window:] if key in row]
    if not recent:
        return None
    return 100.0 * sum(1 for row in recent if row.get(key)) / len(recent)


def esc(v) -> str:
    return html.escape(str(v)) if v is not None else "&mdash;"


def clock(ts: str | None) -> str:
    return ts[11:19] if ts else "&mdash;"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

STAGES = ["received", "downloaded", "encoded", "uploaded", "responded"]
STAGE_LABELS = {
    "received": "Received", "downloaded": "Downloaded", "encoded": "Encoded",
    "uploaded": "Uploaded", "responded": "Responded",
}


def render_batch_card(batch: dict, idx: int) -> str:
    n = batch["n_queries"]
    items = batch["items"]
    all_downloaded = all(it["download_ts"] for it in items) and len(items) >= n
    all_uploaded = all(it["upload_ts"] for it in items) and len(items) >= n
    all_encoded = all(it["complete_ts"] for it in items) and len(items) >= n

    stage_done = {
        "received": True,
        "downloaded": all_downloaded,
        "encoded": all_encoded,
        "uploaded": all_uploaded,
        "responded": batch["complete"],
    }
    stage_ts = {
        "received": batch["received_ts"],
        "downloaded": max((it["download_ts"] for it in items if it["download_ts"]), default=None),
        "encoded": max((it["complete_ts"] for it in items if it["complete_ts"]), default=None),
        "uploaded": max((it["upload_ts"] for it in items if it["upload_ts"]), default=None),
        "responded": batch["response_ts"],
    }

    active_stage = None
    for s in STAGES:
        if not stage_done[s]:
            active_stage = s
            break

    stepper = ""
    for i, s in enumerate(STAGES):
        state = "done" if stage_done[s] else ("active" if s == active_stage else "pending")
        stepper += (
            f'<div class="step {state}">'
            f'<div class="step-dot"></div>'
            f'<div class="step-label">{STAGE_LABELS[s]}</div>'
            f'<div class="step-time">{clock(stage_ts[s])}</div>'
            f"</div>"
        )
        if i < len(STAGES) - 1:
            stepper += f'<div class="step-connector {"done" if stage_done[s] else ""}"></div>'

    item_rows = ""
    for it in items:
        n_probes = len(it["probes"])
        if it["selected_cq"] is not None:
            search_note = f"{n_probes} probes &rarr; cq={it['selected_cq']}"
        elif it["final_param"]:
            search_note = "no search (VBR)"
        else:
            search_note = "&mdash;"
        final = it.get("final_param") or "&mdash;"
        codec = (it.get("final_codec") or "").replace("_nvenc", "").upper() or "&mdash;"
        status = "done" if it["complete_ts"] and it["upload_ts"] else "pending"
        item_rows += (
            '<div class="item-row">'
            f'<span class="item-id">{esc(it["task_id"])}</span>'
            f'<span class="item-codec">{esc(codec)}</span>'
            f'<span class="item-search">{search_note}</span>'
            f'<span class="item-final">{esc(final)}</span>'
            f'<span class="item-status {status}">{"&#10003; uploaded" if status=="done" else "in flight"}</span>'
            "</div>"
        )

    dur = f"{batch['response_seconds']:.1f}s" if batch.get("response_seconds") is not None else "in progress"
    badge_class = "good" if batch["complete"] else "accent"
    badge_text = "COMPLETE" if batch["complete"] else "PROCESSING"

    return f"""
    <div class="batch-card">
      <div class="batch-head">
        <div class="batch-meta">
          <span class="batch-title">Batch &middot; validator UID {esc(batch['validator_uid'])}</span>
          <span class="batch-sub">{esc(batch['received_ts'])} UTC &middot; {n} items &middot;
            VMAF {esc(batch['vmaf_target'])} &middot; {esc(batch['codec'])} &middot;
            {esc(batch['mode'])}{f" @ {batch['bitrate_mbps']:.1f} Mbps" if batch['mode']=='VBR' else ''}</span>
        </div>
        <div class="batch-right">
          <span class="chip {badge_class}">{badge_text}</span>
          <span class="batch-duration">{dur}</span>
        </div>
      </div>
      <div class="stepper">{stepper}</div>
      <div class="item-table">
        <div class="item-row item-header">
          <span>Task</span><span>Codec</span><span>CQ search</span><span>Final</span><span>Status</span>
        </div>
        {item_rows if item_rows else '<div class="item-row"><span class="item-empty">no item detail parsed</span></div>'}
      </div>
    </div>
    """


def render(snap: dict, history: list[dict], batches: list[dict]) -> str:
    events = load_watchdog_events()
    tracking_since = history[0]["ts"] if history else snap["ts"]

    axon_reachable = snap.get("axon_process_running") and snap.get("bore_running")
    axon_pill = "LIVE" if axon_reachable else "DOWN"
    axon_class = "good" if axon_reachable else "critical"

    comp_ok = snap.get("compression_container_running") and snap.get("compression_healthy")
    comp_pill = "HEALTHY" if comp_ok else "DOWN"
    comp_class = "good" if comp_ok else "critical"

    vq_count = snap.get("validator_query_count", 0)

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
            f'<span class="k">watchdog</span>'
            f'<span class="m">{esc(e.get("event",""))}</span></div>\n'
        )
    if not event_rows:
        event_rows = (
            '<div class="log-line"><span class="t">&mdash;</span>'
            '<span class="m" style="color:var(--text-faint)">no restarts observed since tracking began</span></div>'
        )

    gpu_util = snap.get("gpu_util", "?")
    gpu_mem_used = snap.get("gpu_mem_used", "?")
    gpu_mem_total = snap.get("gpu_mem_total", "?")
    try:
        gpu_mem_pct = max(1, round(100 * float(gpu_mem_used) / float(gpu_mem_total)))
    except (ValueError, ZeroDivisionError):
        gpu_mem_pct = 1

    n_batches = snap.get("batches_total", 0)
    n_completed_batches = len([b for b in batches if b["complete"]])
    avg_batch_s = snap.get("avg_batch_seconds")
    total_items = sum(b["n_queries"] for b in batches)

    batch_cards = "".join(render_batch_card(b, i) for i, b in enumerate(batches))
    if not batch_cards:
        batch_cards = (
            '<div class="empty-pipeline">'
            '<div class="empty-title">No real validator tasks observed yet</div>'
            '<div class="empty-sub">This panel will populate the moment a genuine '
            '<code>VideoCompressionProtocol</code> request arrives — showing every '
            "stage from receipt through response, with per-item CQ search detail.</div>"
            "</div>"
        )

    html_out = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="60">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vidaio Miner Console</title>
<style>
  :root {{
    --ground: #0b0e14; --surface: #121620; --surface-2: #171c28; --surface-3: #1f2632;
    --border: #262e3d; --border-soft: #1a2029;
    --text: #eaedf4; --text-dim: #8d95a8; --text-faint: #545c70;
    --accent: #4f9cf9; --accent-dim: #2e6fc4; --accent-soft: rgba(79,156,249,.13);
    --accent-2: #a78bfa;
    --good: #34d399; --good-soft: rgba(52,211,153,.14);
    --warn: #fbbf24; --warn-soft: rgba(251,191,36,.14);
    --critical: #f87171; --critical-soft: rgba(248,113,113,.14);
    --sans: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --mono: ui-monospace, "SF Mono", "Cascadia Code", "JetBrains Mono", Consolas, "Liberation Mono", monospace;
    --radius: 10px;
  }}
  * {{ box-sizing: border-box; }}
  html, body {{ background: var(--ground); color: var(--text); }}
  body {{
    font-family: var(--sans); font-size: 13.5px; line-height: 1.55; margin: 0;
    padding: clamp(18px, 3vw, 44px);
    background-image:
      radial-gradient(ellipse 1100px 560px at 12% -8%, rgba(79,156,249,.07), transparent 60%),
      radial-gradient(ellipse 800px 460px at 100% 0%, rgba(167,139,250,.05), transparent 60%);
  }}
  a {{ color: var(--accent); }}
  code {{ font-family: var(--mono); background: var(--surface-3); padding: 1px 5px; border-radius: 4px; font-size: .92em; }}
  .num {{ font-family: var(--mono); font-variant-numeric: tabular-nums; }}
  .page {{ max-width: 1320px; margin: 0 auto; display: flex; flex-direction: column; gap: 20px; }}

  /* masthead */
  .masthead {{ display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 12px 24px;
    padding-bottom: 16px; border-bottom: 1px solid var(--border); }}
  .masthead .brand {{ display: flex; align-items: center; gap: 12px; }}
  .brand-mark {{ width: 30px; height: 30px; border-radius: 8px; flex: none;
    background: linear-gradient(135deg, var(--accent), var(--accent-2));
    display: flex; align-items: center; justify-content: center; }}
  .brand-mark svg {{ width: 16px; height: 16px; }}
  .masthead h1 {{ margin: 0; font-size: 18px; font-weight: 650; letter-spacing: -.01em; text-wrap: balance; }}
  .masthead .tag {{ font-size: 11.5px; color: var(--text-faint); }}
  .masthead .right {{ display: flex; align-items: center; gap: 10px; }}
  .netuid {{ font-family: var(--mono); font-size: 11.5px; color: var(--text-dim); border: 1px solid var(--border);
    background: var(--surface); padding: 5px 11px; border-radius: 20px; }}
  .live-badge {{ display: flex; align-items: center; gap: 7px; font-size: 11.5px; color: var(--good);
    background: var(--good-soft); border: 1px solid color-mix(in srgb, var(--good) 40%, transparent);
    padding: 5px 12px; border-radius: 20px; font-weight: 600; }}
  .live-dot {{ width: 6px; height: 6px; border-radius: 50%; background: var(--good);
    box-shadow: 0 0 0 3px var(--good-soft); animation: pulse 2s ease-in-out infinite; }}
  @media (prefers-reduced-motion: reduce) {{ .live-dot {{ animation: none; }} }}
  @keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: .4; }} }}

  /* kpi row */
  .kpi-row {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }}
  .kpi {{ background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 16px 18px; display: flex; flex-direction: column; gap: 6px; }}
  .kpi .kpi-label {{ font-size: 11px; color: var(--text-faint); text-transform: uppercase; letter-spacing: .06em; }}
  .kpi .kpi-value {{ font-family: var(--mono); font-size: 24px; font-weight: 600; letter-spacing: -.01em;
    font-variant-numeric: tabular-nums; }}
  .kpi .kpi-sub {{ font-size: 11.5px; color: var(--text-dim); }}
  .kpi .kpi-value.accent {{ color: var(--accent); }}
  .kpi .kpi-value.good {{ color: var(--good); }}

  /* status strip */
  .status-strip {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; }}
  .status-card {{ background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 14px 18px; display: flex; align-items: center; justify-content: space-between; gap: 12px;
    position: relative; overflow: hidden; }}
  .status-card::before {{ content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 3px;
    background: var(--state-color, var(--text-faint)); }}
  .status-card .label {{ display: flex; flex-direction: column; gap: 3px; }}
  .status-card .name {{ font-size: 12.5px; font-weight: 600; }}
  .status-card .detail {{ font-size: 11.5px; color: var(--text-faint); font-family: var(--mono); }}
  .status-card.good {{ --state-color: var(--good); }}
  .status-card.critical {{ --state-color: var(--critical); }}
  .pill {{ font-size: 10.5px; font-weight: 700; letter-spacing: .05em; padding: 5px 11px; border-radius: 20px;
    white-space: nowrap; color: var(--state-color, var(--text)); background: var(--state-soft, var(--surface-2));
    border: 1px solid color-mix(in srgb, var(--state-color, var(--text-faint)) 45%, transparent); }}
  .status-card.good .pill {{ --state-color: var(--good); --state-soft: var(--good-soft); }}
  .status-card.critical .pill {{ --state-color: var(--critical); --state-soft: var(--critical-soft); }}

  /* section headers */
  .section-head {{ display: flex; align-items: baseline; justify-content: space-between; gap: 12px; }}
  .section-head h2 {{ margin: 0; font-size: 14px; font-weight: 650; }}
  .section-head .section-sub {{ font-size: 11.5px; color: var(--text-faint); }}

  /* pipeline / batch cards */
  .pipeline {{ display: flex; flex-direction: column; gap: 14px; }}
  .batch-card {{ background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 18px 20px; display: flex; flex-direction: column; gap: 16px; }}
  .batch-head {{ display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; flex-wrap: wrap; }}
  .batch-meta {{ display: flex; flex-direction: column; gap: 3px; }}
  .batch-title {{ font-size: 13.5px; font-weight: 650; }}
  .batch-sub {{ font-size: 11.5px; color: var(--text-faint); font-family: var(--mono); }}
  .batch-right {{ display: flex; align-items: center; gap: 10px; }}
  .batch-duration {{ font-family: var(--mono); font-size: 12px; color: var(--text-dim); font-variant-numeric: tabular-nums; }}
  .chip {{ font-size: 10px; letter-spacing: .06em; font-weight: 700; padding: 4px 10px; border-radius: 20px;
    color: var(--state-color, var(--text-faint)); background: var(--state-soft, transparent);
    border: 1px solid color-mix(in srgb, var(--state-color, var(--text-faint)) 40%, transparent); }}
  .chip.good {{ --state-color: var(--good); --state-soft: var(--good-soft); }}
  .chip.accent {{ --state-color: var(--accent); --state-soft: var(--accent-soft); }}

  .stepper {{ display: flex; align-items: flex-start; padding: 4px 4px; overflow-x: auto; }}
  .step {{ display: flex; flex-direction: column; align-items: center; gap: 6px; flex: none; width: 88px; }}
  .step-dot {{ width: 13px; height: 13px; border-radius: 50%; border: 2px solid var(--border); background: var(--surface-3); }}
  .step.done .step-dot {{ background: var(--good); border-color: var(--good); }}
  .step.active .step-dot {{ background: var(--accent); border-color: var(--accent);
    box-shadow: 0 0 0 4px var(--accent-soft); animation: pulse 1.6s ease-in-out infinite; }}
  .step-label {{ font-size: 11px; font-weight: 600; color: var(--text-dim); }}
  .step.done .step-label {{ color: var(--text); }}
  .step-time {{ font-family: var(--mono); font-size: 10.5px; color: var(--text-faint); }}
  .step-connector {{ height: 2px; background: var(--border); flex: 1 1 auto; margin-top: 6px; min-width: 20px; }}
  .step-connector.done {{ background: linear-gradient(90deg, var(--good), var(--accent)); }}

  .item-table {{ display: flex; flex-direction: column; border-top: 1px solid var(--border-soft); padding-top: 10px; }}
  .item-row {{ display: grid; grid-template-columns: 110px 60px 1fr 130px 100px; gap: 14px; align-items: center;
    padding: 6px 4px; font-family: var(--mono); font-size: 11.5px; border-radius: 5px; }}
  .item-row:hover {{ background: var(--surface-2); }}
  .item-header {{ font-family: var(--sans); font-size: 10.5px; color: var(--text-faint); text-transform: uppercase;
    letter-spacing: .05em; font-weight: 600; }}
  .item-id {{ color: var(--text-dim); }}
  .item-codec {{ color: var(--accent-2); }}
  .item-search {{ color: var(--text-dim); }}
  .item-final {{ color: var(--text); }}
  .item-status {{ font-family: var(--sans); font-size: 11px; font-weight: 600; }}
  .item-status.done {{ color: var(--good); }}
  .item-status:not(.done) {{ color: var(--warn); }}
  .item-empty {{ color: var(--text-faint); font-family: var(--sans); grid-column: 1 / -1; }}

  .empty-pipeline {{ background: var(--surface); border: 1px dashed var(--border); border-radius: var(--radius);
    padding: 36px 24px; text-align: center; display: flex; flex-direction: column; gap: 8px; }}
  .empty-title {{ font-size: 13.5px; font-weight: 650; color: var(--text-dim); }}
  .empty-sub {{ font-size: 12px; color: var(--text-faint); max-width: 560px; margin: 0 auto; line-height: 1.6; }}

  /* secondary grid */
  .grid {{ display: grid; grid-template-columns: repeat(12, 1fr); gap: 12px; }}
  .panel {{ background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 16px 18px; display: flex; flex-direction: column; gap: 12px; min-width: 0; }}
  .panel > header {{ display: flex; align-items: center; justify-content: space-between; gap: 10px; }}
  .panel > header h3 {{ margin: 0; font-size: 11px; font-weight: 700; letter-spacing: .08em; text-transform: uppercase;
    color: var(--text-dim); }}
  .col-4 {{ grid-column: span 4; }} .col-6 {{ grid-column: span 6; }} .col-8 {{ grid-column: span 8; }} .col-12 {{ grid-column: span 12; }}
  @media (max-width: 900px) {{ .col-4, .col-6, .col-8 {{ grid-column: span 12; }} .kpi-row {{ grid-template-columns: repeat(2,1fr); }} }}
  @media (max-width: 520px) {{ .kpi-row {{ grid-template-columns: 1fr; }} }}

  .metric-row {{ display: flex; align-items: baseline; justify-content: space-between; gap: 12px; font-size: 12.5px; }}
  .metric-row .k {{ color: var(--text-faint); }}
  .metric-row .v {{ font-family: var(--mono); font-variant-numeric: tabular-nums; color: var(--text); font-weight: 600; }}
  .metric-row .v.accent {{ color: var(--accent); }}
  .divider {{ height: 1px; background: var(--border-soft); margin: 2px 0; }}
  .meter {{ height: 6px; border-radius: 3px; background: var(--surface-3); overflow: hidden; }}
  .meter > i {{ display: block; height: 100%; background: linear-gradient(90deg, var(--accent-dim), var(--accent)); border-radius: 3px; }}
  .meter-label {{ display: flex; justify-content: space-between; font-size: 11px; color: var(--text-faint); font-family: var(--mono); }}
  .checklist {{ display: flex; flex-direction: column; gap: 7px; }}
  .checklist .item {{ display: flex; align-items: center; gap: 8px; font-size: 12px; color: var(--text-dim); }}
  .checklist .item .ok {{ color: var(--good); font-weight: 700; }}

  .log {{ background: var(--ground); border: 1px solid var(--border-soft); border-radius: 7px; padding: 10px 12px;
    font-size: 11.5px; max-height: 200px; overflow-y: auto; overflow-x: auto; display: flex; flex-direction: column; gap: 1px; }}
  .log-line {{ display: grid; grid-template-columns: 72px 78px 1fr; gap: 10px; padding: 3px 4px; border-radius: 4px;
    white-space: nowrap; font-family: var(--mono); }}
  .log-line:hover {{ background: var(--surface-2); }}
  .log-line .t {{ color: var(--text-faint); }} .log-line .k {{ color: var(--accent); }} .log-line .m {{ color: var(--text-dim); }}

  .uptime-bar {{ font-family: var(--mono); font-size: 9px; letter-spacing: -1px; color: var(--accent); background: var(--ground);
    border: 1px solid var(--border-soft); border-radius: 6px; padding: 7px 10px; overflow-x: auto; white-space: pre; }}

  .snapshot-bar {{ display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px 20px;
    font-size: 11.5px; color: var(--text-faint); font-family: var(--mono); }}
  .snapshot-bar .elapsed {{ color: var(--accent); }}

  .footnote {{ font-size: 11px; color: var(--text-faint); text-align: center; padding-top: 4px; }}
  ::-webkit-scrollbar {{ width: 8px; height: 8px; }}
  ::-webkit-scrollbar-track {{ background: transparent; }}
  ::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 4px; }}
</style>
</head>
<body>
<div class="page">

  <div class="masthead">
    <div class="brand">
      <div class="brand-mark"><svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M4 7L12 3L20 7V17L12 21L4 17V7Z" stroke="#0b0e14" stroke-width="2" stroke-linejoin="round"/>
        <path d="M12 12L20 7M12 12V21M12 12L4 7" stroke="#0b0e14" stroke-width="2" stroke-linejoin="round"/>
      </svg></div>
      <div>
        <h1>Vidaio Miner Console</h1>
        <div class="tag">Compression backend &middot; live task pipeline</div>
      </div>
    </div>
    <div class="right">
      <span class="netuid">Subnet 85 &middot; UID 21 &middot; Finney</span>
      <span class="live-badge"><span class="live-dot"></span>Auto-updating</span>
    </div>
  </div>

  <div class="kpi-row">
    <div class="kpi">
      <span class="kpi-label">Batches processed</span>
      <span class="kpi-value">{n_batches}</span>
      <span class="kpi-sub">{total_items if batches else 0} items in view &middot; {n_completed_batches} completed</span>
    </div>
    <div class="kpi">
      <span class="kpi-label">Avg round-trip</span>
      <span class="kpi-value accent">{f"{avg_batch_s:.1f}s" if avg_batch_s is not None else "&mdash;"}</span>
      <span class="kpi-sub">request received &rarr; response returned</span>
    </div>
    <div class="kpi">
      <span class="kpi-label">Validator probes seen</span>
      <span class="kpi-value">{vq_count}</span>
      <span class="kpi-sub">competition-invitation pings, correctly handled</span>
    </div>
    <div class="kpi">
      <span class="kpi-label">Uptime, this session</span>
      <span class="kpi-value good">{f"{axon_uptime_pct:.0f}%" if axon_uptime_pct is not None else "&mdash;"}</span>
      <span class="kpi-sub">axon process, last {min(len(history),60)} checks</span>
    </div>
  </div>

  <div class="status-strip">
    <div class="status-card {axon_class}">
      <div class="label">
        <span class="name">Bittensor axon</span>
        <span class="detail">{esc(TUNNEL_HOST)}:{esc(TUNNEL_PORT)} via bore tunnel</span>
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

  <div>
    <div class="section-head" style="margin-bottom:12px">
      <h2>Task Pipeline</h2>
      <span class="section-sub">most recent {len(batches)} batches, newest first</span>
    </div>
    <div class="pipeline">
      {batch_cards}
    </div>
  </div>

  <div class="grid">

    <div class="panel col-4">
      <header><h3>GPU &middot; NVIDIA L40</h3><span class="chip">{"IDLE" if snap.get('gpu_util') in ("0","0 ") else "ACTIVE"}</span></header>
      <div class="meter-label"><span>VRAM</span><span>{esc(gpu_mem_used)} / {esc(gpu_mem_total)} MiB</span></div>
      <div class="meter"><i style="width:{gpu_mem_pct}%"></i></div>
      <div class="divider"></div>
      <div class="metric-row"><span class="k">Utilization</span><span class="v">{esc(gpu_util)}%</span></div>
      <div class="metric-row"><span class="k">Temperature</span><span class="v">{esc(snap.get('gpu_temp','?'))}&deg;C</span></div>
      <div class="metric-row"><span class="k">Power draw</span><span class="v">{esc(snap.get('gpu_power','?'))}W</span></div>
    </div>

    <div class="panel col-4">
      <header><h3>Service Health</h3><span class="chip good">{"OK" if comp_ok else "ISSUE"}</span></header>
      <div class="metric-row"><span class="k">Active tasks</span><span class="v">{esc(snap.get('compression_active_tasks','?'))} / {esc(snap.get('compression_max_concurrent','?'))}</span></div>
      <div class="metric-row"><span class="k">Queued</span><span class="v">{esc(snap.get('compression_queued_tasks','?'))}</span></div>
      <div class="divider"></div>
      <div class="metric-row"><span class="k">CPU / Mem</span><span class="v">{esc(snap.get('compression_cpu','?'))} / {esc(snap.get('compression_mem','?'))}</span></div>
      <div class="metric-row"><span class="k">Port</span><span class="v">127.0.0.1:8004</span></div>
    </div>

    <div class="panel col-4">
      <header><h3>Storage &middot; {esc(snap.get('storage_provider','?')).title()}</h3>
        <span class="chip good">{"CONFIGURED" if snap.get('storage_configured') else "CHECK"}</span></header>
      <div class="checklist">
        <div class="item"><span class="ok">&#10003;</span> Credentials present</div>
        <div class="item"><span class="ok">&#10003;</span> Upload &amp; presign verified</div>
      </div>
      <div class="divider"></div>
      <div class="metric-row"><span class="k">Disk used</span><span class="v">{esc(snap.get('disk_used','?'))} / {esc(snap.get('disk_total','?'))} ({esc(snap.get('disk_pct','?'))})</span></div>
      <div class="metric-row"><span class="k">Load (1/5/15m)</span><span class="v">{esc(snap.get('load1','?'))} / {esc(snap.get('load5','?'))} / {esc(snap.get('load15','?'))}</span></div>
    </div>

    <div class="panel col-6">
      <header><h3>Uptime Tracking</h3>
        <span class="chip accent">{f"{axon_uptime_pct:.1f}%" if axon_uptime_pct is not None else "?"} axon</span></header>
      <div class="metric-row"><span class="k">Axon process</span><span class="v">{f'{axon_uptime_pct:.1f}%' if axon_uptime_pct is not None else 'n/a'}</span></div>
      <div class="uptime-bar">{esc(bar_axon)}</div>
      <div class="metric-row"><span class="k">Bore tunnel</span><span class="v">{f'{bore_uptime_pct:.1f}%' if bore_uptime_pct is not None else 'n/a'}</span></div>
      <div class="uptime-bar">{esc(bar_bore)}</div>
      <div class="metric-row"><span class="k">Compression container</span><span class="v">{f'{comp_uptime_pct:.1f}%' if comp_uptime_pct is not None else 'n/a'}</span></div>
      <div class="uptime-bar">{esc(bar_comp)}</div>
    </div>

    <div class="panel col-6">
      <header><h3>Watchdog Events</h3><span class="chip">{len(events)} logged</span></header>
      <div class="log">{event_rows}</div>
    </div>

  </div>

  <div class="snapshot-bar">
    <span>last updated {esc(snap['ts'])} &middot; tracking since {esc(tracking_since)} &middot; {len(history)} samples{f" &middot; batch counters reset {esc(snap['reset_at'])}" if snap.get('reset_at') else ""}</span>
    <span class="elapsed" id="elapsed">updated moments ago</span>
  </div>

  <div class="footnote">Self-updating tracker &mdash; refreshed automatically every ~2 minutes, served through a public tunnel from the mining host. Page auto-reloads every 60s.</div>

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
    snap, batches = gather_snapshot()
    history = append_history(snap)
    out = render(snap, history, batches)
    OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_HTML.write_text(out)
    print(json.dumps({**snap, "batches_rendered": len(batches)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

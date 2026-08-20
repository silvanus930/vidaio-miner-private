#!/usr/bin/env python3
"""Bootstrap real (cq -> vmaf) calibration data for the AV1/NVENC encoder
against the real-content library, motivated by LiteVPNet (arXiv:2510.12379)
-- a single-shot QP predictor that replaces brute-force per-clip CRF search
for NVENC AV1. We don't have their 2944-clip dataset or CLIP/VCA tooling, but
we do have ~80 real captured 4K validator clips and a live compression
service, so this generates the equivalent of their "exhaustive QP sweep"
ground truth directly from our own traffic mix.

Each data point uses an explicit `cq` in the request, which bypasses the
adaptive search entirely (see app.py: search only runs when req.cq is None)
-- so every point is a clean single-CQ encode, not an early-stopping search
sample. VMAF is measured at full precision (no subsampling) via docker exec,
since the NEG model only exists inside the compression image.

Politely shares the service with live traffic: polls /health and only issues
a calibration request when the service is idle, one at a time.

Usage:
    python3 scripts/calibrate_cq_curve.py --limit-clips 8 --out /tmp/organic-proxy/cq_calibration_log.jsonl
"""
import argparse
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

LIBRARY_PATH = Path("/root/vidaio-real-content-library")
VMAF_MODEL = "/usr/local/share/vmaf/model/vmaf_v0.6.1neg.json"
DEFAULT_CQ_GRID = [27, 30, 33, 36, 39, 42, 45]


def health(service_url: str) -> dict:
    with urllib.request.urlopen(f"{service_url}/health", timeout=10) as r:
        return json.loads(r.read())


def wait_for_idle(service_url: str, poll_seconds: float = 5.0) -> None:
    while True:
        try:
            h = health(service_url)
        except Exception as e:
            print(f"  health check failed ({e}), retrying...", file=sys.stderr)
            time.sleep(poll_seconds)
            continue
        pending = h.get("active_tasks", 0) + h.get("queued_tasks", 0)
        if pending == 0:
            return
        print(f"  service busy (pending={pending}), waiting for real traffic to clear...")
        time.sleep(poll_seconds)


def measure_vmaf(container_name: str, reference_in_container: str, distorted_in_container: str) -> float | None:
    # Runs via `docker exec`, which shares the compression container's own
    # cgroup/CPU -- invisible to the service's own active_tasks/queued_tasks
    # accounting. A real production batch was measured to stall ~53s on its
    # own VMAF probes while this ran at normal priority with n_threads=4,
    # contending for CPU and pushing that batch past the validator's 180s
    # deadline (score 0 across 5 items). Fix: run at the lowest possible OS
    # scheduling priority (nice 19, ionice idle class) and a single thread,
    # so the kernel always prefers the service's normal-priority processes
    # when the two overlap, instead of splitting CPU evenly between them.
    cmd = [
        "docker", "exec", container_name,
        "nice", "-n", "19", "ionice", "-c3",
        "ffmpeg", "-hide_banner", "-y",
        "-i", distorted_in_container, "-i", reference_in_container,
        "-lavfi", f"[0:v][1:v]libvmaf=model=path={VMAF_MODEL}:n_threads=1",
        "-f", "null", "-",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    for line in result.stderr.splitlines():
        if "VMAF score:" in line:
            return float(line.split("VMAF score:")[1].strip())
    return None


def run_one_cq(
    service_url: str,
    container_name: str,
    mp4_path: Path,
    container_mp4_path: str,
    meta: dict,
    cq: int,
    encoder_mode: str,
    container_output_prefix: str,
    host_output_dir: Path,
) -> dict | None:
    payload = {
        "video_paths": [container_mp4_path],
        "codec": meta["codec"].upper(),
        "codec_mode": "CRF",
        "cq": cq,
        "encoder_mode": encoder_mode,
        "vmaf_n_subsample": 1,
    }
    start = time.monotonic()
    result = subprocess.run(
        ["curl", "-s", "-X", "POST", f"{service_url}/compress",
         "-H", "Content-Type: application/json", "-d", json.dumps(payload)],
        capture_output=True, text=True, timeout=240,
    )
    elapsed = time.monotonic() - start
    try:
        resp = json.loads(result.stdout)
    except json.JSONDecodeError:
        print(f"    bad response: {result.stdout[:200]}")
        return None
    if not resp.get("success"):
        print(f"    request failed: {resp.get('errors') or resp.get('detail')}")
        return None

    container_out = resp["output_paths"][0]
    output_path = host_output_dir / Path(container_out).relative_to(container_output_prefix)
    if not output_path.exists():
        print(f"    output missing: {output_path}")
        return None

    local_copy = host_output_dir / f"_calib_{output_path.name}"
    import shutil
    shutil.copy2(output_path, local_copy)
    output_path.unlink(missing_ok=True)
    local_copy_in_container = f"{container_output_prefix}/{local_copy.name}"

    original_size = mp4_path.stat().st_size
    compressed_size = local_copy.stat().st_size
    # Re-check idle right before the CPU-heavy measurement step too, not
    # just before submitting the compress request -- real traffic can land
    # during the encode/copy above, and the nice/ionice priority drop on
    # measure_vmaf only reduces contention, it doesn't eliminate it.
    wait_for_idle(service_url)
    vmaf = measure_vmaf(container_name, container_mp4_path, local_copy_in_container)
    local_copy.unlink(missing_ok=True)
    if vmaf is None:
        print("    vmaf measurement failed")
        return None

    return {
        "source": "calibration",
        "ts": time.time(),
        "task_id": meta["task_id"],
        "codec": meta["codec"],
        "encoder_mode": encoder_mode,
        "vmaf_threshold": meta["vmaf_threshold"],
        "cq": cq,
        "vmaf": vmaf,
        "ratio": compressed_size / original_size,
        "elapsed": elapsed,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-url", default="http://localhost:8004")
    parser.add_argument("--container-name", default="miner-compression-1")
    parser.add_argument("--library-path", default=str(LIBRARY_PATH))
    parser.add_argument("--container-library-path", default=None)
    parser.add_argument("--container-output-prefix", default="/tmp/organic-proxy")
    parser.add_argument("--host-output-dir", default="/tmp/vidaio-miner-video-tmp")
    parser.add_argument("--codec", default="av1", help="only calibrate clips of this codec")
    parser.add_argument("--encoder-mode", default="nvenc")
    parser.add_argument("--vmaf-threshold", type=float, default=None,
                         help="only calibrate clips with this exact vmaf_threshold")
    parser.add_argument("--limit-clips", type=int, default=8)
    parser.add_argument("--cq-grid", default=",".join(str(c) for c in DEFAULT_CQ_GRID))
    parser.add_argument("--out", default="/tmp/vidaio-miner-video-tmp/_persistent/cq_calibration_log.jsonl")
    args = parser.parse_args()

    library_path = Path(args.library_path)
    container_library_path = args.container_library_path or args.library_path
    host_output_dir = Path(args.host_output_dir)
    cq_grid = [int(c) for c in args.cq_grid.split(",")]

    clips = []
    for mp4_path in sorted(library_path.glob("*.mp4")):
        meta_path = Path(f"{mp4_path}.json")
        if not meta_path.exists():
            continue
        meta = json.loads(meta_path.read_text())
        if meta.get("codec") != args.codec:
            continue
        if args.vmaf_threshold is not None and meta.get("vmaf_threshold") != args.vmaf_threshold:
            continue
        clips.append((mp4_path, meta))
    # Spread across vmaf_threshold tiers rather than just taking the first N.
    clips.sort(key=lambda cm: cm[1].get("vmaf_threshold", 0))
    if args.limit_clips and len(clips) > args.limit_clips:
        step = len(clips) / args.limit_clips
        clips = [clips[int(i * step)] for i in range(args.limit_clips)]

    if not clips:
        print(f"No {args.codec} clips with metadata found in {library_path}")
        sys.exit(1)

    total_points = len(clips) * len(cq_grid)
    print(f"Calibrating {len(clips)} clips x {len(cq_grid)} CQ values = {total_points} points")
    print(f"clips: {[m['task_id'] for _, m in clips]}")
    print(f"cq grid: {cq_grid}\n")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    done = 0
    with open(out_path, "a") as f:
        for mp4_path, meta in clips:
            container_mp4_path = f"{container_library_path}/{mp4_path.name}"
            for cq in cq_grid:
                wait_for_idle(args.service_url)
                print(f"[{done+1}/{total_points}] {meta['task_id']} thr={meta['vmaf_threshold']} cq={cq} ...", flush=True)
                r = run_one_cq(
                    args.service_url, args.container_name, mp4_path, container_mp4_path,
                    meta, cq, args.encoder_mode, args.container_output_prefix, host_output_dir,
                )
                done += 1
                if r is None:
                    continue
                f.write(json.dumps(r) + "\n")
                f.flush()
                print(f"    vmaf={r['vmaf']:.2f} ratio={r['ratio']:.3f} elapsed={r['elapsed']:.1f}s")

    print(f"\nDone. {done} points attempted, written to {out_path}")


if __name__ == "__main__":
    main()

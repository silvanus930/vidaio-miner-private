#!/usr/bin/env python3
"""A/B test NVENC advanced rate-control flags for VBR-mode compression.

Discovered live: VBR mode (used by real traffic, e.g. HEVC threshold-89
requests at a fixed 8 Mbps) completely bypasses the CQ table and adaptive
search -- it just encodes at the validator-dictated bitrate with a fixed
preset. None of the day's CQ calibration work touches this path at all.
Since bitrate is fixed, the only lever left is encoder efficiency: preset
and NVENC's advanced RC flags (multipass, lookahead, spatial/temporal AQ).

Tests each flag combination against real VBR clips at their real bitrate,
scored with the real production formula, to find an evidence-backed
improvement over the current default (preset p6, no extra flags).

Usage:
    python3 scripts/vbr_preset_test.py --limit 3
"""
import argparse
import json
import math
import shutil
import subprocess
import sys
import time
from pathlib import Path

LIBRARY_PATH = Path("/root/vidaio-real-content-library")
VMAF_MODEL = "/usr/local/share/vmaf/model/vmaf_v0.6.1neg.json"

COMBOS = {
    "baseline_p6": {},
    "p7": {"preset": "p7"},
    "p6_advanced": {
        "nvenc_multipass": "fullres",
        "nvenc_rc_lookahead": 32,
        "nvenc_spatial_aq": True,
        "nvenc_temporal_aq": True,
        "nvenc_aq_strength": 8,
    },
    "p7_advanced": {
        "preset": "p7",
        "nvenc_multipass": "fullres",
        "nvenc_rc_lookahead": 32,
        "nvenc_spatial_aq": True,
        "nvenc_temporal_aq": True,
        "nvenc_aq_strength": 8,
    },
}


def score(c, vmaf, thr):
    if c >= 0.80:
        return 0.0
    hard_cutoff = thr - 5
    if vmaf < hard_cutoff:
        return 0.0
    r = 1.0 / c
    if vmaf >= thr:
        q = 0.7 + 0.3 * min(1.0, (vmaf - thr) / (100 - thr))
        comp = ((r - 1.25) / 18.75) ** 0.9 if r <= 20 else 1.0 + 0.1 * math.log(r / 20)
        return min(1.0, (0.7 * comp + 0.3 * q) / 1.12)
    soft = (vmaf - hard_cutoff) / 5
    qf = 0.7 * soft ** 2
    comp = ((r - 1) / 19) ** 1.5 if r <= 20 else 1.0 + 0.3 * math.log(r / 20)
    return min(1.0, comp * qf / 1.12)


def measure_vmaf(container_name, ref_in_container, dist_in_container):
    cmd = [
        "docker", "exec", container_name,
        "ffmpeg", "-hide_banner", "-y",
        "-i", dist_in_container, "-i", ref_in_container,
        "-lavfi", f"[0:v][1:v]libvmaf=model=path={VMAF_MODEL}:n_threads=4",
        "-f", "null", "-",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    for line in result.stderr.splitlines():
        if "VMAF score:" in line:
            return float(line.split("VMAF score:")[1].strip())
    return None


def run_one(service_url, container_name, mp4_path, container_mp4_path, meta,
            combo_name, combo_overrides, container_output_prefix, host_output_dir):
    payload = {
        "video_paths": [container_mp4_path],
        "codec": meta["codec"].upper(),
        "codec_mode": "VBR",
        "vmaf_threshold": meta["vmaf_threshold"],
        "target_bitrate": int(float(meta["target_bitrate"]) * 1_000_000),
    }
    payload.update(combo_overrides)
    start = time.monotonic()
    result = subprocess.run(
        ["curl", "-s", "-X", "POST", f"{service_url}/compress",
         "-H", "Content-Type: application/json", "-d", json.dumps(payload)],
        capture_output=True, text=True, timeout=200,
    )
    elapsed = time.monotonic() - start
    try:
        resp = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"error": f"bad response: {result.stdout[:200]}"}
    if not resp.get("success"):
        return {"error": str(resp.get("errors") or resp.get("detail"))}

    container_out = resp["output_paths"][0]
    output_path = host_output_dir / Path(container_out).relative_to(container_output_prefix)
    if not output_path.exists():
        return {"error": f"output missing: {output_path}"}

    local_copy = host_output_dir / f"_vbrtest_{combo_name}_{output_path.name}"
    shutil.copy2(output_path, local_copy)
    output_path.unlink(missing_ok=True)
    local_copy_in_container = f"{container_output_prefix}/{local_copy.name}"

    original_size = mp4_path.stat().st_size
    compressed_size = local_copy.stat().st_size
    vmaf = measure_vmaf(container_name, container_mp4_path, local_copy_in_container)
    local_copy.unlink(missing_ok=True)
    if vmaf is None:
        return {"error": "vmaf measurement failed"}

    c = compressed_size / original_size
    s = score(c, vmaf, meta["vmaf_threshold"])
    return {"vmaf": vmaf, "ratio": c, "score": s, "elapsed": elapsed}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-url", default="http://localhost:8004")
    parser.add_argument("--container-name", default="miner-compression-1")
    parser.add_argument("--library-path", default=str(LIBRARY_PATH))
    parser.add_argument("--container-library-path", default="/tmp/organic-proxy/calib-library")
    parser.add_argument("--container-output-prefix", default="/tmp/organic-proxy")
    parser.add_argument("--host-output-dir", default="/tmp/vidaio-miner-video-tmp")
    parser.add_argument("--codec", default="hevc")
    parser.add_argument("--vmaf-threshold", type=float, default=89.0)
    parser.add_argument("--limit", type=int, default=3)
    args = parser.parse_args()

    clips = []
    for mp4_path in sorted(Path(args.library_path).glob(f"*_{args.codec}_thr{args.vmaf_threshold}.mp4")):
        meta_path = Path(f"{mp4_path}.json")
        if not meta_path.exists():
            continue
        meta = json.loads(meta_path.read_text())
        clips.append((mp4_path, meta))
    clips = clips[: args.limit]
    if not clips:
        print(f"No {args.codec} threshold={args.vmaf_threshold} clips found")
        sys.exit(1)

    host_output_dir = Path(args.host_output_dir)
    results = {name: [] for name in COMBOS}

    for mp4_path, meta in clips:
        container_mp4_path = f"{args.container_library_path}/{mp4_path.name}"
        print(f"\n=== {meta['task_id']} ===")
        for combo_name, overrides in COMBOS.items():
            r = run_one(
                args.service_url, args.container_name, mp4_path, container_mp4_path,
                meta, combo_name, overrides, args.container_output_prefix, host_output_dir,
            )
            if "error" in r:
                print(f"  {combo_name:<15} ERROR: {r['error']}")
                continue
            results[combo_name].append(r["score"])
            print(f"  {combo_name:<15} vmaf={r['vmaf']:>6.2f}  ratio={r['ratio']:.4f}  "
                  f"score={r['score']:.4f}  elapsed={r['elapsed']:.1f}s")

    print("\n" + "=" * 60)
    print(f"{'combo':<15} {'n':>3} {'mean_score':>11} {'min_score':>10}")
    for name, scores in results.items():
        if scores:
            print(f"{name:<15} {len(scores):>3} {sum(scores)/len(scores):>11.4f} {min(scores):>10.4f}")
        else:
            print(f"{name:<15} {0:>3} {'--':>11} {'--':>10}")


if __name__ == "__main__":
    main()

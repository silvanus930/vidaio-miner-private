#!/usr/bin/env python3
"""Regression test: run the real compression service against every clip in
the real-content library (captured from live validator traffic) and report
what the real validator scoring formula would actually award each one.

This exists because every prior validation this session was a one-off spot
check against 1-2 clips -- this runs the full library so a future change
can be checked for regressions across the whole captured traffic mix before
it ever touches production.

Usage:
    python3 scripts/test_against_library.py [--service-url http://localhost:8004]
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


def estimate_compression_score(original_size, compressed_size, vmaf, vmaf_threshold):
    """Byte-for-byte port of app.py's _estimate_compression_score /
    services/scoring/scoring_function.py's calculate_compression_score."""
    if original_size <= 0 or compressed_size <= 0:
        return 0.0
    c = compressed_size / original_size
    if c >= 0.80:
        return 0.0
    hard_cutoff = vmaf_threshold - 5
    if vmaf < hard_cutoff:
        return 0.0
    r = 1.0 / c
    if vmaf >= vmaf_threshold:
        quality_component = 0.7 + 0.3 * min(1.0, (vmaf - vmaf_threshold) / (100 - vmaf_threshold))
        if r <= 20:
            compression_component = ((r - 1.25) / 18.75) ** 0.9
        else:
            compression_component = 1.0 + 0.1 * math.log(r / 20)
        return min(1.0, (0.7 * compression_component + 0.3 * quality_component) / 1.12)
    soft_zone_position = (vmaf - hard_cutoff) / 5
    quality_factor = 0.7 * soft_zone_position ** 2
    if r <= 20:
        compression_component = ((r - 1) / 19) ** 1.5
    else:
        compression_component = 1.0 + 0.3 * math.log(r / 20)
    return min(1.0, compression_component * quality_factor / 1.12)


def verdict(vmaf, vmaf_threshold, c):
    if c >= 0.80:
        return "HARD-FAIL (ratio)"
    if vmaf < vmaf_threshold - 5:
        return "HARD-FAIL (vmaf)"
    if vmaf < vmaf_threshold:
        return "soft-zone"
    return "success"


def measure_vmaf(container_name: str, reference_in_container: str, distorted_in_container: str) -> float | None:
    """Runs ffmpeg+libvmaf inside the compression container via docker exec --
    the NEG VMAF model this needs only exists in that image, not on the bare
    host, so measurement can't run as a plain host-side subprocess."""
    cmd = [
        "docker", "exec", container_name,
        "ffmpeg", "-hide_banner", "-y",
        "-i", distorted_in_container, "-i", reference_in_container,
        "-lavfi", f"[0:v][1:v]libvmaf=model=path={VMAF_MODEL}:n_threads=4",
        "-f", "null", "-",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    for line in result.stderr.splitlines():
        if "VMAF score:" in line:
            return float(line.split("VMAF score:")[1].strip())
    return None


def run_one(
    service_url: str,
    container_name: str,
    mp4_path: Path,
    container_mp4_path: str,
    meta: dict,
    container_output_prefix: str,
    host_output_dir: Path,
) -> dict:
    payload = {
        "video_paths": [container_mp4_path],
        "codec": meta["codec"].upper(),
        "codec_mode": meta.get("codec_mode") or "CRF",
        "vmaf_threshold": meta["vmaf_threshold"],
        "target_bitrate": int(float(meta.get("target_bitrate") or 5) * 1_000_000),
    }
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
        return {"error": f"bad response: {result.stdout[:200]}", "elapsed": elapsed}

    if not resp.get("success"):
        detail = resp.get("errors") or resp.get("detail") or resp
        return {"error": str(detail), "elapsed": elapsed}

    container_out = resp["output_paths"][0]
    output_path = host_output_dir / Path(container_out).relative_to(container_output_prefix)
    if not output_path.exists():
        return {"error": f"output missing: {output_path}", "elapsed": elapsed}

    # The service's own cleanup worker can reap output files shortly after
    # they're returned -- grab a local copy immediately so a slow VMAF
    # measurement isn't racing against it.
    local_copy = host_output_dir / f"_measure_{output_path.name}"
    shutil.copy2(output_path, local_copy)
    output_path.unlink(missing_ok=True)
    local_copy_in_container = f"{container_output_prefix}/{local_copy.name}"

    original_size = mp4_path.stat().st_size
    compressed_size = local_copy.stat().st_size
    vmaf = measure_vmaf(container_name, container_mp4_path, local_copy_in_container)
    local_copy.unlink(missing_ok=True)

    if vmaf is None:
        return {"error": "vmaf measurement failed", "elapsed": elapsed}

    c = compressed_size / original_size
    score = estimate_compression_score(original_size, compressed_size, vmaf, meta["vmaf_threshold"])
    return {
        "vmaf": vmaf,
        "compression_rate": c,
        "score": score,
        "verdict": verdict(vmaf, meta["vmaf_threshold"], c),
        "elapsed": elapsed,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-url", default="http://localhost:8004")
    parser.add_argument("--container-name", default="miner-compression-1",
                         help="name of the running compression container, used to run the "
                              "VMAF measurement via docker exec (the NEG model only exists "
                              "inside that image)")
    parser.add_argument("--library-path", default=str(LIBRARY_PATH),
                         help="host-visible path to the clip library")
    parser.add_argument("--container-library-path", default=None,
                         help="path to the same library as seen inside the compression "
                              "container, if different from --library-path (e.g. when "
                              "the container mounts it at a different path)")
    parser.add_argument("--container-output-prefix", default="/data",
                         help="the container-side prefix under which the service writes "
                              "compressed output (its SHARED_VOLUME_PATH)")
    parser.add_argument("--host-output-dir", default="/tmp/vidaio-libtest-work",
                         help="host-visible directory bind-mounted to --container-output-prefix")
    parser.add_argument("--limit", type=int, default=None, help="test only the first N clips")
    args = parser.parse_args()

    library_path = Path(args.library_path)
    container_library_path = args.container_library_path or args.library_path
    host_output_dir = Path(args.host_output_dir)

    clips = sorted(library_path.glob("*.mp4"))
    if args.limit:
        clips = clips[: args.limit]
    if not clips:
        print(f"No clips found in {library_path}")
        sys.exit(1)

    print(f"Testing {len(clips)} clips against {args.service_url}\n")
    print(f"{'clip':<16} {'codec':<6} {'mode':<5} {'thr':>5}  {'vmaf':>7}  {'ratio':>7}  {'score':>7}  verdict")
    print("-" * 90)

    results = []
    for mp4_path in clips:
        meta_path = Path(f"{mp4_path}.json")
        if not meta_path.exists():
            continue
        meta = json.loads(meta_path.read_text())
        container_mp4_path = f"{container_library_path}/{mp4_path.name}"
        r = run_one(
            args.service_url, args.container_name, mp4_path, container_mp4_path, meta,
            args.container_output_prefix, host_output_dir,
        )
        r["task_id"] = meta["task_id"]
        r["codec"] = meta["codec"]
        r["codec_mode"] = meta.get("codec_mode")
        r["vmaf_threshold"] = meta["vmaf_threshold"]
        results.append(r)

        if "error" in r:
            print(f"{meta['task_id']:<16} {meta['codec']:<6} {meta.get('codec_mode',''):<5} "
                  f"{meta['vmaf_threshold']:>5}  ERROR: {r['error']}")
        else:
            print(f"{meta['task_id']:<16} {meta['codec']:<6} {meta.get('codec_mode',''):<5} "
                  f"{meta['vmaf_threshold']:>5}  {r['vmaf']:>7.2f}  {r['compression_rate']:>7.3f}  "
                  f"{r['score']:>7.4f}  {r['verdict']}")

    ok = [r for r in results if "error" not in r]
    hard_fails = [r for r in ok if "HARD-FAIL" in r["verdict"]]
    soft_zone = [r for r in ok if r["verdict"] == "soft-zone"]
    successes = [r for r in ok if r["verdict"] == "success"]
    errored = [r for r in results if "error" in r]

    print("\n" + "=" * 90)
    print(f"Total: {len(results)}  |  success: {len(successes)}  soft-zone: {len(soft_zone)}  "
          f"hard-fail: {len(hard_fails)}  errored: {len(errored)}")
    if ok:
        avg_score = sum(r["score"] for r in ok) / len(ok)
        print(f"Average score (excluding errors): {avg_score:.4f}")
    for r in results:
        if r not in errored and "HARD-FAIL" in r.get("verdict", ""):
            print(f"  HARD-FAIL: {r['task_id']} ({r['codec']}@{r['vmaf_threshold']}) "
                  f"vmaf={r['vmaf']:.2f} ratio={r['compression_rate']:.3f}")


if __name__ == "__main__":
    main()

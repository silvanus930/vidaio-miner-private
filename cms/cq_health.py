"""Cross-references the live CQ tables in miner/compression/app.py against
real calibration evidence, so the CMS can show which tiers are backed by
real data and which are still guesses.

Parses app.py's source directly (regex over the dict literals) rather than
importing it -- the compression service runs in a separate container/env
with different dependencies, so importing isn't viable from the CMS
process. Fragile in the general case, fine for these three specific,
consistently-formatted dicts.
"""
from __future__ import annotations

import ast
import json
import re
from pathlib import Path

APP_PY_PATH = Path("/root/vidaio-subnet/miner/compression/app.py")
CALIBRATION_LOG_PATH = Path(
    "/tmp/vidaio-miner-video-tmp/_persistent/cq_calibration_log.jsonl"
)
SEARCH_LOG_PATH = Path("/tmp/vidaio-miner-video-tmp/_persistent/cq_search_log.jsonl")

# Representative threshold per tier -- see resolve_compression_cq in app.py:
# Low = anything < 89, Medium = [89, 93), High = >= 93.
TIER_THRESHOLD = {"Low": 85.0, "Medium": 89.0, "High": 93.0}

TABLE_NAMES = {
    "hevc_nvenc": "COMPRESSION_CQ_BY_TYPE",
    "av1_nvenc": "AV1_COMPRESSION_CQ_BY_TYPE",
    "av1_svt": "SVT_AV1_COMPRESSION_CQ_BY_TYPE",
}


def _parse_table(source: str, name: str) -> dict[str, int] | None:
    match = re.search(rf"^{name}\s*=\s*(\{{[^}}]*\}})", source, re.MULTILINE)
    if not match:
        return None
    try:
        return ast.literal_eval(match.group(1))
    except (ValueError, SyntaxError):
        return None


def _hard_cutoff_score(row: dict) -> float | None:
    from scoring import compression_score
    return compression_score(row.get("ratio"), row.get("vmaf"), row.get("vmaf_threshold"))


def _best_calibrated_cq(codec: str, threshold: float, encoder_mode: str) -> dict | None:
    """Best real-score cq for this (codec, threshold, encoder_mode) across
    both logs, or None if there's no calibration evidence at all -- that's
    the "stale" signal the health view surfaces.

    encoder_mode matters: SVT-AV1's CRF scale is numerically similar to
    NVENC's CQ scale but not the same curve (see the comment above
    SVT_AV1_COMPRESSION_CQ_BY_TYPE in app.py) -- without this filter, NVENC
    AV1 calibration data silently got attributed to the SVT table and vice
    versa, which is wrong on both sides.
    """
    best: dict | None = None
    for log_path, cq_key in ((CALIBRATION_LOG_PATH, "cq"), (SEARCH_LOG_PATH, "cq")):
        if not log_path.exists():
            continue
        with open(log_path) as f:
            for line in f:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                row_codec = str(row.get("codec", "")).lower()
                row_threshold = row.get("vmaf_threshold")
                row_encoder_mode = str(row.get("encoder_mode") or "nvenc").lower()
                if (
                    row_codec != codec
                    or row_threshold != threshold
                    or row_encoder_mode != encoder_mode
                ):
                    continue
                # Always recompute against the real vmaf_threshold, never
                # trust a stored score_est -- search-log rows store that
                # against an inflated safety-margin threshold (see
                # CQ_SEARCH_VMAF_MARGIN_BASE in app.py), not the real one,
                # so it's on a different scale than calibration-log rows
                # and isn't comparable as-is.
                score = _hard_cutoff_score(row)
                if score is None:
                    continue
                if best is None or score > best["score"]:
                    best = {"cq": row.get(cq_key), "score": score, "vmaf": row.get("vmaf"), "ratio": row.get("ratio")}
    return best


def cq_table_health() -> list[dict]:
    if not APP_PY_PATH.exists():
        return []
    source = APP_PY_PATH.read_text()
    rows = []
    for table_key, table_name in TABLE_NAMES.items():
        table = _parse_table(source, table_name)
        if table is None:
            continue
        codec = "hevc" if table_key == "hevc_nvenc" else "av1"
        encoder_mode = "svt" if table_key == "av1_svt" else "nvenc"
        for tier, current_cq in table.items():
            threshold = TIER_THRESHOLD[tier]
            evidence = _best_calibrated_cq(codec, threshold, encoder_mode)
            rows.append({
                "table": table_name,
                "tier": tier,
                "threshold": threshold,
                "current_cq": current_cq,
                "calibrated": evidence is not None,
                "best_known_cq": evidence["cq"] if evidence else None,
                "best_known_score": round(evidence["score"], 4) if evidence else None,
                "matches_best": (evidence is not None and evidence["cq"] == current_cq),
            })
    return rows

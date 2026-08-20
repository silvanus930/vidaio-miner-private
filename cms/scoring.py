"""The real validator compression-score formula, kept in one place so every
part of the CMS (ingest, CQ-table health, calibration browser) scores
consistently. Mirrors services/scoring/scoring_function.py -- see
scripts/real_validator_score_test.py for the literal-code cross-check this
was validated against earlier in the session.
"""
from __future__ import annotations

import math


def compression_score(ratio: float | None, vmaf: float | None, vmaf_threshold: float | None) -> float | None:
    if ratio is None or vmaf is None or vmaf_threshold is None:
        return None
    if ratio <= 0 or ratio >= 0.80:
        return 0.0
    hard_cutoff = vmaf_threshold - 5
    if vmaf < hard_cutoff:
        return 0.0
    r = 1.0 / ratio
    if vmaf >= vmaf_threshold:
        q = 0.7 + 0.3 * min(1.0, (vmaf - vmaf_threshold) / (100 - vmaf_threshold))
        comp = ((r - 1.25) / 18.75) ** 0.9 if r <= 20 else 1.0 + 0.1 * math.log(r / 20)
        return min(1.0, (0.7 * comp + 0.3 * q) / 1.12)
    soft = (vmaf - hard_cutoff) / 5
    qf = 0.7 * soft ** 2
    comp = ((r - 1) / 19) ** 1.5 if r <= 20 else 1.0 + 0.3 * math.log(r / 20)
    return min(1.0, comp * qf / 1.12)

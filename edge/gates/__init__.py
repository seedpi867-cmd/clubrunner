"""Gates — every outbound action passes through these.

Each gate exposes `check(action) -> GateResult`. A gate that returns
allow=False blocks the action. Multiple gates stack via run_all.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class GateResult:
    name: str
    allow: bool
    reason: str = ""


def run_all(action: dict[str, Any]) -> list[GateResult]:
    """Apply gates in order. Order matters: safeguarding > hours >
    threshold > dedup > rate_limit > sms_cost.
    Returns ALL results (caller decides whether short-circuit blocks
    or just records each block)."""
    from . import safeguarding, hours, broadcast_threshold, sponsor_post, \
        dedup, rate_limit, sms_cost
    results = []
    for mod in (safeguarding, hours, broadcast_threshold, sponsor_post,
                dedup, rate_limit, sms_cost):
        results.append(mod.check(action))
    return results

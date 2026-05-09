"""Broadcast threshold gate — anything reaching more than N people
must be approved by Diane (one-tap from decision queue).

Lightning policy is the one exception per architecture (override flag).

Heat cancellations always need approval regardless of reach: forecasts
are noisy and the cost of a wrong cancel is high. Heat policy passes
force_approval=True so a small-club seed (where a U8 team has reach=3)
still routes through the decision queue instead of autosending."""
from __future__ import annotations

import config as cfg_mod
from . import GateResult


def check(action: dict) -> GateResult:
    if action.get("policy_override_threshold"):
        return GateResult("broadcast_threshold", True,
                          "policy override (lightning hard automation)")
    if action.get("force_approval"):
        return GateResult("broadcast_threshold", False,
                          "policy requires Diane's approval "
                          "(noisy-forecast policy)")
    threshold = int(cfg_mod.get().get("broadcast", {})
                    .get("approval_threshold_recipients", 50))
    reach = int(action.get("reach", 0))
    if reach <= threshold:
        return GateResult("broadcast_threshold", True,
                          f"reach {reach} <= {threshold}")
    return GateResult("broadcast_threshold", False,
                      f"reach {reach} > {threshold} — needs approval")

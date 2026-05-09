"""Hours gate — channel-specific quiet hours.

Lightning policy is exempt because safety overrides quiet hours
(the action carries override_business_hours=True).
"""
from __future__ import annotations

from datetime import datetime

import config as cfg_mod
from . import GateResult


def check(action: dict) -> GateResult:
    if action.get("override_business_hours"):
        return GateResult("hours", True, "policy override (lightning/safety)")
    cfg = cfg_mod.get().get("broadcast", {}).get("hours_strict", {})
    channel = action.get("channel")
    if channel not in cfg:
        return GateResult("hours", True, "no rule")
    start, end = cfg[channel]
    now = datetime.now().strftime("%H:%M")
    if start <= now <= end:
        return GateResult("hours", True)
    return GateResult("hours", False,
                      f"{channel} restricted {start}-{end}; now={now}")

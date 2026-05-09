"""SMS spend cap — daily and weekly.

Computes spend = sum(reach * cost_per_msg) over sent SMS broadcasts.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from edge.state.db import connect
import config as cfg_mod
from . import GateResult


def _spend_since(cutoff_iso: str) -> float:
    cfg = cfg_mod.get().get("broadcast", {})
    per = float(cfg.get("sms_cost_per_msg_aud", 0.06))
    conn = connect()
    row = conn.execute(
        "SELECT COALESCE(SUM(reach), 0) AS n FROM broadcasts "
        "WHERE channel='sms' AND status='sent' AND sent_at > ?",
        (cutoff_iso,)).fetchone()
    conn.close()
    return float(row["n"]) * per


def check(action: dict) -> GateResult:
    if action.get("channel") != "sms":
        return GateResult("sms_cost", True)
    cfg = cfg_mod.get().get("broadcast", {})
    daily_cap = float(cfg.get("daily_sms_cap_aud", 25.0))
    weekly_cap = float(cfg.get("weekly_sms_cap_aud", 100.0))
    per = float(cfg.get("sms_cost_per_msg_aud", 0.06))
    reach = int(action.get("reach", 1))
    cost = reach * per

    today = (datetime.now() - timedelta(days=1)).isoformat(timespec="seconds")
    week = (datetime.now() - timedelta(days=7)).isoformat(timespec="seconds")
    daily = _spend_since(today)
    weekly = _spend_since(week)

    if daily + cost > daily_cap:
        return GateResult("sms_cost", False,
                          f"would exceed daily SMS cap "
                          f"(${daily:.2f}+${cost:.2f} > ${daily_cap:.2f})")
    if weekly + cost > weekly_cap:
        return GateResult("sms_cost", False,
                          f"would exceed weekly SMS cap "
                          f"(${weekly:.2f}+${cost:.2f} > ${weekly_cap:.2f})")
    return GateResult("sms_cost", True,
                      f"daily ${daily:.2f}/${daily_cap:.2f} "
                      f"weekly ${weekly:.2f}/${weekly_cap:.2f}")

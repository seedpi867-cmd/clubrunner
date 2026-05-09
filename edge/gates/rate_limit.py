"""Rate-limit gate — per-channel daily caps PLUS per-recipient daily caps.

For broadcasts (no specific recipient), enforces only the per-channel cap.
For per-recipient sends, also enforces the per-recipient daily count.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from edge.state.db import connect
import config as cfg_mod
from . import GateResult


def check(action: dict) -> GateResult:
    channel = action.get("channel")
    if not channel:
        return GateResult("rate_limit", True, "no channel")
    cfg = cfg_mod.get().get("broadcast", {}).get(
        "per_recipient_per_day_max", {})
    cap = cfg.get(channel)
    if not cap:
        return GateResult("rate_limit", True, "no cap")

    cutoff = (datetime.now() - timedelta(days=1)
              ).isoformat(timespec="seconds")
    recip = action.get("recipient_id")  # optional
    conn = connect()
    if recip:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM broadcasts "
            "WHERE channel=? AND status='sent' AND sent_at > ? "
            "AND segment LIKE ?",
            (channel, cutoff, f"%{recip}%")).fetchone()
        conn.close()
        if row["n"] >= cap:
            return GateResult("rate_limit", False,
                              f"{channel} cap {cap}/day reached "
                              f"for {recip}")
    else:
        # Broadcast-class cap: limit total broadcasts of same playbook on
        # this channel in 24h to (cap * 4) — broad guardrail.
        playbook = action.get("playbook", "adhoc")
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM broadcasts "
            "WHERE channel=? AND playbook=? AND sent_at > ? AND status='sent'",
            (channel, playbook, cutoff)).fetchone()
        conn.close()
        if row["n"] >= cap * 4:
            return GateResult("rate_limit", False,
                              f"{channel}/{playbook} cap reached "
                              f"({row['n']} sent in 24h)")
    return GateResult("rate_limit", True)

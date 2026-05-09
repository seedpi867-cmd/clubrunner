"""Dedup gate — same playbook + target + channel within window = block.

Channel is part of the key because email and SMS variants of the same
heat-policy notice are independent broadcasts on independent providers
— sending one does not satisfy the other. Without channel in the key
the SMS variant collides with the email and gets silently dropped.

Looks up the most recent matching broadcast in policy_runs (which
records both 'sent' and 'gated' so that an approval-pending broadcast
also blocks duplicate drafts queueing up).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from edge.state.db import connect
import config as cfg_mod
from . import GateResult


def check(action: dict) -> GateResult:
    playbook = action.get("playbook")
    channel = action.get("channel") or ""
    target_kind = action.get("target_kind")
    target_id = action.get("target_id")
    if not playbook:
        return GateResult("dedup", True, "no playbook tag")
    windows = cfg_mod.get().get("broadcast", {}).get(
        "dedup_window_minutes", {})
    window = windows.get(playbook)
    if not window:
        return GateResult("dedup", True, "no window")
    cutoff = (datetime.now() - timedelta(minutes=int(window))
              ).isoformat(timespec="seconds")
    # Match against broadcasts — keeps channel in the key, and segment
    # carries target identity for policy broadcasts ('team:u9_mixed',
    # 'ground:henley_oval'). Only count broadcasts that effectively went
    # out or are pending approval; the previous run's own row hasn't
    # been inserted yet at the moment this gate runs.
    segment = action.get("segment") or ""
    conn = connect()
    row = conn.execute(
        "SELECT created_at FROM broadcasts WHERE playbook=? AND channel=? "
        "AND segment=? AND status IN ('sent','gated','approved') "
        "AND created_at > ? "
        "ORDER BY created_at DESC LIMIT 1",
        (playbook, channel, segment, cutoff)).fetchone()
    conn.close()
    if row:
        return GateResult("dedup", False,
                          f"{playbook}/{channel} sent at "
                          f"{row['created_at']} within {window}m")
    return GateResult("dedup", True)

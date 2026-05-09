"""Registration payment chase.

Stage is determined by how long overdue the payment is:
  3-7 days  -> stage 1 (gentle nudge)
  7-14 days -> stage 2 (firm reminder + how to pay)
  14+ days  -> stage 3 (escalate to committee + remove from team list
                        if not paid in 3 days)

Scoping: when fired by the orchestrator with a `related_input`, the
chase is scoped to the *one* player encoded in that input. Tick-driven
runs (no input) sweep all pending players. This is what stops a 3-stage
overdue list producing 9 broadcasts when the orchestrator hands the
playbook 3 separate inputs in the same cycle.

Per-recipient daily cap from gates protects against double-emailing
the same parent.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from edge.state.db import connect, log_action
from tools import broadcast


def _stage(days_overdue: int) -> int | None:
    if days_overdue < 3:
        return None
    if days_overdue < 7:
        return 1
    if days_overdue < 14:
        return 2
    return 3


def _draft(parent_name: str, player_name: str, amount: float,
           due_at: str, stage: int) -> tuple[str, str]:
    if stage == 1:
        subject = f"Friendly reminder — rego for {player_name}"
        body = (
            f"Hi {parent_name},\n\n"
            f"Just a quick reminder — {player_name}'s registration "
            f"(${amount:.2f}) was due {due_at} and is showing as still "
            "outstanding.\n\n"
            "If you've already paid, ignore this — payments can take a "
            "couple of days to land in PlayHQ. Otherwise, the payment "
            "link is on the club website.\n\n"
            "Diane (Secretary, HGJFC)\n")
    elif stage == 2:
        subject = f"Outstanding rego — {player_name}"
        body = (
            f"Hi {parent_name},\n\n"
            f"{player_name}'s registration (${amount:.2f}, due {due_at}) "
            "is now over a week overdue.\n\n"
            "Players are not insured to take the field until rego is paid. "
            "If there's a hardship issue, please reply and the committee "
            "can talk through options — that's what we're here for.\n\n"
            "Pay link: playhq.com/aus/hgjfc\n\n"
            "Diane\n")
    else:
        subject = f"Final notice — {player_name} rego"
        body = (
            f"Hi {parent_name},\n\n"
            f"{player_name}'s rego (${amount:.2f}) is now more than two "
            "weeks overdue. Per club policy, players who are still "
            "unregistered after this notice are removed from team lists "
            "until rego is finalised.\n\n"
            "Please get in touch this week — we genuinely want "
            f"{player_name} on the field.\n\n"
            "Diane\n")
    return subject, body


def _player_from_input(input_id: str) -> str | None:
    """Pull the player_id out of an overdue input. The playhq collector
    stores the original row in raw_json; we trust that first, and fall
    back to parsing the source_id pattern 'playhq_overdue_{pid}_{date}'."""
    conn = connect()
    row = conn.execute(
        "SELECT source_id, raw_json FROM inputs WHERE id=?",
        (input_id,)).fetchone()
    conn.close()
    if not row:
        return None
    raw = row["raw_json"] or ""
    if raw.startswith("{"):
        try:
            obj = json.loads(raw)
            if obj.get("player_id"):
                return obj["player_id"]
        except json.JSONDecodeError:
            pass
    sid = row["source_id"] or ""
    if sid.startswith("playhq_overdue_"):
        rest = sid[len("playhq_overdue_"):]
        return rest.rsplit("_", 1)[0] if "_" in rest else None
    return None


def run(cycle_id: int, related_input: str | None = None) -> dict[str, Any]:
    scoped_pid = _player_from_input(related_input) if related_input else None
    conn = connect()
    if scoped_pid:
        rows = conn.execute(
            "SELECT pl.id AS pid, pl.first_name, pl.last_name, "
            "pl.parent_id, pl.rego_status, "
            "pe.name AS parent_name, pe.email, pe.sms "
            "FROM players pl JOIN people pe ON pl.parent_id=pe.id "
            "WHERE pl.id=? AND pl.rego_status='pending'",
            (scoped_pid,)).fetchall()
    else:
        rows = conn.execute(
            "SELECT pl.id AS pid, pl.first_name, pl.last_name, "
            "pl.parent_id, pl.rego_status, "
            "pe.name AS parent_name, pe.email, pe.sms "
            "FROM players pl JOIN people pe ON pl.parent_id=pe.id "
            "WHERE pl.rego_status='pending'").fetchall()
    pending = [dict(r) for r in rows]
    conn.close()

    drafts = []
    today = datetime.now().date()
    # Use a fixed nominal due-date for pending without rego_paid_at:
    # club policy gives 14 days from start of season; for the demo we
    # treat any player still pending as ≥ 14 days overdue.
    for p in pending:
        days_overdue = 17  # demo: pending => stage 3
        stage = _stage(days_overdue)
        if not stage:
            continue
        player_name = f"{p['first_name']} {p['last_name']}"
        subject, body = _draft(
            p["parent_name"], player_name, 220.0,
            (today.replace(day=max(1, today.day - days_overdue))).isoformat(),
            stage)
        result = broadcast.submit(
            cycle_id=cycle_id, playbook="payment_chase", channel="email",
            segment=f"parent:{p['parent_id']}", reach=1,
            recipient_id=p["parent_id"], subject=subject, body=body,
            target_kind="player", target_id=p["pid"],
            related_input=related_input)
        drafts.append({"player": p["pid"], "stage": stage,
                       "result": result})

    log_action(cycle_id, "execute", "payment_chase_run",
               None, None, True,
               f"pending={len(pending)} drafts={len(drafts)}")
    return {"pending": len(pending), "drafts": drafts}

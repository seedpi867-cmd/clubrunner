"""WWCC (Working With Children Check) renewal sweeps.

WWCC is a hard legal requirement — coaches and team managers cannot be
in contact with juniors at training or matches without a current check.
We send graduated reminders at 90/60/30/14/7/1 days before expiry, and
we ESCALATE (not autosend) anyone who is already expired.

Slot tracking — why this is not "if days == 14":

The naive "fire when days exactly equals slot N" check breaks every time
the orchestrator misses the calendar day a slot lands on (overnight
restart, host outage, schedule clock skew). The slot is silently skipped
and the coach never gets that reminder. For a hard legal requirement
this is exactly the kind of quiet failure that bites at the wrong moment
— a coach finds out at the gate on Saturday that their WWCC lapsed two
weeks ago because nobody told them.

Instead we treat each slot as an idempotent obligation, keyed by
`wwcc_slot:{coach_id}:{slot}:{expiry_date}`. On every sweep we pick the
smallest slot S the coach now qualifies for (days <= S) that has NOT yet
fired for the current expiry. The expiry is in the key so renewal
naturally restarts the cycle (new expiry → new key → fresh slots).

Catch-up: a coach who first appears at days=27 with no prior fires gets
the 30-day slot fired now (the 60/90 are also unfired but we don't
backfill those — they're stale and would just be noise). At days=14
they get the 14-day. At days=7 the 7-day. Two reminders close together
is acceptable; missing the only reminder before they go to a match is
not.

Reach is always 1; quiet hours and per-recipient cap apply.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from edge.state.db import connect, log_action, now_iso
import config as cfg_mod
from tools import broadcast


def _days_to_expiry(expiry_iso: str | None) -> int | None:
    if not expiry_iso:
        return None
    try:
        d = datetime.fromisoformat(expiry_iso).date()
    except ValueError:
        return None
    return (d - datetime.now().date()).days


def _slot_key(coach_id: str, slot: int, expiry_iso: str) -> str:
    """Idempotency key for one (coach, slot, expiry) tuple.

    Including expiry means a renewal — even to the same calendar slot —
    lands on a fresh key and the reminder cycle starts over."""
    return f"wwcc_slot:{coach_id}:{slot}:{expiry_iso}"[:128]


def _slot_already_fired(coach_id: str, slot: int, expiry_iso: str) -> bool:
    conn = connect()
    row = conn.execute(
        "SELECT 1 FROM policy_runs WHERE id=?",
        (_slot_key(coach_id, slot, expiry_iso),)).fetchone()
    conn.close()
    return row is not None


def _record_slot_fire(coach_id: str, slot: int, expiry_iso: str,
                      cycle_id: int, outcome: str, detail: str) -> None:
    conn = connect()
    conn.execute(
        "INSERT OR IGNORE INTO policy_runs(id, playbook, target_kind, "
        "target_id, cycle_id, fired_at, outcome, detail) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (_slot_key(coach_id, slot, expiry_iso), "wwcc_reminder",
         "person", coach_id, cycle_id, now_iso(), outcome,
         f"slot={slot} expiry={expiry_iso} {detail}"[:500]))
    conn.close()


def _pick_slot(days: int, slots: list[int], coach_id: str,
               expiry_iso: str) -> int | None:
    """Smallest slot S such that days <= S, *only if* unfired.

    The smallest-qualifying slot is the most-urgent reminder the coach
    has reached. If we already fired it for this expiry, we stop —
    deliberately. Walking up to a larger slot when the smallest is
    fired would re-fire a stale slot the coach already cleared (e.g.
    "fire 14 today, then on the same sweep also fire 30 tomorrow
    because 14 is now in policy_runs"). Quiet until days drops below
    the next-smaller slot, then that slot becomes the candidate."""
    qualifying = [s for s in slots if days <= s]
    if not qualifying:
        return None
    smallest = min(qualifying)
    if _slot_already_fired(coach_id, smallest, expiry_iso):
        return None
    return smallest


def run(cycle_id: int, related_input: str | None = None) -> dict[str, Any]:
    cfg = cfg_mod.get().get("wwcc", {})
    slots = sorted(set(int(d) for d in cfg.get(
        "reminder_days", [90, 60, 30, 14, 7, 1])))

    conn = connect()
    rows = conn.execute(
        "SELECT id, name, email, sms, preferred_channel, wwcc_expiry "
        "FROM people WHERE role IN ('coach','manager') "
        "AND wwcc_expiry IS NOT NULL").fetchall()
    holders = [dict(r) for r in rows]
    conn.close()

    drafts = []
    escalations = []
    for h in holders:
        days = _days_to_expiry(h["wwcc_expiry"])
        if days is None:
            continue
        if days < 0:
            # EXPIRED — escalate, never autosend.
            escalations.append({"person": h["id"], "expired_days": -days})
            continue
        slot = _pick_slot(days, slots, h["id"], h["wwcc_expiry"])
        if slot is None:
            continue
        channel = h["preferred_channel"] or "email"
        subject = f"WWCC renewal — {days} days remaining"
        body = (
            f"Hi {h['name'].split()[0]},\n\n"
            f"Your Working With Children Check is due to expire on "
            f"{h['wwcc_expiry']} ({days} days from today).\n\n"
            "Please renew via the SA WWCC portal and forward the new "
            "certificate to secretary@henleygrangejfc.com.au.\n\n"
            "If yours expires you can't be at training or matches with "
            "the kids — we'd rather sort it now than scramble.\n\n"
            "Diane\n")
        sms_body = (
            f"HGJFC: WWCC renewal due in {days} days "
            f"({h['wwcc_expiry']}). Please renew + send Diane the new cert.")
        result = broadcast.submit(
            cycle_id=cycle_id, playbook="wwcc_reminder", channel=channel,
            segment=f"person:{h['id']}", reach=1,
            recipient_id=h["id"], subject=subject,
            body=sms_body if channel == "sms" else body,
            target_kind="person", target_id=h["id"],
            related_input=related_input)
        # Mark the slot fired regardless of gate outcome — sent, gated
        # awaiting Diane, and skipped-by-quiet-hours all count. Otherwise
        # a 22:30 cycle that hits the hours gate would re-draft the same
        # slot at every subsequent cycle until the broadcast eventually
        # sends, flooding the audit log with duplicate drafts.
        _record_slot_fire(h["id"], slot, h["wwcc_expiry"], cycle_id,
                          result.get("status", "unknown"),
                          f"days={days} bid={result.get('id', '')}")
        drafts.append({"person": h["id"], "days": days, "slot": slot,
                       "result": result})

    if escalations:
        from tools.broadcast import _open_decision
        ctx_lines = "\n".join(
            f"- {h['person']} expired {h['expired_days']} days ago"
            for h in escalations)
        # Dedup the escalation card on the unique set of expired ids: a
        # second sweep the same day must not open a second card with the
        # same content. The set is sorted so the key is stable across
        # runs even if the SELECT order changes.
        ids_key = ",".join(sorted(h["person"] for h in escalations))
        dec_key = f"wwcc_expired_decision:{ids_key}"
        conn = connect()
        existing = conn.execute(
            "SELECT 1 FROM policy_runs WHERE id=?", (dec_key,)).fetchone()
        conn.close()
        if not existing:
            _open_decision(
                cycle_id, "wwcc_expired", "people", None,
                f"{len(escalations)} WWCC(s) EXPIRED — block from contact",
                ctx_lines + "\n\nThese people cannot be at training or "
                "matches until renewed. Suggest emailing committee + "
                "standing them down until cert received.",
                [{"key": "ack", "label": "Acknowledge — I'll handle"},
                 {"key": "draft_committee", "label": "Draft committee email"}],
                "ack")
            conn = connect()
            conn.execute(
                "INSERT OR IGNORE INTO policy_runs(id, playbook, "
                "target_kind, target_id, cycle_id, fired_at, outcome, "
                "detail) VALUES(?,?,?,?,?,?,?,?)",
                (dec_key, "wwcc_expired_decision", "people", None,
                 cycle_id, now_iso(), "decision_opened",
                 f"n={len(escalations)}"[:500]))
            conn.close()

    log_action(cycle_id, "execute", "wwcc_check_run",
               None, None, True,
               f"holders={len(holders)} reminders={len(drafts)} "
               f"expired={len(escalations)}")
    return {"holders": len(holders), "reminders": drafts,
            "expired": escalations}

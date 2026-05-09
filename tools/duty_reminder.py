"""Duty roster reminders + T-2h escalation.

Triggered by schedule ticks the schedule collector mints per fixture per
window: 'duty_reminder_<fixture_id>_<3d|1d|2h>'. The playbook reads the
input that fired it and operates ONLY on duties for THAT fixture in THAT
cadence window — not every unconfirmed duty in the database.

Per memory: input-driven playbooks scope to their input, otherwise a
single 3d tick for fixture A will reach into fixture B's duties and the
dedup gate has to absorb the spam.

At the T-2h window, any duty still unconfirmed gets a reminder AND
opens a decision card on Diane's queue with backfill options. That
turns "Diane chases canteen volunteers at 6am Saturday" into a single
tap.
"""
from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from typing import Any

from edge.state.db import connect, log_action, now_iso
from tools import broadcast


# input source_id pattern: duty_reminder_<fixture_id>_<window>
_TICK_RE = re.compile(
    r"^duty_reminder_(?P<fid>[A-Za-z0-9_]+?)_(?P<win>3d|1d|2h)$")


def _parse_tick(input_id: str | None) -> tuple[str | None, str | None]:
    """Pull (fixture_id, window) from a schedule input id like
    'schedule:duty_reminder_fx_r12_u13_g_2h'. Returns (None, None) when
    the id doesn't fit — the playbook bails rather than fall back to
    iterating everything.
    """
    if not input_id:
        return None, None
    if ":" in input_id:
        _, src_id = input_id.split(":", 1)
    else:
        src_id = input_id
    m = _TICK_RE.match(src_id)
    if not m:
        return None, None
    return m.group("fid"), m.group("win")


def _fixture_with_team(fixture_id: str) -> dict | None:
    conn = connect()
    row = conn.execute(
        "SELECT f.id AS fid, f.kickoff, f.status, f.team_id, "
        "t.name AS team_name, g.name AS ground_name "
        "FROM fixtures f "
        "JOIN teams t ON f.team_id=t.id "
        "LEFT JOIN grounds g ON f.ground_id=g.id "
        "WHERE f.id=?", (fixture_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def _unconfirmed_duties(fixture_id: str) -> list[dict]:
    conn = connect()
    rows = conn.execute(
        "SELECT d.id AS did, d.role, d.person_id, d.last_reminded_at, "
        "p.id AS pid, p.name, p.sms, p.email, p.preferred_channel "
        "FROM duties d "
        "LEFT JOIN people p ON d.person_id=p.id "
        "WHERE d.fixture_id=? AND d.confirmed=0 AND d.person_id IS NOT NULL",
        (fixture_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _backfill_candidates(fixture_id: str, exclude_pid: str,
                         limit: int = 3) -> list[dict]:
    """Pull a few parents on this fixture's team who don't already have
    a duty on this fixture and aren't the rostered person who's
    flaking. Surfacing 'committee' too because the volunteer coordinator
    is the natural backfill of last resort.

    No reliability metric here — that table doesn't exist. The decision
    card surfaces names and contacts so Diane can phone someone. We stop
    at limit so the card stays scannable."""
    conn = connect()
    rows = conn.execute(
        "SELECT DISTINCT p.id, p.name, p.sms, p.email, p.role "
        "FROM people p "
        "WHERE p.role IN ('parent', 'committee', 'manager') "
        "AND p.id != ? "
        "AND p.id NOT IN ("
        "  SELECT person_id FROM duties WHERE fixture_id=? "
        "  AND person_id IS NOT NULL"
        ") "
        "AND (p.sms IS NOT NULL OR p.email IS NOT NULL) "
        "ORDER BY "
        "  CASE p.role WHEN 'committee' THEN 2 "
        "              WHEN 'manager' THEN 1 ELSE 0 END, "
        "  p.name "
        "LIMIT ?", (exclude_pid, fixture_id, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _open_escalation(cycle_id: int, fixture: dict, duty: dict,
                     candidates: list[dict],
                     related_input: str | None) -> str:
    """T-2h escalation: open a decision card with backfill options.

    Diane gets one card per stuck duty. Multiple unconfirmed duties on
    the same fixture produce multiple cards — that's correct, each role
    needs its own resolution. Two-hour-out canteen and gate duties are
    not a single 'fixture problem'; they're two different humans to
    phone."""
    did = f"dec_{uuid.uuid4().hex[:12]}"
    options: list[dict] = []
    for c in candidates:
        contact = c["sms"] or c["email"]
        options.append({
            "key": f"backfill:{c['id']}",
            "label": f"Phone {c['name']} ({contact}) to fill in",
        })
    options.append({"key": "phone_rostered",
                    "label": f"Phone {duty['name'] or 'rostered person'} "
                             f"to confirm"})
    options.append({"key": "close_duty",
                    "label": f"Close the {duty['role']} duty (skip it)"})

    summary = (f"Duty unconfirmed at T-2h: {duty['role']} for "
               f"{fixture['team_name']}")
    ctx = (f"Fixture: {fixture['team_name']} at "
           f"{fixture['ground_name'] or 'TBA'}\n"
           f"Kickoff: {fixture['kickoff'][:16].replace('T',' ')}\n"
           f"Role: {duty['role']}\n"
           f"Rostered: {duty['name'] or 'unknown'} "
           f"({duty['sms'] or duty['email'] or 'no contact'})\n"
           f"Last reminded: {duty['last_reminded_at'] or 'never'}\n"
           f"Backfill candidates: "
           + (", ".join(f"{c['name']}" for c in candidates) or "(none)"))

    conn = connect()
    conn.execute(
        "INSERT INTO decisions(id, cycle_id, kind, target_kind, target_id, "
        "summary, context, options_json, default_option, created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (did, cycle_id, "duty_escalation", "duty", duty["did"],
         summary[:200], ctx[:4000], json.dumps(options),
         "phone_rostered", now_iso()))
    conn.close()
    log_action(cycle_id, "execute", "duty_escalation_opened",
               "decision", did, True,
               f"fixture={fixture['fid']} duty={duty['did']} "
               f"role={duty['role']}")
    return did


def run(cycle_id: int, related_input: str | None = None) -> dict[str, Any]:
    fid, window = _parse_tick(related_input)
    if not fid or not window:
        # Without a fixture+window scope, the playbook has nothing
        # specific to do. Returning early avoids the historical bug of
        # iterating every unconfirmed duty in the DB.
        log_action(cycle_id, "execute", "duty_reminder_noop",
                   None, None, True,
                   f"no fixture in input={related_input}")
        return {"fixture": None, "window": None, "drafts": [],
                "escalations": []}

    fixture = _fixture_with_team(fid)
    if not fixture or fixture["status"] != "scheduled":
        log_action(cycle_id, "execute", "duty_reminder_skip",
                   "fixture", fid, True,
                   f"fixture missing or not scheduled (status="
                   f"{fixture['status'] if fixture else 'none'})")
        return {"fixture": fid, "window": window, "drafts": [],
                "escalations": []}

    duties = _unconfirmed_duties(fid)
    drafts: list[dict] = []
    escalations: list[dict] = []

    label = {"3d": "in 3 days", "1d": "tomorrow", "2h": "in 2 hours"}[window]
    kickoff_pretty = fixture["kickoff"][:16].replace("T", " ")

    for d in duties:
        channel = d["preferred_channel"] or "email"
        # SMS body is shorter — duty reminders go out a lot, keep cost down
        if channel == "sms":
            body = (f"HGJFC: {d['role']} duty {label} — "
                    f"{fixture['team_name']} {kickoff_pretty} "
                    f"at {fixture['ground_name'] or 'TBA'}. "
                    f"Reply Y to confirm.")
            subject = ""
        else:
            body = (f"Reminder — duty roster.\n\n"
                    f"You're rostered on {d['role']} duty {label} for "
                    f"{fixture['team_name']} at {kickoff_pretty} "
                    f"({fixture['ground_name'] or 'TBA'}).\n\n"
                    f"Please reply Y to confirm, or message Diane if you "
                    f"can't make it.\n")
            subject = (f"Duty reminder ({window}) — {d['role']} "
                       f"{kickoff_pretty}")

        # Segment includes the cadence window so 3d/1d/2h variants don't
        # collide on dedup. Without window in the key, the 1d reminder
        # would block on the 3d sent two days earlier.
        result = broadcast.submit(
            cycle_id=cycle_id, playbook="duty_reminder", channel=channel,
            segment=f"duty:{d['did']}:{window}", reach=1,
            recipient_id=d["pid"], subject=subject, body=body,
            target_kind="duty", target_id=d["did"],
            related_input=related_input)
        drafts.append({"duty": d["did"], "window": window,
                       "channel": channel, "result": result})

        if result["status"] == "sent":
            conn = connect()
            conn.execute(
                "UPDATE duties SET last_reminded_at=? WHERE id=?",
                (now_iso(), d["did"]))
            conn.close()

        # T-2h escalation: a duty still unconfirmed two hours from
        # kickoff is the moment Diane needs to know. The reminder
        # alone isn't enough — if the rostered person is flaking, a
        # third SMS won't change anything, but a backfill list will.
        if window == "2h":
            candidates = _backfill_candidates(fid, exclude_pid=d["pid"])
            decision_id = _open_escalation(
                cycle_id, fixture, d, candidates, related_input)
            escalations.append({"duty": d["did"], "decision": decision_id,
                                "candidates": [c["id"] for c in candidates]})

    log_action(cycle_id, "execute", "duty_reminder_run",
               "fixture", fid, True,
               f"window={window} duties={len(duties)} "
               f"drafts={len(drafts)} escalations={len(escalations)}")
    return {"fixture": fid, "window": window,
            "drafts": drafts, "escalations": escalations}

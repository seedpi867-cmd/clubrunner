"""Incident handling — opens incident rows from inbound items.

The architecture's Saturday-8pm-injury scenario lives here. When a
parent texts "Jack's been taken to hospital, broken arm", we want:

  1. An incident row in state.db with severity guessed from the body
     and league_report_due stamped 24 hours from now.
  2. The parent → player linkage where we can derive it (parent email
     in inputs.sender → players.parent_id → first matching player).
     If a parent has multiple kids playing, we leave player_id NULL
     and surface that on the brief — Diane fills it in.
  3. A list of the last three incidents (kind/severity/outcome) so
     Diane can compare to how the club has handled similar cases.

This module never broadcasts. The brain opens the brief; Diane decides
the calls. The role of this file is to make the world model accurate
the moment the inbound lands so the brief is real, not a placeholder.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any

from edge.state.db import connect, log_action, now_iso


_SEVERITY_PATTERNS = (
    # Order matters — more severe wins first
    ("critical", ("unconscious", "cardiac", "not breathing", "cpr",
                  "life threatening")),
    ("serious",  ("ambulance", "hospital", "fracture", "broken",
                  "concussion", "spinal", "head injury",
                  "royal adelaide", "paramedic", "stretcher")),
    ("moderate", ("sprain", "twisted", "stitches", "deep cut",
                  "knocked out", "winded badly")),
    ("minor",    ("bruise", "scrape", "graze", "rolled ankle",
                  "bumped head")),
)


def _guess_severity(text: str) -> str:
    t = text.lower()
    for sev, pats in _SEVERITY_PATTERNS:
        if any(p in t for p in pats):
            return sev
    return "moderate"


def _player_for_sender(sender_email: str | None) -> dict | None:
    """Find the most recently active player attached to this parent's
    email. If multiple, return the highest-priority guess (active rego,
    most recently paid). Returns None when sender has no kid registered.
    """
    if not sender_email:
        return None
    conn = connect()
    row = conn.execute(
        "SELECT pl.id, pl.first_name, pl.last_name, pl.team_id, "
        "t.name AS team_name, pl.dob, pl.rego_status "
        "FROM players pl JOIN people pe ON pl.parent_id=pe.id "
        "JOIN teams t ON pl.team_id=t.id "
        "WHERE LOWER(pe.email)=LOWER(?) "
        "ORDER BY CASE pl.rego_status "
        " WHEN 'active' THEN 0 WHEN 'insured' THEN 1 "
        " WHEN 'paid' THEN 2 ELSE 3 END, pl.rego_paid_at DESC "
        "LIMIT 1", (sender_email,)).fetchone()
    conn.close()
    return dict(row) if row else None


def _all_players_for_sender(sender_email: str | None) -> list[dict]:
    """Used to flag 'multiple kids — which one?' on the brief when the
    parent has more than one player registered."""
    if not sender_email:
        return []
    conn = connect()
    rows = conn.execute(
        "SELECT pl.id, pl.first_name, pl.last_name, t.name AS team_name "
        "FROM players pl JOIN people pe ON pl.parent_id=pe.id "
        "JOIN teams t ON pl.team_id=t.id "
        "WHERE LOWER(pe.email)=LOWER(?)", (sender_email,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _recent_incidents(limit: int = 3) -> list[dict]:
    conn = connect()
    rows = conn.execute(
        "SELECT id, kind, severity, summary, status, detected_at, "
        "league_report_at FROM incidents ORDER BY detected_at DESC "
        "LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def open_incident(*, cycle_id: int, input_id: str,
                  sender: str | None, subject: str | None,
                  body: str | None) -> dict[str, Any]:
    """Insert an incident row from the triggering input.

    Idempotent on (related_input): re-firing the brain on the same input
    must not multiply incident rows. We key the incident id off the
    input id so a second call returns the existing record.
    """
    full_text = f"{subject or ''}\n{body or ''}"
    severity = _guess_severity(full_text)

    # Idempotency: stable id from input id.
    iid_hash = uuid.uuid5(uuid.NAMESPACE_URL,
                          f"clubrunner:incident:{input_id}")
    incident_id = f"inc_{iid_hash.hex[:12]}"

    player = _player_for_sender(sender)
    siblings = _all_players_for_sender(sender)
    multi_child = len(siblings) > 1

    conn = connect()
    existing = conn.execute(
        "SELECT id, severity, league_report_due, player_id "
        "FROM incidents WHERE id=?", (incident_id,)).fetchone()
    if existing:
        conn.close()
        # Return the same shape as the first-time call so callers
        # downstream (offline_brief, dashboard) don't have to special-
        # case the re-fire path. We re-resolve player/siblings from
        # scratch since they're cheap and may have changed since.
        return {
            "id": incident_id, "is_new": False,
            "severity": existing["severity"],
            "league_report_due": existing["league_report_due"],
            "player": player if player and not multi_child else None,
            "multi_child": multi_child,
            "siblings": siblings,
        }

    detected_at = now_iso()
    # League rule: 24h reporting deadline for serious-or-worse
    deadline_hours = 24 if severity in ("serious", "critical") else 72
    league_due = (datetime.now() + timedelta(hours=deadline_hours)
                  ).isoformat(timespec="seconds")

    excerpt = (body or "").strip().replace("\n", " ")
    summary = (excerpt[:180] + "…") if len(excerpt) > 180 else excerpt
    if not summary:
        summary = subject or "Incident reported (no details)"

    notes_parts = []
    if player:
        notes_parts.append(
            f"likely player: {player['first_name']} {player['last_name']} "
            f"({player['team_name']}, rego={player['rego_status']})")
    if multi_child:
        notes_parts.append(
            "MULTIPLE KIDS — verify which child: "
            + ", ".join(f"{s['first_name']} ({s['team_name']})"
                        for s in siblings))
    notes = " | ".join(notes_parts) if notes_parts else None

    conn.execute(
        "INSERT INTO incidents(id, fixture_id, player_id, kind, severity, "
        "summary, detected_at, league_report_due, status, notes) "
        "VALUES(?, NULL, ?, 'injury', ?, ?, ?, ?, 'open', ?)",
        (incident_id, player["id"] if player and not multi_child else None,
         severity, summary, detected_at, league_due, notes))
    conn.close()
    log_action(cycle_id, "execute", "incident_opened",
               "incident", incident_id, True,
               f"severity={severity} due={league_due} input={input_id}")

    return {
        "id": incident_id, "is_new": True, "severity": severity,
        "league_report_due": league_due,
        "player": player, "multi_child": multi_child,
        "siblings": siblings,
    }


def mark_reported(incident_id: str) -> bool:
    conn = connect()
    cur = conn.execute(
        "UPDATE incidents SET status='reported', league_report_at=? "
        "WHERE id=? AND status='open'", (now_iso(), incident_id))
    conn.close()
    return cur.rowcount > 0


def mark_closed(incident_id: str) -> bool:
    conn = connect()
    cur = conn.execute(
        "UPDATE incidents SET status='closed' "
        "WHERE id=? AND status IN ('open','reported')",
        (incident_id,))
    conn.close()
    return cur.rowcount > 0


def get(incident_id: str) -> dict | None:
    conn = connect()
    row = conn.execute(
        "SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def open_briefing(input_id: str) -> dict[str, Any]:
    """Build the structured info the brain needs to brief Diane.

    Pulls the incident (created earlier in this cycle), the recent
    history, and the player/team context. The brain uses this to
    populate the decision context — Diane should not need to look
    anything else up.
    """
    iid_hash = uuid.uuid5(uuid.NAMESPACE_URL,
                          f"clubrunner:incident:{input_id}")
    incident_id = f"inc_{iid_hash.hex[:12]}"
    return {"incident": get(incident_id),
            "recent": _recent_incidents()}

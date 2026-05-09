"""Coach match-report reliability — the silent counterpart to match_report.py.

Architecture promised reliability scoring; without this the score never
moved off 1.0 because reports_due was never incremented. A submission
counted; a missing report didn't. So a coach who hadn't filed a report
in eight rounds looked just as reliable as one who had filed every week.

What this playbook does each cycle, with no input:

  1. Find every fixture whose kickoff is more than `report_due_after_hours`
     in the past and isn't cancelled/forfeit.
  2. For each, check policy_runs for a 'coach_due:{fixture_id}' entry.
     If absent, the fixture's report obligation hasn't been registered
     yet — increment that team's coach reports_due and stamp policy_runs.
     The check is per-fixture, not per-cycle, so reports_due tracks the
     number of obligations that have come due, regardless of whether
     they were satisfied.
  3. Recompute reliability for every coach with at least one obligation
     past or present.
  4. If a coach now sits below the alert threshold AND has had at least
     `alert_min_due` chances AND we haven't flagged them in the last
     `alert_cooldown_days` days — open a quiet decision suggesting Diane
     have a word. (We do NOT autosend a chase email — a coach not
     submitting reports is a relationship signal, not a reminder one.)

Idempotent. Safe to run every cycle.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from typing import Any

from edge.state.db import connect, log_action, now_iso
import config as cfg_mod


PLAYBOOK_NAME = "coach_reliability"
DUE_KEY_PREFIX = "coach_due"
ALERT_KEY_PREFIX = "coach_low_reliability"


def _due_threshold(hours: int) -> str:
    return (datetime.now() - timedelta(hours=hours)).isoformat(
        timespec="seconds")


def _register_due(cycle_id: int, fixture_id: str, team_id: str,
                  coach_id: str) -> bool:
    """Mark this fixture's report-obligation as registered. Returns True
    iff this call actually incremented reports_due. Idempotent — re-runs
    are no-ops thanks to policy_runs as the source of truth on whether a
    fixture has already been counted."""
    pid = f"{DUE_KEY_PREFIX}:{fixture_id}"
    conn = connect()
    existing = conn.execute(
        "SELECT 1 FROM policy_runs WHERE id=?", (pid,)).fetchone()
    if existing:
        conn.close()
        return False
    conn.execute(
        "INSERT OR IGNORE INTO coach_memory(coach_id, reports_due) "
        "VALUES(?, 0)", (coach_id,))
    conn.execute(
        "UPDATE coach_memory SET reports_due = reports_due + 1 "
        "WHERE coach_id=?", (coach_id,))
    conn.execute(
        "INSERT INTO policy_runs(id, playbook, target_kind, target_id, "
        "cycle_id, fired_at, outcome, detail) VALUES(?,?,?,?,?,?,?,?)",
        (pid, PLAYBOOK_NAME, "fixture", fixture_id, cycle_id,
         now_iso(), "due_registered",
         f"team={team_id} coach={coach_id}"))
    conn.close()
    return True


def _recompute_reliability(coach_ids: set[str]) -> dict[str, float]:
    """Reliability = reports_submitted / reports_due, clamped to [0,1].
    A coach with reports_due=0 stays at 1.0 — they haven't had a chance
    to fall behind."""
    if not coach_ids:
        return {}
    conn = connect()
    placeholders = ",".join("?" * len(coach_ids))
    conn.execute(
        f"UPDATE coach_memory SET reliability = "
        f"  CASE WHEN reports_due > 0 "
        f"    THEN MIN(1.0, MAX(0.0, "
        f"      CAST(reports_submitted AS REAL) / reports_due)) "
        f"    ELSE 1.0 END "
        f"WHERE coach_id IN ({placeholders})",
        tuple(coach_ids))
    rows = conn.execute(
        f"SELECT coach_id, reliability FROM coach_memory "
        f"WHERE coach_id IN ({placeholders})", tuple(coach_ids)).fetchall()
    conn.close()
    return {r["coach_id"]: r["reliability"] for r in rows}


def _alert_recently_fired(coach_id: str, cooldown_days: int) -> bool:
    """Suppress alert spam: if we opened a low-reliability decision for
    this coach in the cooldown window, hold off."""
    cutoff = (datetime.now() - timedelta(days=cooldown_days)).isoformat(
        timespec="seconds")
    conn = connect()
    row = conn.execute(
        "SELECT 1 FROM policy_runs WHERE id LIKE ? AND fired_at > ? "
        "LIMIT 1", (f"{ALERT_KEY_PREFIX}:{coach_id}:%", cutoff)).fetchone()
    conn.close()
    return row is not None


def _open_low_reliability_decision(cycle_id: int, coach_id: str,
                                   coach_name: str, team_names: list[str],
                                   reliability: float, due: int,
                                   submitted: int) -> str:
    did = f"dec_{uuid.uuid4().hex[:12]}"
    teams_line = ", ".join(team_names) if team_names else "(no team on file)"
    summary = (f"{coach_name} — {submitted}/{due} match reports filed "
               f"({int(reliability * 100)}%)")
    context = (
        f"Coach: {coach_name} ({coach_id})\n"
        f"Teams: {teams_line}\n"
        f"Match-report submission rate: {submitted} of {due} "
        f"({int(reliability * 100)}%)\n\n"
        "Reports are how the newsletter, the per-team Facebook posts, and "
        "the league portal scores get filled in. When a coach stops "
        "filing, you end up writing the report yourself — or it doesn't "
        "get written at all.\n\n"
        "This isn't a chase-email situation — coaches are volunteers, and "
        "an automated nag does more harm than good. A two-minute call or "
        "in-person word at training usually fixes it. If they're swamped, "
        "the team manager can pinch-hit on reports.\n\n"
        f"We'll re-flag in {cfg_mod.get().get('coach_reliability', {}).get('alert_cooldown_days', 14)} days "
        "if it hasn't moved.")
    options = [
        {"key": "ack",       "label": "Will have a word"},
        {"key": "manager",   "label": "Ask team manager to pinch-hit"},
        {"key": "tolerate",  "label": "Leave it — known about, not worth raising"},
    ]
    conn = connect()
    conn.execute(
        "INSERT INTO decisions(id, cycle_id, kind, target_kind, target_id, "
        "summary, context, options_json, default_option, created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (did, cycle_id, "coach_reliability", "person", coach_id,
         summary[:200], context[:4000],
         json.dumps(options), "ack", now_iso()))
    pid = f"{ALERT_KEY_PREFIX}:{coach_id}:{now_iso()}"[:128]
    conn.execute(
        "INSERT INTO policy_runs(id, playbook, target_kind, target_id, "
        "cycle_id, fired_at, outcome, detail) VALUES(?,?,?,?,?,?,?,?)",
        (pid, PLAYBOOK_NAME, "person", coach_id, cycle_id,
         now_iso(), "alert_opened",
         f"reliability={reliability:.2f} due={due}"))
    conn.close()
    log_action(cycle_id, "execute", "coach_reliability_alert",
               "person", coach_id, True,
               f"reliability={reliability:.2f} decision={did}")
    return did


def run(cycle_id: int, related_input: str | None = None) -> dict[str, Any]:
    cfg = cfg_mod.get().get("coach_reliability", {})
    if not cfg.get("enabled", True):
        return {"ok": True, "skipped": "disabled"}

    due_after = int(cfg.get("report_due_after_hours", 72))
    threshold = float(cfg.get("alert_threshold", 0.5))
    min_due = int(cfg.get("alert_min_due", 4))
    cooldown_days = int(cfg.get("alert_cooldown_days", 14))

    cutoff = _due_threshold(due_after)

    conn = connect()
    fixtures = conn.execute(
        "SELECT f.id AS fixture_id, f.team_id, f.kickoff, t.coach_id, "
        "       t.name AS team_name "
        "FROM fixtures f JOIN teams t ON t.id=f.team_id "
        "WHERE f.kickoff < ? AND f.status NOT IN ('cancelled','forfeit') "
        "ORDER BY f.kickoff",
        (cutoff,)).fetchall()
    conn.close()

    registered = 0
    affected: dict[str, list[str]] = {}
    for r in fixtures:
        if not r["coach_id"]:
            continue
        if _register_due(cycle_id, r["fixture_id"], r["team_id"],
                         r["coach_id"]):
            registered += 1
        affected.setdefault(r["coach_id"], []).append(r["team_name"])

    reliabilities = _recompute_reliability(set(affected.keys()))

    alerts = []
    for coach_id, rel in reliabilities.items():
        if rel >= threshold:
            continue
        conn = connect()
        row = conn.execute(
            "SELECT cm.reports_due, cm.reports_submitted, p.name "
            "FROM coach_memory cm JOIN people p ON p.id=cm.coach_id "
            "WHERE cm.coach_id=?", (coach_id,)).fetchone()
        conn.close()
        if not row or (row["reports_due"] or 0) < min_due:
            continue
        if _alert_recently_fired(coach_id, cooldown_days):
            continue
        team_names = sorted(set(affected.get(coach_id, [])))
        did = _open_low_reliability_decision(
            cycle_id, coach_id, row["name"], team_names, rel,
            row["reports_due"], row["reports_submitted"])
        alerts.append({"coach": coach_id, "reliability": rel,
                       "decision": did})

    log_action(cycle_id, "execute", "coach_reliability_run",
               None, None, True,
               f"fixtures_scanned={len(fixtures)} "
               f"newly_registered={registered} alerts={len(alerts)}")
    return {"ok": True, "fixtures_scanned": len(fixtures),
            "newly_registered": registered,
            "reliabilities": reliabilities, "alerts": alerts}

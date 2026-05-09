"""Memory — three layers: working, short-term, long-term.

  - working.json:    snapshot of the just-finished cycle, used by the
                     dashboard and read at the top of the next cycle.
  - short_term.jsonl: append-only audit of last N cycles' summaries.
  - long_term/*.json: parents, sponsors, grounds, weather decisions,
                     coaches, incidents history.

REFLECT writes here at the end of every cycle.
The brain reads parent_history during _gather_context.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from edge.state.db import connect

ROOT = Path(__file__).resolve().parent
WORKING = ROOT / "working.json"
SHORT = ROOT / "short_term.jsonl"
LONG = ROOT / "long_term"
LONG.mkdir(exist_ok=True)


def _read_long(name: str) -> dict:
    p = LONG / f"{name}.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError:
        return {}


def _write_long(name: str, data: dict) -> None:
    (LONG / f"{name}.json").write_text(json.dumps(data, indent=2,
                                                   sort_keys=True))


def update_long_term() -> dict:
    """Refresh long-term memory files from current state."""
    conn = connect()

    # Parents — channel pref + complaint count from input + decisions
    parents = {}
    rows = conn.execute(
        "SELECT id, name, email, sms, preferred_channel "
        "FROM people WHERE role='parent'").fetchall()
    for r in rows:
        complaints = conn.execute(
            "SELECT COUNT(*) AS n FROM inputs "
            "WHERE LOWER(sender)=LOWER(?) AND classification='complaint'",
            (r["email"] or "",)).fetchone()["n"]
        last = conn.execute(
            "SELECT MAX(received_at) AS last FROM inputs "
            "WHERE LOWER(sender)=LOWER(?) AND classification='complaint'",
            (r["email"] or "",)).fetchone()["last"]
        parents[r["id"]] = {
            "name": r["name"],
            "channel_pref": r["preferred_channel"],
            "complaint_count": complaints,
            "last_complaint_at": last,
        }
        # mirror into parent_memory table for brain context
        conn.execute(
            "INSERT INTO parent_memory(parent_id, complaint_count, "
            "last_complaint_at, sentiment_trend, channel_pref) "
            "VALUES(?,?,?,?,?) "
            "ON CONFLICT(parent_id) DO UPDATE SET "
            "complaint_count=excluded.complaint_count, "
            "last_complaint_at=excluded.last_complaint_at, "
            "channel_pref=excluded.channel_pref",
            (r["id"], complaints, last,
             "negative" if complaints >= 2 else "neutral",
             r["preferred_channel"]))
    _write_long("parents", parents)

    # Sponsors
    sponsors = {}
    for r in conn.execute(
        "SELECT id, name, tier, contract_value, contract_end, "
        "posts_owed, last_post_at FROM sponsors").fetchall():
        sponsors[r["id"]] = dict(r)
    _write_long("sponsors", sponsors)

    # Grounds
    grounds = {}
    for r in conn.execute(
        "SELECT id, name, postcode, council, surface, quirks "
        "FROM grounds").fetchall():
        grounds[r["id"]] = dict(r)
    _write_long("grounds", grounds)

    # Coaches — base record (WWCC + role) plus the live reliability
    # numbers from coach_memory (reports_due / submitted / reliability /
    # no_shows). The architecture promised report_submission_rate in this
    # archive, and dropping it meant the coach reliability score lived
    # only in state.db — disappearing when the DB was rebuilt at season
    # changeover. The JSON is the cross-season record a future secretary
    # can read without SQL access.
    coaches = {}
    for r in conn.execute(
        "SELECT p.id, p.name, p.role, p.wwcc_expiry, "
        "       cm.reports_due, cm.reports_submitted, cm.no_shows, "
        "       cm.reliability "
        "FROM people p "
        "LEFT JOIN coach_memory cm ON cm.coach_id = p.id "
        "WHERE p.role IN ('coach','manager')").fetchall():
        d = dict(r)
        # report_submission_rate is the canonical name used by the
        # architecture; mirror it from reliability so external readers
        # don't have to know our internal column name. But ONLY when
        # the coach has history — coach_memory defaults reliability to
        # 1.0 even with zero reports_due, which would falsely advertise
        # "perfect" for every brand-new coach in the archive. Without
        # this guard a future secretary reads the file and sees 14
        # coaches at 100% when 12 of them have never been measured.
        has_history = (d.get("reports_due") or 0) > 0 or \
            (d.get("reports_submitted") or 0) > 0 or \
            (d.get("no_shows") or 0) > 0
        d["report_submission_rate"] = (
            d.get("reliability") if has_history else None)
        coaches[r["id"]] = d
    _write_long("coaches", coaches)

    # Weather history — append today's snapshot if not empty
    today = datetime.now().date().isoformat()
    weather = _read_long("weather_history")
    weather.setdefault(today, [])
    rows = conn.execute(
        "SELECT ground_id, forecast_max_c, lightning_km, observed_at "
        "FROM weather_state WHERE substr(observed_at,1,10)=?",
        (today,)).fetchall()
    weather[today] = [dict(r) for r in rows]
    _write_long("weather_history", weather)

    # Incidents history — every incident the agent has seen this season,
    # with timeliness so the brain can lean on prior handling. The brain
    # already pulls the last 3 from state.db at think-time; this file is
    # the persistable archive Diane (and a future secretary) can browse.
    incidents = {}
    for r in conn.execute(
        "SELECT id, kind, severity, summary, status, detected_at, "
        "league_report_due, league_report_at, player_id, fixture_id, notes "
        "FROM incidents ORDER BY detected_at").fetchall():
        d = dict(r)
        # Hours-from-detection-to-report — the league cares about <24h;
        # this is the easy KPI to spot in a crisis review.
        if d["league_report_at"] and d["detected_at"]:
            try:
                lr = datetime.fromisoformat(d["league_report_at"])
                det = datetime.fromisoformat(d["detected_at"])
                d["hours_to_report"] = round(
                    (lr - det).total_seconds() / 3600, 1)
            except ValueError:
                pass
        incidents[d["id"]] = d
    _write_long("incidents_history", incidents)

    conn.close()
    return {"parents": len(parents), "sponsors": len(sponsors),
            "grounds": len(grounds), "coaches": len(coaches),
            "incidents": len(incidents)}


def write_working(snapshot: dict) -> None:
    WORKING.write_text(json.dumps(snapshot, indent=2, default=str))


def append_short(summary: dict) -> None:
    with SHORT.open("a") as f:
        f.write(json.dumps(summary, default=str) + "\n")


def read_working() -> dict:
    if WORKING.exists():
        try:
            return json.loads(WORKING.read_text())
        except json.JSONDecodeError:
            return {}
    return {}

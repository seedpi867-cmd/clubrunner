"""Saturday match-day briefing.

A short personal email to Diane at 06:00 on match days. The committee
brief is for Sunday-evening situational awareness across the whole
committee; this one is for Diane's pre-match scan from her phone over
coffee. Different audience, different tone, different cadence.

Sections — kept tight so it reads at a glance:
  - Today's fixtures by kickoff (status flags inline: cancelled/paused)
  - Live weather per active ground (heat + lightning state right now)
  - Duty roster confirmations: who hasn't acknowledged for today
  - Coach match-report status from the prior round
  - Anything sitting on Diane's queue that touches today's fixtures

Reach is 1 (Diane). Auto-sends through the gate stack. Dedup keyed on
the date so re-runs inside the same Saturday don't double-send. Fires
only on config'd match_days; the schedule collector enforces that.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from edge.state.db import connect, log_action
from tools import broadcast
import config as cfg_mod


def _today_window() -> tuple[str, str]:
    now = datetime.now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start.isoformat(), end.isoformat()


def _todays_fixtures() -> list[dict]:
    start, end = _today_window()
    conn = connect()
    rows = conn.execute(
        "SELECT f.id, f.round, f.team_id, f.opponent, f.kickoff, "
        "f.home_or_away, f.status, f.status_reason, f.ground_id, "
        "t.name AS team_name, t.age_grade, g.name AS ground_name "
        "FROM fixtures f "
        "LEFT JOIN teams t ON f.team_id=t.id "
        "LEFT JOIN grounds g ON f.ground_id=g.id "
        "WHERE f.kickoff >= ? AND f.kickoff < ? "
        "ORDER BY f.kickoff", (start, end)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _active_grounds_today() -> list[str]:
    """Ground ids hosting at least one not-cancelled fixture today."""
    fxs = _todays_fixtures()
    return sorted({
        f["ground_id"] for f in fxs
        if f["status"] != "cancelled" and f["ground_id"]})


def _weather_per_ground(ground_ids: list[str]) -> dict[str, dict]:
    """Latest BOM observation per ground from weather_state, plus the
    inferred policy state (heat_cancel / heat_warn / lightning_pause /
    clear)."""
    if not ground_ids:
        return {}
    cfg = cfg_mod.get()
    heat_cancel = float(cfg.get("heat_policy", {})
                          .get("cancel_threshold_c", 36.0))
    heat_warn = float(cfg.get("heat_policy", {})
                        .get("warning_threshold_c", 33.0))
    light_km = float(cfg.get("lightning_policy", {})
                       .get("pause_distance_km", 10.0))
    qmarks = ",".join("?" * len(ground_ids))
    conn = connect()
    rows = conn.execute(
        f"SELECT w.ground_id, w.forecast_max_c, w.lightning_km, "
        f"w.observed_at, g.name AS ground_name "
        f"FROM weather_state w "
        f"LEFT JOIN grounds g ON g.id=w.ground_id "
        f"WHERE w.ground_id IN ({qmarks}) "
        f"ORDER BY w.observed_at DESC", ground_ids).fetchall()
    conn.close()
    seen: dict[str, dict] = {}
    for r in rows:
        gid = r["ground_id"]
        if gid in seen:
            continue
        d = dict(r)
        flags = []
        if d["forecast_max_c"] is not None:
            if d["forecast_max_c"] >= heat_cancel:
                flags.append(f"heat-cancel ({d['forecast_max_c']:.0f}°C)")
            elif d["forecast_max_c"] >= heat_warn:
                flags.append(f"heat-warn ({d['forecast_max_c']:.0f}°C)")
        if d["lightning_km"] is not None:
            if d["lightning_km"] <= light_km:
                flags.append(
                    f"LIGHTNING {d['lightning_km']:.1f}km — pause active")
        d["flags"] = flags or ["clear"]
        seen[gid] = d
    return seen


def _duty_unconfirmed_today() -> list[dict]:
    """Duty roster slots with confirmed=0 for today's fixtures. Empty
    list means every assignee has acknowledged."""
    fxs = _todays_fixtures()
    if not fxs:
        return []
    fixture_ids = [f["id"] for f in fxs]
    qmarks = ",".join("?" * len(fixture_ids))
    conn = connect()
    rows = conn.execute(
        f"SELECT d.id, d.fixture_id, d.role, d.confirmed, "
        f"d.last_reminded_at, p.name AS person_name, "
        f"t.name AS team_name "
        f"FROM duties d "
        f"LEFT JOIN people p ON d.person_id=p.id "
        f"LEFT JOIN fixtures f ON d.fixture_id=f.id "
        f"LEFT JOIN teams t ON f.team_id=t.id "
        f"WHERE d.fixture_id IN ({qmarks}) "
        f"AND COALESCE(d.confirmed,0)=0 "
        f"ORDER BY d.fixture_id, d.role", fixture_ids).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _last_round_match_reports() -> dict:
    """Quick summary of how many coaches have submitted for the most
    recent completed round — what Diane wants to know before the
    newsletter window opens. Joins fixtures→match_reports because the
    submission is in a separate table, not a fixture column."""
    conn = connect()
    last_round = conn.execute(
        "SELECT MAX(round) AS r FROM fixtures "
        "WHERE status='completed'").fetchone()
    if not last_round or last_round["r"] is None:
        conn.close()
        return {"round": None, "submitted": 0, "total": 0,
                "missing_teams": []}
    rnd = last_round["r"]
    rows = conn.execute(
        "SELECT f.id, f.team_id, t.name AS team_name, "
        "(SELECT COUNT(*) FROM match_reports mr WHERE mr.fixture_id=f.id) "
        "  AS report_count "
        "FROM fixtures f LEFT JOIN teams t ON f.team_id=t.id "
        "WHERE f.round=? AND f.status='completed'", (rnd,)).fetchall()
    conn.close()
    total = len(rows)
    submitted = sum(1 for r in rows if (r["report_count"] or 0) > 0)
    missing = [r["team_name"] for r in rows
               if (r["report_count"] or 0) == 0]
    return {"round": rnd, "submitted": submitted, "total": total,
            "missing_teams": missing}


def _todays_pending_decisions() -> list[dict]:
    """Anything still pending on Diane's queue that is incident-,
    safeguarding-, or council-related — these are the items she might
    need to action before leaving for the grounds."""
    conn = connect()
    rows = conn.execute(
        "SELECT id, kind, summary, created_at FROM decisions "
        "WHERE status='pending' "
        "AND kind IN ('incident_brief','safeguarding_brief',"
        "'council_brief','approval_required') "
        "ORDER BY created_at DESC LIMIT 8").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _open_incidents() -> list[dict]:
    conn = connect()
    rows = conn.execute(
        "SELECT id, kind, severity, summary, league_report_due "
        "FROM incidents WHERE status IN ('open','reported') "
        "ORDER BY CASE severity WHEN 'critical' THEN 0 "
        "WHEN 'serious' THEN 1 WHEN 'moderate' THEN 2 ELSE 3 END "
        "LIMIT 5").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def assemble_body() -> tuple[str, str]:
    today = datetime.now().date()
    fixtures = _todays_fixtures()
    grounds = _active_grounds_today()
    weather = _weather_per_ground(grounds)
    unconf = _duty_unconfirmed_today()
    reports = _last_round_match_reports()
    pending = _todays_pending_decisions()
    incidents = _open_incidents()

    cancelled = [f for f in fixtures if f["status"] == "cancelled"]
    paused = [f for f in fixtures if f["status"] == "paused"]
    live = [f for f in fixtures
            if f["status"] not in ("cancelled", "paused")]

    subject = (f"Match-day briefing — {today.isoformat()} "
               f"({len(live)} fixtures live, {len(cancelled)} cancelled)")

    parts = [
        f"Good morning. Briefing for Saturday {today.isoformat()}.",
        "",
        f"## Today: {len(fixtures)} fixtures across "
        f"{len(grounds)} active grounds",
    ]
    if not fixtures:
        parts.append("- No fixtures scheduled today.")
    else:
        for f in fixtures:
            ko = f["kickoff"][11:16] if f["kickoff"] else "?"
            grd = f["ground_name"] or f["ground_id"] or "?"
            tag = ""
            if f["status"] == "cancelled":
                tag = (f"  [CANCELLED — "
                       f"{f.get('status_reason') or 'reason on file'}]")
            elif f["status"] == "paused":
                tag = (f"  [PAUSED — "
                       f"{f.get('status_reason') or 'reason on file'}]")
            parts.append(
                f"- {ko}  {f['team_name'] or f['team_id']} "
                f"vs {f['opponent']} ({f['home_or_away']}) — {grd}{tag}")

    parts += ["", "## Live weather per ground"]
    if not weather:
        parts.append("- No weather observations recorded yet today. "
                     "BOM collectors will catch up by the next cycle.")
    else:
        for gid, w in weather.items():
            obs = (w["observed_at"] or "")[:16].replace("T", " ")
            parts.append(
                f"- {w['ground_name'] or gid}: {', '.join(w['flags'])} "
                f"(obs {obs})")

    parts += ["", "## Duty roster — unconfirmed for today"]
    if not unconf:
        parts.append("- All duty slots confirmed. No chase needed.")
    else:
        for d in unconf[:10]:
            reminded = ""
            if d["last_reminded_at"]:
                reminded = (
                    f" (last reminded "
                    f"{d['last_reminded_at'][:16].replace('T', ' ')})")
            who = d["person_name"] or "(unassigned)"
            parts.append(
                f"- {d['team_name'] or d['fixture_id']} — "
                f"{d['role']}: {who} [pending]{reminded}")
        if len(unconf) > 10:
            parts.append(f"- (+{len(unconf) - 10} more on the dashboard)")

    parts += ["", "## Last round's match reports"]
    if reports["round"] is None:
        parts.append("- No completed fixtures on file yet.")
    else:
        line = (f"- Round {reports['round']}: "
                f"{reports['submitted']}/{reports['total']} "
                f"reports in.")
        if reports["missing_teams"]:
            line += " Outstanding: " + ", ".join(reports["missing_teams"])
        parts.append(line)

    parts += ["", "## On your queue right now"]
    if not pending and not incidents:
        parts.append("- Clean. Nothing escalated overnight.")
    else:
        for d in pending:
            parts.append(f"- [{d['kind']}] {(d['summary'] or '')[:90]}")
        for i in incidents:
            tag = ""
            if i["league_report_due"]:
                try:
                    due = datetime.fromisoformat(i["league_report_due"])
                    delta_h = (due - datetime.now()).total_seconds() / 3600
                    if delta_h < 0:
                        tag = (f" — LEAGUE REPORT OVERDUE by "
                               f"{abs(delta_h):.0f}h")
                    elif delta_h < 24:
                        tag = f" — {delta_h:.0f}h to league deadline"
                except ValueError:
                    pass
            parts.append(
                f"- [incident:{i['severity']}] "
                f"{(i['summary'] or '')[:90]}{tag}")

    parts += ["",
              "Have a good one. Lightning watcher is live; you'll get a "
              "notification the moment a strike crosses 10km of any "
              "active ground.",
              "",
              "— clubrunner"]

    return subject, "\n".join(parts)


def run(cycle_id: int,
        related_input: str | None = None) -> dict[str, Any]:
    cfg = cfg_mod.get()
    sec_email = cfg.get("club", {}).get("secretary_email")
    if not sec_email:
        log_action(cycle_id, "execute", "match_day_brief_skipped",
                   None, None, True, "no secretary email in config")
        return {"reach": 0, "result": {"status": "skipped"}}

    subject, body = assemble_body()
    today_iso = datetime.now().date().isoformat()

    result = broadcast.submit(
        cycle_id=cycle_id, playbook="match_day_brief", channel="email",
        segment=f"secretary:{sec_email}", reach=1,
        subject=subject, body=body,
        target_kind="match_day_brief", target_id=today_iso,
        related_input=related_input)

    log_action(cycle_id, "execute", "match_day_brief_run",
               "match_day_brief", today_iso, True,
               f"reach=1 status={result['status']}")
    return {"reach": 1, "result": result, "subject": subject}

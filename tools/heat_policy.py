"""Heat policy playbook.

Triggered by weather_observation inputs from BOM. Reads the latest
forecast per ground, finds fixtures within the heat-policy horizon at
applicable age grades, and:

  cancel-level (>= cancel_threshold_c)
    - marks each affected fixture status='cancelled' immediately
      (binding policy decision; lightning policy must not later
      flip it back to 'paused')
    - drafts ONE clubwide email + ONE clubwide SMS bundling all the
      cancelled matches into a single message
    - both broadcasts force_approval=True so they wait on Diane's tap
      regardless of reach. Per architecture: forecasts are noisy and
      the parent-mass-comm cost of a wrong cancel is high, so even
      a small club's heat cancel goes through approval, not the
      >50-reach threshold

  warn-level (>= warning_threshold_c, < cancel_threshold_c)
    - per-team email + SMS heat-stress advisory
    - autosend (small reach, low stakes — "bring water + hat")

The split between cancel (bundled + held) and warn (per-team +
autosend) matters: cancellations are a club-wide event Diane needs to
own; warnings are routine and per-coach.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from edge.state.db import connect, log_action, now_iso
import config as cfg_mod
from tools import broadcast


def _mark_fixture_cancelled(fixture_id: str, forecast: float,
                            threshold: float) -> None:
    """Heat cancellations are policy, not communication: once the
    forecast crosses threshold the fixture IS cancelled, regardless of
    whether the broadcast has been approved yet. Without this, the
    lightning policy that runs immediately after will see status=
    'scheduled' and re-stamp the fixture as 'paused' — which then
    shows up on Diane's dashboard as a pause notice for a match that
    is actually heat-cancelled.

    Idempotent: only updates 'scheduled' rows so a re-run won't
    overwrite a 'paused' decision the league has overridden, etc."""
    conn = connect()
    conn.execute(
        "UPDATE fixtures SET status='cancelled', "
        "status_reason=?, last_status_change=? "
        "WHERE id=? AND status='scheduled'",
        (f"heat policy: forecast {forecast:.0f}°C >= {threshold:.0f}°C",
         now_iso(), fixture_id))
    conn.close()


def _affected_fixtures(grades: list[str], horizon_h: int):
    cutoff = (datetime.now() + timedelta(hours=horizon_h)
              ).isoformat(timespec="seconds")
    qs = ",".join("?" * len(grades))
    conn = connect()
    rows = conn.execute(
        f"SELECT f.id, f.team_id, f.opponent, f.kickoff, f.ground_id, "
        f"t.name AS team_name, t.age_grade "
        f"FROM fixtures f JOIN teams t ON f.team_id=t.id "
        f"WHERE f.status='scheduled' AND f.kickoff <= ? "
        f"AND t.age_grade IN ({qs})",
        (cutoff, *grades)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _ground_forecast(ground_id: str) -> float | None:
    conn = connect()
    row = conn.execute(
        "SELECT forecast_max_c FROM weather_state WHERE ground_id=? "
        "ORDER BY observed_at DESC LIMIT 1", (ground_id,)).fetchone()
    conn.close()
    return float(row["forecast_max_c"]) if (
        row and row["forecast_max_c"] is not None) else None


def _team_reach(team_id: str) -> int:
    """Active parents + coach + manager."""
    conn = connect()
    rows = conn.execute(
        "SELECT DISTINCT pe.id "
        "FROM players p JOIN people pe ON p.parent_id=pe.id "
        "WHERE p.team_id=? AND p.rego_status IN ('paid','active','insured')",
        (team_id,)).fetchall()
    parent_ids = [r["id"] for r in rows]
    team = conn.execute(
        "SELECT coach_id, manager_id FROM teams WHERE id=?",
        (team_id,)).fetchone()
    extras = [team["coach_id"], team["manager_id"]] if team else []
    conn.close()
    return len(set(parent_ids + [e for e in extras if e]))


def _bundle_cancel_broadcasts(cycle_id: int, cancelled: list[dict],
                              cancel_t: float,
                              related_input: str | None) -> list[dict]:
    """One clubwide email + one clubwide SMS listing every cancelled
    fixture. Reach = sum of distinct contacts across all affected
    teams; force_approval=True so Diane's tap is required."""
    if not cancelled:
        return []

    # Deduplicate contacts across teams — a coach who runs both U8 and
    # U9 (or a parent with kids on multiple Auskick groups) should be
    # counted once. Without dedup, reach inflates and the decision
    # context misleads Diane on actual broadcast cost.
    conn = connect()
    contacts: set[str] = set()
    team_ids = {fx["team_id"] for fx in cancelled}
    if team_ids:
        qs = ",".join("?" * len(team_ids))
        rows = conn.execute(
            f"SELECT DISTINCT pe.id FROM players p "
            f"JOIN people pe ON p.parent_id=pe.id "
            f"WHERE p.team_id IN ({qs}) "
            f"AND p.rego_status IN ('paid','active','insured')",
            list(team_ids)).fetchall()
        contacts.update(r["id"] for r in rows)
        team_extras = conn.execute(
            f"SELECT coach_id, manager_id FROM teams WHERE id IN ({qs})",
            list(team_ids)).fetchall()
        for t in team_extras:
            if t["coach_id"]:
                contacts.add(t["coach_id"])
            if t["manager_id"]:
                contacts.add(t["manager_id"])
    conn.close()
    reach = len(contacts)
    if reach == 0:
        return []

    lines = []
    for fx in sorted(cancelled, key=lambda x: x["kickoff"]):
        kickoff = fx["kickoff"][:16].replace("T", " ")
        lines.append(
            f"  • {fx['team_name']} vs {fx['opponent']} — "
            f"{kickoff} (forecast {fx['forecast']:.0f}°C)")
    listing = "\n".join(lines)

    today = cancelled[0]["kickoff"][:10]
    subject = (f"HEAT CANCELLATION — Auskick / U8 / U9 matches "
               f"{today} ({len(cancelled)} match"
               f"{'es' if len(cancelled) != 1 else ''})")
    body = (
        f"Heat policy cancellation.\n\n"
        f"The following matches are CANCELLED today due to forecast "
        f"≥{cancel_t:.0f}°C at kickoff:\n\n"
        f"{listing}\n\n"
        f"League will be notified. No make-up date yet — we will advise.\n"
        f"Older grades (U10+) play as scheduled; check the website for "
        f"any ground-specific updates.\n\n"
        f"Stay cool,\nDiane (Secretary, HGJFC)\n")

    sms_summary = ", ".join(
        f"{fx['team_name']} {fx['kickoff'][11:16]}"
        for fx in sorted(cancelled, key=lambda x: x["kickoff"]))
    sms_body = (
        f"HGJFC: heat policy — {len(cancelled)} U8/U9/Auskick "
        f"match{'es' if len(cancelled) != 1 else ''} CANCELLED today "
        f"({sms_summary}). Stay cool. — Diane")

    target_id = f"heat_cancel:{today}"
    results = []
    results.append({
        "channel": "email",
        "submit": broadcast.submit(
            cycle_id=cycle_id, playbook="heat_policy", channel="email",
            segment="club:auskick_u8_u9", reach=reach,
            subject=subject, body=body,
            target_kind="club_announcement", target_id=target_id,
            related_input=related_input,
            force_approval=True),
        "reach": reach,
    })
    results.append({
        "channel": "sms",
        "submit": broadcast.submit(
            cycle_id=cycle_id, playbook="heat_policy", channel="sms",
            segment="club:auskick_u8_u9", reach=reach,
            subject="", body=sms_body,
            target_kind="club_announcement", target_id=target_id,
            related_input=related_input,
            force_approval=True),
        "reach": reach,
    })
    return results


def _warn_broadcasts(cycle_id: int, warnings: list[dict],
                     related_input: str | None) -> list[dict]:
    """Per-team heat-stress advisory. Small reach, autosends through
    the gate stack."""
    drafts = []
    for fx in warnings:
        reach = _team_reach(fx["team_id"])
        if reach == 0:
            continue
        kickoff = fx["kickoff"][:16].replace("T", " ")
        team_name = fx["team_name"]
        forecast = fx["forecast"]
        subject = f"Heat advisory — {team_name} match {kickoff}"
        body = (
            f"Heat advisory: forecast {forecast:.0f}°C at kickoff. "
            f"Match is going ahead.\n"
            f"Please bring extra water, sunscreen, and a hat. "
            f"Coach will use longer breaks.\n\n"
            f"Diane (Secretary, HGJFC)\n")
        sms_body = (
            f"HGJFC: {team_name} match {kickoff} "
            f"heat advisory ({forecast:.0f}°C). Bring water + hat.")
        seg = f"team:{fx['team_id']}"
        result_email = broadcast.submit(
            cycle_id=cycle_id, playbook="heat_policy", channel="email",
            segment=seg, reach=reach, subject=subject, body=body,
            target_kind="fixture", target_id=fx["id"],
            related_input=related_input)
        result_sms = broadcast.submit(
            cycle_id=cycle_id, playbook="heat_policy", channel="sms",
            segment=seg, reach=reach, subject="", body=sms_body,
            target_kind="fixture", target_id=fx["id"],
            related_input=related_input)
        drafts.append({"fixture": fx["id"], "level": "warn",
                       "reach": reach, "forecast": forecast,
                       "email": result_email, "sms": result_sms})
    return drafts


def run(cycle_id: int, related_input: str | None = None) -> dict[str, Any]:
    cfg = cfg_mod.get().get("heat_policy", {})
    grades = [g.lower() for g in cfg.get("applies_to_age_grades", [])]
    cancel_t = float(cfg.get("cancel_threshold_c", 36.0))
    warn_t = float(cfg.get("warning_threshold_c", 33.0))
    horizon = int(cfg.get("forecast_horizon_hours", 24))

    affected = _affected_fixtures(grades, horizon)
    cancelled: list[dict] = []
    warnings: list[dict] = []

    for fx in affected:
        forecast = _ground_forecast(fx["ground_id"])
        if forecast is None:
            continue
        fx_with = dict(fx)
        fx_with["forecast"] = forecast
        if forecast >= cancel_t:
            # Mark cancelled BEFORE drafting the broadcast so lightning
            # policy and downstream playbooks see the right status even
            # if Diane hasn't tapped approve yet (or never does).
            _mark_fixture_cancelled(fx["id"], forecast, cancel_t)
            cancelled.append(fx_with)
        elif forecast >= warn_t:
            warnings.append(fx_with)

    cancel_drafts = _bundle_cancel_broadcasts(
        cycle_id, cancelled, cancel_t, related_input)
    warn_drafts = _warn_broadcasts(cycle_id, warnings, related_input)

    log_action(cycle_id, "execute", "heat_policy_run",
               None, None, True,
               f"affected={len(affected)} cancelled={len(cancelled)} "
               f"warnings={len(warnings)}")
    return {"affected": len(affected),
            "cancelled": len(cancelled),
            "warnings": len(warnings),
            "cancel_drafts": cancel_drafts,
            "warn_drafts": warn_drafts}

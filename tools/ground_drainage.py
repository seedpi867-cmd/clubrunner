"""Ground-drainage policy.

Architecture promised: ground quirks like "Henley Oval eastern oval —
closes after >20mm overnight rain" should fire a closure decision when
the threshold is crossed. Without this playbook the quirks were just a
text string; the agent knew the rule but couldn't act on it.

Trigger: weather_state.rain_24h_mm >= grounds.drainage_threshold_mm for
any ground that has fixtures scheduled in the next horizon. Action:

  - Mark every affected fixture status='cancelled' immediately so
    lightning policy and downstream playbooks see the right state even
    if Diane hasn't tapped approve. (World-model writes outside
    branches, per the recurring lesson.)
  - Bundle one clubwide email + one clubwide SMS listing the cancelled
    fixtures, force_approval=True so a small-club seed still routes
    through Diane — rain-call-from-rule is noisy by definition (a
    forecast model + a 24h rolling sum + a council call about whether
    they actually pulled the cones up). The cancel cost of getting it
    wrong is high; the comm cost of waiting 30s for her tap is low.

Idempotent within a day: policy_runs key = drainage:{ground}:{date}.
A re-fire on the same ground for the same morning is a no-op; if rain
worsens overnight and we cross the threshold a second time later, the
date key changes so we'd post again — which is what we want.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from edge.state.db import connect, log_action, now_iso
import config as cfg_mod
from tools import broadcast


def _grounds_with_threshold() -> list[dict]:
    conn = connect()
    rows = conn.execute(
        "SELECT id, name, drainage_threshold_mm FROM grounds "
        "WHERE drainage_threshold_mm IS NOT NULL").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _latest_rain(ground_id: str) -> tuple[float | None, str | None]:
    conn = connect()
    row = conn.execute(
        "SELECT rain_24h_mm, observed_at FROM weather_state "
        "WHERE ground_id=? AND rain_24h_mm IS NOT NULL "
        "ORDER BY observed_at DESC LIMIT 1", (ground_id,)).fetchone()
    conn.close()
    if not row:
        return None, None
    return (row["rain_24h_mm"], row["observed_at"])


def _affected_fixtures(ground_id: str, horizon_h: int) -> list[dict]:
    """Fixtures on this ground in the next horizon that haven't already
    been cancelled or paused. Heat-cancelled or already-drained fixtures
    are skipped — once is enough."""
    cutoff = (datetime.now() + timedelta(hours=horizon_h)
              ).isoformat(timespec="seconds")
    conn = connect()
    rows = conn.execute(
        "SELECT f.id, f.team_id, f.opponent, f.kickoff, "
        "       t.name AS team_name, t.age_grade "
        "FROM fixtures f JOIN teams t ON f.team_id=t.id "
        "WHERE f.ground_id=? AND f.kickoff <= ? AND f.kickoff >= ? "
        "AND f.status='scheduled'",
        (ground_id, cutoff,
         datetime.now().isoformat(timespec="seconds"))).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _mark_cancelled(fixture_ids: list[str], threshold_mm: float,
                    rain_mm: float, ground_id: str) -> None:
    if not fixture_ids:
        return
    conn = connect()
    qs = ",".join("?" * len(fixture_ids))
    reason = (f"drainage policy: {rain_mm:.1f}mm rain >= "
              f"{threshold_mm:.0f}mm at {ground_id}")
    conn.execute(
        f"UPDATE fixtures SET status='cancelled', status_reason=?, "
        f"last_status_change=? WHERE id IN ({qs}) AND status='scheduled'",
        (reason, now_iso(), *fixture_ids))
    conn.close()


def _ground_reach(ground_id: str, fixture_ids: list[str]) -> int:
    if not fixture_ids:
        return 0
    conn = connect()
    qs = ",".join("?" * len(fixture_ids))
    parents = conn.execute(
        f"SELECT DISTINCT pe.id FROM players p "
        f"JOIN people pe ON p.parent_id=pe.id "
        f"JOIN fixtures f ON f.team_id=p.team_id "
        f"WHERE f.id IN ({qs}) "
        f"AND p.rego_status IN ('paid','active','insured')",
        fixture_ids).fetchall()
    contacts = {r["id"] for r in parents}
    extras = conn.execute(
        f"SELECT DISTINCT t.coach_id, t.manager_id FROM teams t "
        f"JOIN fixtures f ON f.team_id=t.id WHERE f.id IN ({qs})",
        fixture_ids).fetchall()
    for r in extras:
        if r["coach_id"]:
            contacts.add(r["coach_id"])
        if r["manager_id"]:
            contacts.add(r["manager_id"])
    conn.close()
    return len(contacts)


def _already_fired_today(ground_id: str, today: str) -> bool:
    """Idempotency guard — one drainage broadcast per ground per day.
    Without this, every cycle on a wet morning re-fires the broadcast
    and Diane sees the same approval request stacking up."""
    key = f"drainage:{ground_id}:{today}"
    conn = connect()
    row = conn.execute(
        "SELECT 1 FROM policy_runs WHERE id=?", (key,)).fetchone()
    conn.close()
    return row is not None


def _record_run(ground_id: str, today: str, cycle_id: int, outcome: str,
                detail: str) -> None:
    key = f"drainage:{ground_id}:{today}"
    conn = connect()
    conn.execute(
        "INSERT OR IGNORE INTO policy_runs(id, playbook, target_kind, "
        "target_id, cycle_id, fired_at, outcome, detail) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (key, "ground_drainage", "ground", ground_id, cycle_id,
         now_iso(), outcome, detail))
    conn.close()


def _bundle_close_broadcasts(cycle_id: int, ground: dict,
                             cancelled: list[dict], rain_mm: float,
                             threshold: float, related_input: str | None,
                             reach: int) -> list[dict]:
    if not cancelled or reach == 0:
        return []

    today = cancelled[0]["kickoff"][:10]
    lines = [f"  • {fx['team_name']} vs {fx['opponent']} — "
             f"{fx['kickoff'][11:16]}"
             for fx in sorted(cancelled, key=lambda x: x["kickoff"])]
    listing = "\n".join(lines)

    subject = (f"GROUND CLOSED — {ground['name']} ({today}) — "
               f"{len(cancelled)} match"
               f"{'es' if len(cancelled) != 1 else ''} cancelled")
    body = (
        f"Drainage policy — {ground['name']} closed.\n\n"
        f"24h rain {rain_mm:.1f}mm exceeded the {threshold:.0f}mm "
        f"threshold for this ground. The following matches are CANCELLED "
        f"today:\n\n"
        f"{listing}\n\n"
        f"League and council will be notified. No make-up date yet.\n"
        f"Other grounds play as scheduled — check the website.\n\n"
        f"— Diane (Secretary, HGJFC)\n")

    sms_summary = ", ".join(
        f"{fx['team_name']} {fx['kickoff'][11:16]}"
        for fx in sorted(cancelled, key=lambda x: x["kickoff"]))
    sms_body = (
        f"HGJFC: {ground['name']} CLOSED today — {rain_mm:.0f}mm rain, "
        f"ground unsafe. {len(cancelled)} match"
        f"{'es' if len(cancelled) != 1 else ''} cancelled "
        f"({sms_summary}). — Diane")

    target_id = f"drainage_close:{ground['id']}:{today}"
    results = []
    for ch, subj, txt in (("email", subject, body), ("sms", "", sms_body)):
        results.append({
            "channel": ch,
            "submit": broadcast.submit(
                cycle_id=cycle_id, playbook="ground_drainage", channel=ch,
                segment=f"ground:{ground['id']}", reach=reach,
                subject=subj, body=txt,
                target_kind="ground_closure", target_id=target_id,
                related_input=related_input,
                force_approval=True),
            "reach": reach,
        })
    return results


def run(cycle_id: int, related_input: str | None = None) -> dict[str, Any]:
    cfg = cfg_mod.get().get("ground_drainage", {})
    horizon = int(cfg.get("forecast_horizon_hours", 24))
    if not cfg.get("enabled", True):
        return {"skipped": True}

    today = datetime.now().date().isoformat()
    closures: list[dict] = []
    skipped: list[dict] = []

    for g in _grounds_with_threshold():
        rain_mm, _ = _latest_rain(g["id"])
        threshold = float(g["drainage_threshold_mm"])
        if rain_mm is None:
            continue
        if rain_mm < threshold:
            continue
        if _already_fired_today(g["id"], today):
            skipped.append({"ground": g["id"], "reason": "already_fired"})
            continue
        affected = _affected_fixtures(g["id"], horizon)
        if not affected:
            # Threshold crossed but nothing to cancel — record the run so
            # we don't re-evaluate every cycle, but no broadcast.
            _record_run(g["id"], today, cycle_id, "no_fixtures",
                        f"rain={rain_mm:.1f}mm threshold={threshold:.0f}mm")
            skipped.append({"ground": g["id"], "reason": "no_fixtures"})
            continue
        # World-model write FIRST, before any draft branch.
        fx_ids = [a["id"] for a in affected]
        _mark_cancelled(fx_ids, threshold, rain_mm, g["id"])
        reach = _ground_reach(g["id"], fx_ids)
        bcs = _bundle_close_broadcasts(
            cycle_id, g, affected, rain_mm, threshold,
            related_input, reach)
        _record_run(g["id"], today, cycle_id, "drafted",
                    f"rain={rain_mm:.1f}mm threshold={threshold:.0f}mm "
                    f"fixtures={len(fx_ids)} reach={reach}")
        closures.append({"ground": g["id"], "rain_mm": rain_mm,
                         "threshold": threshold, "fixtures": fx_ids,
                         "reach": reach, "broadcasts": bcs})

    log_action(cycle_id, "execute", "ground_drainage_run",
               None, None, True,
               f"closures={len(closures)} skipped={len(skipped)}")
    return {"closures": closures, "skipped": skipped}

"""Lightning policy — the only true zero-approval automation.

If a strike is observed within pause_distance_km of any active ground
during a match window, post the pause notice immediately. After
resume_after_minutes without a strike <pause_distance_km, post resume.

Per architecture: this overrides quiet hours and the broadcast threshold.
The justification is league rule + safety: parents and coaches NEED to
know now, not in five minutes when Diane checks her phone.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from edge.state.db import connect, log_action, now_iso
import config as cfg_mod
from tools import broadcast


def _active_grounds(window_hours: float = 6.0) -> list[dict]:
    """Grounds with a fixture starting in the next window or that started
    in the last window. Lightning only applies during play."""
    now = datetime.now()
    lo = (now - timedelta(hours=window_hours)).isoformat(timespec="seconds")
    hi = (now + timedelta(hours=window_hours)).isoformat(timespec="seconds")
    conn = connect()
    rows = conn.execute(
        "SELECT DISTINCT g.id, g.name FROM grounds g "
        "JOIN fixtures f ON f.ground_id=g.id "
        "WHERE f.kickoff BETWEEN ? AND ? "
        "AND f.status IN ('scheduled', 'paused')", (lo, hi)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _latest_lightning(ground_id: str) -> tuple[float | None, str | None]:
    conn = connect()
    row = conn.execute(
        "SELECT lightning_km, observed_at FROM weather_state "
        "WHERE ground_id=? ORDER BY observed_at DESC LIMIT 1",
        (ground_id,)).fetchone()
    conn.close()
    if not row:
        return None, None
    return (row["lightning_km"], row["observed_at"])


def _ground_segment(ground_id: str) -> tuple[str, int]:
    """Reach = team managers + coaches at fixtures on this ground in
    the active window. Without the kickoff filter the SMS reach inflates
    by every future-round team using this ground — the league rule is
    'people standing on the oval right now', not the season roster."""
    lo, hi = _active_window()
    conn = connect()
    rows = conn.execute(
        "SELECT DISTINCT t.coach_id, t.manager_id FROM fixtures f "
        "JOIN teams t ON f.team_id=t.id "
        "WHERE f.ground_id=? AND f.kickoff BETWEEN ? AND ? "
        "AND f.status IN ('scheduled','paused')",
        (ground_id, lo, hi)).fetchall()
    conn.close()
    contacts = set()
    for r in rows:
        if r["coach_id"]:
            contacts.add(r["coach_id"])
        if r["manager_id"]:
            contacts.add(r["manager_id"])
    return f"ground:{ground_id}", len(contacts)


def _active_window(window_hours: float = 6.0) -> tuple[str, str]:
    """The same +/- window _active_grounds uses to pick grounds. Pulled
    out so pause/resume writes can match it: lightning is a now-event,
    not a season-long status change. With multiple rounds in the
    fixtures table, an unscoped UPDATE pauses next Saturday's matches
    when this Saturday's lightning hits the same ground."""
    now = datetime.now()
    lo = (now - timedelta(hours=window_hours)).isoformat(timespec="seconds")
    hi = (now + timedelta(hours=window_hours)).isoformat(timespec="seconds")
    return lo, hi


def _set_fixtures_status(ground_id: str, status: str) -> int:
    lo, hi = _active_window()
    conn = connect()
    cur = conn.execute(
        "UPDATE fixtures SET status=?, status_reason='lightning policy', "
        "last_status_change=? WHERE ground_id=? "
        "AND kickoff BETWEEN ? AND ? "
        "AND status IN ('scheduled','paused')",
        (status, now_iso(), ground_id, lo, hi))
    n = cur.rowcount
    conn.close()
    return n


def _ground_state(ground_id: str) -> dict[str, int]:
    """Count how many fixtures on this ground are paused vs scheduled
    in the active window — same lens _set_fixtures_status uses. Without
    the kickoff filter a future-round paused fixture could mask the
    'no live play to pause' state and trigger spurious resume actions."""
    lo, hi = _active_window()
    conn = connect()
    rows = conn.execute(
        "SELECT status, COUNT(*) AS n FROM fixtures WHERE ground_id=? "
        "AND kickoff BETWEEN ? AND ? "
        "AND status IN ('scheduled','paused') GROUP BY status",
        (ground_id, lo, hi)).fetchall()
    conn.close()
    return {r["status"]: r["n"] for r in rows}


def _last_close_strike(ground_id: str, pause_km: float) -> str | None:
    conn = connect()
    row = conn.execute(
        "SELECT MAX(observed_at) AS last FROM weather_state "
        "WHERE ground_id=? AND lightning_km IS NOT NULL "
        "AND lightning_km<?", (ground_id, pause_km)).fetchone()
    conn.close()
    return row["last"] if row and row["last"] else None


def _try_resume(cycle_id: int, g: dict, pause_km: float,
                resume_after: int, related_input: str | None,
                now: datetime) -> dict | None:
    """If this ground has lightning-paused fixtures and the resume window
    has elapsed since the last close strike, fire resume and flip
    fixtures back to 'scheduled'. Returns an action dict or None.

    The resume condition is independent of the latest observation:
    a clear obs (lightning_km IS NULL) and a distant obs (lightning_km
    >= pause_km) are equivalent here. Both mean "no current threat";
    what gates resume is the time since the last <pause_km strike."""
    gid = g["id"]
    lo, hi = _active_window()
    conn = connect()
    paused = conn.execute(
        "SELECT id FROM fixtures WHERE ground_id=? AND "
        "kickoff BETWEEN ? AND ? AND "
        "status='paused' AND status_reason='lightning policy' LIMIT 1",
        (gid, lo, hi)).fetchone()
    conn.close()
    if not paused:
        return None
    last_iso = _last_close_strike(gid, pause_km)
    if not last_iso:
        return None
    elapsed = (now - datetime.fromisoformat(last_iso)).total_seconds() / 60
    if elapsed < resume_after:
        return None
    seg, reach = _ground_segment(gid)
    body = (
        f"PLAY RESUMED — {g['name']}\n"
        f"No lightning observed within {pause_km:.0f}km "
        f"for {resume_after} minutes. Matches resume.\n"
        "Coaches: please confirm with umpires.")
    res = broadcast.submit(
        cycle_id=cycle_id, playbook="lightning_resume",
        channel="sms", segment=seg, reach=reach,
        body=body, target_kind="ground", target_id=gid,
        related_input=related_input,
        policy_override_threshold=True,
        override_business_hours=True)
    n = _set_fixtures_status(gid, "scheduled")
    return {"ground": gid, "action": "resume", "fixtures": n,
            "reach": reach, "result": res}


def run(cycle_id: int, related_input: str | None = None) -> dict[str, Any]:
    cfg = cfg_mod.get().get("lightning_policy", {})
    pause_km = float(cfg.get("pause_distance_km", 10.0))
    resume_after = int(cfg.get("resume_after_minutes", 30))

    active = _active_grounds()
    actions: list[dict] = []

    now = datetime.now()
    for g in active:
        gid = g["id"]
        dist, _ = _latest_lightning(gid)

        # Pause check first — a fresh close strike trumps a stale resume
        # window. (If the resume window had elapsed AND a new close strike
        # arrived this cycle, we want to pause, not resume-then-pause.)
        if dist is not None and dist < pause_km:
            state = _ground_state(gid)
            if state.get("scheduled", 0) > 0:
                seg, reach = _ground_segment(gid)
                body = (
                    f"PLAY PAUSED — {g['name']}\n"
                    f"Lightning observed {dist:.1f}km away. League rule: "
                    f"pause all play, take cover, do not return for "
                    f"{resume_after} minutes after the next clear "
                    f"observation.\n"
                    "Coaches: confirm with umpires and shelter players.")
                res = broadcast.submit(
                    cycle_id=cycle_id, playbook="lightning_pause",
                    channel="sms", segment=seg, reach=reach, body=body,
                    target_kind="ground", target_id=gid,
                    related_input=related_input,
                    policy_override_threshold=True,
                    override_business_hours=True)
                n = _set_fixtures_status(gid, "paused")
                actions.append({"ground": gid, "action": "pause",
                                "dist_km": dist, "fixtures": n,
                                "reach": reach, "result": res})
            continue

        # No fresh close strike — see if a prior pause is ready to lift.
        # Runs whether dist is None (clear obs) or dist >= pause_km
        # (distant strike). Both leave the ground safe; only the timer
        # decides resume.
        resumed = _try_resume(cycle_id, g, pause_km, resume_after,
                              related_input, now)
        if resumed:
            actions.append(resumed)

    log_action(cycle_id, "execute", "lightning_policy_run",
               None, None, True,
               f"active_grounds={len(active)} actions={len(actions)}")
    return {"active_grounds": len(active), "actions": actions}

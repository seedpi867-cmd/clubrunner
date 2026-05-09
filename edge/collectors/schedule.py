"""Time-based playbook trigger collector.

Mints synthetic 'tick' inputs that the orchestrator's EXECUTE phase
notices and routes to scheduled playbooks (newsletter assembly,
duty-roster reminders, payment chase passes, WWCC checks).

It does NOT itself fire playbooks — it just records that the wall
clock has crossed a threshold so the playbook gets a chance.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from edge.state.db import connect, upsert_input, log_action
import config as cfg_mod


def _last_fired(playbook: str) -> str | None:
    conn = connect()
    row = conn.execute(
        "SELECT MAX(fired_at) AS last FROM policy_runs WHERE playbook=?",
        (playbook,)).fetchone()
    conn.close()
    return row["last"] if row else None


def run(cycle_id: int) -> dict[str, Any]:
    cfg = cfg_mod.get()
    if not cfg.get("collectors", {}).get("schedule", {}).get(
            "enabled", True):
        return {"skipped": True}

    now = datetime.now()
    today_iso = now.date().isoformat()
    minted = []

    # Sunday newsletter — assemble at 17:00 Sunday (weekday 6),
    # emit a tick once per Sunday.
    nl = cfg.get("newsletter", {})
    if now.weekday() == int(nl.get("send_day_of_week", 6)):
        ready_h, ready_m = nl.get("draft_ready_at", "17:00").split(":")
        ready_dt = now.replace(hour=int(ready_h), minute=int(ready_m),
                               second=0, microsecond=0)
        if now >= ready_dt:
            sid = f"newsletter_{today_iso}"
            _, is_new = upsert_input(
                "schedule", sid,
                sender="schedule.newsletter",
                subject="Sunday newsletter assembly tick",
                body="time to assemble Sunday newsletter")
            if is_new:
                minted.append(sid)

    # Duty-roster reminders — fire 3 days, 1 day, 2 hours before kickoff
    conn = connect()
    upcoming = conn.execute(
        "SELECT id, kickoff, team_id FROM fixtures "
        "WHERE status='scheduled' AND kickoff > ? "
        "ORDER BY kickoff LIMIT 50", (now.isoformat(),)).fetchall()
    conn.close()
    # Window bounds — mirrors the gcal collector pattern.
    # 3d / 1d are strict in-band advance-notice anchors. A fixture
    # noticed at T-12h shouldn't fire a stale 3d reminder; the 2h
    # anchor will catch it.
    # 2h is the LAST chance before kickoff and uses lo=0 so a cycle
    # that misses the 30-min in-band slice (off-hours interval, deploy,
    # restart) OR a late-add fixture (rostered at T-30m for a 4pm
    # match) still mints the panic-time reminder + escalation card.
    # upsert_input UNIQUE(source, source_id) — keyed on (fixture, label)
    # — absorbs consecutive cycles inside the wider window.
    duty_windows = (
        ("3d", timedelta(days=3, hours=1), timedelta(days=2, hours=23)),
        ("1d", timedelta(days=1, hours=1), timedelta(days=0, hours=23)),
        ("2h", timedelta(hours=2, minutes=15), timedelta(seconds=0)),
    )
    for fx in upcoming:
        ko = datetime.fromisoformat(fx["kickoff"])
        delta = ko - now
        for label, hi, lo in duty_windows:
            if lo <= delta <= hi:
                sid = f"duty_reminder_{fx['id']}_{label}"
                _, is_new = upsert_input(
                    "schedule", sid,
                    sender="schedule.duty",
                    subject=f"Duty roster reminder ({label}) — {fx['id']}",
                    body=f"Fire duty-roster reminder for fixture {fx['id']}")
                if is_new:
                    minted.append(sid)

    # Daily anchor-passed ticks. The previous form here was
    # `if now.hour == N and now.minute < 30` — a strict 30-min slice that
    # silently lost the day's tick whenever a cycle missed the window
    # (deploy + restart between 09:00 and 09:30, an off-hours interval
    # extended past the slice, or any config tweak to interval_minutes).
    # The anchor-passed form mirrors newsletter / match_day_brief /
    # committee_brief: fire at the first cycle on/after the anchor each
    # day, idempotent via upsert UNIQUE on `<tick>_<today_iso>`. A
    # cold-start at 11:00 still fires today's pass; a second cycle at
    # 11:30 is absorbed by the unique key.
    daily_ticks = [
        (8, 0, "wwcc_check", "schedule.wwcc",
         "WWCC expiry check", "Daily WWCC expiry sweep", True),
        (9, 0, "payment_chase_pass", "schedule.payments",
         "Payment chase pass",
         "Daily payment chase pass — review overdue regos", True),
        (9, 30, "insurance_check", "schedule.insurance",
         "Insurance check",
         "Daily insurance certificate forwarding + expiry sweep",
         bool(cfg.get("insurance", {}).get("enabled", False))),
    ]
    for hh, mm, prefix, sender, subject, body, enabled in daily_ticks:
        if not enabled:
            continue
        anchor = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if now < anchor:
            continue
        sid = f"{prefix}_{today_iso}"
        _, is_new = upsert_input(
            "schedule", sid, sender=sender,
            subject=subject, body=body)
        if is_new:
            minted.append(sid)

    # Match-day briefing — fires once at first cycle on/after the
    # configured ready time on a configured match day. Idempotent on
    # the date so a second cycle that morning doesn't re-mint. Diane
    # gets one personal email at 06:00 Saturday with everything she
    # needs to scan from her phone before leaving the house. Distinct
    # from committee_brief (Sunday, all 7 committee members).
    mb = cfg.get("match_day_brief", {})
    if mb.get("enabled", True):
        match_days = cfg.get("cycle", {}).get("match_days", [5])
        if now.weekday() in match_days:
            ready_h, ready_m = mb.get("send_at", "06:00").split(":")
            ready_dt = now.replace(hour=int(ready_h), minute=int(ready_m),
                                   second=0, microsecond=0)
            if now >= ready_dt:
                sid = f"match_day_brief_{today_iso}"
                _, is_new = upsert_input(
                    "schedule", sid,
                    sender="schedule.match_day",
                    subject="Match-day briefing tick",
                    body="Saturday match-day briefing assembly")
                if is_new:
                    minted.append(sid)

    # Committee briefing — Sunday 21:00, after the newsletter has gone
    # out. Idempotent on the calendar week (year+ISO-week) so a Monday
    # 00:05 cycle does not re-fire.
    cb = cfg.get("committee_brief", {})
    if cb.get("enabled", True):
        send_dow = int(cb.get("send_day_of_week", 6))
        send_at = cb.get("send_at", "21:00")
        if now.weekday() == send_dow:
            ready_h, ready_m = send_at.split(":")
            ready_dt = now.replace(hour=int(ready_h), minute=int(ready_m),
                                   second=0, microsecond=0)
            if now >= ready_dt:
                iso = now.isocalendar()
                sid = f"committee_brief_{iso[0]}W{iso[1]:02d}"
                _, is_new = upsert_input(
                    "schedule", sid,
                    sender="schedule.committee",
                    subject="Committee briefing tick",
                    body="Weekly committee briefing assembly")
                if is_new:
                    minted.append(sid)

    log_action(cycle_id, "collect", "schedule_ticks",
               None, None, True, f"minted={len(minted)}")
    return {"ingested": len(minted), "ticks": minted}

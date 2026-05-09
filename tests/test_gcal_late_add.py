"""Late-added events still need a reminder.

The 2h reminder window in gcal.py is hard-banded at (1h45m, 2h15m). That
30-minute slice is narrower than the off-hours cycle interval (120 min)
AND narrower than the human window in which Diane realistically remembers
to add the event ("oh — there's a committee call at 6pm tonight, let me
add that to the calendar"). Two failure modes that share one fix:

  (a) Off-hours skip: event at +5h on a non-match-day Sunday morning.
      Cycles run at 1am, 3am, 5am, 7am. The 2h window for a 5am event is
      2:45-3:15am, which the 3am cycle catches — fine.
      But for an event at 7:30am, the window is 5:15-5:45am. 5am cycle
      sits at 2:30 to start (above 2h15m, miss); 7am cycle sits at 30 min
      to start (below 1h45m, miss). The 2h reminder never fires.

  (b) Late add: Diane adds a 4pm meeting at 3pm. delta = 1h. Below the
      1h45m floor — the agent stays silent.

Either path the architecture promise ("audience-appropriate broadcast
when an event is approaching") goes unkept.

Fix: widen the 2h window's lo to 0. The upsert_input UNIQUE(source,
source_id) constraint already prevents double-firing across consecutive
cycles, so dropping the floor is safe — once minted, it's minted.

Existing tests guard against accidentally widening the OUTER edge:
  - test_reminder_windows_fire_only_in_band asserts a +5d event mints
    nothing. 5d > 2h15m so 2h still doesn't fire — that test stays green.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from _helpers import use_temp_db, reset_volatile
use_temp_db()

from edge.state.db import init_db, connect, now_iso
from edge.state.seed import seed
from edge.collectors import gcal
from tools import playbooks


SCEN = ROOT / "scenarios" / "today.json"


def _start_cycle() -> int:
    c = connect()
    cur = c.execute("INSERT INTO cycles(started_at) VALUES(?)",
                    (now_iso(),))
    cid = cur.lastrowid
    c.close()
    return cid


def _passed(name): print(f"  \N{check mark} {name}")


def _scenario_set(events: list[dict]):
    d = json.loads(SCEN.read_text())
    d["gcal_events"] = events
    SCEN.write_text(json.dumps(d, indent=2))


def _save():
    return SCEN.read_text()


def _restore(text: str):
    SCEN.write_text(text)


def test_event_added_within_2h_still_mints_reminder():
    """Late-add scenario. Event +1h. Old code: no mint. Fixed code: mint
    a 2h reminder so the audience gets told."""
    init_db(); seed(); reset_volatile()
    saved = _save()
    try:
        now = datetime.now()
        _scenario_set([{
            "uid": "late-add-1h",
            "summary": "[committee] Emergency call",
            "description": "Sponsor signed; need to ratify before AGM.",
            "location": "Zoom",
            "start": (now + timedelta(hours=1)).isoformat(timespec="seconds"),
        }])
        cid = _start_cycle()
        gcal.run(cid)
        c = connect()
        ids = [r["id"] for r in c.execute(
            "SELECT id FROM inputs WHERE source='gcal'").fetchall()]
        c.close()
        assert any(":2h" in i for i in ids), (
            f"expected a 2h reminder for a late-add event, got {ids}")
        _passed(f"+1h event mints 2h reminder ({ids})")
    finally:
        _restore(saved)


def test_event_at_30min_still_mints_reminder():
    """Even tighter — Diane adds an event 30 min before start. Still a
    legitimate broadcast moment; better an imperfect reminder than none.
    """
    init_db(); seed(); reset_volatile()
    saved = _save()
    try:
        now = datetime.now()
        _scenario_set([{
            "uid": "very-late-30m",
            "summary": "[coaches] Quick brief",
            "description": "Reminder: bibs in red bag tonight.",
            "start": (now + timedelta(minutes=30)).isoformat(
                timespec="seconds"),
        }])
        cid = _start_cycle()
        gcal.run(cid)
        c = connect()
        ids = [r["id"] for r in c.execute(
            "SELECT id FROM inputs WHERE source='gcal'").fetchall()]
        c.close()
        assert any(":2h" in i for i in ids), (
            f"expected a 2h reminder for a +30min event, got {ids}")
        _passed("+30m event mints 2h reminder")
    finally:
        _restore(saved)


def test_already_started_event_does_not_mint():
    """If start has already passed, _mark_past flips status='past' and
    the reminder loop skips it — the agent must not send a 'reminder'
    for an event that's already underway."""
    init_db(); seed(); reset_volatile()
    saved = _save()
    try:
        now = datetime.now()
        _scenario_set([{
            "uid": "already-started",
            "summary": "[committee] Already started",
            "start": (now - timedelta(minutes=15)).isoformat(
                timespec="seconds"),
        }])
        cid = _start_cycle()
        gcal.run(cid)
        c = connect()
        ids = [r["id"] for r in c.execute(
            "SELECT id FROM inputs WHERE source='gcal'").fetchall()]
        c.close()
        assert not ids, f"past event should not mint reminders: {ids}"
        _passed("past event mints nothing")
    finally:
        _restore(saved)


def test_late_add_idempotent_across_cycles():
    """First cycle mints the catch-up 2h. Second cycle sees the same
    event still in band; must not double-mint or double-broadcast."""
    init_db(); seed(); reset_volatile()
    saved = _save()
    try:
        now = datetime.now()
        _scenario_set([{
            "uid": "late-idempotent",
            "summary": "[committee] Late add",
            "start": (now + timedelta(hours=1)).isoformat(timespec="seconds"),
        }])
        cid1 = _start_cycle()
        gcal.run(cid1)
        c = connect()
        ticks = [r["id"] for r in c.execute(
            "SELECT id FROM inputs WHERE source='gcal'").fetchall()]
        c.close()
        for t in ticks:
            playbooks.fire("event_reminder", cid1, t)

        cid2 = _start_cycle()
        gcal.run(cid2)
        c = connect()
        n_inputs = c.execute(
            "SELECT COUNT(*) FROM inputs WHERE source='gcal'").fetchone()[0]
        n_bcs = c.execute(
            "SELECT COUNT(*) FROM broadcasts WHERE playbook='event_reminder'"
        ).fetchone()[0]
        c.close()
        assert n_inputs == 1, (
            f"second cycle re-minted reminder, got {n_inputs}")
        assert n_bcs == 1, (
            f"second cycle re-broadcast, got {n_bcs}")
        _passed("late-add reminder is idempotent across cycles")
    finally:
        _restore(saved)


def test_late_add_body_phrases_actual_delta():
    """When the catch-up fires for a +30m event, the broadcast body
    should not say 'in about 2 hours' — that misleads the recipient.
    Phrasing should reflect actual delta."""
    init_db(); seed(); reset_volatile()
    saved = _save()
    try:
        now = datetime.now()
        _scenario_set([{
            "uid": "phrase-check",
            "summary": "[committee] Emergency call",
            "start": (now + timedelta(minutes=30)).isoformat(
                timespec="seconds"),
        }])
        cid = _start_cycle()
        gcal.run(cid)
        c = connect()
        ticks = [r["id"] for r in c.execute(
            "SELECT id FROM inputs WHERE source='gcal'").fetchall()]
        c.close()
        for t in ticks:
            playbooks.fire("event_reminder", cid, t)
        c = connect()
        bc = c.execute(
            "SELECT body FROM broadcasts WHERE playbook='event_reminder'"
        ).fetchone()
        c.close()
        assert bc, "no broadcast"
        body = bc["body"].lower()
        assert "in about 2 hours" not in body, (
            f"misleading 2h phrase in a +30m reminder: {bc['body']}")
        _passed("body phrasing reflects actual delta, not the window label")
    finally:
        _restore(saved)


if __name__ == "__main__":
    tests = [
        test_event_added_within_2h_still_mints_reminder,
        test_event_at_30min_still_mints_reminder,
        test_already_started_event_does_not_mint,
        test_late_add_idempotent_across_cycles,
        test_late_add_body_phrases_actual_delta,
    ]
    passed = failed = 0
    for fn in tests:
        try:
            fn()
            passed += 1
        except AssertionError as e:
            print(f"  \N{ballot x} {fn.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ! {fn.__name__} crashed: {e!r}")
            failed += 1
    print()
    print(f"{passed} passed, {failed} failed")
    if failed:
        sys.exit(1)

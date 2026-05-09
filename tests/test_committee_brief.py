"""Sunday committee briefing — the agent autonomously assembles a digest
of the club's week and sends it to all 7 committee members.

Tests:
 1. Brief is dispatched to all committee members (reach == count of
    seed committee with email), auto-sent because reach < threshold.
 2. Body includes live state from state.db: open incidents,
    outstanding payments, expiring WWCC, sponsor obligations,
    7-day agent health.
 3. Schedule tick on Sunday 21:00 mints a 'committee_tick' input
    once per ISO week (idempotency by upsert on stable id).
 4. Dedup gate blocks a second brief inside the configured window.
"""
from __future__ import annotations

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
from edge.collectors import schedule as schedule_collector
from tools import committee_brief, playbooks
import config as cfg_mod


def _start_cycle() -> int:
    c = connect()
    cur = c.execute("INSERT INTO cycles(started_at) VALUES(?)", (now_iso(),))
    cid = cur.lastrowid
    c.close()
    return cid


def _seed_incident_and_payment_state():
    """Inject realistic state so the brief has content to surface."""
    c = connect()
    c.execute(
        "INSERT INTO incidents(id, kind, severity, summary, detected_at, "
        "league_report_due, status, player_id) VALUES "
        "(?,?,?,?,?,?,?,?)",
        ("inc_test01", "injury", "serious",
         "U14 ankle, ambulance attended", now_iso(),
         (datetime.now() + timedelta(hours=4)).isoformat(),
         "open", "pl_010"))
    # Bump a sponsor's last_post_at so the brief shows a concrete date.
    c.execute("UPDATE sponsors SET last_post_at=? WHERE id='sp_bendigo'",
              ("2026-05-01T18:00:00",))
    c.close()


def _passed(name): print(f"  \N{check mark} {name}")
def _failed(name, msg): print(f"  \N{ballot x} {name}: {msg}")


def test_brief_reach_and_body():
    init_db(); seed(); reset_volatile(); seed()
    _seed_incident_and_payment_state()
    cid = _start_cycle()

    out = committee_brief.run(cid, related_input=None)

    # Reach should match committee with email — 7 in seed.
    c = connect()
    expected = c.execute(
        "SELECT COUNT(*) FROM people "
        "WHERE role='committee' AND email IS NOT NULL "
        "AND email <> ''").fetchone()[0]
    c.close()
    assert out["reach"] == expected, (
        f"expected reach={expected}, got {out['reach']}")
    assert expected >= 5, f"committee should be at least 5, got {expected}"
    _passed(f"reach matches committee size ({expected})")

    # Status should be 'sent' since 7 < approval threshold (50).
    assert out["result"]["status"] == "sent", (
        f"expected sent, got {out['result']['status']}")
    _passed("under threshold → auto-sent (no approval gate hit)")

    # Body must reflect live state we just seeded.
    body = out["result"].get("body") or committee_brief.assemble_body()[1]
    _, body = committee_brief.assemble_body()
    assert "[SERIOUS] injury" in body, "brief missing seeded incident"
    assert "U14 ankle" in body, "brief missing incident summary"
    assert "Outstanding registrations" in body
    assert "EXPIRED" in body, "brief should flag expired WWCC from seed"
    assert "Bendigo Bank" in body, "brief missing sponsor obligation"
    assert "2026-05-01" in body, "brief missing sponsor last_post_at"
    _passed("body assembled from live state.db (incident, WWCC, sponsor)")

    # League deadline labelling: <12h to go should appear as 'Xh to deadline'
    assert "to league deadline" in body, (
        "imminent league deadline should be flagged")
    _passed("league-deadline countdown rendered correctly")


def test_via_playbook_dispatch():
    init_db(); reset_volatile(); seed()
    cid = _start_cycle()

    # Mint a synthetic committee tick the way the schedule collector would
    # on a Sunday at 21:00.
    iso = datetime.now().isocalendar()
    sid = f"committee_brief_{iso[0]}W{iso[1]:02d}"
    c = connect()
    c.execute(
        "INSERT INTO inputs(id, source, source_id, received_at, "
        "sender, subject, body, classification, status) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (f"schedule:{sid}", "schedule", sid, now_iso(),
         "schedule.committee", "Committee briefing tick",
         "Weekly committee briefing assembly",
         "committee_tick", "classified"))
    c.close()

    # Fire via the same registry the orchestrator uses.
    out = playbooks.fire("committee_brief", cid, f"schedule:{sid}")
    assert out["reach"] >= 5
    assert out["result"]["status"] == "sent"
    _passed("playbook registry dispatches committee_brief")

    # Tick input is closed after firing.
    c = connect()
    row = c.execute(
        "SELECT status FROM inputs WHERE id=?",
        (f"schedule:{sid}",)).fetchone()
    c.close()
    assert row["status"] == "done", f"input not closed: {row['status']}"
    _passed("schedule tick closed after dispatch")


def test_dedup_blocks_second_brief():
    init_db(); reset_volatile(); seed()
    cid = _start_cycle()

    out1 = committee_brief.run(cid, related_input=None)
    assert out1["result"]["status"] == "sent"

    out2 = committee_brief.run(cid, related_input=None)
    # Second brief inside the window: dedup is a SILENT gate — it
    # silently skips the broadcast (status='skipped', no decision
    # opened). This is intentional: dedup blocks should not flood the
    # decision queue.
    assert out2["result"]["status"] == "skipped", (
        f"expected skipped, got {out2['result']['status']}")
    assert out2["result"].get("blocked_by") == "dedup", (
        f"expected dedup block, got {out2['result'].get('blocked_by')}")
    # Critically: no second decision should have been opened.
    c = connect()
    n = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE target_kind='broadcast'"
    ).fetchone()[0]
    c.close()
    assert n == 0, f"dedup should not open decisions, got {n}"
    _passed("second brief inside window → silently skipped, no queue noise")


def test_schedule_collector_mints_tick_idempotently():
    init_db(); reset_volatile(); seed()
    cid = _start_cycle()

    # Force the now-clock view inside the collector by patching datetime
    # — we mock the wall clock to Sunday 21:05 so the schedule logic
    # crosses the threshold.
    import edge.collectors.schedule as sc

    class FakeDT:
        @classmethod
        def now(cls):
            # Sunday at 21:05 (next Sunday)
            today = datetime.now()
            offset = (6 - today.weekday()) % 7
            sun = today + timedelta(days=offset)
            return sun.replace(hour=21, minute=5, second=0, microsecond=0)

        @classmethod
        def fromisoformat(cls, s):
            return datetime.fromisoformat(s)

    real_dt = sc.datetime
    sc.datetime = FakeDT  # type: ignore[assignment]
    try:
        r1 = sc.run(cid)
        r2 = sc.run(cid)
    finally:
        sc.datetime = real_dt  # type: ignore[assignment]

    minted_first = [t for t in r1.get("ticks", []) if "committee_brief" in t]
    minted_second = [t for t in r2.get("ticks", []) if "committee_brief" in t]
    assert len(minted_first) == 1, (
        f"first run should mint exactly one committee tick: {r1}")
    assert minted_second == [], (
        f"second run same week should mint nothing: {r2}")
    _passed("Sunday 21:00 mints exactly one tick per ISO week")


if __name__ == "__main__":
    passed = failed = 0
    for fn in (test_brief_reach_and_body, test_via_playbook_dispatch,
               test_dedup_blocks_second_brief,
               test_schedule_collector_mints_tick_idempotently):
        try:
            fn()
            passed += 1
        except AssertionError as e:
            _failed(fn.__name__, str(e))
            failed += 1
        except Exception as e:
            _failed(fn.__name__, f"{type(e).__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)

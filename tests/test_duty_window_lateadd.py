"""schedule.py duty-reminder windows — terminal anchor must use lo=0.

Mirrors the gcal collector pattern: the 2h window is the LAST chance to
warn the rostered volunteer + open the panic-time backfill card on
Diane's queue. If the cycle that would have minted the 2h tick gets
skipped (off-hours interval, restart, deploy, brief cycle pause), the
duty silently misses its panic reminder. Same shape for late-add
fixtures: a parent rosters a canteen volunteer for a 4pm-today match at
3:30pm — under the old logic delta=30min sits below the 1h45m floor and
no tick is minted, so the reminder + the T-2h decision card never fire.

The fix is to drop the 2h window's lower bound to zero. The upper bound
plus upsert_input UNIQUE(source, source_id) handle idempotency: each
(fixture, window) pair mints at most once, so consecutive cycles inside
the window don't double-mint.

The 3d / 1d windows stay strict in-band — those are 'natural advance
notice' anchors. A fixture noticed at T-12h doesn't need a stale 3d
reminder; the 2h will catch it.

Tests:
  * 2h late-add (delta below old floor) → tick minted
  * 2h on-cycle (delta inside old in-band) → tick minted, idempotent
  * 1d / 3d strict bounds preserved (no spam on late-add)
  * Past kickoff → nothing minted
  * Cycle that would have skipped the in-band 2h slice still mints
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
from edge.collectors import schedule


def _passed(name): print(f"  \N{check mark} {name}")


def _start_cycle() -> int:
    c = connect()
    cur = c.execute("INSERT INTO cycles(started_at) VALUES(?)",
                    (now_iso(),))
    cid = cur.lastrowid
    c.close()
    return cid


def _set_kickoff(fixture_id: str, dt: datetime) -> None:
    """Pin a fixture's kickoff and force every other fixture into the
    past, so schedule.py's upcoming sweep only considers ours."""
    c = connect()
    c.execute(
        "UPDATE fixtures SET kickoff=?, status='scheduled' WHERE id=?",
        (dt.isoformat(timespec="seconds"), fixture_id))
    # Push everything else outside the upcoming window.
    c.execute(
        "UPDATE fixtures SET status='completed' WHERE id != ?",
        (fixture_id,))
    c.close()


def _minted_for(fid: str) -> list[str]:
    c = connect()
    rows = c.execute(
        "SELECT source_id FROM inputs WHERE source='schedule' "
        "AND source_id LIKE ? ORDER BY source_id",
        (f"duty_reminder_{fid}_%",)).fetchall()
    c.close()
    return [r["source_id"] for r in rows]


# ---------------------------------------------------------------------


def test_2h_late_add_below_old_floor_still_mints():
    """Fixture whose kickoff is 30 minutes from now — below the old
    1h45m floor. The 2h tick MUST mint. This is the late-add panic case
    the architecture promises (parent flakes at 3:30pm on a 4pm match,
    the duty card has to open on Diane's queue)."""
    init_db(); seed(); reset_volatile()

    fid = "fx_r12_u11"
    _set_kickoff(fid, datetime.now() + timedelta(minutes=30))

    cid = _start_cycle()
    schedule.run(cid)

    minted = _minted_for(fid)
    assert f"duty_reminder_{fid}_2h" in minted, (
        f"2h tick must mint for late-add fixture, got: {minted}")
    # 1d and 3d should NOT fire on a 30-min-out fixture — they're
    # advance-notice anchors and a late-add gets just the terminal one.
    assert f"duty_reminder_{fid}_1d" not in minted, (
        f"1d should not fire on late-add: {minted}")
    assert f"duty_reminder_{fid}_3d" not in minted, (
        f"3d should not fire on late-add: {minted}")
    _passed("late-add (delta=30m) → 2h tick mints, 1d/3d quiet")


def test_2h_in_band_still_mints():
    """Fixture 2 hours out — the original in-band sweet spot. Must
    still mint a 2h tick (no regression on the happy path)."""
    init_db(); seed(); reset_volatile()

    fid = "fx_r12_u12"
    _set_kickoff(fid, datetime.now() + timedelta(hours=2))

    cid = _start_cycle()
    schedule.run(cid)

    assert f"duty_reminder_{fid}_2h" in _minted_for(fid), (
        f"2h tick must mint for in-band fixture, got: {_minted_for(fid)}")
    _passed("in-band (delta=2h) → 2h tick mints")


def test_2h_idempotent_across_cycles():
    """A fixture sitting inside the 2h window across two cycles must
    only mint one tick. upsert_input UNIQUE keys absorb the repeat."""
    init_db(); seed(); reset_volatile()

    fid = "fx_r12_u13_g"
    _set_kickoff(fid, datetime.now() + timedelta(hours=1))

    cid1 = _start_cycle()
    schedule.run(cid1)
    cid2 = _start_cycle()
    schedule.run(cid2)

    minted = _minted_for(fid)
    twoh = [m for m in minted if m.endswith("_2h")]
    assert len(twoh) == 1, (
        f"2h tick should mint exactly once across cycles, got: {twoh}")
    _passed("idempotent: 2 cycles → 1 tick")


def test_3d_strict_bound_preserved():
    """The 3d window is an advance-notice anchor — a fixture noticed
    at T-12h must NOT fire a stale 3d. Only 2h fires."""
    init_db(); seed(); reset_volatile()

    fid = "fx_r12_u14"
    _set_kickoff(fid, datetime.now() + timedelta(hours=12))

    cid = _start_cycle()
    schedule.run(cid)

    minted = _minted_for(fid)
    assert f"duty_reminder_{fid}_3d" not in minted, (
        f"3d shouldn't fire 12h before kickoff: {minted}")
    assert f"duty_reminder_{fid}_1d" not in minted, (
        f"1d shouldn't fire 12h before kickoff (above 25h hi): {minted}")
    _passed("strict 3d/1d preserved: T-12h → 0 advance ticks")


def test_past_kickoff_mints_nothing():
    """Kickoff already in the past — schedule must not mint anything,
    no matter how the windows are configured."""
    init_db(); seed(); reset_volatile()

    fid = "fx_r12_u15_g"
    _set_kickoff(fid, datetime.now() - timedelta(hours=1))

    cid = _start_cycle()
    schedule.run(cid)

    assert _minted_for(fid) == [], (
        f"past fixture must not mint: {_minted_for(fid)}")
    _passed("past kickoff → 0 ticks (no late firing on completed match)")


def test_2h_off_hours_skip_recovery():
    """Simulates the off-hours skip case. Cycle A would have run when
    fixture was at delta=2h30m (above the old hi=2h15m, so no tick).
    Next cycle B doesn't run for 75 minutes (off-hours interval). At
    cycle B fixture is at delta=1h15m — below the old lo=1h45m floor.
    With the old narrow window the 2h tick is silently lost. With
    lo=0 it mints at cycle B."""
    init_db(); seed(); reset_volatile()

    fid = "fx_r12_u16"
    _set_kickoff(fid, datetime.now() + timedelta(hours=1, minutes=15))

    cid = _start_cycle()
    schedule.run(cid)

    assert f"duty_reminder_{fid}_2h" in _minted_for(fid), (
        f"2h must mint after window-skip recovery, got: {_minted_for(fid)}")
    _passed("off-hours skip recovery: T-1h15 → 2h tick mints")


# ---------------------------------------------------------------------


if __name__ == "__main__":
    tests = [
        test_2h_late_add_below_old_floor_still_mints,
        test_2h_in_band_still_mints,
        test_2h_idempotent_across_cycles,
        test_3d_strict_bound_preserved,
        test_past_kickoff_mints_nothing,
        test_2h_off_hours_skip_recovery,
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

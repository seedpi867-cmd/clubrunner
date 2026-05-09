"""Lightning resume — the only true zero-approval automation.

Architecture promise:
  > Lightning resume — 30 minutes since last strike <pause_distance_km
  >                  → post resume notice

The resume path has zero coverage, and the live code conflates two
different "all clear" states:

  (a) The latest weather observation has lightning_km IS NULL
      (BOM observed and reported "no strike").
  (b) The latest weather observation has lightning_km >= pause_km
      (a distant strike — outside threshold, equivalent to all-clear
      for the purposes of resuming play).

The current implementation only resumes on (a). On (b), the ground
stays paused indefinitely even after the strike has moved well outside
the danger zone — kids sitting at the ground while every other club has
been told to go home.

This test exercises the (b) path: paused ground, a distant-strike
observation 31 minutes after the last close strike, expect resume.
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

from edge.state.db import connect, init_db, now_iso
from edge.state.seed import seed
from tools import lightning_policy


def _new_cycle() -> int:
    init_db()
    seed()
    reset_volatile()
    conn = connect()
    cur = conn.execute(
        "INSERT INTO cycles(started_at) VALUES(datetime('now'))")
    cid = cur.lastrowid
    conn.close()
    return cid


def _force_live_fixture(ground_id: str) -> str:
    """Stamp a fixture on this ground to start 30 minutes from now so it
    falls inside the lightning-policy active window. Returns fixture id."""
    ko = (datetime.now() + timedelta(minutes=30)).isoformat(timespec="seconds")
    conn = connect()
    fid = conn.execute(
        "SELECT id FROM fixtures WHERE ground_id=? LIMIT 1",
        (ground_id,)).fetchone()["id"]
    conn.execute("UPDATE fixtures SET kickoff=?, status='paused', "
                 "status_reason='lightning policy', last_status_change=? "
                 "WHERE id=?", (ko, now_iso(), fid))
    conn.close()
    return fid


def _seed_weather(ground_id: str, lkm: float | None,
                  minutes_ago: int = 0) -> None:
    """Stamp a weather_state row at now-minutes_ago."""
    obs = (datetime.now() - timedelta(minutes=minutes_ago)
           ).isoformat(timespec="seconds")
    conn = connect()
    conn.execute(
        "INSERT OR REPLACE INTO weather_state(ground_id, observed_at, "
        "forecast_max_c, forecast_for, lightning_km) VALUES(?,?,?,?,?)",
        (ground_id, obs, 22.0, obs[:16], lkm))
    conn.close()


def _ground_with_fixtures() -> str:
    conn = connect()
    gid = conn.execute(
        "SELECT ground_id FROM fixtures GROUP BY ground_id "
        "ORDER BY COUNT(*) DESC LIMIT 1").fetchone()["ground_id"]
    conn.close()
    return gid


def _fixture_status(fid: str) -> tuple[str, str | None]:
    conn = connect()
    row = conn.execute(
        "SELECT status, status_reason FROM fixtures WHERE id=?",
        (fid,)).fetchone()
    conn.close()
    return row["status"], row["status_reason"]


def _resume_broadcasts(cycle_id: int, ground_id: str) -> int:
    conn = connect()
    n = conn.execute(
        "SELECT COUNT(*) AS n FROM broadcasts "
        "WHERE cycle_id=? AND playbook='lightning_resume' "
        "AND segment=? AND status='sent'",
        (cycle_id, f"ground:{ground_id}")).fetchone()["n"]
    conn.close()
    return n


def test_resume_fires_on_distant_strike_after_window():
    """Paused ground; close strike 35 min ago; latest obs is distant
    (15km — outside threshold). Resume must fire."""
    cid = _new_cycle()
    gid = _ground_with_fixtures()
    fid = _force_live_fixture(gid)

    # Close strike 35 min ago — placed it inside threshold.
    _seed_weather(gid, lkm=4.0, minutes_ago=35)
    # Most recent observation: distant (15km, outside 10km threshold), now.
    _seed_weather(gid, lkm=15.0, minutes_ago=0)

    out = lightning_policy.run(cid)

    actions = [a for a in out["actions"] if a["ground"] == gid]
    assert any(a["action"] == "resume" for a in actions), (
        f"expected resume action for {gid}, got {actions}")

    status, reason = _fixture_status(fid)
    assert status == "scheduled", (
        f"fixture should be back to scheduled, got status={status} "
        f"reason={reason}")

    n = _resume_broadcasts(cid, gid)
    assert n == 1, f"expected exactly 1 resume broadcast, got {n}"
    print("  ✓ distant-strike observation resumes paused fixtures")


def test_resume_fires_on_null_strike_after_window():
    """Original happy path. Paused ground; close strike 35 min ago; now
    BOM reports no strike (lightning_km IS NULL). Resume must fire.
    Guards against regressing the existing behaviour while we widen it."""
    cid = _new_cycle()
    gid = _ground_with_fixtures()
    fid = _force_live_fixture(gid)

    _seed_weather(gid, lkm=4.0, minutes_ago=35)
    _seed_weather(gid, lkm=None, minutes_ago=0)

    out = lightning_policy.run(cid)

    actions = [a for a in out["actions"] if a["ground"] == gid]
    assert any(a["action"] == "resume" for a in actions), (
        f"expected resume action, got {actions}")
    status, _ = _fixture_status(fid)
    assert status == "scheduled"
    assert _resume_broadcasts(cid, gid) == 1
    print("  ✓ null-strike observation still resumes (no regression)")


def test_close_strike_within_window_does_not_resume():
    """Paused ground; the most recent close strike was only 10 min ago.
    Even though there is a clear observation right now, the resume
    window has not elapsed. Must NOT resume yet."""
    cid = _new_cycle()
    gid = _ground_with_fixtures()
    fid = _force_live_fixture(gid)

    # Close strike 10 min ago — INSIDE the resume window.
    _seed_weather(gid, lkm=4.0, minutes_ago=10)
    # Right now: clear.
    _seed_weather(gid, lkm=None, minutes_ago=0)

    out = lightning_policy.run(cid)
    actions = [a for a in out["actions"] if a["ground"] == gid]
    assert not any(a["action"] == "resume" for a in actions), (
        f"resume fired prematurely: {actions}")

    status, _ = _fixture_status(fid)
    assert status == "paused", (
        f"fixture should stay paused while strike window still open, "
        f"got {status}")
    assert _resume_broadcasts(cid, gid) == 0
    print("  ✓ resume holds while strike still inside window")


def test_distant_strike_inside_window_does_not_resume():
    """Paused ground; close strike 5 min ago; latest is now distant
    (20km). The distant strike does NOT short-circuit the resume timer
    — what matters is when the last <pause_km strike was."""
    cid = _new_cycle()
    gid = _ground_with_fixtures()
    fid = _force_live_fixture(gid)

    _seed_weather(gid, lkm=4.0, minutes_ago=5)
    _seed_weather(gid, lkm=20.0, minutes_ago=0)

    out = lightning_policy.run(cid)
    actions = [a for a in out["actions"] if a["ground"] == gid]
    assert not any(a["action"] == "resume" for a in actions), (
        f"resume should not fire when last close strike is recent: {actions}")
    status, _ = _fixture_status(fid)
    assert status == "paused"
    print("  ✓ distant 'now' does not reset the strike-window clock")


def test_resume_idempotent_across_cycles():
    """After a successful resume, a follow-up cycle must not fire a
    second resume notice. The pause→scheduled flip means the resume
    branch shouldn't even be considered, but if anything re-stamps the
    ground as paused, the dedup gate must still hold."""
    cid = _new_cycle()
    gid = _ground_with_fixtures()
    fid = _force_live_fixture(gid)

    _seed_weather(gid, lkm=4.0, minutes_ago=35)
    _seed_weather(gid, lkm=15.0, minutes_ago=0)

    lightning_policy.run(cid)
    # Second cycle, same state of the world.
    conn = connect()
    cid2 = conn.execute(
        "INSERT INTO cycles(started_at) VALUES(datetime('now'))").lastrowid
    conn.close()
    out2 = lightning_policy.run(cid2)
    actions = [a for a in out2["actions"] if a["ground"] == gid]
    assert not any(a["action"] == "resume" for a in actions), (
        f"second cycle must not re-fire resume: {actions}")
    print("  ✓ resume fires once across consecutive cycles")


def main():
    tests = [
        test_resume_fires_on_distant_strike_after_window,
        test_resume_fires_on_null_strike_after_window,
        test_close_strike_within_window_does_not_resume,
        test_distant_strike_inside_window_does_not_resume,
        test_resume_idempotent_across_cycles,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except AssertionError as e:
            print(f"  ✗ {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ! {t.__name__} crashed: {e!r}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

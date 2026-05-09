"""Ground drainage policy — closure when 24h rain crosses threshold.

The bug we are guarding against: ground quirks were free-text strings
("Eastern oval poor drainage — closes after >20mm overnight rain") with
no structured trigger. The agent KNEW the rule but couldn't ACT on it.
A wet Friday night produced no broadcast, no fixture cancellation, no
decision — just a string sitting in grounds.quirks.

These tests prove the rule is now live:
  - rain at threshold fires the closure broadcast for that ground only
  - other grounds (no threshold OR threshold not crossed) stay scheduled
  - the closure broadcast holds in 'gated' awaiting Diane's tap
  - fixtures on the closed ground flip to 'cancelled' immediately
    (world-model write before any draft branch)
  - re-firing within the same day is a no-op (idempotent via
    policy_runs)
  - reach counts the parents + coach + manager of every cancelled
    fixture, deduped across teams sharing the ground
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from _helpers import use_temp_db, reset_volatile
use_temp_db()

from edge.state.db import connect, init_db, now_iso
from edge.state.seed import seed
from tools import ground_drainage


def _new_cycle() -> int:
    init_db()
    seed()
    reset_volatile()
    conn = connect()
    cur = conn.execute(
        "INSERT INTO cycles(started_at) VALUES(?)", (now_iso(),))
    cid = cur.lastrowid
    conn.close()
    return cid


def _stamp_rain(ground_id: str, mm: float) -> None:
    """Write a single fresh rain observation for this ground."""
    conn = connect()
    conn.execute(
        "INSERT OR REPLACE INTO weather_state(ground_id, observed_at, "
        "rain_24h_mm) VALUES(?,?,?)",
        (ground_id, now_iso(), mm))
    conn.close()


def _move_fixtures_into_horizon(team_ids: list[str]) -> None:
    """Push at least one fixture per team into 'today + 4h' so the
    horizon picks it up regardless of when the test runs. Without this,
    fixtures seeded a week into the future are outside the 24h window
    and the playbook (correctly) finds nothing to close."""
    from datetime import datetime, timedelta
    soon = (datetime.now() + timedelta(hours=4)).isoformat(timespec="seconds")
    conn = connect()
    qs = ",".join("?" * len(team_ids))
    conn.execute(
        f"UPDATE fixtures SET kickoff=?, status='scheduled' "
        f"WHERE id IN (SELECT MIN(id) FROM fixtures "
        f"WHERE team_id IN ({qs}) GROUP BY team_id)",
        (soon, *team_ids))
    conn.close()


def _fixtures_on_ground(ground_id: str, status: str | None = None) -> list[dict]:
    conn = connect()
    if status:
        rows = conn.execute(
            "SELECT id, status, status_reason FROM fixtures "
            "WHERE ground_id=? AND status=? "
            "ORDER BY kickoff", (ground_id, status)).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, status, status_reason FROM fixtures "
            "WHERE ground_id=? ORDER BY kickoff", (ground_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _drainage_broadcasts(ground_id: str) -> list[dict]:
    conn = connect()
    rows = conn.execute(
        "SELECT id, channel, status, blocked_by, reach, subject "
        "FROM broadcasts WHERE playbook='ground_drainage' "
        "AND segment=? ORDER BY id",
        (f"ground:{ground_id}",)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def test_below_threshold_no_action():
    cid = _new_cycle()
    # Henley threshold is 20mm; 18mm should NOT trip.
    _stamp_rain("henley_oval", 18.0)
    out = ground_drainage.run(cid)
    assert out["closures"] == [], (
        f"closure fired below threshold: {out}")
    bcs = _drainage_broadcasts("henley_oval")
    assert bcs == [], (
        f"broadcast drafted below threshold: {bcs}")
    print("  ✓ 18mm < 20mm threshold → no action")


def test_above_threshold_cancels_henley_only():
    cid = _new_cycle()
    # Henley fixtures need to be inside the horizon
    henley_teams = ["auskick_a", "auskick_b", "u8_mixed", "u9_mixed"]
    _move_fixtures_into_horizon(henley_teams)

    _stamp_rain("henley_oval", 24.0)        # over threshold
    _stamp_rain("grange_reserve", 24.0)     # no threshold set — must NOT fire
    _stamp_rain("fulham_north", 24.0)       # no threshold set — must NOT fire

    out = ground_drainage.run(cid)
    assert len(out["closures"]) == 1, (
        f"expected exactly Henley to close, got {out['closures']}")
    closed = out["closures"][0]
    assert closed["ground"] == "henley_oval"
    assert closed["rain_mm"] == 24.0
    assert len(closed["fixtures"]) >= 1
    assert closed["reach"] > 0, (
        "reach should include at least the coaches/managers of cancelled "
        "fixtures")

    cancelled = _fixtures_on_ground("henley_oval", "cancelled")
    assert len(cancelled) >= 1
    assert all("drainage policy" in (c["status_reason"] or "")
               for c in cancelled), (
        f"cancellation reason missing drainage tag: {cancelled}")

    grange_rows = _fixtures_on_ground("grange_reserve")
    assert all(r["status"] == "scheduled" for r in grange_rows), (
        f"Grange has no threshold but got cancelled: {grange_rows}")

    bcs = _drainage_broadcasts("henley_oval")
    assert len(bcs) == 2, f"expected email + sms, got {len(bcs)}: {bcs}"
    statuses = {b["status"] for b in bcs}
    assert statuses == {"gated"}, (
        f"closure broadcasts must hold for Diane's approval, got "
        f"{statuses} (broadcast_threshold blocked={[b['blocked_by'] for b in bcs]})")
    channels = {b["channel"] for b in bcs}
    assert channels == {"email", "sms"}
    print(f"  ✓ 24mm > 20mm → Henley closed "
          f"({len(cancelled)} fixtures, reach={closed['reach']}); "
          f"other grounds untouched")


def test_idempotent_within_day():
    cid = _new_cycle()
    _move_fixtures_into_horizon(["auskick_a", "u8_mixed"])
    _stamp_rain("henley_oval", 30.0)

    first = ground_drainage.run(cid)
    assert len(first["closures"]) == 1

    second = ground_drainage.run(cid)
    assert len(second["closures"]) == 0, (
        f"second run on the same ground/day re-fired: {second}")
    assert any(s["reason"] == "already_fired"
               for s in second["skipped"]), (
        f"expected already_fired skip, got {second['skipped']}")

    bcs = _drainage_broadcasts("henley_oval")
    assert len(bcs) == 2, (
        f"second run double-drafted: {len(bcs)} broadcasts")
    print("  ✓ second run on same day skipped via policy_runs")


def test_no_fixtures_records_run_no_broadcast():
    """Threshold crossed but no fixtures within horizon (e.g. midweek
    rain, no matches) — record a policy_runs row so we don't churn
    every cycle, but no broadcast and no fixture mutations."""
    cid = _new_cycle()
    # Don't move any fixtures into the horizon — leave them on their
    # seed dates which are this Saturday-ish but may not all be inside
    # the 24h window depending on the test clock. Push every fixture on
    # Henley out beyond the horizon to be sure.
    from datetime import datetime, timedelta
    far = (datetime.now() + timedelta(days=10)).isoformat(timespec="seconds")
    conn = connect()
    conn.execute(
        "UPDATE fixtures SET kickoff=? WHERE ground_id='henley_oval'",
        (far,))
    conn.close()

    _stamp_rain("henley_oval", 30.0)
    out = ground_drainage.run(cid)
    assert out["closures"] == []
    assert any(s["reason"] == "no_fixtures"
               for s in out["skipped"]), out

    bcs = _drainage_broadcasts("henley_oval")
    assert bcs == []

    # And the run is recorded so re-evaluation tomorrow can decide fresh.
    conn = connect()
    today = datetime.now().date().isoformat()
    row = conn.execute(
        "SELECT outcome FROM policy_runs WHERE id=?",
        (f"drainage:henley_oval:{today}",)).fetchone()
    conn.close()
    assert row and row["outcome"] == "no_fixtures"
    print("  ✓ wet midweek (no fixtures) records run without spamming")


def test_world_model_write_precedes_draft():
    """The cancel must hit the fixtures table even if the broadcast
    submit ends up gated. Without this, lightning policy or another
    downstream playbook running after drainage would still see
    'scheduled' on a closed ground and produce inconsistent state."""
    cid = _new_cycle()
    _move_fixtures_into_horizon(["auskick_a", "u8_mixed"])
    _stamp_rain("henley_oval", 25.0)

    ground_drainage.run(cid)

    # The broadcasts are gated (force_approval=True).
    bcs = _drainage_broadcasts("henley_oval")
    assert all(b["status"] == "gated" for b in bcs), (
        f"broadcasts should be gated awaiting approval: {bcs}")

    # But the fixtures are cancelled regardless.
    cancelled = _fixtures_on_ground("henley_oval", "cancelled")
    assert len(cancelled) >= 1, (
        "fixtures must be cancelled even though the broadcast is held")
    print(f"  ✓ fixtures cancelled before draft branch "
          f"({len(cancelled)} flipped while broadcasts gated)")


def main():
    tests = [
        ("18mm below 20mm threshold → no action",
         test_below_threshold_no_action),
        ("24mm above threshold cancels Henley only",
         test_above_threshold_cancels_henley_only),
        ("second run same day is idempotent",
         test_idempotent_within_day),
        ("threshold crossed but no fixtures records run, no broadcast",
         test_no_fixtures_records_run_no_broadcast),
        ("world-model write precedes draft branch",
         test_world_model_write_precedes_draft),
    ]
    passed = failed = 0
    for label, fn in tests:
        try:
            fn()
            passed += 1
        except AssertionError as ex:
            print(f"  ✗ {label}: {ex}")
            failed += 1
        except Exception as ex:
            print(f"  ✗ {label}: {type(ex).__name__}: {ex}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

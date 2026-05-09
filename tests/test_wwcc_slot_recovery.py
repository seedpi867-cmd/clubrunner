"""WWCC slot tracking — survives missed cycles.

The naive "fire when days_to_expiry == exactly N" check breaks every
time the orchestrator misses the calendar day a slot lands on. For a
hard legal requirement, that is exactly the kind of quiet failure that
bites at the wrong moment. The playbook now keys each (coach, slot,
expiry) tuple in policy_runs so:

  - a coach who was offline on day-14 still gets the 14-day reminder on
    day-13 (catch-up: pick smallest slot S where days <= S, fired = no)
  - re-running the playbook within the same day does not re-draft the
    same slot (idempotent)
  - a coach who renews their WWCC starts a fresh slot cycle (the new
    expiry date is part of the key)
  - a coach who has already been reminded at slot 14 stays quiet until
    days drops to 7 — no spam between slots
  - expired coaches escalate, never autosend (unchanged), and the
    expiry decision dedups across sweeps via policy_runs too

The test seeds a small set of coaches with engineered expiry dates and
asserts the slot-fire pattern under repeated and gap-skipping sweeps.
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
from tools import wwcc_check


def _start_cycle() -> int:
    c = connect()
    cur = c.execute("INSERT INTO cycles(started_at) VALUES(?)", (now_iso(),))
    cid = cur.lastrowid
    c.close()
    return cid


def _set_expiry(coach_id: str, days_from_today: int) -> str:
    """Set a coach's WWCC expiry exactly N days from today and return
    the iso date — needed so the test can pass the same key into the
    policy_runs lookup later. Existing seed expiries are overwritten so
    only the seeded test coaches drive the count."""
    target = (datetime.now().date() + timedelta(days=days_from_today)
              ).isoformat()
    c = connect()
    c.execute("UPDATE people SET wwcc_expiry=? WHERE id=?",
              (target, coach_id))
    c.close()
    return target


def _wipe_all_expiries():
    """Null out every WWCC expiry, both seeded and test-engineered.

    Tests share the same temp state.db within a process (use_temp_db
    initialises once), and reset_volatile() does not touch the people
    table. Without this, a date set by an earlier test silently leaks
    into a later one — e.g. dean=42d from the first test triggers a
    slot 60 fire when a later test only meant to set marko and sara."""
    c = connect()
    c.execute("UPDATE people SET wwcc_expiry=NULL WHERE role IN "
              "('coach','manager')")
    c.close()


def _broadcasts_for(coach_id: str) -> list[dict]:
    c = connect()
    rows = c.execute(
        "SELECT id, status, subject, body FROM broadcasts "
        "WHERE segment=? AND playbook='wwcc_reminder' "
        "ORDER BY created_at",
        (f"person:{coach_id}",)).fetchall()
    c.close()
    return [dict(r) for r in rows]


def _slot_fired(coach_id: str, slot: int, expiry_iso: str) -> bool:
    c = connect()
    row = c.execute(
        "SELECT 1 FROM policy_runs WHERE id=?",
        (f"wwcc_slot:{coach_id}:{slot}:{expiry_iso}",)).fetchone()
    c.close()
    return row is not None


def _passed(name): print(f"  \N{check mark} {name}")
def _failed(name, msg): print(f"  \N{ballot x} {name}: {msg}")


def test_first_sweep_picks_correct_slot_per_coach():
    """Each coach is engineered into a different distance from expiry.
    A single sweep should pick the right slot for each."""
    init_db(); reset_volatile(); seed(); _wipe_all_expiries()

    # Engineered: marko=14d, sara=7d, dean=42d (in 30-slot range), nina=200d
    e_marko = _set_expiry("coach_marko", 14)
    e_sara  = _set_expiry("coach_sara",  7)
    e_dean  = _set_expiry("coach_dean",  42)
    e_nina  = _set_expiry("coach_nina",  200)

    cid = _start_cycle()
    out = wwcc_check.run(cid)

    # Marko must have fired slot 14 (smallest unfired slot where days<=14).
    assert _slot_fired("coach_marko", 14, e_marko), (
        f"marko should have fired slot 14: out={out}")
    _passed("marko (14 days) → slot 14 fired")

    # Sara must have fired slot 7 — not 14 (smallest where days<=slot).
    assert _slot_fired("coach_sara", 7, e_sara)
    assert not _slot_fired("coach_sara", 14, e_sara), (
        "sara at days=7 should fire slot 7, NOT 14")
    _passed("sara (7 days) → slot 7 only (no spam at 14)")

    # Dean at 42d qualifies for 60/90 but NOT 30 (42 > 30). Smallest
    # qualifying = 60. So slot 60 should fire.
    assert _slot_fired("coach_dean", 60, e_dean)
    assert not _slot_fired("coach_dean", 30, e_dean)
    _passed("dean (42 days) → slot 60 (smallest qualifying, 30 not yet due)")

    # Nina at 200d does not qualify for any slot — quiet.
    nina_bcs = _broadcasts_for("coach_nina")
    assert nina_bcs == [], (
        f"nina at 200 days should be silent, got {nina_bcs}")
    _passed("nina (200 days) → silent until first slot reaches 90")

    # Total drafts == 3 (marko, sara, dean), expired==0 (all in future).
    assert len(out["reminders"]) == 3, (
        f"expected 3 drafts, got {len(out['reminders'])}: {out}")
    assert out["expired"] == [], (
        f"no expired in this fixture, got {out['expired']}")


def test_idempotent_repeated_sweeps_same_day():
    """Re-running the playbook in the same day must not re-draft."""
    init_db(); reset_volatile(); seed(); _wipe_all_expiries()
    _set_expiry("coach_marko", 14)
    _set_expiry("coach_sara",  7)
    _set_expiry("coach_dean",  42)
    _set_expiry("coach_nina",  200)

    cid = _start_cycle()
    out1 = wwcc_check.run(cid)
    out2 = wwcc_check.run(cid)
    out3 = wwcc_check.run(cid)

    assert len(out1["reminders"]) == 3
    assert len(out2["reminders"]) == 0, (
        f"second sweep same day should be quiet, got {out2}")
    assert len(out3["reminders"]) == 0
    # Total broadcast rows for marko: exactly 1.
    bcs = _broadcasts_for("coach_marko")
    assert len(bcs) == 1, f"expected 1 broadcast, got {len(bcs)}: {bcs}"
    _passed("three sweeps same day → still one broadcast per coach")


def test_missed_day_recovers_next_cycle():
    """Coach goes from days=15 (no slot — too far for 14, too close for
    30 since 15>30 is false, so qualifying slots are 30/60/90, smallest
    not fired = 30 only if not fired yet... wait. 15 <= 30, so 30 fires
    on day-15. We want the case where the orchestrator was OFFLINE on
    days=14 and comes back at days=13. The 14-slot must still fire."""
    init_db(); reset_volatile(); seed(); _wipe_all_expiries()

    # Day-1 of test: coach at days=15. Smallest qualifying slot is 30
    # (15 <= 30, 15 > 14). Slot 30 should fire today.
    e_marko = _set_expiry("coach_marko", 15)
    cid = _start_cycle()
    wwcc_check.run(cid)
    assert _slot_fired("coach_marko", 30, e_marko)
    assert not _slot_fired("coach_marko", 14, e_marko)
    _passed("days=15 → slot 30 fired (slot 14 still pending)")

    # Day-2 of test: coach is now at days=13 (we missed the day-14
    # cycle entirely — orchestrator restart, host reboot, anything).
    # Smallest unfired slot where days <= S: 30 fired, so check 14.
    # 13 <= 14: fire 14 (catch-up).
    e_marko_2 = _set_expiry("coach_marko", 13)
    # Same expiry date because it's the same calendar week — recompute
    # to be robust to clock-tick crossing midnight during the test.
    cid = _start_cycle()
    wwcc_check.run(cid)
    assert _slot_fired("coach_marko", 14, e_marko_2), (
        "missed-day catch-up: slot 14 must fire on days=13 if not "
        "already fired")
    _passed("missed day-14 → slot 14 still fires on day-13 (catch-up)")

    # And running again does nothing.
    bcs_before = len(_broadcasts_for("coach_marko"))
    wwcc_check.run(cid)
    bcs_after = len(_broadcasts_for("coach_marko"))
    assert bcs_after == bcs_before, (
        f"re-run should not re-fire slot 14, got {bcs_after} vs "
        f"{bcs_before}")
    _passed("catch-up slot is itself idempotent")


def test_renewal_starts_fresh_cycle():
    """If a coach renews (new expiry date), the slot cycle restarts —
    fresh keys mean fresh slot fires."""
    init_db(); reset_volatile(); seed(); _wipe_all_expiries()

    # Coach is at 14 days from today; fire 14-slot.
    e1 = _set_expiry("coach_marko", 14)
    cid = _start_cycle()
    wwcc_check.run(cid)
    assert _slot_fired("coach_marko", 14, e1)

    # Coach renews — new expiry 365 days out. Old 14-slot key becomes
    # stale; new expiry is far away, so no slots fire (until the next
    # 90-day window).
    e2 = _set_expiry("coach_marko", 365)
    wwcc_check.run(cid)
    # The 14-slot under the OLD expiry remains fired (history) but no
    # new fires happen for the new expiry.
    assert _slot_fired("coach_marko", 14, e1), "history must persist"
    assert not _slot_fired("coach_marko", 14, e2), (
        "new expiry cycle has not reached 14 yet")
    _passed("renewal preserves old history and quiets the next 90 days")

    # Now move to days=13 against a *different* future expiry. (Setting
    # 14d twice would land on the same calendar date and therefore the
    # same policy_runs key — there'd be nothing for the test to prove.)
    # 13d puts the coach in slot 14, with a fresh expiry calendar date.
    e3 = _set_expiry("coach_marko", 13)
    assert e3 != e1, (
        "test sanity: post-renewal expiry must differ from pre-renewal")
    wwcc_check.run(cid)
    assert _slot_fired("coach_marko", 14, e3)
    bcs = _broadcasts_for("coach_marko")
    # We expect 2 broadcast rows total: one from the original 14-slot,
    # one from the post-renewal 14-slot.
    assert len(bcs) == 2, f"expected 2 broadcasts, got {len(bcs)}: {bcs}"
    _passed("post-renewal slot 14 fires anew (different expiry → new key)")


def test_expired_escalates_and_dedups():
    """Expired coaches must escalate (decision card) but not flood the
    queue across sweeps."""
    init_db(); reset_volatile(); seed(); _wipe_all_expiries()
    _set_expiry("coach_marko", -5)  # expired 5 days ago
    _set_expiry("coach_sara",  -10)  # expired 10 days ago

    cid = _start_cycle()
    out1 = wwcc_check.run(cid)
    assert len(out1["expired"]) == 2, (
        f"expected 2 expired, got {out1['expired']}")
    assert len(out1["reminders"]) == 0, (
        f"expired coaches must not get a reminder: {out1}")

    c = connect()
    n1 = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE kind='wwcc_expired' "
        "AND status='pending'").fetchone()[0]
    c.close()
    assert n1 == 1, f"expected one expired decision, got {n1}"
    _passed("two expired holders → single grouped decision card")

    # Second sweep within the same set of expired ids: dedup blocks the
    # decision, no new card opens.
    out2 = wwcc_check.run(cid)
    c = connect()
    n2 = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE kind='wwcc_expired'"
    ).fetchone()[0]
    c.close()
    assert n2 == 1, f"second sweep must not open a second card, got {n2}"
    _passed("repeat sweep with same expired set → no second decision")


def main():
    tests = [
        ("first sweep picks slot per coach",
         test_first_sweep_picks_correct_slot_per_coach),
        ("idempotent repeated sweeps", test_idempotent_repeated_sweeps_same_day),
        ("missed day catches up", test_missed_day_recovers_next_cycle),
        ("renewal restarts cycle", test_renewal_starts_fresh_cycle),
        ("expired escalates and dedups", test_expired_escalates_and_dedups),
    ]
    passed = failed = 0
    for label, fn in tests:
        try:
            fn()
            passed += 1
        except AssertionError as ex:
            _failed(label, str(ex))
            failed += 1
        except Exception as ex:
            import traceback
            traceback.print_exc()
            _failed(label, f"{type(ex).__name__}: {ex}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

"""Daily anchor-passed schedule ticks.

The bug we're guarding against: WWCC, payment_chase and insurance_check
used to fire only inside a 30-minute strict window
(`if now.hour == N and now.minute < 30`). A cycle that booted at 09:35
would silently lose that day's payment + wwcc tick, and the agent would
go a full extra day without sweeping overdue regos or expiring WWCCs.
This is the exact "Exact-day triggers skip silently" pattern: the slot
disappears with no queue entry, no audit row, no decision.

The fixed form fires at the first cycle on or after the anchor each
day, idempotent via upsert UNIQUE on `<tick>_<today_iso>`. A cold-start
at 11:00 still mints today's pass; a second cycle at 11:30 is absorbed.

These tests pin the new behaviour:
  - mid-morning cold-start (10:30) mints all three ticks even though
    every strict window has already closed
  - second run the same day mints nothing (UNIQUE absorbs)
  - early-morning cold-start (06:00) mints nothing — anchors not yet
    crossed
  - insurance is gated by config; off → never mints
  - explicit anchor-time inversion: payment fires at 09:00 but not
    08:30; WWCC fires at 08:00 but not 07:30
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
import edge.collectors.schedule as sc


def _start_cycle() -> int:
    c = connect()
    cur = c.execute("INSERT INTO cycles(started_at) VALUES(?)", (now_iso(),))
    cid = cur.lastrowid
    c.close()
    return cid


def _ticks_for_today(prefix: str) -> list[str]:
    """Pull schedule inputs whose source_id starts with the given prefix.
    Each tick is keyed `<prefix>_<YYYY-MM-DD>` so any row present means
    that day's tick fired and is idempotent."""
    c = connect()
    rows = c.execute(
        "SELECT id FROM inputs WHERE source='schedule' "
        "AND id LIKE ?", (f"schedule:{prefix}_%",)).fetchall()
    c.close()
    return [r["id"] for r in rows]


def _patch_now(target: datetime):
    class FakeDT:
        @classmethod
        def now(cls):
            return target

        @classmethod
        def fromisoformat(cls, s):
            return datetime.fromisoformat(s)
    return FakeDT


def _run_at(target: datetime) -> dict:
    fake = _patch_now(target)
    real = sc.datetime
    sc.datetime = fake  # type: ignore[assignment]
    try:
        return sc.run(_start_cycle())
    finally:
        sc.datetime = real  # type: ignore[assignment]


def _wednesday_at(h: int, m: int = 0) -> datetime:
    """A Wednesday at the requested local time. Wed (weekday=2) keeps us
    out of Saturday match-day cadence and Sunday newsletter/committee
    territory so this test only exercises the daily anchors."""
    today = datetime.now()
    offset = (2 - today.weekday()) % 7
    if offset == 0:
        offset = 7
    wed = today + timedelta(days=offset)
    return wed.replace(hour=h, minute=m, second=0, microsecond=0)


def test_cold_start_after_anchors_still_mints():
    """An agent restarted at 10:30 should still fire today's WWCC,
    payment_chase, and insurance_check ticks. The strict-window form
    silently lost all three; the anchor-passed form catches up."""
    init_db(); reset_volatile(); seed()

    target = _wednesday_at(10, 30)
    out = _run_at(target)

    today_iso = target.date().isoformat()
    expected = {
        f"schedule:wwcc_check_{today_iso}",
        f"schedule:payment_chase_pass_{today_iso}",
        f"schedule:insurance_check_{today_iso}",
    }
    minted = set(out.get("ticks", []))
    # Only check daily ticks — duty/newsletter/etc may also mint
    # depending on fixture data, those are not the subject here.
    actual = {t for t in minted if any(
        f"{prefix}_{today_iso}" in t for prefix in (
            "wwcc_check", "payment_chase_pass", "insurance_check"))}
    # `ticks` returns just the source_id; normalize for comparison.
    actual_ids = {f"schedule:{t}" if not t.startswith("schedule:") else t
                  for t in actual}
    assert expected.issubset(actual_ids), (
        f"cold-start past anchors should mint all three; missing "
        f"{expected - actual_ids}; got {minted}")
    print("  ✓ 10:30 cold-start mints WWCC + payment + insurance "
          "despite every strict window being closed")


def test_second_run_same_day_absorbed():
    """Idempotency: re-running the collector later the same day must
    not re-mint. UNIQUE(source, source_id) is the seam; a regression
    that drops it would surface as duplicate ticks here."""
    init_db(); reset_volatile(); seed()

    target = _wednesday_at(11, 0)
    first = _run_at(target)
    second_target = target.replace(hour=14, minute=15)
    second = _run_at(second_target)

    today_iso = target.date().isoformat()
    daily_keys = ("wwcc_check", "payment_chase_pass", "insurance_check")

    first_daily = [t for t in first.get("ticks", [])
                   if any(f"{k}_{today_iso}" in t for k in daily_keys)]
    second_daily = [t for t in second.get("ticks", [])
                    if any(f"{k}_{today_iso}" in t for k in daily_keys)]

    assert len(first_daily) >= 3, (
        f"first run should mint the three daily ticks: {first_daily}")
    assert second_daily == [], (
        f"second run same day must mint zero new daily ticks: "
        f"{second_daily}")
    print("  ✓ second run same day absorbed by UNIQUE — no re-mint")


def test_pre_anchor_no_mint():
    """Before 08:00 nothing daily should mint — none of the anchors
    have passed yet."""
    init_db(); reset_volatile(); seed()

    target = _wednesday_at(6, 30)
    out = _run_at(target)
    today_iso = target.date().isoformat()
    daily_keys = ("wwcc_check", "payment_chase_pass", "insurance_check")
    early = [t for t in out.get("ticks", [])
             if any(f"{k}_{today_iso}" in t for k in daily_keys)]
    assert early == [], (
        f"06:30 cold-start must not mint any daily ticks: {early}")
    print("  ✓ 06:30 pre-anchor → nothing daily fires")


def test_anchor_inversion_per_tick():
    """At 08:30 the WWCC anchor (08:00) has passed but the payment
    anchor (09:00) and insurance anchor (09:30) have not. Each anchor
    should fire independently — no shared gate that turns the whole
    block on or off."""
    init_db(); reset_volatile(); seed()

    target = _wednesday_at(8, 30)
    out = _run_at(target)
    today_iso = target.date().isoformat()
    minted = out.get("ticks", [])

    has_wwcc = any(f"wwcc_check_{today_iso}" in t for t in minted)
    has_pay = any(f"payment_chase_pass_{today_iso}" in t for t in minted)
    has_ins = any(f"insurance_check_{today_iso}" in t for t in minted)

    assert has_wwcc, f"08:30 should fire WWCC (anchor 08:00); got {minted}"
    assert not has_pay, (
        f"08:30 must not fire payment yet (anchor 09:00); got {minted}")
    assert not has_ins, (
        f"08:30 must not fire insurance yet (anchor 09:30); got {minted}")
    print("  ✓ 08:30 → WWCC fires, payment + insurance both wait")


def test_insurance_disabled_in_config_never_mints():
    """The insurance tick is config-gated. With insurance.enabled=False
    the daily tick must not mint regardless of clock — Diane plugged
    insurance later in the season and we don't want a stale tick from
    before her config flip."""
    init_db(); reset_volatile(); seed()

    import config as cfg_mod
    original_get = cfg_mod.get

    def fake_get():
        cfg = original_get()
        # Shallow copy + overwrite insurance.enabled
        new = dict(cfg)
        new["insurance"] = {**cfg.get("insurance", {}), "enabled": False}
        return new

    cfg_mod.get = fake_get
    try:
        target = _wednesday_at(11, 0)
        out = _run_at(target)
    finally:
        cfg_mod.get = original_get

    today_iso = target.date().isoformat()
    minted = out.get("ticks", [])
    has_ins = any(f"insurance_check_{today_iso}" in t for t in minted)
    has_wwcc = any(f"wwcc_check_{today_iso}" in t for t in minted)

    assert not has_ins, (
        f"insurance disabled — must not mint; got {minted}")
    assert has_wwcc, (
        f"WWCC unaffected by insurance config — should still mint; "
        f"got {minted}")
    print("  ✓ insurance.enabled=False → no insurance tick; "
          "WWCC unaffected")


def main():
    tests = [
        ("cold-start after anchors mints all daily ticks",
         test_cold_start_after_anchors_still_mints),
        ("second run same day absorbed by UNIQUE",
         test_second_run_same_day_absorbed),
        ("pre-anchor → no daily mints",
         test_pre_anchor_no_mint),
        ("each anchor fires independently",
         test_anchor_inversion_per_tick),
        ("insurance config-gate respected",
         test_insurance_disabled_in_config_never_mints),
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

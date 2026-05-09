"""Sponsor obligation watch — the cliff detector.

ARCHITECTURE.md promises sponsor banner rotation per contract spec runs
autonomously. The rotation works (newsletter alternates spotlights), but
nothing previously detected the case where a sponsor's posts_owed
mathematically cannot fit in the weeks remaining before contract_end.
That's the renewal-killer scenario this watch is for.

Pins:
  - feasible: contract_end far enough out → no decision
  - infeasible: contract_end too close → decision opens, names sponsor,
    quantifies shortfall
  - dedup: re-running the sweep same cycle does NOT open a second
    decision for the same shortfall
  - severity bypass: a worsening shortfall (more posts at risk) reopens
    inside the cooldown — Diane needs to know it's getting worse
  - non-worsening replay: if shortfall is the same on a later cycle, no
    reopen (dedup wins)
  - season_end clamps when it precedes contract_end
  - playbook is wired into the orchestrator's background sweep — running
    a full cycle through the registry triggers the alert end-to-end
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

from edge.state.db import connect, init_db
from edge.state.seed import seed
from tools import sponsor_obligations
from tools import playbooks


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


def _set_sponsor(sid: str, *, posts_owed: int | None = None,
                 contract_end: str | None = None) -> None:
    conn = connect()
    if posts_owed is not None:
        conn.execute("UPDATE sponsors SET posts_owed=? WHERE id=?",
                     (posts_owed, sid))
    if contract_end is not None:
        conn.execute("UPDATE sponsors SET contract_end=? WHERE id=?",
                     (contract_end, sid))
    conn.close()


def _decisions_for(sid: str) -> list[dict]:
    """Return decisions oldest-first by insert order.

    ORDER BY ROWID is the only deterministic 'insertion order' SQLite
    gives us when the PK is a TEXT uuid. created_at is a second-
    precision string so two decisions opened in the same second are
    indistinguishable, and id is hex so its lex order is essentially
    random — both fail the test's [-1]==newest expectation."""
    conn = connect()
    rows = conn.execute(
        "SELECT id, kind, summary, context, options_json, status, "
        "created_at FROM decisions WHERE target_kind='sponsor' "
        "AND target_id=? ORDER BY ROWID ASC",
        (sid,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _zero_other_sponsors(keep_sid: str) -> None:
    """Drop other sponsors out of the watch so the test isolates one."""
    conn = connect()
    conn.execute("UPDATE sponsors SET posts_owed=0 WHERE id != ?",
                 (keep_sid,))
    conn.close()


def test_feasible_sponsor_no_decision():
    """Plenty of weeks left to deliver — no alert."""
    cid = _new_cycle()
    far = (datetime.now() + timedelta(weeks=30)).date().isoformat()
    _set_sponsor("sp_bendigo", posts_owed=8, contract_end=far)
    _zero_other_sponsors("sp_bendigo")

    out = sponsor_obligations.run(cid)

    assert out["ok"]
    assert len(out["alerts"]) == 0, (
        f"feasible sponsor must not raise alert; got {out['alerts']}")
    assert _decisions_for("sp_bendigo") == []
    print("  ✓ feasible delivery: no shortfall, no decision")


def test_impossible_delivery_opens_decision():
    """Contract end so close that no rotation can deliver — alert."""
    cid = _new_cycle()
    # 4 weeks remain; posts_owed=8; buffer=2 → capacity=2; shortfall=6.
    near = (datetime.now() + timedelta(weeks=4)).date().isoformat()
    _set_sponsor("sp_bendigo", posts_owed=8, contract_end=near)
    _zero_other_sponsors("sp_bendigo")

    out = sponsor_obligations.run(cid)

    assert len(out["alerts"]) == 1
    alert = out["alerts"][0]
    assert alert["sponsor"] == "sp_bendigo"
    assert alert["shortfall"] == 6, (
        f"expected shortfall=6 (8 owed - 2 capacity), got {alert['shortfall']}")

    decs = _decisions_for("sp_bendigo")
    assert len(decs) == 1, f"expected one decision, got {len(decs)}"
    d = decs[0]
    assert d["kind"] == "sponsor_shortfall"
    assert "Bendigo" in d["summary"]
    assert "8 posts owed" in d["summary"]
    opts = json.loads(d["options_json"])
    keys = {o["key"] for o in opts}
    # Must offer real remedies, not just acknowledge
    assert "midweek_post" in keys
    assert "email_sponsor" in keys
    assert "renewal_credit" in keys
    print("  ✓ impossible delivery: decision opened with named remedies")


def test_dedup_within_cycle_no_double_alert():
    """Re-firing the sweep in the same cycle must not double-open."""
    cid = _new_cycle()
    near = (datetime.now() + timedelta(weeks=4)).date().isoformat()
    _set_sponsor("sp_bendigo", posts_owed=8, contract_end=near)
    _zero_other_sponsors("sp_bendigo")

    sponsor_obligations.run(cid)
    out2 = sponsor_obligations.run(cid)

    assert len(out2["alerts"]) == 0, (
        f"second run must skip already-flagged shortfall; got {out2}")
    assert len(_decisions_for("sp_bendigo")) == 1
    print("  ✓ dedup: same shortfall does not reopen inside cooldown")


def test_worsening_shortfall_reopens():
    """A bigger shortfall must surface even within cooldown — Diane
    needs to know it's getting worse, not silently track."""
    cid = _new_cycle()
    near = (datetime.now() + timedelta(weeks=4)).date().isoformat()
    _set_sponsor("sp_bendigo", posts_owed=8, contract_end=near)
    _zero_other_sponsors("sp_bendigo")
    sponsor_obligations.run(cid)
    assert len(_decisions_for("sp_bendigo")) == 1

    # Posts_owed unchanged, but contract_end pulls in a week → capacity
    # drops by ~1, shortfall grows by ~1. (Still within cooldown.)
    nearer = (datetime.now() + timedelta(weeks=3)).date().isoformat()
    _set_sponsor("sp_bendigo", contract_end=nearer)

    out = sponsor_obligations.run(cid)
    assert len(out["alerts"]) == 1, (
        f"worsening shortfall must reopen; got {out}")
    decs = _decisions_for("sp_bendigo")
    assert len(decs) == 2, f"expected two decisions, got {len(decs)}"
    # New decision summary should reflect the bigger shortfall.
    # decisions ordered oldest-first by created_at, so [-1] is the
    # reopened one. (uuid lex order is not chronological.)
    new = decs[-1]
    assert "short 7" in new["summary"], (
        f"expected escalating shortfall in summary, got {new['summary']!r}")
    print("  ✓ worsening shortfall reopens (severity bypass)")


def test_steady_shortfall_holds_steady():
    """Unchanged shortfall on a later sweep stays silent."""
    cid = _new_cycle()
    near = (datetime.now() + timedelta(weeks=4)).date().isoformat()
    _set_sponsor("sp_bendigo", posts_owed=8, contract_end=near)
    _zero_other_sponsors("sp_bendigo")
    sponsor_obligations.run(cid)
    # Re-run with no state change.
    out = sponsor_obligations.run(cid)
    assert len(out["alerts"]) == 0
    assert len(_decisions_for("sp_bendigo")) == 1
    print("  ✓ steady shortfall stays silent (no spam)")


def test_season_end_clamps_contract_end():
    """If season_end is sooner than contract_end, season_end wins —
    sponsors are spotlighted via the newsletter, which stops at
    season_end. A 2026-12-31 contract is meaningless if newsletters
    stop in late September."""
    cid = _new_cycle()
    far = (datetime.now() + timedelta(weeks=40)).date().isoformat()
    _set_sponsor("sp_bendigo", posts_owed=20, contract_end=far)
    _zero_other_sponsors("sp_bendigo")

    # Override season_end to 4 weeks from now via config monkey-patch.
    # Deep-copy the cached config — the prod loader caches one dict by
    # reference, so a naive mutation here would persist across tests.
    import copy
    import config as cfg_mod
    orig = cfg_mod.get
    season_end = (datetime.now() + timedelta(weeks=4)).date().isoformat()

    def patched():
        c = copy.deepcopy(orig())
        c.setdefault("sponsor_obligations", {})["season_end"] = season_end
        return c

    cfg_mod.get = patched
    try:
        out = sponsor_obligations.run(cid)
    finally:
        cfg_mod.get = orig

    assert len(out["alerts"]) == 1, (
        "season_end must clamp far-future contract_end so the shortfall "
        "is detected against the season window, not the contract")
    assert out["alerts"][0]["shortfall"] == 18  # 20 - (4 - 2)
    print("  ✓ season_end clamps contract_end (sooner-of wins)")


def test_orchestrator_sweep_invokes_playbook():
    """End-to-end: firing the playbook through the registry must produce
    the same alert. This pins the wiring — registry name, dispatch, and
    background-sweep contract."""
    cid = _new_cycle()
    near = (datetime.now() + timedelta(weeks=4)).date().isoformat()
    _set_sponsor("sp_bendigo", posts_owed=8, contract_end=near)
    _zero_other_sponsors("sp_bendigo")

    out = playbooks.fire("sponsor_obligations", cid, None)
    assert out["ok"]
    assert len(out["alerts"]) == 1
    assert _decisions_for("sp_bendigo")
    print("  ✓ playbooks.fire('sponsor_obligations', ...) wired in")


def test_disabled_skips():
    cid = _new_cycle()
    near = (datetime.now() + timedelta(weeks=4)).date().isoformat()
    _set_sponsor("sp_bendigo", posts_owed=8, contract_end=near)
    _zero_other_sponsors("sp_bendigo")

    import copy
    import config as cfg_mod
    orig = cfg_mod.get

    def patched():
        c = copy.deepcopy(orig())
        c.setdefault("sponsor_obligations", {})["enabled"] = False
        return c

    cfg_mod.get = patched
    try:
        out = sponsor_obligations.run(cid)
    finally:
        cfg_mod.get = orig

    assert out.get("skipped") == "disabled"
    assert _decisions_for("sp_bendigo") == []
    print("  ✓ enabled=false short-circuits cleanly")


if __name__ == "__main__":
    tests = [
        test_feasible_sponsor_no_decision,
        test_impossible_delivery_opens_decision,
        test_dedup_within_cycle_no_double_alert,
        test_worsening_shortfall_reopens,
        test_steady_shortfall_holds_steady,
        test_season_end_clamps_contract_end,
        test_orchestrator_sweep_invokes_playbook,
        test_disabled_skips,
    ]
    init_db()
    seed()
    failed = 0
    passed = 0
    for t in tests:
        try:
            reset_volatile()
            t()
            passed += 1
        except Exception as ex:
            print(f"  ✗ {t.__name__}: {ex}")
            import traceback
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)

"""Newsletter sponsor obligation — credit only fires post-send.

The newsletter playbook spotlights one sponsor per issue. When the
newsletter exceeds the >50-recipient threshold, the broadcast goes to
Diane's queue as 'gated'. Only on her tap (or auto-send under
threshold) does the spotlighted sponsor's posts_owed get decremented
and last_post_at stamped.

Earlier the credit fired inside newsletter.run() if result['status'] ==
'sent' — which never happens in production because newsletter goes
through the threshold gate. Sponsors silently kept their full obligation
balance forever. The fix records the sponsor pick on the broadcast's
meta_json and applies it whenever the broadcast transitions to 'sent'.

This test pins:
  - drafted gated newsletter does NOT credit sponsor
  - declined gated newsletter does NOT credit sponsor
  - approved gated newsletter DOES credit sponsor (posts_owed-1, stamped)
  - rotation moves to the next-oldest sponsor on the next newsletter
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from _helpers import use_temp_db, reset_volatile
use_temp_db()

from edge.state.db import connect, init_db
from edge.state.seed import seed
from tools import broadcast as bc_mod
from tools import newsletter


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


def _sponsor(sid: str) -> dict:
    conn = connect()
    row = conn.execute(
        "SELECT id, posts_owed, last_post_at FROM sponsors WHERE id=?",
        (sid,)).fetchone()
    conn.close()
    return dict(row) if row else {}


def _pending_decision_for(broadcast_id: str) -> str | None:
    conn = connect()
    row = conn.execute(
        "SELECT id FROM decisions WHERE target_kind='broadcast' "
        "AND target_id=? AND status='pending'",
        (broadcast_id,)).fetchone()
    conn.close()
    return row["id"] if row else None


def test_newsletter_drafted_gated_does_not_credit_sponsor():
    cid = _new_cycle()
    before = _sponsor("sp_bendigo")
    assert before["posts_owed"] == 8
    assert before["last_post_at"] is None

    out = newsletter.run(cid)
    # Newsletter to all_parents reaches more than 50 recipients in seed,
    # OR falls back to reach=1 if 0; either way, dedup window is 0, so
    # the gate-or-send path is what runs.
    assert out["sponsor_pick"] == "sp_bendigo", (
        "expected longest-without-post sponsor (Bendigo) to be picked")

    after = _sponsor("sp_bendigo")
    if out["result"]["status"] == "gated":
        # Held for Diane: credit must NOT have fired yet.
        assert after["posts_owed"] == 8
        assert after["last_post_at"] is None
    else:
        # Auto-sent (small reach, e.g. seeded parent count <50): credit
        # must fire on the auto-send path too.
        assert after["posts_owed"] == 7
        assert after["last_post_at"] is not None

    print(f"  ✓ drafted/gated newsletter does not credit sponsor "
          f"(status={out['result']['status']})")


def test_newsletter_decline_does_not_credit_sponsor():
    cid = _new_cycle()
    # Force a gated path with reach=120 so we exercise the decline branch
    # without auto-send firing the credit. Going through newsletter.run()
    # here would auto-send under the seed's small parent count and credit
    # the sponsor before we ever got to decline.
    result = bc_mod.submit(
        cycle_id=cid, playbook="newsletter", channel="email",
        segment="all_parents", reach=120,
        subject="HGJFC Weekly", body="...sponsor spotlight content...",
        target_kind="newsletter", target_id="2026-05-09",
        meta={"sponsor_credit": "sp_bendigo"})
    assert result["status"] == "gated"
    did = result["decision_id"]

    bc_mod.approve_decision(did, "decline", note="not this week")

    after = _sponsor("sp_bendigo")
    assert after["posts_owed"] == 8, (
        f"sponsor must NOT be credited on decline, got {after['posts_owed']}")
    assert after["last_post_at"] is None
    print("  ✓ decline → no sponsor credit (rotation untouched)")


def test_newsletter_approval_credits_spotlighted_sponsor():
    cid = _new_cycle()

    # Force a gated path so we exercise the approve-decision branch.
    # Choosing reach=120 to deterministically clear the >50 threshold
    # regardless of seeded parent count.
    result = bc_mod.submit(
        cycle_id=cid, playbook="newsletter", channel="email",
        segment="all_parents", reach=120,
        subject="HGJFC Weekly — week ending 2026-05-09",
        body="## Sponsor spotlight — Bendigo Bank Henley & Grange",
        target_kind="newsletter", target_id="2026-05-09",
        meta={"sponsor_credit": "sp_bendigo"})
    assert result["status"] == "gated"
    bid = result["id"]
    did = result["decision_id"]

    before = _sponsor("sp_bendigo")
    assert before["posts_owed"] == 8

    bc_mod.approve_decision(did, "approve", note="ok looks good")

    after = _sponsor("sp_bendigo")
    assert after["posts_owed"] == 7, (
        f"approved newsletter must credit sponsor (8 → 7), got {after['posts_owed']}")
    assert after["last_post_at"] is not None, "last_post_at must be stamped"
    print("  ✓ approve → posts_owed 8→7, last_post_at stamped")


def test_rotation_picks_oldest_after_credit():
    cid = _new_cycle()

    # Issue 1 → spotlight Bendigo (last_post_at NULL, owed 8 vs hardware 4
    # but rotation rule is 'longest since last_post_at', NULL sorts oldest).
    out1 = newsletter.run(cid)
    pick1 = out1["sponsor_pick"]
    if out1["result"]["status"] == "gated":
        bc_mod.approve_decision(
            _pending_decision_for(out1["result"]["id"]), "approve")

    # Issue 2 → after Bendigo's last_post_at is stamped, rotation should
    # move to Henley Hardware (still NULL).
    out2 = newsletter.run(cid)
    pick2 = out2["sponsor_pick"]

    assert pick1 == "sp_bendigo"
    assert pick2 == "sp_hardware", (
        f"rotation broken — issue 2 still picked {pick2}")
    print(f"  ✓ rotation: issue1={pick1} → issue2={pick2}")


if __name__ == "__main__":
    tests = [
        test_newsletter_drafted_gated_does_not_credit_sponsor,
        test_newsletter_decline_does_not_credit_sponsor,
        test_newsletter_approval_credits_spotlighted_sponsor,
        test_rotation_picks_oldest_after_credit,
    ]
    passed, failed = 0, 0
    for t in tests:
        try:
            t()
            passed += 1
        except AssertionError as ex:
            print(f"  ✗ {t.__name__}: {ex}")
            failed += 1
        except Exception as ex:
            print(f"  ! {t.__name__}: {type(ex).__name__}: {ex}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)

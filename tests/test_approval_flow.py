"""End-to-end test of the >50-recipient approval gate + one-tap approve.

This is clubrunner's autonomy centrepiece: a club-wide broadcast (heat
cancellation reaching 60 parents) is drafted, the threshold gate blocks
auto-send, a decision opens on Diane's queue, she taps "Approve & send",
and the broadcast flips to 'sent' through the wire stub.

In the demo seed parents-per-team are small, so this test injects a
synthetic 73-recipient draft directly via tools.broadcast.submit — the
same code path the heat playbook uses. We then resolve the decision
through the same /decide POST handler the dashboard uses.
"""
from __future__ import annotations

import json
import sys
import threading
import time
import urllib.request
import urllib.parse
from http.server import HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from _helpers import use_temp_db, reset_volatile
use_temp_db()

from edge.state.db import connect, init_db
from edge.state.seed import seed
from tools import broadcast as bc_mod
import dashboard.server as dash


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


def _fetch(table: str, key: str) -> dict | None:
    conn = connect()
    row = conn.execute(
        f"SELECT * FROM {table} WHERE id=?", (key,)).fetchone()
    conn.close()
    return dict(row) if row else None


def test_threshold_blocks_then_one_tap_sends():
    cid = _new_cycle()

    # Submit a club-wide cancellation. Reach 73 > config threshold 50.
    result = bc_mod.submit(
        cycle_id=cid,
        playbook="heat_policy",
        channel="email",
        segment="club:all_juniors",
        reach=73,
        subject="CANCELLED — all U8/U9/Auskick fixtures Saturday (heat)",
        body=("Club-wide heat policy cancellation. Forecast 38°C exceeds "
              "the 36°C threshold for our youngest age groups. We will "
              "advise on a make-up date.\n\nDiane"),
        target_kind="club_announcement",
        target_id=f"heat:{cid}")

    assert result["status"] == "gated", (
        f"expected gated, got {result['status']} "
        f"(blocked_by={result.get('blocked_by')})")
    assert "broadcast_threshold" in (result.get("blocked_by") or "")
    assert result.get("decision_id"), "no decision created"

    bid = result["id"]
    did = result["decision_id"]

    # Broadcast row should be 'gated', decision should be 'pending'
    bc_row = _fetch("broadcasts", bid)
    dec_row = _fetch("decisions", did)
    assert bc_row["status"] == "gated"
    assert dec_row["status"] == "pending"
    assert dec_row["kind"] == "approval_required"
    options = json.loads(dec_row["options_json"])
    assert any(o["key"] == "approve" for o in options), (
        "approve option missing from decision")

    # Diane taps approve.
    out = bc_mod.approve_decision(did, "approve", note="ok")
    assert out.get("ok") is True

    # Broadcast should now be 'sent'; decision 'resolved'.
    bc_row2 = _fetch("broadcasts", bid)
    dec_row2 = _fetch("decisions", did)
    assert bc_row2["status"] == "sent", (
        f"expected sent, got {bc_row2['status']}")
    assert bc_row2["sent_at"], "sent_at not stamped"
    assert dec_row2["status"] == "resolved"
    assert dec_row2["chosen"] == "approve"

    print(f"  ✓ gate blocked at reach=73, decision={did}, approve→sent")


def test_threshold_blocks_then_decline_cancels():
    cid = _new_cycle()
    result = bc_mod.submit(
        cycle_id=cid, playbook="heat_policy", channel="sms",
        segment="club:all_juniors", reach=120,
        subject="", body="Club-wide test", target_kind="club_announcement",
        target_id=f"sms_decline:{cid}")
    assert result["status"] == "gated"
    did = result["decision_id"]
    bid = result["id"]

    bc_mod.approve_decision(did, "decline", note="too soon")

    bc_row = _fetch("broadcasts", bid)
    assert bc_row["status"] == "cancelled", (
        f"expected cancelled, got {bc_row['status']}")
    print(f"  ✓ decline → broadcast cancelled (no wire send)")


def test_under_threshold_autosends():
    cid = _new_cycle()
    result = bc_mod.submit(
        cycle_id=cid, playbook="duty_reminder", channel="email",
        segment="duty:abc", reach=12,
        subject="Duty reminder", body="Don't forget your shift.",
        target_kind="duty", target_id=f"under:{cid}")
    assert result["status"] == "sent", (
        f"expected sent, got {result['status']}")
    print(f"  ✓ reach=12 under threshold → autosent without decision")


def test_dashboard_post_decide_round_trip():
    """Boot the real dashboard HTTP server on an ephemeral port,
    submit a synthetic gated broadcast, then POST /decide just like the
    button click. Verify the broadcast moves to 'sent'."""
    cid = _new_cycle()
    result = bc_mod.submit(
        cycle_id=cid, playbook="heat_policy", channel="email",
        segment="club:all_juniors_http", reach=88,
        subject="Heat — full cancel", body="Club-wide heat cancel.",
        target_kind="club_announcement", target_id=f"http:{cid}")
    assert result["status"] == "gated"
    did = result["decision_id"]
    bid = result["id"]

    server = HTTPServer(("127.0.0.1", 0), dash.Handler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        # GET / first — confirms render works with state
        with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/", timeout=5) as r:
            assert r.status == 200
            html = r.read().decode("utf-8")
            assert did in html, "pending decision not on dashboard"
            assert "Approve &amp; send" in html, "approve button missing"

        # POST /decide approve
        body = urllib.parse.urlencode(
            {"decision_id": did, "choice": "approve"}).encode("ascii")
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/decide", data=body,
            method="POST")
        # Don't auto-follow the 303 — we want to inspect it.
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                # urllib follows redirects by default; if we got here
                # we're at the redirected target.
                assert r.status == 200
        except urllib.error.HTTPError as ex:
            # Some servers return 303 with no body for HEAD; we only
            # mind if the status is an actual error.
            if ex.code >= 400:
                raise
    finally:
        server.shutdown()
        server.server_close()

    bc_row = _fetch("broadcasts", bid)
    assert bc_row["status"] == "sent", (
        f"expected sent after POST /decide, got {bc_row['status']}")
    print(f"  ✓ HTTP /decide approve → broadcast {bid} sent")


def test_threshold_plus_dedup_does_not_double_open_decision():
    """Real scenario: heat_policy fires, gates hit broadcast_threshold,
    decision opens, broadcast goes to 'gated'. Next cycle (still inside
    the 6h dedup window), the same policy re-evaluates and re-submits
    the same draft.

    The first decision is still pending. Opening a SECOND decision for
    the same logical broadcast turns Diane's queue into garbage. The
    silent dedup hit should win — second submit returns 'skipped' and
    no new decision is opened.
    """
    cid = _new_cycle()

    first = bc_mod.submit(
        cycle_id=cid, playbook="heat_policy", channel="email",
        segment="club:all_juniors", reach=73,
        subject="Heat cancel", body="Heat cancel body",
        target_kind="club_announcement", target_id=f"dup:{cid}")
    assert first["status"] == "gated"
    assert first.get("decision_id"), "first submit must open decision"

    second = bc_mod.submit(
        cycle_id=cid, playbook="heat_policy", channel="email",
        segment="club:all_juniors", reach=73,
        subject="Heat cancel", body="Heat cancel body",
        target_kind="club_announcement", target_id=f"dup:{cid}")
    assert second["status"] == "skipped", (
        f"expected skipped (silent dedup wins over threshold), "
        f"got {second['status']} blocked_by={second.get('blocked_by')}")
    assert second.get("decision_id") is None, (
        "second submit must NOT open a duplicate decision card")

    conn = connect()
    n_pending = conn.execute(
        "SELECT COUNT(*) AS n FROM decisions "
        "WHERE target_id=? AND status='pending'",
        (first["id"],)).fetchone()["n"]
    conn.close()
    assert n_pending == 1, (
        f"exactly one pending decision expected, got {n_pending}")
    print("  ✓ threshold+dedup → skipped, single decision preserved")


def main():
    tests = [
        ("threshold blocks + one-tap approve", test_threshold_blocks_then_one_tap_sends),
        ("threshold blocks + decline cancels", test_threshold_blocks_then_decline_cancels),
        ("under threshold autosends",          test_under_threshold_autosends),
        ("dashboard HTTP /decide round-trip",  test_dashboard_post_decide_round_trip),
        ("threshold+dedup does not double-open", test_threshold_plus_dedup_does_not_double_open_decision),
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

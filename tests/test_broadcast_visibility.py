"""Diane has to be able to read what the agent said in her name.

The agent runs ~25-50 broadcasts a cycle on a match weekend. If she
gets a phone call Monday saying 'you sent me X at 7pm Friday', and
the dashboard only shows counts, she has nothing. This test boots the
real HTTP server and exercises:

  1. The 'Sent in your name' panel lists today's broadcasts.
  2. Each row links to /broadcast/<id> with the full body.
  3. Cancelled and gated broadcasts also appear, so she can audit
     things the agent decided NOT to send.
  4. /broadcast/<unknown> returns 404 and not a server error.
  5. The panel surfaces the related input + decision trail for
     brain-drafted replies, since that's where she most needs context.
"""
from __future__ import annotations

import sys
import threading
import urllib.request
import urllib.error
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
from brain import executive
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


def _serve():
    server = HTTPServer(("127.0.0.1", 0), dash.Handler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server, port


def _get(port, path):
    with urllib.request.urlopen(
            f"http://127.0.0.1:{port}{path}", timeout=5) as r:
        return r.status, r.read().decode("utf-8")


def test_sent_broadcasts_appear_with_clickable_detail():
    cid = _new_cycle()
    sent = bc_mod.submit(
        cycle_id=cid, playbook="duty_reminder", channel="email",
        segment="duty:saturday", reach=12,
        subject="Saturday duty reminder",
        body=("Hi all — quick reminder you're rostered for canteen at "
              "Henley Oval 9am Saturday. Reply STOP to opt out."),
        target_kind="duty", target_id=f"vis_sent:{cid}")
    assert sent["status"] == "sent"
    sent_bid = sent["id"]

    gated = bc_mod.submit(
        cycle_id=cid, playbook="heat_policy", channel="sms",
        segment="club:all_juniors", reach=80,
        subject="", body="Heat cancel club-wide.",
        target_kind="club_announcement", target_id=f"vis_gated:{cid}")
    assert gated["status"] == "gated"
    gated_bid = gated["id"]

    bc_mod.approve_decision(gated["decision_id"], "decline",
                            note="forecast revised down")
    server, port = _serve()
    try:
        code, html = _get(port, "/")
        assert code == 200
        assert "Sent in your name" in html, "panel header missing"
        assert sent_bid in html, "sent broadcast missing from panel"
        assert gated_bid in html, "cancelled broadcast missing from panel"
        # Cancelled rows should be visually distinguishable so Diane
        # doesn't mistake them for things that went out.
        assert "bc-cancelled" in html, "cancelled row class missing"

        code, html = _get(port, f"/broadcast/{sent_bid}")
        assert code == 200
        assert "rostered for canteen" in html, "body not rendered"
        assert "Saturday duty reminder" in html
        assert "duty_reminder" in html  # playbook surfaced
        assert "← back to dashboard" in html

        code, html = _get(port, f"/broadcast/{gated_bid}")
        assert code == 200
        assert "Heat cancel club-wide" in html
        assert "decline" in html, ("decision trail must show what Diane "
                                   "chose for this broadcast")

        try:
            _get(port, "/broadcast/bc_doesnotexist_zzz")
            assert False, "expected 404 for missing broadcast"
        except urllib.error.HTTPError as e:
            assert e.code == 404
    finally:
        server.shutdown()
        server.server_close()
    print(f"  ✓ sent/cancelled both visible; bodies fetchable; 404 clean")


def test_brain_drafted_reply_surfaces_input_and_decision():
    """The most important transparency case: Claude/dry_run drafts a
    reply on Diane's behalf, holds it, she taps approve. She has to be
    able to open the broadcast detail later and see (a) what triggered
    it, (b) what it said, (c) that SHE approved it. Otherwise an
    audit-from-memory ('did I send Naomi a sponsor reply on Tuesday?')
    has no answer."""
    cid = _new_cycle()
    conn = connect()
    conn.execute(
        "INSERT INTO inputs(id, source, source_id, received_at, sender, "
        "subject, body, classification, sentiment, status, priority) "
        "VALUES('imap:sponsor_vis_test','imap','sponsor_vis_test',"
        "datetime('now'),"
        "'naomi.phillips@bendigoadelaide.com.au','Banner artwork update',"
        "'Hi Diane, please use the attached banner for the rest of the "
        "season — thanks.','sponsor','neu','classified',60)")
    conn.close()
    out = executive.think(cid, "imap:sponsor_vis_test")
    assert out["ok"]
    draft_bid = out["draft_broadcast_id"]
    decision_id = out["decision_id"]
    assert draft_bid, "brain must hold a drafted reply for sponsor inbound"

    # Diane approves the draft via the same code path the dashboard uses
    bc_mod.approve_decision(decision_id, "send_draft",
                            note="banner refresh confirmed")

    server, port = _serve()
    try:
        code, html = _get(port, f"/broadcast/{draft_bid}")
        assert code == 200
        assert "Triggered by" in html, ("input section must surface so "
                                        "Diane can see what prompted the reply")
        assert "Banner artwork update" in html
        assert "naomi.phillips" in html
        assert "Decision trail" in html
        assert "send_draft" in html, ("the chosen option must appear so the "
                                      "audit answers 'did I approve this?'")
        assert "banner refresh confirmed" in html
    finally:
        server.shutdown()
        server.server_close()
    print(f"  ✓ brain reply detail surfaces input + decision trail")


def test_overdue_and_coach_reliability_panels_render():
    """Two pieces of state the agent maintains every cycle that Diane
    can't see anywhere else: who hasn't paid rego, and which coaches
    are falling behind on match reports.

    Both sit on quietly-running playbooks (payment_chase, coach_reliability).
    If the dashboard query runs but the render drops the result on the
    floor, the playbook fires invisibly forever. This locks the wiring.
    """
    cid = _new_cycle()
    # Seed already includes 3 players with rego_status='pending' and the
    # coach_memory rows are stamped as fixtures pass through their grace
    # window. Force a coach_memory row in for determinism — the playbook
    # tested in test_coach_reliability writes the same shape.
    conn = connect()
    conn.execute(
        "INSERT OR REPLACE INTO coach_memory(coach_id, reports_due, "
        "reports_submitted, reliability, no_shows, notes) "
        "VALUES('coach_marko', 4, 1, 0.25, 0, NULL)")
    conn.close()

    server, port = _serve()
    try:
        code, html = _get(port, "/")
        assert code == 200
        assert "Overdue registrations" in html, ("panel header missing — "
                                                  "payment_chase running blind")
        # At least one of the seeded pending players surfaces by name.
        assert ("Otis" in html or "Felix" in html or "Jack Dorling" in html), (
            "no overdue player rows rendered — the query was dropped or "
            "the panel is decorative")
        assert "Coach reliability" in html, ("coach_reliability sweep "
                                              "running invisibly")
        assert "coach_marko" in html, "coach row missing from panel"
        assert "rel-low" in html, ("low-reliability coach must be visually "
                                    "distinguishable so Diane can spot the "
                                    "recruiting risk at a glance")
    finally:
        server.shutdown()
        server.server_close()
    print(f"  ✓ overdue + coach reliability panels render with live state")


def main():
    tests = [
        ("sent + cancelled visible, detail clickable",
         test_sent_broadcasts_appear_with_clickable_detail),
        ("brain reply audit trail end-to-end",
         test_brain_drafted_reply_surfaces_input_and_decision),
        ("overdue + coach reliability panels render",
         test_overdue_and_coach_reliability_panels_render),
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

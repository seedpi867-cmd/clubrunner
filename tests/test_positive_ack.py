"""Positive-ack pre-rule check.

Architecture promise (cycle 108 explore):
  > "Three Facebook replies arrived; clubrunner classified them
  >  (one rant, one FAQ, one thanks), drafted responses, and put the
  >  rant in her decision queue tagged 'tone-sensitive.'"

Without a positive-ack short-circuit, every inbound FB/SMS/TeamApp
'thanks team!' falls through the catch-all rules into brain → opens
a generic_brief decision. Across a season that's hundreds of items
in Diane's queue with no judgment to make. The router silent-records
positive_ack so the audit row stays but the queue does not balloon.

Counter-cases this test pins:
  - a 'thanks' that contains a question still escalates (the question
    is the actual ask)
  - 'thanks but ...' still escalates (the qualifier is the real signal)
  - safeguarding pattern beats positive_ack (pre-check ordering)
  - league/IMAP boilerplate 'thanks' is NOT mis-silenced — only
    parent channels (fb/sms/teamapp) get the silent treatment
  - the input ends in status='done' after router runs, NOT 'routed_brain'
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
from triage import classifier, router


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


def _seed_fb(iid: str, body: str, sender: str = "Sam") -> str:
    conn = connect()
    conn.execute(
        "INSERT INTO inputs(id, source, source_id, sender, subject, body, "
        "received_at, status) VALUES(?,?,?,?,?,?, datetime('now'), 'new')",
        (iid, "fb", iid.split(":", 2)[-1], sender,
         "FB message", body))
    conn.close()
    return iid


def _seed_imap(iid: str, body: str, subject: str = "Round 12 fixtures",
               sender: str = "ops@adelaidefootyjuniors.com.au") -> str:
    conn = connect()
    conn.execute(
        "INSERT INTO inputs(id, source, source_id, sender, subject, body, "
        "received_at, status) VALUES(?,?,?,?,?,?, datetime('now'), 'new')",
        (iid, "imap", iid.split(":", 1)[-1], sender, subject, body))
    conn.close()
    return iid


def _status(iid: str) -> str:
    conn = connect()
    row = conn.execute(
        "SELECT status, classification FROM inputs WHERE id=?",
        (iid,)).fetchone()
    conn.close()
    return (row["status"], row["classification"])


def test_pure_thanks_silent_records():
    cid = _new_cycle()
    iid = _seed_fb("fb:message:msg_thanks",
                   "Thanks team — Auskick is great!")
    classifier.run(cid)
    out = router.run(cid)

    assert iid in out["silent"], (
        f"pure positive thanks must silent-record; got router={out}")
    status, cls = _status(iid)
    assert status == "done"
    assert cls == "positive_ack"
    print("  ✓ pure thanks → silent (no brain item, no decision)")


def test_thanks_with_question_escalates():
    """A 'thanks, when is U10 training?' is a FAQ, not an ack — must
    NOT silent-record. The training-FAQ rule should win, sending it to
    a faq_training playbook (NOT brain)."""
    cid = _new_cycle()
    iid = _seed_fb("fb:message:msg_thanks_q",
                   "Thanks Diane! When is U10 training this week?")
    classifier.run(cid)
    out = router.run(cid)

    assert iid not in out["silent"], (
        f"thanks-with-question must not be silent; got {out}")
    # It should match the parent_faq_training rule and route to playbook.
    assert iid in out["playbook"], (
        f"thanks+question should hit faq_training playbook; got {out}")
    # Confirm the rule that fired was specifically the training one —
    # the new split categories are how the orchestrator's playbook
    # dispatcher distinguishes faq_training from faq_register without
    # re-discriminating on body keywords.
    conn = connect()
    cls = conn.execute(
        "SELECT classification FROM inputs WHERE id=?",
        (iid,)).fetchone()["classification"]
    conn.close()
    assert cls == "parent_faq_training", (
        f"expected parent_faq_training classification, got {cls}")
    print("  ✓ thanks + training question → parent_faq_training, not silent")


def test_thanks_but_complaint_escalates():
    """'Thanks but...' is the classic Australian complaint preamble.
    Must NOT silent-record."""
    cid = _new_cycle()
    iid = _seed_fb("fb:message:msg_thanks_but",
                   "Thanks but the U9 game was an absolute shambles.")
    classifier.run(cid)
    out = router.run(cid)

    assert iid not in out["silent"], (
        f"'thanks but ...' must escalate; got {out}")
    print("  ✓ 'thanks but ...' → not silent (qualifier wins)")


def test_safeguarding_beats_positive_ack():
    """Safeguarding pre-check runs first; a 'thanks!' with a
    safeguarding trigger phrase still goes to the brain."""
    cid = _new_cycle()
    # Configured safeguarding patterns include 'alone with' and 'private
    # message'. A 'thanks coach is great, my kid loves the private
    # messages' wording is contrived but it's exactly what we want
    # safeguarding to catch over the positive-ack short-circuit.
    iid = _seed_fb("fb:message:msg_sg",
                   "Thanks team! Coach has been sending private messages "
                   "to my son — he loves the attention!")
    classifier.run(cid)
    out = router.run(cid)

    assert iid in out["brain"], (
        f"safeguarding signal must beat positive_ack; got {out}")
    status, cls = _status(iid)
    assert cls == "safeguarding"
    print("  ✓ safeguarding pattern wins over positive ack")


def test_imap_thanks_not_silenced():
    """League email confirming fixtures often ends '...thanks!' as
    boilerplate. The pre-rule check is scoped to fb/sms/teamapp so
    these still go through full rule classification."""
    cid = _new_cycle()
    iid = _seed_imap("imap:msg_league_thx",
                     "Round 12 fixtures attached. Thanks!")
    classifier.run(cid)
    status, cls = _status(iid)

    assert cls != "positive_ack", (
        f"IMAP boilerplate thanks must not be silent_ack; got cls={cls}")
    print(f"  ✓ IMAP league boilerplate stays in normal classification "
          f"(cls={cls})")


def test_positive_ack_short_only():
    """A long FB message with the word 'thanks' buried in it should not
    silent-record — long messages usually have a real ask too."""
    cid = _new_cycle()
    long_body = ("Thanks for organising last week. " * 12)
    iid = _seed_fb("fb:message:msg_long_thx", long_body)
    classifier.run(cid)
    status, cls = _status(iid)

    # The message has no negative trigger or question, but it's longer
    # than 240 chars — bypasses the pre-check, falls through to the
    # fb_unsorted catch-all and escalates.
    assert cls != "positive_ack", (
        f"long message must not be silent_ack; got cls={cls}")
    print(f"  ✓ long thanks falls through to normal rules (cls={cls})")

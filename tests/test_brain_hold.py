"""Regression: brain-drafted replies must NOT autosend.

The bug: brain.executive.think() was calling broadcast.submit(), which
runs the standard gate stack. With reach=1, no rate-limit collision,
and inside business hours, every gate cleared — so a refund-request
reply or a sponsor reply went out the wire before Diane ever saw the
decision card. The decision options ('Send drafted reply', 'Edit',
'Decline') were theatre because the outbound had already happened.

These tests prove the fix:
  - the brain's draft_reply is held in status='drafted' (not 'sent')
  - the decision the brain opens points to the input
  - choosing a send-option resolves the draft to 'sent'
  - choosing any other option (decline/phone/edit/ack) cancels it

These together close the loop: the brain produces a brief AND a
held draft; Diane resolves once; the broadcast fires (or doesn't)
based on her actual choice.
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
from brain import executive
from tools import broadcast as bc_mod


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


def _seed_input(cid: int, source_id: str, sender: str, subject: str,
                body: str, classification: str = "refund_request",
                sentiment: str = "neg", safeguarding: int = 0) -> str:
    iid = f"imap:{source_id}"
    conn = connect()
    conn.execute(
        "INSERT OR REPLACE INTO inputs(id, source, source_id, received_at, "
        "sender, subject, body, classification, sentiment, safeguarding, "
        "priority, status) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,80,'classified')",
        (iid, "imap", source_id, now_iso(), sender, subject, body,
         classification, sentiment, safeguarding))
    conn.close()
    return iid


def _broadcasts_for(input_id: str) -> list[dict]:
    conn = connect()
    rows = conn.execute(
        "SELECT id, status, channel, body FROM broadcasts "
        "WHERE related_input=?", (input_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _decision_for(input_id: str) -> dict | None:
    conn = connect()
    row = conn.execute(
        "SELECT id, kind, status, target_kind, target_id, context "
        "FROM decisions WHERE target_kind='input' AND target_id=? "
        "ORDER BY created_at DESC LIMIT 1", (input_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def test_refund_brain_holds_draft_until_human_chooses():
    cid = _new_cycle()
    iid = _seed_input(
        cid, "ref_001", "kev.d@hotmail.com",
        "Refund request — Jack's rego",
        "Diane, Jack's broken his arm and won't play this season. "
        "Please refund the rego, $260 paid in March.",
        classification="refund_request", sentiment="neg")

    out = executive.think(cid, iid)
    assert out["ok"] is True, out

    bcs = _broadcasts_for(iid)
    assert len(bcs) == 1, f"expected 1 draft, got {len(bcs)}: {bcs}"
    assert bcs[0]["status"] == "drafted", (
        f"brain draft autosent — status={bcs[0]['status']} "
        f"(must be 'drafted' until Diane resolves)")

    dec = _decision_for(iid)
    assert dec is not None, "no decision opened"
    assert dec["status"] == "pending"
    assert "DRAFTED REPLY" in dec["context"], (
        "drafted reply text not surfaced on the decision card — "
        "Diane can't see what she'd send")

    # Diane taps "Send drafted reply" — broadcast should flip to sent.
    res = bc_mod.approve_decision(dec["id"], "send_draft")
    assert res["ok"] is True
    bcs = _broadcasts_for(iid)
    assert bcs[0]["status"] == "sent"
    print("  ✓ refund draft held → 'send_draft' → sent")


def test_complaint_decline_cancels_draft():
    cid = _new_cycle()
    iid = _seed_input(
        cid, "cmp_001", "angry.parent@gmail.com",
        "Disgusted with the umpire",
        "I am furious with the way the umpire treated my child today. "
        "This is appalling.",
        classification="complaint", sentiment="neg")

    executive.think(cid, iid)
    bcs = _broadcasts_for(iid)
    assert bcs[0]["status"] == "drafted"

    dec = _decision_for(iid)
    bc_mod.approve_decision(dec["id"], "phone")  # phone instead

    bcs = _broadcasts_for(iid)
    assert bcs[0]["status"] == "cancelled", (
        f"phone-instead must cancel the draft, got {bcs[0]['status']}")
    print("  ✓ complaint draft held → 'phone' → cancelled (no wire send)")


def test_safeguarding_brain_no_draft_just_decision():
    """Safeguarding patterns produce no draft_reply by design — the
    only thing the brain returns is a brief telling Diane to call the
    president. Verify zero broadcasts come out of this path."""
    cid = _new_cycle()
    iid = _seed_input(
        cid, "sg_001", "concerned.parent@gmail.com",
        "URGENT — coach behaviour",
        "Coach was alone with my child after training and asked her not "
        "to tell me. I'm extremely uncomfortable.",
        classification="safeguarding", sentiment="neg", safeguarding=1)

    executive.think(cid, iid)
    bcs = _broadcasts_for(iid)
    assert len(bcs) == 0, (
        f"safeguarding produced a broadcast — must never autodraft: {bcs}")
    dec = _decision_for(iid)
    assert dec["kind"] == "safeguarding_brief"
    print("  ✓ safeguarding → decision only, zero broadcasts drafted")


def main():
    tests = [
        ("refund draft held until human chooses send",
         test_refund_brain_holds_draft_until_human_chooses),
        ("complaint draft cancelled when Diane chooses phone",
         test_complaint_decline_cancels_draft),
        ("safeguarding never produces an outbound draft",
         test_safeguarding_brain_no_draft_just_decision),
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

"""Insurance certificate forwarding — quarterly cadence + expiry watch.

ARCHITECTURE.md lists 'Insurance certificate forwarding to council'
as one of the autonomous routine playbooks. Without these tests the
playbook can degrade silently — quiet recurring playbooks are exactly
the kind that get skipped while loud ones absorb sprint time.

Covered:
 1. First-ever cycle (no prior forwards) → quarterly forward fires,
    one decision absent, broadcast sent through gate stack.
 2. Cycle inside the 90-day cadence → no new forward (skipped).
 3. Inbound council request for proof of insurance → forwards
    immediately regardless of cadence.
 4. Expiry inside reminder window → escalates to Diane, no double-
    open if the daily tick re-fires inside the same window.
 5. Past-expiry → no forward (sending stale CoC is worse than none),
    decision still escalates with URGENT framing.
 6. End-to-end through the orchestrator: a council IMAP request +
    a schedule tick on the same day land in the right places.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from _helpers import use_temp_db, reset_volatile
use_temp_db()

from edge.state.db import connect, init_db, now_iso
from edge.state.seed import seed
import config as cfg_mod
from tools import insurance_check
from tools import broadcast as bc_mod


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


def _set_expiry(days_from_today: int) -> str:
    """Mutate the in-memory config to put expiry N days from today."""
    cfg_mod.reload()
    cfg = cfg_mod.get()
    new_expiry = (date.today() + timedelta(days=days_from_today)).isoformat()
    cfg["insurance"]["expiry"] = new_expiry
    return new_expiry


def _restore_config() -> None:
    cfg_mod.reload()


def _count(table: str, where: str = "1=1", params: tuple = ()) -> int:
    conn = connect()
    n = conn.execute(
        f"SELECT COUNT(*) AS n FROM {table} WHERE {where}", params).fetchone()["n"]
    conn.close()
    return n


def test_first_cycle_forwards_and_sends():
    """No prior forwards → cadence_due → broadcast goes to gates →
    reach=1 to council passes every gate → 'sent' on the wire stub."""
    cid = _new_cycle()
    _set_expiry(150)  # well clear of any reminder window

    out = insurance_check.run(cid)
    assert out["mode"] == "cadence_due", (
        f"expected cadence_due on first run, got {out['mode']}")
    assert out["forwarded"]["status"] == "sent", (
        f"reach=1 council forward should clear all gates; "
        f"got {out['forwarded']}")
    assert out.get("escalated") is None, (
        "150-day expiry should not escalate")

    # Audit + policy_runs evidence
    assert _count("policy_runs",
                  "playbook='insurance_forward' AND outcome='sent'") == 1
    assert _count("broadcasts",
                  "playbook='insurance_forward' AND status='sent'") == 1
    print("  ✓ first run → forwarded + sent")
    _restore_config()


def test_inside_cadence_skips():
    """A second tick inside 90 days does nothing."""
    cid = _new_cycle()
    _set_expiry(150)

    insurance_check.run(cid)            # first forward
    out = insurance_check.run(cid)       # immediate re-tick

    assert out["mode"] == "cadence_skip", (
        f"expected cadence_skip, got {out['mode']}")
    assert out["forwarded"] is None
    # Still only one sent broadcast
    assert _count("broadcasts",
                  "playbook='insurance_forward' AND status='sent'") == 1
    print("  ✓ second tick inside cadence → skipped")
    _restore_config()


def test_inbound_council_request_forwards_immediately():
    """A council email asking for the CoC bypasses cadence — even if a
    quarterly forward went out yesterday, the council asked, send it."""
    cid = _new_cycle()
    _set_expiry(150)
    insurance_check.run(cid)             # exhaust the quarterly slot

    # Now the council writes asking for it. requested_by carries through
    # so the playbook bypasses cadence and uses a distinct dedup key.
    out = insurance_check.run(
        cid, requested_by="council.bookings@charlessturt.sa.gov.au")
    assert out["mode"] == "inbound_request", (
        f"expected inbound_request mode, got {out['mode']}")
    assert out["forwarded"]["status"] == "sent", (
        f"council-requested forward must send; got {out['forwarded']}")

    # Two separate sent rows now — quarterly + on-request.
    n_sent = _count("broadcasts",
                    "playbook='insurance_forward' AND status='sent'")
    assert n_sent == 2, f"expected 2 sent rows, got {n_sent}"
    print("  ✓ inbound council request → forwarded outside cadence")
    _restore_config()


def test_expiry_inside_window_opens_decision_no_duplicates():
    """Set expiry to exactly a reminder threshold (30 days). The first
    tick opens a renewal decision; the next tick on the same day must
    not open a second card."""
    cid = _new_cycle()
    _set_expiry(30)

    out1 = insurance_check.run(cid)
    assert out1["escalated"], "expected expiry escalation at 30 days"
    assert out1["escalated"].get("days") == 30
    did1 = out1["escalated"]["decision_id"]
    assert _count("decisions",
                  "kind='insurance_renewal_required' AND status='pending'") == 1

    out2 = insurance_check.run(cid)
    assert out2["escalated"]["reused"] is True, (
        "second tick must reuse the open decision card")
    assert out2["escalated"]["decision_id"] == did1
    assert _count("decisions",
                  "kind='insurance_renewal_required' AND status='pending'") == 1
    print("  ✓ 30-day expiry → one decision, no duplicates")
    _restore_config()


def test_past_expiry_no_forward_but_escalates():
    """Stale CoCs should not be sent to council. Escalation still fires."""
    cid = _new_cycle()
    _set_expiry(-3)

    out = insurance_check.run(cid)
    assert out["mode"] == "expired_no_forward", (
        f"expected expired_no_forward, got {out['mode']}")
    assert out["forwarded"] is None
    assert out["escalated"], "must still escalate when expired"
    assert _count("decisions",
                  "kind='insurance_renewal_required' "
                  "AND status='pending'") == 1
    assert _count("broadcasts",
                  "playbook='insurance_forward' AND status='sent'") == 0
    # The renewal decision context should mention URGENT
    conn = connect()
    ctx = conn.execute(
        "SELECT context FROM decisions "
        "WHERE kind='insurance_renewal_required' LIMIT 1").fetchone()["context"]
    conn.close()
    assert "URGENT" in ctx, "expired/<7d should flag URGENT"
    print("  ✓ expired → no forward + URGENT decision")
    _restore_config()


def test_disabled_config_no_op():
    cid = _new_cycle()
    cfg_mod.reload()
    cfg_mod.get()["insurance"]["enabled"] = False
    out = insurance_check.run(cid)
    assert out.get("skipped") is True
    assert _count("broadcasts",
                  "playbook='insurance_forward'") == 0
    print("  ✓ disabled config → no-op")
    _restore_config()


def test_orchestrator_dispatch_inbound():
    """Inject a council IMAP-style insurance request input directly,
    run classifier+router+execute the way orchestrator does, and prove
    the forward broadcast lands."""
    from edge.state.db import upsert_input
    from triage import classifier as cls_mod
    from triage import router as rt_mod
    from tools import playbooks

    cid = _new_cycle()
    _set_expiry(150)

    iid, _ = upsert_input(
        "imap", "msg_test_insurance",
        sender="council.bookings@charlessturt.sa.gov.au",
        subject="Reminder — certificate of currency required",
        body=("Hi Diane, ahead of next month's bookings could you please "
              "forward your current proof of insurance? Thanks."),
        raw_json="{}")
    assert iid

    cls_mod.run(cid)
    rt = rt_mod.run(cid)
    assert iid in rt["playbook"], (
        f"insurance_request must route to playbook bucket; got {rt}")

    # Mimic orchestrator's _insurance_request branch
    conn = connect()
    row = conn.execute(
        "SELECT sender FROM inputs WHERE id=?", (iid,)).fetchone()
    conn.close()
    out = playbooks.fire_with(
        "insurance_check", cid, iid, requested_by=row["sender"])
    assert out["mode"] == "inbound_request"
    assert out["forwarded"]["status"] == "sent"
    # Broadcast went to council, not to a generic catch-all
    conn = connect()
    seg = conn.execute(
        "SELECT segment FROM broadcasts "
        "WHERE playbook='insurance_forward' "
        "ORDER BY created_at DESC LIMIT 1").fetchone()["segment"]
    conn.close()
    assert "charlessturt.sa.gov.au" in seg, (
        f"forward should target council, got segment={seg}")
    print("  ✓ orchestrator dispatch: imap → triage → forward to council")
    _restore_config()


def main():
    tests = [
        ("first run forwards and sends",
         test_first_cycle_forwards_and_sends),
        ("inside cadence skips", test_inside_cadence_skips),
        ("inbound request bypasses cadence",
         test_inbound_council_request_forwards_immediately),
        ("expiry window: one decision, no duplicates",
         test_expiry_inside_window_opens_decision_no_duplicates),
        ("past expiry: no forward, urgent escalate",
         test_past_expiry_no_forward_but_escalates),
        ("disabled config no-op", test_disabled_config_no_op),
        ("orchestrator dispatch — inbound",
         test_orchestrator_dispatch_inbound),
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

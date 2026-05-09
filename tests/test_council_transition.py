"""Council clash status transition — pending → resolved.

The collector polls the council booking portal once per cycle. A
clash is born 'pending' (escalates to brain → Diane's queue), then
upstream the council usually resolves it within 24h. Before this
fix, both states keyed on the same `clash_<ground>_<window_start>`
source_id, so the second poll's resolution silently collided with
the first poll's pending row. The agent never minted a 'resolved'
input, the council_clash_resolved playbook never fired, and Diane
saw a stale 'council_brief — requires response' card forever.

The fix encodes the status into source_id. A status flip is now a
genuinely new (source, source_id) tuple → upsert_input mints → the
classifier routes the resolved state through council_clash_resolved
→ that playbook closes the resolved input AND auto-resolves the
council_brief decision still pending on the matching 'pending'
sibling input.

Guards:

  1. Pending-only state: classifier escalates, decision row is
     created, no resolution side-effects fire.
  2. pending → resolved transition: a fresh resolved input is
     minted, council_clash_resolved fires, the pending sibling
     input closes, and the council_brief decision auto-resolves.
  3. Resolved-only state (clash already closed when first seen):
     no decision opens; the playbook is a clean noop.
  4. Repeated polls of the same status are absorbed by upsert
     idempotency — no duplicate inputs.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from _helpers import use_temp_db, reset_volatile
use_temp_db()

from edge.state.db import connect, init_db, now_iso
from edge.state.seed import seed
from edge.collectors import council_portal
from triage import classifier, router
from tools import playbooks
from brain import executive


SCENARIO_BAK_PATH = council_portal.SCENARIO


def _write_scenario(payload: dict) -> None:
    """Use a per-test scenario file so we don't trample the live
    today.json that the demo cycle relies on. The collector reads
    SCENARIO directly, so we point its module attribute at our temp
    file for the duration of the test."""
    council_portal.SCENARIO = Path("/tmp/clubrunner_council_test.json")
    council_portal.SCENARIO.write_text(json.dumps(payload))


def _restore_scenario() -> None:
    council_portal.SCENARIO = SCENARIO_BAK_PATH


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


def _drive_full(cid: int) -> None:
    """Slice of the orchestrator: classify → route → playbook OR
    brain. Mirrors what _phase_triage + _phase_brain + _phase_execute
    do, but without the time-based always-on weather sweeps that we
    don't care about for this test."""
    classifier.run(cid)
    rt = router.run(cid)
    for iid in rt["playbook"]:
        conn = connect()
        row = conn.execute(
            "SELECT classification FROM inputs WHERE id=?",
            (iid,)).fetchone()
        conn.close()
        cls = row["classification"] if row else ""
        if cls == "council_resolved":
            playbooks.fire("council_clash_resolved", cid, iid)
    for iid in rt["brain"]:
        executive.think(cid, iid)


def _pending_clash_payload(status: str) -> dict:
    return {
        "council_portal_clashes": [{
            "ground_id": "henley_oval",
            "datetime_window": "2026-05-15T08:00/2026-05-15T10:00",
            "competing_event": "Community fun-run",
            "status": status,
            "note": "Council reviewing overlap with U13 home match.",
        }]
    }


def test_pending_only_escalates_and_holds() -> None:
    cid = _new_cycle()
    try:
        _write_scenario(_pending_clash_payload("pending"))
        council_portal.run(cid)
        _drive_full(cid)

        conn = connect()
        inputs = conn.execute(
            "SELECT id, classification, status FROM inputs "
            "WHERE source='council' ORDER BY received_at").fetchall()
        decisions = conn.execute(
            "SELECT id, kind, status, target_id FROM decisions "
            "WHERE kind='council_brief'").fetchall()
        conn.close()

        assert len(inputs) == 1, (
            f"expected one pending input, got {[dict(r) for r in inputs]}")
        assert inputs[0]["id"].endswith(":pending")
        assert decisions, (
            "pending clash must escalate — no council_brief decision opened")
        assert decisions[0]["status"] == "pending"
        assert decisions[0]["target_id"] == inputs[0]["id"]
        print(f"  ✓ pending clash escalated to council_brief decision "
              f"({decisions[0]['id']})")
    finally:
        _restore_scenario()


def test_pending_then_resolved_transition() -> None:
    cid = _new_cycle()
    try:
        # Cycle A: clash arrives pending.
        _write_scenario(_pending_clash_payload("pending"))
        council_portal.run(cid)
        _drive_full(cid)

        conn = connect()
        pending_input = conn.execute(
            "SELECT id FROM inputs WHERE source='council' "
            "AND id LIKE '%:pending'").fetchone()
        pending_decision = conn.execute(
            "SELECT id, status FROM decisions WHERE kind='council_brief' "
            "AND target_id=?",
            (pending_input["id"],)).fetchone()
        conn.close()
        assert pending_input is not None, "pending input missing after cycle A"
        assert pending_decision is not None, (
            "council_brief decision missing after cycle A")
        assert pending_decision["status"] == "pending"

        # Cycle B: same clash flips to resolved upstream. New cycle
        # row so the policy_runs / log_action FK is happy.
        cid2 = _new_cycle_keep_state()
        _write_scenario(_pending_clash_payload("resolved"))
        council_portal.run(cid2)
        _drive_full(cid2)

        conn = connect()
        all_inputs = conn.execute(
            "SELECT id, classification, status FROM inputs "
            "WHERE source='council' ORDER BY id").fetchall()
        resolved_input = next(
            (r for r in all_inputs if r["id"].endswith(":resolved")), None)
        pending_after = next(
            (r for r in all_inputs if r["id"].endswith(":pending")), None)
        decision_after = conn.execute(
            "SELECT id, status, chosen, resolved_note FROM decisions "
            "WHERE id=?", (pending_decision["id"],)).fetchone()
        conn.close()

        assert resolved_input is not None, (
            "resolution should mint a new ':resolved' input — got "
            f"{[r['id'] for r in all_inputs]}")
        assert resolved_input["classification"] == "council_resolved", (
            "resolved input should classify as council_resolved, got "
            f"{resolved_input['classification']}")
        assert resolved_input["status"] == "done", (
            "resolved input should be closed by the playbook")
        assert pending_after is not None, (
            "pending sibling row should still exist for audit")
        assert pending_after["status"] == "done", (
            "pending sibling should be marked done by the resolution "
            f"playbook, got {pending_after['status']}")
        assert decision_after["status"] == "resolved", (
            "council_brief decision should auto-resolve, got "
            f"{decision_after['status']}")
        assert decision_after["chosen"] == "auto_resolved_upstream"
        assert "auto-closed" in (decision_after["resolved_note"] or "").lower()
        print("  ✓ pending→resolved transition closes sibling input + "
              "auto-resolves council_brief decision")
    finally:
        _restore_scenario()


def test_resolved_only_is_clean_noop() -> None:
    """Clash first seen already-resolved (rare but possible if the
    cycle restarted between pending and resolution upstream). The
    resolution playbook should not crash trying to find a non-
    existent pending sibling."""
    cid = _new_cycle()
    try:
        _write_scenario(_pending_clash_payload("resolved"))
        council_portal.run(cid)
        _drive_full(cid)

        conn = connect()
        resolved_input = conn.execute(
            "SELECT id, status FROM inputs WHERE source='council' "
            "AND id LIKE '%:resolved'").fetchone()
        decisions = conn.execute(
            "SELECT id FROM decisions WHERE kind='council_brief'").fetchall()
        conn.close()

        assert resolved_input is not None
        assert resolved_input["status"] == "done"
        assert not decisions, (
            "no council_brief decision should open for a clash that "
            "is resolved-on-first-sight, got "
            f"{[r['id'] for r in decisions]}")
        print("  ✓ resolved-on-first-sight is a clean noop")
    finally:
        _restore_scenario()


def test_repeated_pending_polls_dedup() -> None:
    """Polling the same pending clash across multiple cycles must not
    mint a new input each cycle — the upsert UNIQUE constraint
    should absorb repeats."""
    cid = _new_cycle()
    try:
        _write_scenario(_pending_clash_payload("pending"))
        for _ in range(3):
            council_portal.run(cid)
        conn = connect()
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM inputs WHERE source='council' "
            "AND id LIKE '%:pending'").fetchone()["n"]
        conn.close()
        assert n == 1, f"expected exactly one pending input, got {n}"
        print(f"  ✓ {3} repeated pending polls produced 1 input")
    finally:
        _restore_scenario()


def _new_cycle_keep_state() -> int:
    """Open a new cycle row WITHOUT wiping volatile state. The
    pending→resolved test needs the prior cycle's input row + decision
    to still exist; we just need a fresh cycle id so the next
    log_action / policy_runs FK resolves."""
    conn = connect()
    cur = conn.execute(
        "INSERT INTO cycles(started_at) VALUES(?)", (now_iso(),))
    cid = cur.lastrowid
    conn.close()
    return cid


if __name__ == "__main__":
    for fn_name, fn in list(globals().items()):
        if fn_name.startswith("test_") and callable(fn):
            print(f"--- {fn_name} ---")
            fn()
    print("OK")

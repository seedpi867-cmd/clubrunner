"""Saturday-8pm injury escalation — end-to-end regression.

The architecture's centerpiece scenario: a parent texts that their kid
has been taken to hospital with a broken arm. The agent must:

  1. Classify the inbound as 'incident' (not refund/complaint).
  2. Open a row in the `incidents` table with severity inferred and the
     league 24h reporting deadline stamped.
  3. Link the player from the parent's email when unambiguous.
  4. Open a decision card on Diane's queue with severity, deadline, the
     last 3 incidents, and operational options. NO email/SMS goes out
     to the parent — Diane phones.
  5. When Diane taps 'League report submitted', the incidents row flips
     to 'reported' with league_report_at stamped.

If any of those steps regresses, the agent has stopped being useful for
the highest-stakes thing it does. These tests guard that.
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
from triage import classifier
from brain import executive
from tools import broadcast as bc_mod
from tools import incidents as inc_mod


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


def _seed_input(cid: int, source_id: str, sender: str,
                subject: str, body: str) -> str:
    iid = f"imap:{source_id}"
    conn = connect()
    conn.execute(
        "INSERT OR REPLACE INTO inputs(id, source, source_id, "
        "received_at, sender, subject, body, status) "
        "VALUES(?,?,?,?,?,?,?, 'new')",
        (iid, "imap", source_id, now_iso(), sender, subject, body))
    conn.close()
    return iid


def test_injury_email_classifies_as_incident():
    """The classifier must put 'broken arm' on the incident path,
    not the refund or complaint paths. Without this, the league
    deadline never starts and Diane misses the 24h window."""
    _new_cycle()
    inp = {
        "source": "imap",
        "sender": "kev.d@hotmail.com",
        "subject": "Jack — at hospital",
        "body": ("Diane, Jack went down hard at Henley Oval. "
                 "Ambulance just left. We're at the Royal Adelaide. "
                 "Looks like a broken arm. Will keep you posted."),
    }
    out = classifier.classify_one(inp)
    assert out["classification"] == "incident", (
        f"expected incident, got {out['classification']}")
    assert out["escalate"] is True
    assert out["priority"] >= 90
    print(f"  ✓ classified as incident (pri={out['priority']})")


def test_brain_opens_incident_row_with_severity_and_deadline():
    """The brain must create a real incidents row, not just a brief.
    Severity is guessed; serious-or-worse gets the 24h deadline."""
    cid = _new_cycle()
    iid = _seed_input(
        cid, "inj_001", "kev.d@hotmail.com",
        "Jack — at hospital",
        "Ambulance was called. He's at the Royal Adelaide with a "
        "suspected broken arm. Concussion possible too.")

    # Pre-classify so router would have routed it to brain
    conn = connect()
    conn.execute(
        "UPDATE inputs SET classification='incident', sentiment='neg', "
        "priority=95, status='classified' WHERE id=?", (iid,))
    conn.close()

    out = executive.think(cid, iid)
    assert out["ok"] is True, out

    # Verify an incident row was created
    conn = connect()
    rows = conn.execute(
        "SELECT id, severity, status, league_report_due, player_id, notes "
        "FROM incidents").fetchall()
    conn.close()
    assert len(rows) == 1, f"expected 1 incident row, got {len(rows)}"
    inc = dict(rows[0])
    assert inc["severity"] in ("serious", "critical"), (
        f"hospital + ambulance must be serious-or-worse, "
        f"got {inc['severity']}")
    assert inc["status"] == "open"
    assert inc["league_report_due"], "league deadline not stamped"
    # Kev → Jack Dorling → u14_boys (only one kid) — should auto-link
    assert inc["player_id"] == "pl_010", (
        f"player auto-link missed: got player_id={inc['player_id']}")

    # Decision opened with operational options
    conn = connect()
    dec = conn.execute(
        "SELECT * FROM decisions WHERE target_id=? ORDER BY created_at "
        "DESC LIMIT 1", (iid,)).fetchone()
    conn.close()
    assert dec is not None, "no decision opened"
    assert dec["kind"] == "incident_brief"
    import json
    options = json.loads(dec["options_json"])
    keys = {o["key"] for o in options}
    assert "report_league" in keys, (
        "league-report option missing — Diane has no path to close "
        "the deadline")
    assert "close_incident" in keys
    assert "ack" in keys
    # No drafted broadcast — incidents are phone-handled, never autodrafted
    bcs = conn.execute(
        "SELECT id FROM broadcasts WHERE related_input=?", (iid,)
    ) if False else None
    conn = connect()
    n_bcs = conn.execute(
        "SELECT COUNT(*) FROM broadcasts WHERE related_input=?",
        (iid,)).fetchone()[0]
    conn.close()
    assert n_bcs == 0, (
        f"incident produced {n_bcs} drafted broadcast(s) — must NEVER "
        "autodraft a public reply to an injury")
    print(f"  ✓ incident {inc['id']} severity={inc['severity']} "
          f"player={inc['player_id']} due={inc['league_report_due'][:16]}")


def test_multi_child_parent_flagged_for_human_disambiguation():
    """If a parent has multiple kids registered, we must NOT guess.
    Notes should flag the ambiguity and player_id stays NULL."""
    cid = _new_cycle()
    # Amelia Chen has two kids: pl_001 (Liam) and pl_002 (Mira)
    iid = _seed_input(
        cid, "inj_002", "amelia.chen@gmail.com",
        "broken wrist at training",
        "She's at the hospital. Broken wrist. We need to talk.")
    conn = connect()
    conn.execute(
        "UPDATE inputs SET classification='incident', sentiment='neg', "
        "priority=95, status='classified' WHERE id=?", (iid,))
    conn.close()

    executive.think(cid, iid)
    conn = connect()
    inc = dict(conn.execute(
        "SELECT player_id, notes FROM incidents").fetchone())
    conn.close()
    assert inc["player_id"] is None, (
        f"multi-kid parent must not auto-link, got "
        f"player_id={inc['player_id']}")
    assert "MULTIPLE KIDS" in (inc["notes"] or ""), (
        f"ambiguity not surfaced in notes: {inc['notes']}")
    print(f"  ✓ multi-child parent → no auto-link, notes flag set")


def test_resolve_report_league_marks_reported():
    """Diane taps 'League report submitted' — incident flips to
    'reported' and gets league_report_at stamped."""
    cid = _new_cycle()
    iid = _seed_input(
        cid, "inj_003", "kev.d@hotmail.com",
        "ambulance called",
        "Jack's been taken to hospital. Suspected concussion + "
        "broken collarbone.")
    conn = connect()
    conn.execute(
        "UPDATE inputs SET classification='incident', sentiment='neg', "
        "priority=95, status='classified' WHERE id=?", (iid,))
    conn.close()

    executive.think(cid, iid)

    conn = connect()
    dec = dict(conn.execute(
        "SELECT id FROM decisions WHERE target_id=? AND "
        "kind='incident_brief'", (iid,)).fetchone())
    inc_before = dict(conn.execute(
        "SELECT id, status, league_report_at FROM incidents").fetchone())
    conn.close()

    assert inc_before["status"] == "open"
    assert inc_before["league_report_at"] is None

    res = bc_mod.approve_decision(dec["id"], "report_league",
                                  note="filed at 22:14")
    assert res["ok"] is True

    conn = connect()
    inc_after = dict(conn.execute(
        "SELECT id, status, league_report_at FROM incidents").fetchone())
    conn.close()
    assert inc_after["status"] == "reported", (
        f"expected reported, got {inc_after['status']}")
    assert inc_after["league_report_at"], (
        "league_report_at not stamped after resolution")
    print(f"  ✓ report_league → incident reported "
          f"(at {inc_after['league_report_at'][:16]})")


def test_idempotent_brain_does_not_double_open():
    """Re-running brain.think on the same input must not create a
    second incidents row. The id is uuid5(input_id) so a re-run
    looks up the existing row instead of inserting a new one."""
    cid = _new_cycle()
    iid = _seed_input(
        cid, "inj_004", "kev.d@hotmail.com",
        "concussion",
        "ambulance came, hospital, suspected concussion")
    conn = connect()
    conn.execute(
        "UPDATE inputs SET classification='incident', sentiment='neg', "
        "priority=95, status='classified' WHERE id=?", (iid,))
    conn.close()

    executive.think(cid, iid)
    # Reset input status so the second think doesn't bail
    conn = connect()
    conn.execute("UPDATE inputs SET status='classified' WHERE id=?", (iid,))
    conn.close()
    executive.think(cid, iid)

    conn = connect()
    n = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    conn.close()
    assert n == 1, f"re-run created duplicate incident: count={n}"
    print(f"  ✓ idempotent — single incident row after two thinks")


def main():
    tests = [
        ("classifier routes 'broken arm' to incident",
         test_injury_email_classifies_as_incident),
        ("brain opens incidents row + brief",
         test_brain_opens_incident_row_with_severity_and_deadline),
        ("multi-kid parent flagged for human pick",
         test_multi_child_parent_flagged_for_human_disambiguation),
        ("'report_league' option flips incident to reported",
         test_resolve_report_league_marks_reported),
        ("brain.think is idempotent on same input",
         test_idempotent_brain_does_not_double_open),
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
            import traceback
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

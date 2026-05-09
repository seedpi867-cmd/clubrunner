"""Coach no-show — manager reports, world-model writes, ambiguity.

The architecture promises: a manager reports their coach didn't turn
up, and clubrunner increments coach_memory.no_shows for that coach,
resolves the team, brief-frames it for Diane, and (after enough
recurrences) flips the framing from welfare-call to recruiting. The
counter is the load-bearing part — Diane's coach panel and the brain's
own framing both read it. If it doesn't increment, the column dies.

These tests guard six things:

  1. Single-team manager OR multi-team manager who names the team in
     the email — coach resolves unambiguously, no_shows +1, brief frames
     as welfare on the first occurrence.
  2. Multi-team manager who does NOT name the team — we MUST NOT pick a
     team at random and bump the wrong coach. Brief flags the ambiguity
     and asks Diane to confirm; no counter increment.
  3. A parent (untrusted reporter) — no counter increment regardless of
     how plausibly they describe it; the brief surfaces the rumour and
     asks Diane to phone the manager first.
  4. Idempotent on (input_id, coach_id): re-running the brain on the
     same input does not double-count, even after a status reset.
  5. Pattern framing: a coach already at 3 no-shows on file flips to
     recruiting language, not welfare.
  6. Renewal/different team: a manager who *is* on file but for a
     team that has no coach assigned — we don't crash, we surface the
     gap so Diane can fix the team record.

Most managers in the seed manage 2-5 teams (Henley & Grange's reality).
That makes the multi-team disambiguation path the dominant one — fixing
it isn't an edge case, it's the common case.
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
        "received_at, sender, subject, body, classification, "
        "sentiment, priority, status) "
        "VALUES(?,?,?,?,?,?,?, 'coach_no_show', 'neg', 78, "
        "'classified')",
        (iid, "imap", source_id, now_iso(), sender, subject, body))
    conn.close()
    return iid


def _no_shows(coach_id: str) -> int:
    conn = connect()
    row = conn.execute(
        "SELECT no_shows FROM coach_memory WHERE coach_id=?",
        (coach_id,)).fetchone()
    conn.close()
    return int(row["no_shows"]) if row else 0


def _decision_for_input(iid: str) -> dict:
    conn = connect()
    row = conn.execute(
        "SELECT * FROM decisions WHERE target_id=? ORDER BY "
        "created_at DESC LIMIT 1", (iid,)).fetchone()
    conn.close()
    return dict(row) if row else {}


def test_classifier_routes_no_show_subject():
    """Subject-line 'coach didn't show' must classify as
    coach_no_show — not as a generic complaint."""
    _new_cycle()
    inp = {
        "source": "imap",
        "sender": "anita.g@example.com",
        "subject": "Coach didn't show at U10 training tonight",
        "body": "Diane, just letting you know the U10 Mixed coach "
                "didn't show at training. Parents covered it.",
    }
    out = classifier.classify_one(inp)
    assert out["classification"] == "coach_no_show", (
        f"expected coach_no_show, got {out['classification']}")
    assert out["escalate"] is True
    print(f"  ✓ classified as coach_no_show (pri={out['priority']})")


def test_manager_with_team_named_increments():
    """Anita manages U8/U9/U10. She names U10 in the body. The
    resolver must pick u10_mixed (coach_alex) and increment."""
    cid = _new_cycle()
    iid = _seed_input(
        cid, "ns_001", "anita.g@example.com",
        "U10 coach no-show",
        "Hi Diane, U10 Mixed coach didn't turn up to training "
        "tonight. Parents handled it but we'll need a chat.")

    before = _no_shows("coach_alex")
    out = executive.think(cid, iid)
    assert out["ok"] is True, out
    after = _no_shows("coach_alex")
    assert after == before + 1, (
        f"coach_alex no_shows: expected +1, got {before}->{after}")
    # Other coaches under same manager untouched
    assert _no_shows("coach_nina") == 0
    assert _no_shows("coach_pete") == 0

    dec = _decision_for_input(iid)
    assert dec.get("kind") == "coach_no_show"
    ctx = dec.get("context") or ""
    assert "U10" in ctx or "u10" in ctx.lower(), (
        f"team identifier missing from brief: {ctx[:300]}")
    assert "Alex Tran" in ctx or "alex.t@example.com" in ctx, (
        f"resolved coach not surfaced: {ctx[:300]}")
    # First-occurrence framing reads as welfare, not recruiting
    assert "welfare" in ctx.lower() or "one-off" in ctx.lower(), (
        f"first no-show should be welfare framing: {ctx[:400]}")
    print(f"  ✓ multi-team manager + team in body → coach_alex +1, "
          f"welfare framing")


def test_multi_team_manager_no_hint_is_ambiguous():
    """Brett manages u11/u12/u13/u14/u16. He emails 'coach no-show'
    with no team identifier. We MUST NOT pick a team at random and
    bump a coach. Brief flags ambiguity, no counter touched."""
    cid = _new_cycle()
    iid = _seed_input(
        cid, "ns_002", "brett.t@example.com",
        "coach didn't show",
        "Diane — coach was a no-show at training tonight. "
        "We need to talk.")

    # Snapshot every coach Brett could be reporting on
    snapshot = {c: _no_shows(c) for c in (
        "coach_jamie", "coach_louise", "coach_ravi", "coach_marko")}

    out = executive.think(cid, iid)
    assert out["ok"] is True, out

    # No coach got incremented — the resolver MUST refuse to guess
    for cid_, before in snapshot.items():
        now = _no_shows(cid_)
        assert now == before, (
            f"coach {cid_} got incremented despite ambiguity: "
            f"{before}->{now}")

    dec = _decision_for_input(iid)
    ctx = (dec.get("context") or "")
    # The brief must explicitly tell Diane the team isn't resolved
    assert ("not auto-resolved" in ctx.lower()
            or "manages multiple teams" in ctx.lower()
            or "team unknown" in ctx.lower()
            or "ambigu" in ctx.lower()), (
        f"brief did not flag ambiguity: {ctx[:400]}")
    print("  ✓ multi-team manager + no hint → no increments, "
          "brief flags ambiguity")


def test_parent_reporter_does_not_increment():
    """A parent reports a coach didn't show. Even if the team is
    obvious (single-team manager would resolve it), the rule is:
    parents are not credible reporters for coach attendance.
    No increment until the manager confirms."""
    cid = _new_cycle()
    iid = _seed_input(
        cid, "ns_003", "kev.d@hotmail.com",
        "U14 coach didn't turn up",
        "Diane — heard the U14 coach didn't show at training.")

    before = _no_shows("coach_ravi")  # u14_boys coach
    out = executive.think(cid, iid)
    assert out["ok"] is True, out
    after = _no_shows("coach_ravi")
    assert after == before, (
        f"parent rumour incremented coach_ravi: {before}->{after}")
    dec = _decision_for_input(iid)
    ctx = (dec.get("context") or "").lower()
    assert ("not on file" in ctx or "unverified" in ctx
            or "manager to confirm" in ctx or "phone" in ctx), (
        f"parent-rumour brief should ask Diane to verify: "
        f"{dec.get('context','')[:400]}")
    print("  ✓ parent reporter → no increment, brief asks Diane to "
          "verify")


def test_idempotent_rerun_does_not_double_count():
    """Re-running the brain on the same input must not double-count,
    even after a status reset (which is how a cycle re-run looks)."""
    cid = _new_cycle()
    iid = _seed_input(
        cid, "ns_004", "anita.g@example.com",
        "U9 coach no-show",
        "U9 Mixed coach didn't make it to training.")

    executive.think(cid, iid)
    assert _no_shows("coach_pete") == 1

    # Cycle re-run — orchestrator might re-classify and re-think
    conn = connect()
    conn.execute(
        "UPDATE inputs SET status='classified' WHERE id=?", (iid,))
    conn.close()
    executive.think(cid, iid)
    executive.think(cid, iid)
    assert _no_shows("coach_pete") == 1, (
        f"re-think double-counted: now {_no_shows('coach_pete')}")
    print("  ✓ three thinks on same input → still +1")


def test_pattern_framing_recruiting_after_three():
    """When a coach already has 3 no-shows on file BEFORE this report,
    the brief framing flips from welfare ('phone them, are they OK')
    to recruiting ('time for an assistant or co-coach')."""
    cid = _new_cycle()
    # Pre-stamp 3 prior no-shows on coach_alex. reset_volatile() wipes
    # coach_memory before seed has a chance to re-insert (seed early-
    # returns when teams is non-empty), so we INSERT OR REPLACE to be
    # robust to either state.
    conn = connect()
    conn.execute(
        "INSERT OR REPLACE INTO coach_memory(coach_id, reports_due, "
        "reports_submitted, no_shows, reliability) "
        "VALUES('coach_alex', 8, 4, 3, 0.5)")
    conn.close()
    iid = _seed_input(
        cid, "ns_005", "anita.g@example.com",
        "U10 coach absent again",
        "U10 Mixed coach didn't turn up — fourth time this season.")

    executive.think(cid, iid)
    # Now at 4
    assert _no_shows("coach_alex") == 4
    dec = _decision_for_input(iid)
    ctx = (dec.get("context") or "").lower()
    assert ("recruiting" in ctx or "assistant" in ctx
            or "co-coach" in ctx or "co coach" in ctx), (
        f"pattern brief should suggest recruiting: "
        f"{dec.get('context','')[:500]}")
    # And the brief should NOT downplay it as welfare-only on the 4th
    assert "first recorded no-show" not in ctx
    print("  ✓ 4th no-show → recruiting framing, not welfare")


def test_unknown_sender_does_not_crash_and_no_increment():
    """A no-show report from someone not on the people table must
    not crash, must not increment any counter, and the brief must
    clearly say the reporter is unverified."""
    cid = _new_cycle()
    iid = _seed_input(
        cid, "ns_006", "random.parent@gmail.com",
        "coach didn't show",
        "Just letting you know the coach was a no-show.")
    # Snapshot all coaches
    coaches = ["coach_marko", "coach_sara", "coach_dean", "coach_alex",
               "coach_pete", "coach_nina", "coach_jamie", "coach_louise",
               "coach_ravi", "coach_helen"]
    snap = {c: _no_shows(c) for c in coaches}

    out = executive.think(cid, iid)
    assert out["ok"] is True, out
    for c, b in snap.items():
        assert _no_shows(c) == b, f"{c} bumped on unknown reporter"
    dec = _decision_for_input(iid)
    ctx = (dec.get("context") or "").lower()
    assert ("not on file" in ctx or "unverified" in ctx
            or "manager to confirm" in ctx), (
        f"unknown reporter brief: {dec.get('context','')[:400]}")
    print("  ✓ unknown sender → no increments, brief says unverified")


def main():
    tests = [
        ("classifier routes no-show subject",
         test_classifier_routes_no_show_subject),
        ("multi-team mgr names team → resolves + increments",
         test_manager_with_team_named_increments),
        ("multi-team mgr no hint → ambiguous, no increment",
         test_multi_team_manager_no_hint_is_ambiguous),
        ("parent reporter does not increment",
         test_parent_reporter_does_not_increment),
        ("idempotent on same input",
         test_idempotent_rerun_does_not_double_count),
        ("pattern framing flips on 4th occurrence",
         test_pattern_framing_recruiting_after_three),
        ("unknown sender does not crash",
         test_unknown_sender_does_not_crash_and_no_increment),
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
            import traceback
            traceback.print_exc()
            print(f"  ✗ {label}: {type(ex).__name__}: {ex}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

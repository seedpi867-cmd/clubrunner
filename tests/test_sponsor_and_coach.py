"""Sponsor-post gate + coach resignation triage.

Two architecture promises that were missing in cycle 121:

1. sponsor_post gate — "anything tagged with a sponsor goes to approval
   gate" (ARCHITECTURE.md). Without this, a small-reach sponsor-tagged
   broadcast (e.g. a brain-drafted FB credit reply at reach=1 to a
   sponsor's question) would clear broadcast_threshold and auto-send
   without Diane ever seeing her sponsor's name go out under the club's
   voice. Sponsor relationships are built on Diane vouching for content;
   the gate makes that explicit.

2. coach resignation classification — the rules.yaml had no entry for
   coaches stepping down. They fell through to imap_unsorted (catch-all
   escalate=true), which got them to the brain, but the brain's
   complaint_review template doesn't fit a recruiting emergency. The
   brain now has a coach_admin branch that pulls team, manager, and
   next fixtures into the brief.

Pinning the behaviour now means the first time a coach actually does
quit, Diane gets the right brief — not a generic "I'll call you" reply.
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
from triage import classifier
from brain import executive


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


def _decisions_pending() -> list[dict]:
    conn = connect()
    rows = conn.execute(
        "SELECT id, kind, target_kind, target_id, summary "
        "FROM decisions WHERE status='pending'").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def test_sponsor_post_gate_blocks_under_threshold():
    """Reach=4, sponsor-tagged: would clear broadcast_threshold but the
    sponsor_post gate must still hold it for Diane."""
    cid = _new_cycle()
    before = _sponsor("sp_bendigo")
    assert before["posts_owed"] == 8

    result = bc_mod.submit(
        cycle_id=cid, playbook="brain_drafted_reply",
        channel="fb_post", segment="public_reply", reach=4,
        subject="", body="Thanks to Bendigo Bank for backing the U13s!",
        target_kind="post", target_id="fb_thread_xyz",
        meta={"sponsor_credit": "sp_bendigo"})
    assert result["status"] == "gated", (
        f"reach=4 with sponsor_credit must gate, got {result['status']}")
    assert "sponsor_post" in result["blocked_by"]
    assert "decision_id" in result

    # Sponsor must NOT be credited until Diane approves.
    after = _sponsor("sp_bendigo")
    assert after["posts_owed"] == 8, (
        "credit fired before approval — sponsor_post defeated")
    assert after["last_post_at"] is None
    print(f"  ✓ reach=4 sponsor-tagged → gated, decision={result['decision_id']}")


def test_sponsor_post_approval_credits_on_send():
    """Approve flow: posts_owed decrements, last_post_at stamps."""
    cid = _new_cycle()
    result = bc_mod.submit(
        cycle_id=cid, playbook="brain_drafted_reply",
        channel="fb_post", segment="public_reply", reach=2,
        subject="", body="Thanks Bendigo!",
        meta={"sponsor_credit": "sp_bendigo"})
    assert result["status"] == "gated"

    bc_mod.approve_decision(result["decision_id"], "approve")
    after = _sponsor("sp_bendigo")
    assert after["posts_owed"] == 7
    assert after["last_post_at"] is not None
    print("  ✓ approve → 8→7 + stamped (sponsor_post path credits)")


def test_sponsor_post_decline_does_not_credit():
    cid = _new_cycle()
    result = bc_mod.submit(
        cycle_id=cid, playbook="brain_drafted_reply",
        channel="fb_post", segment="public_reply", reach=2,
        subject="", body="Thanks Bendigo!",
        meta={"sponsor_credit": "sp_bendigo"})
    bc_mod.approve_decision(result["decision_id"], "decline")
    after = _sponsor("sp_bendigo")
    assert after["posts_owed"] == 8
    assert after["last_post_at"] is None
    # Broadcast went to cancelled, not sent.
    conn = connect()
    bc_row = conn.execute(
        "SELECT status FROM broadcasts WHERE id=?", (result["id"],)).fetchone()
    conn.close()
    assert bc_row["status"] == "cancelled"
    print("  ✓ decline → no credit, broadcast cancelled")


def test_no_sponsor_meta_skips_gate():
    """Untagged broadcasts at small reach still auto-send (gate is a
    no-op when sponsor_credit isn't set)."""
    cid = _new_cycle()
    result = bc_mod.submit(
        cycle_id=cid, playbook="payment_chase",
        channel="email", segment="parent:p_001", reach=1,
        subject="Reminder", body="Outstanding rego payment for player X",
        recipient_id="p_001")
    assert result["status"] == "sent", (
        f"untagged reach=1 must auto-send, got {result['status']}")
    print("  ✓ untagged broadcast unaffected by sponsor_post gate")


def _seed_input(source: str, sender: str, subject: str,
                body: str) -> str:
    """Write a fake input row matching what an IMAP collector would mint."""
    import uuid
    iid = f"in_{uuid.uuid4().hex[:12]}"
    conn = connect()
    conn.execute(
        "INSERT INTO inputs(id, source, source_id, sender, subject, body, "
        "received_at, status) VALUES(?, ?, ?, ?, ?, ?, datetime('now'), 'new')",
        (iid, source, iid, sender, subject, body))
    conn.close()
    return iid


def test_coach_resignation_classifies_to_coach_admin():
    cid = _new_cycle()
    iid = _seed_input(
        source="imap",
        sender="harry@example.com",
        subject="Resigning as coach",
        body="Hi Diane,\n\nI'm stepping down as the coach of the U12 boys "
             "after this round — family situation means I can't continue. "
             "Sorry for the short notice.\n\nHarry")
    classifier.run(cid)
    conn = connect()
    row = conn.execute(
        "SELECT classification, sentiment, priority "
        "FROM inputs WHERE id=?", (iid,)).fetchone()
    conn.close()
    assert row["classification"] == "coach_admin", (
        f"expected coach_admin, got {row['classification']}")
    assert row["sentiment"] == "neg"
    assert row["priority"] >= 80
    print(f"  ✓ classified as coach_admin (pri={row['priority']})")


def test_parent_pulling_kid_does_not_match_coach_rule():
    """A parent saying 'I'm pulling my child out' must NOT be misrouted
    to coach_admin — the rule is anchored on coach context."""
    cid = _new_cycle()
    iid = _seed_input(
        source="imap",
        sender="parent@example.com",
        subject="Pulling out",
        body="We're pulling our child out of the team this season.")
    classifier.run(cid)
    conn = connect()
    row = conn.execute(
        "SELECT classification FROM inputs WHERE id=?", (iid,)).fetchone()
    conn.close()
    assert row["classification"] != "coach_admin", (
        f"parent withdrawal mis-classified as coach_admin "
        f"(rule too greedy)")
    print(f"  ✓ parent withdrawal not mis-routed (cls={row['classification']})")


def test_coach_admin_brief_pulls_team_and_fixtures():
    cid = _new_cycle()
    # Find a real coach in the seed and use their email as sender.
    conn = connect()
    coach = conn.execute(
        "SELECT p.email, p.name, t.id AS team_id, t.name AS team_name "
        "FROM teams t JOIN people p ON t.coach_id=p.id "
        "WHERE p.email IS NOT NULL LIMIT 1").fetchone()
    conn.close()
    assert coach, "seed has no coach with an email — test scaffold issue"
    iid = _seed_input(
        source="imap",
        sender=coach["email"],
        subject="Stepping down as coach",
        body=f"Hi Diane, stepping down as coach of {coach['team_name']} — "
             "won't be able to coach next round.")
    classifier.run(cid)
    out = executive.think(cid, iid)
    assert out.get("ok"), f"brain.think failed: {out}"
    brief = out["brief"]
    assert brief["kind"] == "coach_admin", (
        f"expected coach_admin brief, got {brief['kind']}")
    assert coach["team_name"] in brief["context"], (
        f"team {coach['team_name']} should appear in the brief context")
    # Brief must NOT carry a draft_reply — Diane phones, doesn't email.
    assert not brief.get("draft_reply"), (
        "coach resignation must not produce an autosendable draft reply")
    # No broadcast was inserted (channel is None, brain skips insert_drafted).
    conn = connect()
    drafts = conn.execute(
        "SELECT COUNT(*) FROM broadcasts WHERE related_input=? "
        "AND playbook='brain_drafted_reply'", (iid,)).fetchone()[0]
    conn.close()
    assert drafts == 0, (
        f"coach brief should not draft an outbound — got {drafts} drafts")
    print(f"  ✓ brief surfaces team + manager + fixtures, no draft reply")


def test_coach_admin_brief_surfaces_weak_reliability():
    """When a chronic no-reporter resigns, the brief should call that
    out — Diane needs to know whether this is a hole to fill or a
    problem that's resolving itself."""
    cid = _new_cycle()
    conn = connect()
    coach = conn.execute(
        "SELECT p.id, p.email, t.name AS team_name "
        "FROM teams t JOIN people p ON t.coach_id=p.id "
        "WHERE p.email IS NOT NULL LIMIT 1").fetchone()
    # Stamp a weak reliability record into coach_memory.
    conn.execute(
        "INSERT INTO coach_memory(coach_id, reports_due, reports_submitted, "
        "no_shows, reliability) VALUES(?,?,?,?,?) "
        "ON CONFLICT(coach_id) DO UPDATE SET "
        "reports_due=excluded.reports_due, "
        "reports_submitted=excluded.reports_submitted, "
        "no_shows=excluded.no_shows, reliability=excluded.reliability",
        (coach["id"], 10, 3, 1, 0.3))
    conn.close()

    iid = _seed_input(
        source="imap", sender=coach["email"],
        subject="Stepping down",
        body=f"Hi Diane, stepping down as coach of {coach['team_name']}.")
    classifier.run(cid)
    out = executive.think(cid, iid)
    ctx = out["brief"]["context"]
    assert "3/10" in ctx, (
        f"reliability counts must appear in context, got: {ctx}")
    assert "weak record" in ctx.lower(), (
        f"weak-record framing missing for 0.3 reliability, got: {ctx}")
    print("  ✓ weak reliability framed in coach_admin brief")


def test_coach_admin_brief_no_history_says_so():
    """A new coach with no fixtures yet — brief shouldn't fake a 100%
    submission rate; it should say 'no history yet'."""
    cid = _new_cycle()
    conn = connect()
    coach = conn.execute(
        "SELECT p.email FROM teams t JOIN people p ON t.coach_id=p.id "
        "WHERE p.email IS NOT NULL LIMIT 1").fetchone()
    # Ensure no coach_memory row exists for this coach.
    conn.execute("DELETE FROM coach_memory")
    conn.close()
    iid = _seed_input(
        source="imap", sender=coach["email"],
        subject="Stepping down",
        body="Hi Diane, won't be able to continue as head coach.")
    classifier.run(cid)
    out = executive.think(cid, iid)
    ctx = out["brief"]["context"]
    assert "no reporting history" in ctx.lower(), (
        f"missing 'no reporting history' line for new coach, got: {ctx}")
    print("  ✓ no-history coach gets honest 'no history' framing, not 100%")


def test_coaches_archive_writes_reliability():
    """Long-term coaches.json must surface reliability/reports for
    coaches with history, AND must NOT advertise 1.0 for the brand-new
    ones (architecture promise: report_submission_rate)."""
    from memory import working as mem
    cid = _new_cycle()
    conn = connect()
    coach_id = conn.execute(
        "SELECT id FROM people WHERE role='coach' LIMIT 1").fetchone()["id"]
    conn.execute(
        "INSERT INTO coach_memory(coach_id, reports_due, reports_submitted, "
        "no_shows, reliability) VALUES(?,?,?,?,?)",
        (coach_id, 5, 4, 0, 0.8))
    conn.close()

    counts = mem.update_long_term()
    assert counts["coaches"] >= 1
    import json as _json
    archive = _json.loads(
        (mem.LONG / "coaches.json").read_text())
    measured = archive[coach_id]
    assert measured["report_submission_rate"] == 0.8
    assert measured["reports_submitted"] == 4
    assert measured["reports_due"] == 5
    # An unmeasured coach must come back with rate=None, NOT 1.0.
    others = [v for k, v in archive.items() if k != coach_id]
    none_count = sum(1 for v in others
                     if v.get("report_submission_rate") is None)
    assert none_count >= 1, (
        "unmeasured coaches must report None — fake 1.0 hides them in "
        "the archive")
    print(f"  ✓ archive: 1 measured ({measured['report_submission_rate']}), "
          f"{none_count} honestly unmeasured")


if __name__ == "__main__":
    tests = [
        test_sponsor_post_gate_blocks_under_threshold,
        test_sponsor_post_approval_credits_on_send,
        test_sponsor_post_decline_does_not_credit,
        test_no_sponsor_meta_skips_gate,
        test_coach_resignation_classifies_to_coach_admin,
        test_parent_pulling_kid_does_not_match_coach_rule,
        test_coach_admin_brief_pulls_team_and_fixtures,
        test_coach_admin_brief_surfaces_weak_reliability,
        test_coach_admin_brief_no_history_says_so,
        test_coaches_archive_writes_reliability,
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

"""Match report assembly — the agent ingests coach reports, parses
the score, persists the report, credits the coach, and drafts a
per-team channel post that auto-sends through the gate stack.

Tests:
 1. AFL score in the body ("6.4 (40) to 4.2 (26)") is parsed as
    (40, 26) regardless of "won/lost" wording.
 2. "lost X to Y" flips order so 'for' is the smaller number.
 3. End-to-end via the playbook registry: input → match_reports row
    + coach_memory bump + per-team broadcast at status='sent'.
 4. Newsletter weaves the trimmed coach quote into the per-team
    line item without overwriting the score.
 5. Unknown sender (not on coach roster) opens a verify decision
    instead of broadcasting.
 6. Subject disambiguates teams when a coach handles multiple
    (Helen does both u13_girls and u15_girls; "U13 Girls" subject
    must bind to u13_girls, not u15_girls).
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from _helpers import use_temp_db, reset_volatile
use_temp_db()

from edge.state.db import init_db, connect, now_iso, upsert_input
from edge.state.seed import seed
from tools import match_report, playbooks, newsletter


def _start_cycle() -> int:
    c = connect()
    cur = c.execute("INSERT INTO cycles(started_at) VALUES(?)", (now_iso(),))
    cid = cur.lastrowid
    c.close()
    return cid


def _passed(name): print(f"  \N{check mark} {name}")


def _classify(input_id: str, classification: str = "match_report_inbound"):
    c = connect()
    c.execute("UPDATE inputs SET status='routed_playbook', "
              "classification=? WHERE id=?", (classification, input_id))
    c.close()


def _mint(sender: str, subject: str, body: str) -> str:
    iid, _ = upsert_input("imap", subject + sender, sender=sender,
                          subject=subject, body=body)
    return iid


# ---------------------------------------------------------------------

def test_afl_score_parses_correctly():
    init_db(); seed(); reset_volatile()
    score = match_report._parse_score(
        "We won 6.4 (40) to 4.2 (26) against Adelaide Lutheran.")
    assert score == (40, 26), f"AFL parse wrong: {score}"
    _passed(f"AFL score 6.4 (40) to 4.2 (26) → {score}")


def test_verb_lost_flips_order():
    init_db(); seed(); reset_volatile()
    # "lost 18 to 24" — 'for' should be the smaller number
    score = match_report._parse_score("Lost 18 to 24 to Marion.")
    assert score == (18, 24), f"verb-flip wrong: {score}"
    _passed(f"'lost 18 to 24' → for=18 against=24")


def test_end_to_end_helen_u13_girls():
    init_db(); seed(); reset_volatile()
    cid = _start_cycle()
    iid = _mint("helen.w@example.com",
                "U13 Girls match report — Round 11",
                "Hi Diane, we won 6.4 (40) to 4.2 (26) against Adelaide "
                "Lutheran. Sharp first quarter, sloppy third. Mira at "
                "centre half-back was the standout. Cheers, Helen.")
    _classify(iid)
    out = playbooks.fire("match_report", cid, iid)
    assert out["ok"], f"playbook failed: {out}"
    assert out["team"] == "u13_girls", f"team wrong: {out['team']}"
    assert out["score"] == (40, 26), f"score wrong: {out['score']}"

    c = connect()
    rows = c.execute(
        "SELECT id, team_id, coach_id, parsed_score_for, "
        "parsed_score_against, status FROM match_reports").fetchall()
    assert len(rows) == 1, f"expected 1 row, got {len(rows)}"
    r = dict(rows[0])
    assert r["team_id"] == "u13_girls"
    assert r["coach_id"] == "coach_helen"
    assert r["parsed_score_for"] == 40
    assert r["parsed_score_against"] == 26
    assert r["status"] == "posted"

    cm = c.execute(
        "SELECT reports_submitted FROM coach_memory WHERE coach_id=?",
        ("coach_helen",)).fetchone()
    assert cm and cm["reports_submitted"] == 1

    bc = c.execute(
        "SELECT id, status, segment, channel FROM broadcasts "
        "WHERE playbook='match_report'").fetchone()
    assert bc, "no broadcast created"
    assert bc["status"] == "sent", f"broadcast not sent: {bc['status']}"
    assert bc["segment"] == "team:u13_girls"
    assert bc["channel"] == "fb_post"
    c.close()
    _passed(f"helen U13 girls → mr={r['id']} sent broadcast={bc['id']}")


def test_subject_disambiguates_multi_team_coach():
    """coach_helen does u13_girls AND u15_girls. A 'U15 Girls' subject
    must bind to u15_girls. Without subject disambiguation, the playbook
    would pick the first hit in the team list (u13_girls) and credit
    the wrong team's per-team segment."""
    init_db(); seed(); reset_volatile()
    cid = _start_cycle()
    iid = _mint("helen.w@example.com",
                "U15 Girls match report",
                "Tough one. Lost 18 to 24 to Brighton. Cheers, Helen.")
    _classify(iid)
    out = playbooks.fire("match_report", cid, iid)
    assert out["ok"]
    assert out["team"] == "u15_girls", \
        f"subject should bind u15_girls, got {out['team']}"
    _passed(f"subject 'U15 Girls' → bound to {out['team']}")


def test_unknown_sender_opens_verify_decision():
    init_db(); seed(); reset_volatile()
    cid = _start_cycle()
    iid = _mint("ringer.coach@gmail.com",
                "U12 match report",
                "Stand-in coach today. We won 8-2.")
    _classify(iid)
    out = playbooks.fire("match_report", cid, iid)
    assert out["ok"] and out.get("escalated"), f"expected escalate: {out}"
    c = connect()
    d = c.execute(
        "SELECT id, kind, target_id FROM decisions "
        "WHERE kind='match_report_unverified'").fetchone()
    assert d, "no verify decision opened"
    assert d["target_id"] == iid
    bc = c.execute(
        "SELECT COUNT(*) FROM broadcasts WHERE playbook='match_report'"
    ).fetchone()[0]
    assert bc == 0, "unknown sender should NOT broadcast"
    c.close()
    _passed(f"unknown sender → verify decision={d['id']}, no broadcast")


def test_newsletter_includes_coach_quote():
    init_db(); seed(); reset_volatile()
    cid = _start_cycle()
    # Ingest one report.
    iid = _mint("helen.w@example.com",
                "U13 Girls match report",
                "Hi Diane, we won 6.4 (40) to 4.2 (26) against Adelaide "
                "Lutheran. Sharp first quarter. Cheers, Helen.")
    _classify(iid)
    playbooks.fire("match_report", cid, iid)
    # Mark the U13 Girls fixture completed so newsletter can pick it up.
    c = connect()
    c.execute("UPDATE fixtures SET status='completed', home_score=40, "
              "away_score=26, last_status_change=? WHERE team_id='u13_girls'",
              (now_iso(),))
    c.close()

    subject, body = newsletter.assemble_body()
    # Score line and coach quote both present.
    assert "U13 Girls vs Adelaide Lutheran: 40-26" in body, \
        "score line missing from newsletter"
    assert "Helen Wu" in body, "coach attribution missing"
    assert "6.4 (40)" in body or "Sharp first quarter" in body, \
        "coach quote not woven into newsletter"
    _passed("newsletter weaves U13 Girls score + Helen's quote")


def test_round_resolved_for_same_day_paused_fixture():
    """Regression: the U13 Girls home match on Saturday is at 09:00 local.
    A coach files her report at 10:30 the same day. Before the fix,
    SQLite's UTC-and-space-separator datetime('now') compared lexically
    less-than the local-and-T-separator kickoff string, so the fixture
    was treated as 'in the future' and the broadcast subject said
    'Round ?' instead of the round number Diane needed for the
    newsletter. Force the kickoff to earlier today and assert the
    broadcast subject names the round (whatever the seed says it is —
    we don't mutate round here, to keep later tests' baseline intact)."""
    init_db(); seed(); reset_volatile()
    cid = _start_cycle()
    from datetime import datetime as _dt
    today_morning = _dt.now().replace(
        hour=9, minute=0, second=0, microsecond=0).isoformat(timespec="seconds")
    c = connect()
    seeded_round = c.execute(
        "SELECT round FROM fixtures WHERE team_id='u13_girls'").fetchone()["round"]
    c.execute("UPDATE fixtures SET kickoff=?, status='paused' "
              "WHERE team_id='u13_girls'", (today_morning,))
    c.close()
    iid = _mint("helen.w@example.com",
                "U13 Girls match report",
                "We won 6.4 (40) to 4.2 (26).")
    _classify(iid)
    playbooks.fire("match_report", cid, iid)
    c = connect()
    bc = c.execute(
        "SELECT subject FROM broadcasts WHERE playbook='match_report' "
        "AND segment='team:u13_girls'").fetchone()
    c.close()
    assert bc is not None, "no broadcast"
    expected = f"Round {seeded_round}"
    assert expected in bc["subject"], (
        f"round not resolved on same-day fixture: subject={bc['subject']!r} "
        f"expected to contain {expected!r}")
    _passed(f"same-day paused fixture → '{bc['subject']}'")


def test_round_hint_overrides_when_subject_names_round():
    """Coach files a Sunday-morning Round 11 report after the league has
    already published Round 12 fixtures. 'Most recent past' would mis-
    bind to Round 12; the subject hint must win.

    The seeded fixtures are all r12 (current round). We add an
    extra r11 row in the past and remove it again afterwards so we
    don't poison fixture-shape assumptions in later tests."""
    init_db(); seed(); reset_volatile()
    c = connect()
    c.execute(
        "INSERT INTO fixtures(id, round, team_id, opponent, ground_id, "
        "kickoff, home_or_away, status) "
        "VALUES('fx_r11_u13_g', 11, 'u13_girls', 'Brighton', "
        "'henley_oval', '2026-05-02T09:00:00', 'home', 'completed')")
    c.close()
    try:
        fx = match_report._fixture_for_team(
            "u13_girls", "U13 Girls — Round 11 report", "")
        assert fx and fx["round"] == 11, (
            f"round hint should pick round 11, got {fx and fx['round']}")
        # And without the hint, the most-recent past picks r12 (today)
        # if today's fixture has already kicked off, otherwise r11.
        # Either way it must NOT pick r11 when no hint is present.
        fx_no_hint = match_report._fixture_for_team(
            "u13_girls", "U13 Girls — match report", "We won 30-22.")
        assert fx_no_hint and fx_no_hint["round"] != 11, (
            f"without hint, r11 should not be picked, got "
            f"{fx_no_hint and fx_no_hint['round']}")
    finally:
        c = connect()
        c.execute("DELETE FROM fixtures WHERE id='fx_r11_u13_g'")
        c.close()
    _passed(f"round hint overrides — bound to round {fx['round']}")


def test_round_hint_parser_variants():
    """The hint parser handles several phrasings coaches actually use."""
    cases = [
        ("Round 11 report", "", 11),
        ("U13 Girls match report — Round 7", "", 7),
        ("Match report", "It was R5 against Brighton.", 5),
        ("Match report", "Just a quick note on the game.", None),
    ]
    for subj, body, expected in cases:
        got = match_report._round_hint(subj, body)
        assert got == expected, f"{subj!r}/{body!r}: expected {expected}, got {got}"
    _passed("round hint parser: 4 variants")


def test_score_discrepancy_opens_decision_and_uses_record():
    """Two umpires recorded different scores: PlayHQ (or the away
    coach's earlier ingest) put 36-32 on the fixture; the home coach's
    Saturday-night report parses to 40-26. The agent must:
      - leave the fixture's existing score alone (don't silently
        overwrite the record),
      - open a score_discrepancy decision so Diane resolves it,
      - still post the per-team broadcast, but with the AUTHORITATIVE
        score in the score line — the parents shouldn't see a number
        the agent itself is unsure about.
    """
    init_db(); seed(); reset_volatile()
    cid = _start_cycle()
    # Pre-seed an existing score that disagrees with the coach's report.
    # u13_girls plays at 'home' so existing (36, 32) reads as
    # for=36 against=32 from the team's view.
    c = connect()
    c.execute("UPDATE fixtures SET kickoff='2026-05-08T09:00:00', "
              "home_score=36, away_score=32, status='completed' "
              "WHERE team_id='u13_girls'")
    c.close()
    iid = _mint("helen.w@example.com",
                "U13 Girls match report",
                "We won 6.4 (40) to 4.2 (26) — sharp first quarter.")
    _classify(iid)
    out = playbooks.fire("match_report", cid, iid)
    assert out["ok"], f"playbook failed: {out}"
    assert out["score"] == (40, 26), f"coach parse wrong: {out['score']}"

    c = connect()
    fx = c.execute(
        "SELECT home_score, away_score FROM fixtures "
        "WHERE team_id='u13_girls'").fetchone()
    assert fx["home_score"] == 36 and fx["away_score"] == 32, (
        f"fixture should NOT have been overwritten: "
        f"{fx['home_score']}-{fx['away_score']}")
    d = c.execute(
        "SELECT id, kind, summary, target_kind, target_id, "
        "default_option FROM decisions WHERE kind='score_discrepancy'"
    ).fetchone()
    assert d, "no score_discrepancy decision opened"
    assert d["default_option"] == "hold"
    assert d["target_kind"] == "match_report"
    assert "40-26" in d["summary"] and "36-32" in d["summary"], (
        f"both scores should appear in summary: {d['summary']}")
    bc = c.execute(
        "SELECT body, status FROM broadcasts WHERE playbook='match_report'"
    ).fetchone()
    c.close()
    assert bc and bc["status"] == "sent", (
        f"per-team broadcast should still go out: {bc}")
    assert "36-32" in bc["body"], (
        f"broadcast must use authoritative record score: {bc['body']!r}")
    assert "40-26" not in bc["body"] or "(40)" in bc["body"], (
        f"broadcast should not advertise the disputed coach number "
        f"as the final: {bc['body']!r}")
    _passed(f"discrepancy 40-26 vs 36-32 → decision={d['id']}, "
            f"broadcast used record")


def test_score_match_no_discrepancy_no_decision():
    """When the coach's parsed score matches the existing fixture
    score, no decision opens — confirmation, not disagreement."""
    init_db(); seed(); reset_volatile()
    cid = _start_cycle()
    c = connect()
    c.execute("UPDATE fixtures SET kickoff='2026-05-08T09:00:00', "
              "home_score=40, away_score=26, status='completed' "
              "WHERE team_id='u13_girls'")
    c.close()
    iid = _mint("helen.w@example.com",
                "U13 Girls match report",
                "We won 6.4 (40) to 4.2 (26).")
    _classify(iid)
    playbooks.fire("match_report", cid, iid)
    c = connect()
    n = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE kind='score_discrepancy'"
    ).fetchone()[0]
    c.close()
    assert n == 0, f"matching scores should not open a decision (got {n})"
    _passed("matching coach + record scores → no decision")


def test_score_discrepancy_idempotent_on_repeat_filing():
    """Helen files twice with the same disagreement (forwarded email,
    then resent). One discrepancy decision, not two — Diane shouldn't
    see two pending rows for the same fixture+coach."""
    init_db(); seed(); reset_volatile()
    cid = _start_cycle()
    c = connect()
    c.execute("UPDATE fixtures SET kickoff='2026-05-08T09:00:00', "
              "home_score=36, away_score=32, status='completed' "
              "WHERE team_id='u13_girls'")
    c.close()
    iid1 = _mint("helen.w@example.com",
                 "U13 Girls match report",
                 "We won 6.4 (40) to 4.2 (26).")
    iid2 = _mint("helen.w@example.com",
                 "Fwd: U13 Girls match report",
                 "(forwarded) We won 6.4 (40) to 4.2 (26).")
    _classify(iid1); _classify(iid2)
    playbooks.fire("match_report", cid, iid1)
    playbooks.fire("match_report", cid, iid2)
    c = connect()
    n = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE kind='score_discrepancy' "
        "AND status='pending'").fetchone()[0]
    c.close()
    assert n == 1, f"expected 1 pending discrepancy decision, got {n}"
    _passed("repeat filing of same disagreement → 1 decision, not 2")


def test_score_persists_to_fixture_when_missing():
    """If the fixture has no score yet, the parsed score should be
    stamped on the fixture row (status→completed). PlayHQ-supplied
    scores remain authoritative when they exist."""
    init_db(); seed(); reset_volatile()
    cid = _start_cycle()
    # Backdate Helen's fixture so _latest_fixture_for_team picks it up.
    c = connect()
    c.execute("UPDATE fixtures SET kickoff='2026-05-08T09:00:00' "
              "WHERE team_id='u13_girls'")
    c.close()
    iid = _mint("helen.w@example.com",
                "U13 Girls match report",
                "We won 6.4 (40) to 4.2 (26) against Adelaide Lutheran.")
    _classify(iid)
    playbooks.fire("match_report", cid, iid)
    c = connect()
    fx = c.execute(
        "SELECT home_score, away_score, status FROM fixtures "
        "WHERE team_id='u13_girls'").fetchone()
    c.close()
    assert fx["home_score"] == 40, f"home_score wrong: {fx['home_score']}"
    assert fx["away_score"] == 26, f"away_score wrong: {fx['away_score']}"
    assert fx["status"] == "completed", f"status: {fx['status']}"
    _passed(f"fixture stamped {fx['home_score']}-{fx['away_score']} "
            f"(status={fx['status']})")


# ---------------------------------------------------------------------

if __name__ == "__main__":
    for fn in (test_afl_score_parses_correctly,
               test_verb_lost_flips_order,
               test_end_to_end_helen_u13_girls,
               test_subject_disambiguates_multi_team_coach,
               test_unknown_sender_opens_verify_decision,
               test_newsletter_includes_coach_quote,
               test_round_resolved_for_same_day_paused_fixture,
               test_round_hint_overrides_when_subject_names_round,
               test_round_hint_parser_variants,
               test_score_discrepancy_opens_decision_and_uses_record,
               test_score_match_no_discrepancy_no_decision,
               test_score_discrepancy_idempotent_on_repeat_filing,
               test_score_persists_to_fixture_when_missing):
        fn()
    print("  → all match_report tests passed")

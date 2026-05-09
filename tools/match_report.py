"""Match report assembly from coach inputs.

Promised in ARCHITECTURE.md and AGENT.md as an autonomous workflow:
each Saturday, coaches email a short report on their team's match.
Diane used to read all 14 reports, edit them for tone, and paste them
into the Sunday newsletter and the club Facebook page. The agent does
that itself.

What this playbook does, given a coach's inbound email:

  1. Resolve the coach (by sender email) and their team(s).
  2. Pull the most recent fixture for that team (the one this report
     describes — typically yesterday's match).
  3. Parse a score line from the body — patterns like "we won 6.4 (40)
     to 4.2 (26)", "30-22", "lost 18 to 24". Numbers go into the
     fixtures row if it doesn't already have a score.
  4. Persist the report in match_reports.
  5. Bump coach_memory.reports_submitted (the reliability metric).
  6. Draft a per-team channel post (Facebook page) from the trimmed
     body. Reach is the team's parent count — well under the 50-reach
     threshold so it auto-sends through gates. The body is light-tone
     edited (drops "I think" / "I reckon" / "lol"; collapses runs of
     exclamations) but otherwise preserves the coach's voice.
  7. Closes the input.

If the coach is unknown or the team can't be resolved, opens a brain
brief instead — it's not a routine ack, it's an "is this a real
report?" question.
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta
from typing import Any

from edge.state.db import connect, log_action, now_iso
from tools import broadcast


# --- score parsers ---------------------------------------------------

# AFL score: "6.4 (40) to 4.2 (26)" — total in parens wins.
# No trailing \b because the closing ")" / end-of-string boundary is not
# a word boundary in regex terms (both sides non-word) and the pattern
# would silently fail on the most common shape coaches actually write.
_AFL = re.compile(
    r"(\d{1,2})\.(\d{1,2})\s*\((\d{1,3})\)\s*(?:to|-|vs?\.?)\s*"
    r"(\d{1,2})\.(\d{1,2})\s*\((\d{1,3})\)", re.I)
# Plain "30-22" or "30 to 22"
_PLAIN = re.compile(r"\b(\d{1,3})\s*(?:-|to)\s*(\d{1,3})\b", re.I)
# "lost 18 to 24" / "won 30-22"
_VERB = re.compile(
    r"\b(won|lost|drew)\b[^0-9]{0,40}?(\d{1,3})\s*(?:-|to)\s*(\d{1,3})\b",
    re.I)


def _parse_score(body: str) -> tuple[int, int] | None:
    """Best-effort score extraction. Returns (for_, against) or None.

    AFL takes precedence — if the coach gave us full ladders, use those.
    Verb hints come second and let us flip order from "lost 18 to 24".
    Plain "30-22" is the fallback and assumes home-team-first ordering.
    """
    if not body:
        return None
    # Look for a verb anywhere — used to flip order on AFL or PLAIN.
    verb_match = _VERB.search(body)
    verb = verb_match.group(1).lower() if verb_match else None

    afl = _AFL.search(body)
    if afl:
        a, b = int(afl.group(3)), int(afl.group(6))
        if verb == "lost":
            return (min(a, b), max(a, b))
        if verb == "won":
            return (max(a, b), min(a, b))
        return (a, b)

    if verb_match:
        a, b = int(verb_match.group(2)), int(verb_match.group(3))
        if verb == "lost":
            return (min(a, b), max(a, b))
        if verb == "won":
            return (max(a, b), min(a, b))
        return (a, b)

    plain = _PLAIN.search(body)
    if plain:
        return (int(plain.group(1)), int(plain.group(2)))
    return None


# --- coach + team resolution -----------------------------------------

def _coach_by_email(sender: str) -> dict | None:
    if not sender:
        return None
    conn = connect()
    row = conn.execute(
        "SELECT id, name FROM people "
        "WHERE LOWER(email)=LOWER(?) AND role='coach'",
        (sender,)).fetchone()
    conn.close()
    return dict(row) if row else None


def _team_for_coach(coach_id: str, subject: str) -> dict | None:
    """A coach often coaches more than one team (Marko does u16+auskick_a;
    Helen does u13_girls+u15_girls). The subject usually names the team
    explicitly ('U13 Girls match report'); when it does, use that. When
    it doesn't, fall back to the team whose most recent fixture is the
    most-recently-played one — that's the one the report is about."""
    conn = connect()
    rows = conn.execute(
        "SELECT id, name, age_grade, division FROM teams "
        "WHERE coach_id=?", (coach_id,)).fetchall()
    teams = [dict(r) for r in rows]
    conn.close()
    if not teams:
        return None

    if subject:
        sub_lc = subject.lower()
        for t in teams:
            # Match age (u13) AND division (girls/boys/mixed/auskick) when both
            # are present. Without the division check, "U13 girls match report"
            # binds to U13 Boys whenever it appears first in the coach's list.
            tokens = [t["age_grade"].lower(), t["division"].lower()]
            if all(tok in sub_lc for tok in tokens if tok):
                return t
        # Fallback: just match age token (e.g. "U16 report" + only one
        # u16 team for this coach).
        age_matches = [t for t in teams if t["age_grade"].lower() in sub_lc]
        if len(age_matches) == 1:
            return age_matches[0]

    # No subject hint or no unique match — pick the team whose most
    # recently scheduled fixture is the closest to "yesterday".
    conn = connect()
    placeholders = ",".join("?" * len(teams))
    row = conn.execute(
        f"SELECT team_id FROM fixtures "
        f"WHERE team_id IN ({placeholders}) "
        f"ORDER BY ABS(julianday(kickoff) - julianday('now')) ASC LIMIT 1",
        [t["id"] for t in teams]).fetchone()
    conn.close()
    if row:
        for t in teams:
            if t["id"] == row["team_id"]:
                return t
    return teams[0]


_ROUND_HINT = re.compile(
    r"(?i)\bround\s*(\d{1,2})\b|\br(\d{1,2})\b")


def _round_hint(subject: str, body: str) -> int | None:
    """Pull a round number from the coach's subject or body if they named
    one. Coaches sometimes file Saturday's report on Sunday morning, by
    which time the next round's fixture has been published — taking
    'most recent past' would then bind the report to the wrong week.
    'Round 11', 'R11', 'round 11 report' all match."""
    for src in (subject or "", body or ""):
        m = _ROUND_HINT.search(src)
        if m:
            return int(m.group(1) or m.group(2))
    return None


def _fixture_for_team(team_id: str, subject: str = "",
                      body: str = "") -> dict | None:
    """The fixture this report is about. Resolution order:
      1. If subject/body names a round number AND we have a fixture for
         that team in that round, prefer it. Survives the case where a
         late report describes a round that's no longer the most recent.
      2. Else the most-recent fixture with kickoff <= now (local).
      3. Else the most-recent fixture overall (covers a coach filing
         pre-emptively, or a fixture in the future-tense state machine).

    Why not `kickoff <= datetime('now')`? SQLite's datetime('now') is
    UTC and uses a space separator, while our kickoff strings are local
    (Adelaide) and use the ISO 'T' separator. Lexical compare flips the
    sign in two ways: (a) UTC < local for any same-day Adelaide kickoff
    because Adelaide is +9:30, and (b) ' ' < 'T' so the separator drifts
    even when the dates would tie. A paused fixture at 09:00 local on
    the same day reads as 'in the future' and the round falls back to
    '?'. Pass an ISO local string from Python and the comparison lines
    up.
    """
    cols = ("id, round, opponent, kickoff, home_score, away_score, "
            "home_or_away, status")
    conn = connect()
    rnd = _round_hint(subject, body)
    if rnd is not None:
        row = conn.execute(
            f"SELECT {cols} FROM fixtures WHERE team_id=? AND round=? "
            f"ORDER BY kickoff DESC LIMIT 1", (team_id, rnd)).fetchone()
        if row:
            conn.close()
            return dict(row)
    row = conn.execute(
        f"SELECT {cols} FROM fixtures WHERE team_id=? AND kickoff <= ? "
        f"ORDER BY kickoff DESC LIMIT 1",
        (team_id, now_iso())).fetchone()
    if not row:
        row = conn.execute(
            f"SELECT {cols} FROM fixtures WHERE team_id=? "
            f"ORDER BY kickoff DESC LIMIT 1", (team_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


# Back-compat alias — older tests imported the older name.
_latest_fixture_for_team = _fixture_for_team


# --- body cleaning ---------------------------------------------------

_FILLER = re.compile(
    r"\b(i think|i reckon|honestly|tbh|like really|like|just saying)\b",
    re.I)
_BANG_RUN = re.compile(r"!{2,}")
_LOL = re.compile(r"\b(lol|haha+|lmao)\b", re.I)


def _light_edit(body: str) -> str:
    s = body or ""
    s = _FILLER.sub("", s)
    s = _LOL.sub("", s)
    s = _BANG_RUN.sub("!", s)
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


# --- reach ------------------------------------------------------------

def _team_parent_reach(team_id: str) -> int:
    conn = connect()
    n = conn.execute(
        "SELECT COUNT(DISTINCT parent_id) FROM players "
        "WHERE team_id=? AND parent_id IS NOT NULL", (team_id,)).fetchone()[0]
    conn.close()
    return int(n)


# --- coach memory -----------------------------------------------------

def _bump_reliability(coach_id: str) -> None:
    """Coach reliability = reports_submitted / max(reports_due, 1).

    reports_due is incremented elsewhere (by a 'fixture completed'
    side-effect that doesn't exist in v1); the field is here so the
    architecture's promised reliability scoring lights up the moment a
    real source starts feeding it. For now we use submission count
    directly as the reliability signal — every report submitted bumps
    it; never decays inside this playbook."""
    conn = connect()
    conn.execute(
        "INSERT INTO coach_memory(coach_id, reports_submitted, reliability) "
        "VALUES(?, 1, 1.0) "
        "ON CONFLICT(coach_id) DO UPDATE SET "
        "reports_submitted = reports_submitted + 1",
        (coach_id,))
    # Recompute reliability against reports_due if it's been seeded by
    # the schedule path; otherwise leave reliability where it is.
    conn.execute(
        "UPDATE coach_memory SET reliability = "
        "  CASE WHEN reports_due > 0 "
        "    THEN MIN(1.0, CAST(reports_submitted AS REAL)/reports_due) "
        "    ELSE reliability END "
        "WHERE coach_id=?", (coach_id,))
    conn.close()


# --- escalation -------------------------------------------------------

def _open_unknown_coach_decision(cycle_id: int, input_id: str,
                                  sender: str) -> str:
    import json
    did = f"dec_{uuid.uuid4().hex[:12]}"
    conn = connect()
    conn.execute(
        "INSERT INTO decisions(id, cycle_id, kind, target_kind, target_id, "
        "summary, context, options_json, default_option, created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (did, cycle_id, "match_report_unverified", "input", input_id,
         f"Match report from unknown sender — {sender}",
         (f"From: {sender}\n\nThis email looks like a match report but the "
          f"sender isn't on the coach roster. Could be a parent, a guest "
          f"coach, or someone using a personal address."),
         '[{"key":"link_coach","label":"Link sender to a coach"},'
         '{"key":"discard","label":"Not a match report — discard"}]',
         "link_coach", now_iso()))
    conn.close()
    return did


def _open_score_discrepancy(cycle_id: int, report_id: str, fixture: dict,
                             team: dict, coach: dict,
                             disc: dict) -> str:
    """Open a decision when the coach's parsed score disagrees with the
    fixture's existing record. Idempotent on (fixture_id, coach_id):
    if a coach files twice with the same disagreement, we don't double-
    open. A *different* coach reporting the same fixture with a third
    number opens its own decision — that's a separate disagreement to
    resolve.

    The decision keys the broadcast at the report level (target_kind=
    match_report) so the dashboard ties it back to the source row, but
    the body names the fixture for human readability.
    """
    import json
    from tools.broadcast import _open_decision
    fid = fixture.get("id") if fixture else None
    # Idempotency: same fixture + same coach + still pending → skip.
    if fid:
        conn = connect()
        existing = conn.execute(
            "SELECT d.id FROM decisions d "
            "JOIN match_reports m ON m.id = d.target_id "
            "WHERE d.kind='score_discrepancy' AND d.status='pending' "
            "AND m.fixture_id=? AND m.coach_id=? AND m.id != ?",
            (fid, coach["id"], report_id)).fetchone()
        conn.close()
        if existing:
            return existing["id"]
    e_for, e_against = disc["existing"]
    c_for, c_against = disc["coach"]
    summary = (f"Score discrepancy: {team['name']} "
               f"{c_for}-{c_against} per coach vs "
               f"{e_for}-{e_against} per record")
    rnd = (fixture or {}).get("round") or "?"
    opp = (fixture or {}).get("opponent") or "?"
    context = (
        f"{team['name']} vs {opp} (Round {rnd})\n\n"
        f"Coach {coach['name']}'s match report parsed a final score of "
        f"{c_for}-{c_against} (for-against, from {team['name']}'s view).\n"
        f"The fixture record currently shows {e_for}-{e_against} "
        f"(for-against, from {team['name']}'s view).\n\n"
        f"Per-team broadcast went out using the fixture record. If "
        f"the coach's score is correct, use 'use_coach' — the fixture "
        f"will be re-stamped and a brief correction posted to the team "
        f"channel. If the record is correct, dismiss; the report "
        f"stays archived with its parsed score for audit.")
    options = [
        {"key": "use_coach",
         "label": f"Coach is correct — record {c_for}-{c_against}"},
        {"key": "use_record",
         "label": f"Record is correct — keep {e_for}-{e_against}"},
        {"key": "hold",
         "label": "Hold — verify with the umpire first"},
    ]
    return _open_decision(
        cycle_id, "score_discrepancy", "match_report", report_id,
        summary, context, options, "hold")


# --- main entrypoint --------------------------------------------------

def run(cycle_id: int, related_input: str | None = None) -> dict[str, Any]:
    if not related_input:
        # match_report is purely input-driven — there's no schedule
        # tick that fires it. If the orchestrator dispatched without
        # an input, something upstream is wrong.
        return {"ok": False, "reason": "no input"}

    conn = connect()
    inp = conn.execute(
        "SELECT id, sender, subject, body FROM inputs WHERE id=?",
        (related_input,)).fetchone()
    conn.close()
    if not inp:
        return {"ok": False, "reason": "input not found"}

    coach = _coach_by_email(inp["sender"])
    if not coach:
        did = _open_unknown_coach_decision(
            cycle_id, related_input, inp["sender"] or "(no sender)")
        log_action(cycle_id, "execute", "match_report_unverified",
                   "input", related_input, True,
                   f"sender={inp['sender']} decision={did}")
        return {"ok": True, "escalated": True, "decision_id": did}

    team = _team_for_coach(coach["id"], inp["subject"] or "")
    if not team:
        log_action(cycle_id, "execute", "match_report_no_team",
                   "input", related_input, False,
                   f"coach={coach['id']}")
        return {"ok": False, "reason": "no team"}

    fixture = _fixture_for_team(team["id"], inp["subject"] or "",
                                inp["body"] or "")
    score = _parse_score(inp["body"] or "")

    # Discrepancy detection: when the fixture already has a score (from
    # PlayHQ, an umpire entry, or an earlier coach report) AND this
    # coach's report parses to a different score, that's a real "two
    # umpires recorded different scores" event from the explore doc.
    # We don't silently keep the existing or silently overwrite — both
    # mask the disagreement. The fixture's existing score stays
    # authoritative for the per-team broadcast and the newsletter; a
    # decision row carries the discrepancy to Diane so she can pick.
    discrepancy = None
    if fixture and score and fixture["home_score"] is not None:
        if fixture["home_or_away"] == "home":
            existing = (fixture["home_score"], fixture["away_score"])
        else:
            existing = (fixture["away_score"], fixture["home_score"])
        if existing != score:
            discrepancy = {"existing": existing, "coach": score}

    # Persist the report.
    rid = f"mr_{uuid.uuid4().hex[:12]}"
    conn = connect()
    conn.execute(
        "INSERT INTO match_reports(id, fixture_id, team_id, coach_id, "
        "round, body, parsed_score_for, parsed_score_against, "
        "submitted_at, source_input, status) "
        "VALUES(?,?,?,?,?,?,?,?,?,?, 'received')",
        (rid, (fixture or {}).get("id"), team["id"], coach["id"],
         (fixture or {}).get("round"),
         (inp["body"] or "")[:4000],
         (score or (None, None))[0], (score or (None, None))[1],
         now_iso(), related_input))
    # Stamp the fixture's score if we parsed one and the fixture is
    # missing it. Don't overwrite — PlayHQ's score is authoritative
    # if it's already been ingested. Discrepancies are surfaced via the
    # decision queue, not by silent overwrite.
    if fixture and score and fixture["home_score"] is None:
        if fixture["home_or_away"] == "home":
            home, away = score
        else:
            away, home = score
        conn.execute(
            "UPDATE fixtures SET home_score=?, away_score=?, "
            "status='completed', last_status_change=? WHERE id=?",
            (home, away, now_iso(), fixture["id"]))
    conn.close()

    _bump_reliability(coach["id"])

    if discrepancy:
        _open_score_discrepancy(
            cycle_id, rid, fixture, team, coach, discrepancy)

    # Per-team Facebook post from the lightly-edited body. When there's
    # a discrepancy, the score line uses the authoritative existing
    # score (PlayHQ / first-recorded) — broadcasting the coach's parsed
    # number when it disagrees with the record would put a contradicted
    # score in front of 30 parents and is worse than no score at all.
    edited = _light_edit(inp["body"])
    short = edited if len(edited) <= 800 else edited[:780].rstrip() + "…"
    score_line = ""
    if discrepancy:
        score_line = (f"Final: {discrepancy['existing'][0]}-"
                      f"{discrepancy['existing'][1]} (per league record).\n\n")
    elif score:
        score_line = f"Final: {score[0]}-{score[1]}.\n\n"
    subject = (f"{team['name']} — Round "
               f"{(fixture or {}).get('round') or '?'} match report")
    body = (
        f"{team['name']}'s match report from coach {coach['name']}.\n\n"
        f"{score_line}{short}\n\n"
        "— posted by clubrunner on behalf of the coach.")
    reach = _team_parent_reach(team["id"]) or 1
    res = broadcast.submit(
        cycle_id=cycle_id, playbook="match_report", channel="fb_post",
        segment=f"team:{team['id']}", reach=reach,
        subject=subject, body=body,
        target_kind="match_report", target_id=rid,
        related_input=related_input)

    if res["status"] == "sent":
        conn = connect()
        conn.execute(
            "UPDATE match_reports SET status='posted', "
            "posted_broadcast_id=? WHERE id=?", (res["id"], rid))
        conn.close()

    log_action(cycle_id, "execute", "match_report_processed",
               "match_report", rid, True,
               f"coach={coach['id']} team={team['id']} "
               f"score={score} reach={reach} status={res['status']}")
    return {"ok": True, "report_id": rid, "team": team["id"],
            "coach": coach["id"], "score": score,
            "broadcast": res}

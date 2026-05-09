"""Coach no-show world-model writes.

When the brain handles a `coach_no_show` input, two things must happen
before the offline/Claude brief is built:

  1. Resolve which coach the report is about. The most reliable signal
     is sender → manager → team → coach. A team manager reporting on
     their own coach is the credible reporter. A random parent saying
     "I heard the coach didn't show" is not — the manager hasn't
     confirmed it yet, and we don't increment the counter on rumour.

     A manager often handles multiple teams (Henley & Grange's reality:
     mgr_brett covers U11-U14 + U16, mgr_kelly covers all 3 Auskick
     groups). When the manager-team-coach link isn't 1:1, we look in
     the subject + body for an unambiguous team identifier (age grade
     "U10", division "U13 Girls", or "Auskick A/B/C"). If we still
     can't pin it down, we MUST NOT pick a team at random — the
     no_shows counter would land on the wrong coach. We mark the case
     ambiguous and let the brief ask Diane to confirm.

  2. Increment `coach_memory.no_shows` exactly once per (input_id,
     coach_id). Idempotency key lives in `policy_runs` keyed
     `coach_no_show:{input_id}:{coach_id}`. Re-thinking the same input
     (cycle re-run, dry_run flip) must not double-count.

The architecture promised this counter and the brain reads it. Without
this writer the column is dead — schema-only. The dashboard's coach
panel and the resignation brief's "{ns} no-shows" framing both go
quiet on a real recurring no-show pattern.

The reliability formula stays anchored on reports (reports_submitted
/ reports_due) — no-shows are a parallel signal surfaced in framing,
not folded into the same number. A coach with 1.0 reliability AND
3 no-shows reads differently to a 1.0/0 coach, and Diane needs to
see both.
"""
from __future__ import annotations

import re
from typing import Any

from edge.state.db import connect, log_action, now_iso


def _team_tokens(team: dict) -> list[str]:
    """Build the case-folded tokens we'll match against subject+body to
    decide if the manager named this team. Order doesn't matter — we
    only need to know whether *any* token hits.

    For an age-grade team like 'U13 Girls':
      - 'u13 girls' (the canonical division phrasing)
      - 'u13g'      (compact form used on TeamApp / fixture sheets)
      - bare 'u13' is intentionally NOT included for boys/girls splits,
        because two of Brett's teams (U13 Boys, U14 Boys) share an age
        bracket with the girls' grades — a bare 'u13' could match both.

    For Auskick groups:
      - the full 'auskick group a' and the shorthand 'auskick a'.
    """
    name = (team.get("name") or "").lower()
    age = (team.get("age_grade") or "").lower()
    div = (team.get("division") or "").lower() if team.get("division") else ""
    toks: list[str] = []
    if name:
        toks.append(name)
    # age + division (handles "U13 Girls" vs "U13 Boys")
    if age and div and div not in ("mixed", "auskick"):
        toks.append(f"{age} {div}")
        toks.append(f"{age}{div[:1]}")  # u13g / u13b
    # mixed grades: bare age is unambiguous within a manager's slate
    if age and div in ("mixed", "auskick", ""):
        toks.append(age)
    # Auskick subgroup letter from the team name (Auskick Group A → 'a')
    if "auskick" in name:
        m = re.search(r"auskick\s+(?:group\s+)?([a-z])\b", name)
        if m:
            toks.append(f"auskick {m.group(1)}")
            toks.append(f"auskick group {m.group(1)}")
    return [t for t in toks if t]


def _disambiguate_by_text(teams: list[dict],
                          subject: str | None,
                          body: str | None) -> dict | None:
    """Return the single team whose tokens hit subject+body, or None
    if zero or >1 teams match. The caller decides what to do with
    None — usually flag ambiguity and skip the increment."""
    hay = " ".join(s for s in (subject or "", body or "") if s).lower()
    if not hay:
        return None
    hits = []
    for t in teams:
        for tok in _team_tokens(t):
            if tok and tok in hay:
                hits.append(t)
                break
    if len(hits) == 1:
        return hits[0]
    return None


def _resolve_coach_for_no_show(sender: str | None,
                               subject: str | None = None,
                               body: str | None = None) -> dict[str, Any]:
    """Walk sender → manager → team → coach. Returns:
        {coach: row|None, team: row|None, manager: row|None,
         reporter_role: 'manager'|'coach'|'unknown',
         ambiguous: bool, candidate_teams: list[dict]}

    Manager-sender wins. When the manager owns a single team, the
    resolution is exact. When they own multiple, we try to extract a
    team identifier from subject+body. If that fails, we set ambiguous
    and leave coach/team None — the brain frames the ask to Diane.
    """
    sender_l = (sender or "").lower().strip()
    out: dict[str, Any] = {
        "coach": None, "team": None, "manager": None,
        "reporter_role": "unknown",
        "ambiguous": False, "candidate_teams": [],
    }
    if not sender_l:
        return out

    conn = connect()
    manager = conn.execute(
        "SELECT id, name, email, sms FROM people "
        "WHERE LOWER(email)=? AND role='manager'", (sender_l,)).fetchone()
    if manager:
        teams = [dict(r) for r in conn.execute(
            "SELECT id, name, age_grade, division, coach_id, manager_id "
            "FROM teams WHERE manager_id=? ORDER BY id",
            (manager["id"],)).fetchall()]
        team: dict | None = None
        if len(teams) == 1:
            team = teams[0]
        elif len(teams) > 1:
            team = _disambiguate_by_text(teams, subject, body)
            if team is None:
                out["ambiguous"] = True
                out["candidate_teams"] = teams
        coach = None
        if team and team.get("coach_id"):
            crow = conn.execute(
                "SELECT id, name, email, sms FROM people WHERE id=?",
                (team["coach_id"],)).fetchone()
            coach = dict(crow) if crow else None
        out.update({
            "coach": coach,
            "team": team,
            "manager": dict(manager),
            "reporter_role": "manager",
        })
        conn.close()
        return out

    # Sender is a coach — assistant or another coach reporting. We still
    # resolve the team's head coach via teams.coach_id (the assistant
    # might also be on a different team's roster). Without a manager
    # match we can't link sender → team unambiguously, so we skip the
    # team lookup and rely on the brain to ask Diane to verify.
    sender_coach = conn.execute(
        "SELECT id, name, email, sms FROM people "
        "WHERE LOWER(email)=? AND role='coach'", (sender_l,)).fetchone()
    if sender_coach:
        out["reporter_role"] = "coach"
        # If the coach happens to manage a team via teams.coach_id, surface it.
        team = conn.execute(
            "SELECT id, name, age_grade, division, coach_id, manager_id "
            "FROM teams WHERE coach_id=?", (sender_coach["id"],)).fetchone()
        if team:
            out["team"] = dict(team)
            out["coach"] = dict(sender_coach)
            if team["manager_id"]:
                mgr = conn.execute(
                    "SELECT id, name, email, sms FROM people WHERE id=?",
                    (team["manager_id"],)).fetchone()
                if mgr:
                    out["manager"] = dict(mgr)
    conn.close()
    return out


def _already_counted(input_id: str, coach_id: str) -> bool:
    key = f"coach_no_show:{input_id}:{coach_id}"
    conn = connect()
    row = conn.execute(
        "SELECT 1 FROM policy_runs WHERE id=?", (key,)).fetchone()
    conn.close()
    return row is not None


def _increment_no_shows(cycle_id: int, input_id: str,
                        coach_id: str) -> dict[str, int]:
    """Idempotent +1 on coach_memory.no_shows. Returns the new
    (no_shows, reports_due, reports_submitted) for the brain to
    surface in the brief."""
    key = f"coach_no_show:{input_id}:{coach_id}"
    conn = connect()
    conn.execute(
        "INSERT OR IGNORE INTO coach_memory(coach_id, reports_due, "
        "reports_submitted, no_shows, reliability) "
        "VALUES(?, 0, 0, 0, 1.0)", (coach_id,))
    conn.execute(
        "UPDATE coach_memory SET no_shows = COALESCE(no_shows, 0) + 1 "
        "WHERE coach_id=?", (coach_id,))
    conn.execute(
        "INSERT INTO policy_runs(id, playbook, target_kind, target_id, "
        "cycle_id, fired_at, outcome, detail) VALUES(?,?,?,?,?,?,?,?)",
        (key, "coach_no_show", "person", coach_id, cycle_id,
         now_iso(), "incremented", f"input={input_id}"))
    row = conn.execute(
        "SELECT no_shows, reports_due, reports_submitted, reliability "
        "FROM coach_memory WHERE coach_id=?", (coach_id,)).fetchone()
    conn.close()
    log_action(cycle_id, "brain", "coach_no_show_recorded",
               "person", coach_id, True,
               f"input={input_id} no_shows={row['no_shows']}")
    return dict(row)


def record_world_model(cycle_id: int, input_id: str,
                       sender: str | None,
                       subject: str | None = None,
                       body: str | None = None) -> dict[str, Any]:
    """Single entry point used by the brain's `think()` BEFORE the
    dry/online brief fork. Resolves the coach, increments the counter
    when the reporter is credible AND the team is unambiguous, and
    returns ctx the brief can use.

    Following the recurring "world-model writes outside branches"
    lesson — the increment happens here regardless of dry_run, so
    flipping the brain config doesn't silently desync the dashboard.
    """
    resolved = _resolve_coach_for_no_show(sender, subject, body)
    coach = resolved.get("coach")
    incremented = False
    record = None
    if coach and resolved["reporter_role"] == "manager":
        if not _already_counted(input_id, coach["id"]):
            record = _increment_no_shows(cycle_id, input_id, coach["id"])
            incremented = True
        else:
            # Already counted — read the current row for the brief
            conn = connect()
            row = conn.execute(
                "SELECT no_shows, reports_due, reports_submitted, "
                "reliability FROM coach_memory WHERE coach_id=?",
                (coach["id"],)).fetchone()
            conn.close()
            record = dict(row) if row else None
    elif coach:
        # Untrusted reporter — just read the current record so the brief
        # can still frame the history honestly without writing.
        conn = connect()
        row = conn.execute(
            "SELECT no_shows, reports_due, reports_submitted, "
            "reliability FROM coach_memory WHERE coach_id=?",
            (coach["id"],)).fetchone()
        conn.close()
        record = dict(row) if row else None

    fixtures: list = []
    if resolved.get("team"):
        conn = connect()
        rows = conn.execute(
            "SELECT round, opponent, kickoff, home_or_away "
            "FROM fixtures WHERE team_id=? AND status='scheduled' "
            "AND kickoff > ? ORDER BY kickoff LIMIT 3",
            (resolved["team"]["id"], now_iso())).fetchall()
        conn.close()
        fixtures = [dict(r) for r in rows]

    return {
        **resolved,
        "incremented": incremented,
        "record": record,
        "fixtures": fixtures,
    }

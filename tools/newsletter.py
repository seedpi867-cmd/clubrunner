"""Sunday newsletter assembly.

Pulls together the weekly newsletter from real club state:
  - last round results (from fixtures with completed status + scores)
  - next round fixtures (scheduled, ordered by kickoff)
  - sponsor spotlight (rotated; longest-since-last-post)
  - rego reminders (count of pending players)
  - WWCC reminders (count of upcoming expiries)
  - president's column (placeholder; the brain or Diane fills it)

Newsletter goes through the broadcast threshold gate (>50 reach)
because it goes to the full parent list.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any

from edge.state.db import connect, log_action, now_iso
from tools import broadcast


def _sponsor_spotlight() -> dict | None:
    conn = connect()
    row = conn.execute(
        "SELECT id, name, tier FROM sponsors WHERE posts_owed > 0 "
        "ORDER BY COALESCE(last_post_at, '0000-01-01') ASC LIMIT 1"
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def _last_round_results() -> list[dict]:
    """Last round result line per team. Joins to match_reports so the
    coach's quote (when available) can be folded into the newsletter
    body without doubling up on the per-team listing."""
    conn = connect()
    rows = conn.execute(
        "SELECT t.id AS team_id, t.name AS team_name, f.opponent, "
        "f.home_score, f.away_score, f.home_or_away, "
        "(SELECT body FROM match_reports mr WHERE mr.team_id=t.id "
        " ORDER BY submitted_at DESC LIMIT 1) AS report_body, "
        "(SELECT p.name FROM match_reports mr "
        " JOIN people p ON mr.coach_id=p.id "
        " WHERE mr.team_id=t.id "
        " ORDER BY submitted_at DESC LIMIT 1) AS coach_name "
        "FROM fixtures f JOIN teams t ON f.team_id=t.id "
        "WHERE f.status='completed' AND f.home_score IS NOT NULL "
        "ORDER BY f.kickoff DESC LIMIT 14").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _trim_quote(body: str, max_chars: int = 220) -> str:
    """Two-sentence-or-fewer trim, kept short so the newsletter doesn't
    drown in coach prose. Sentence boundary on '. ', '! ', '? ' — falls
    back to a hard char cut with an ellipsis when the report is one
    long sentence (which it often is)."""
    if not body:
        return ""
    # Strip leading greetings/sign-offs ("Hi Diane,", "Cheers, Marko").
    s = body.strip()
    s = re.sub(r"^(hi|hey|g'?day|hello)[^,\n]{0,40}[,:]?\s*", "", s,
               flags=re.I)
    s = re.sub(r"\n+\s*(cheers|thanks|ta|regards)[\s\S]*$", "", s,
               flags=re.I)
    if len(s) <= max_chars:
        return s.strip()
    # Take up to two sentences within the budget.
    parts = re.split(r"(?<=[.!?])\s+", s)
    out = ""
    for p in parts[:2]:
        if len(out) + len(p) + 1 > max_chars:
            break
        out = (out + " " + p).strip()
    if not out:
        out = s[:max_chars].rstrip() + "…"
    return out


def _next_round_fixtures() -> list[dict]:
    conn = connect()
    rows = conn.execute(
        "SELECT t.name AS team_name, f.opponent, f.kickoff, "
        "g.name AS ground_name "
        "FROM fixtures f JOIN teams t ON f.team_id=t.id "
        "LEFT JOIN grounds g ON f.ground_id=g.id "
        "WHERE f.status='scheduled' AND f.kickoff > ? "
        "ORDER BY f.kickoff LIMIT 14",
        (now_iso(),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _pending_count() -> int:
    conn = connect()
    n = conn.execute("SELECT COUNT(*) FROM players "
                     "WHERE rego_status='pending'").fetchone()[0]
    conn.close()
    return n


def _wwcc_count(days: int = 30) -> int:
    until = (datetime.now().date() + timedelta(days=days)).isoformat()
    conn = connect()
    n = conn.execute(
        "SELECT COUNT(*) FROM people WHERE wwcc_expiry IS NOT NULL "
        "AND wwcc_expiry <= ?", (until,)).fetchone()[0]
    conn.close()
    return n


def _all_parents_segment() -> tuple[str, int]:
    conn = connect()
    n = conn.execute(
        "SELECT COUNT(DISTINCT parent_id) FROM players "
        "WHERE rego_status IN ('paid','active','insured') "
        "AND parent_id IS NOT NULL").fetchone()[0]
    conn.close()
    return "all_parents", int(n)


def assemble_body() -> tuple[str, str]:
    results = _last_round_results()
    fixtures = _next_round_fixtures()
    spotlight = _sponsor_spotlight()
    pending = _pending_count()
    wwcc_due = _wwcc_count()

    today = datetime.now().date()
    subject = f"HGJFC Weekly — week ending {today.isoformat()}"

    parts = [
        f"# Henley & Grange JFC — week ending {today.isoformat()}",
        "",
        "## Last round",
    ]
    if results:
        for r in results[:14]:
            parts.append(
                f"- {r['team_name']} vs {r['opponent']}: "
                f"{r['home_score']}-{r['away_score']}")
            if r.get("report_body"):
                quote = _trim_quote(r["report_body"])
                if quote:
                    parts.append(f"    > _{quote}_  "
                                 f"— {r.get('coach_name') or 'coach'}")
    else:
        parts.append("_No results yet from the last round._")
    parts += ["", "## Next round"]
    if fixtures:
        for f in fixtures[:14]:
            ko = f["kickoff"][:16].replace("T", " ")
            parts.append(
                f"- {f['team_name']} vs {f['opponent']} — {ko} "
                f"@ {f['ground_name'] or 'TBA'}")
    else:
        parts.append("_Fixtures coming through midweek._")
    parts += ["", "## President's column",
              "_(placeholder — Diane to confirm)_", ""]
    if spotlight:
        parts += [f"## Sponsor spotlight — {spotlight['name']}",
                  f"Thank you to our **{spotlight['tier']}** partner "
                  f"{spotlight['name']} for backing junior footy in our "
                  "community.", ""]
    if pending:
        parts.append(f"## Rego reminders — {pending} outstanding")
        parts.append("Parents: please clear your registration this week so "
                     "everyone is insured for the next round.")
        parts.append("")
    if wwcc_due:
        parts.append(f"## WWCC — {wwcc_due} renewing in next 30 days")
        parts.append("Coaches/managers: keep us posted with new certs.")
        parts.append("")

    parts += ["Diane",
              "Secretary, Henley & Grange JFC",
              "secretary@henleygrangejfc.com.au"]
    return subject, "\n".join(parts)


def run(cycle_id: int, related_input: str | None = None) -> dict[str, Any]:
    subject, body = assemble_body()
    seg, reach = _all_parents_segment()
    if reach == 0:
        # Still try with a tiny fallback so the broadcast is recorded
        seg, reach = "all_parents", 1
    # Pin the sponsor pick on the broadcast so the credit is deterministic
    # against the body Diane actually approved. If we re-derived the pick
    # at approval time, _sponsor_spotlight() could pick a different sponsor
    # than the one named in the body she just looked at — sponsor B gets
    # credited while sponsor A stays in the spotlight rotation.
    spot = _sponsor_spotlight()
    meta = {"sponsor_credit": spot["id"]} if spot else None
    result = broadcast.submit(
        cycle_id=cycle_id, playbook="newsletter", channel="email",
        segment=seg, reach=reach, subject=subject, body=body,
        target_kind="newsletter",
        target_id=datetime.now().date().isoformat(),
        related_input=related_input,
        meta=meta)
    log_action(cycle_id, "execute", "newsletter_run",
               None, None, True,
               f"reach={reach} status={result['status']} "
               f"sponsor={(spot or {}).get('id')}")
    return {"reach": reach, "result": result, "subject": subject,
            "sponsor_pick": (spot or {}).get("id")}

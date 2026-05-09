"""Sunday committee briefing.

Once a week (Sunday 21:00, after the newsletter is out and before the
Monday morning email pile-on), assemble a single email to the 7
committee members. Pulls live state from state.db so they see the same
picture Diane sees.

Sections:
  - Open incidents (severity + league deadline status)
  - Pending decisions on Diane's queue (counts by kind)
  - Outstanding rego payments (count + dollars)
  - WWCC expiring inside 60 days (people at risk)
  - Fixture status changes this week (cancelled/paused/completed)
  - Sponsor obligations remaining (posts owed by tier)
  - Agent health — cycles, broadcasts, breakers

Reach is the number of committee email recipients (typically 7), well
under the >50 approval threshold, so this auto-sends. The dedup gate
ensures the brief only fires once per week per recipient list.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from edge.state.db import connect, log_action, now_iso
from tools import broadcast


def _committee_recipients() -> list[dict]:
    conn = connect()
    rows = conn.execute(
        "SELECT id, name, email FROM people "
        "WHERE role='committee' AND email IS NOT NULL AND email <> '' "
        "ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _open_incidents() -> list[dict]:
    conn = connect()
    rows = conn.execute(
        "SELECT id, kind, severity, summary, status, league_report_due, "
        "league_report_at, player_id "
        "FROM incidents WHERE status IN ('open','reported') "
        "ORDER BY CASE severity WHEN 'critical' THEN 0 "
        "WHEN 'serious' THEN 1 WHEN 'moderate' THEN 2 ELSE 3 END, "
        "detected_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _pending_decisions() -> dict[str, int]:
    conn = connect()
    rows = conn.execute(
        "SELECT kind, COUNT(*) AS n FROM decisions "
        "WHERE status='pending' GROUP BY kind ORDER BY n DESC").fetchall()
    conn.close()
    return {r["kind"]: r["n"] for r in rows}


def _outstanding_payments() -> tuple[int, float]:
    conn = connect()
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM players WHERE rego_status='pending'"
    ).fetchone()
    n = row["n"] if row else 0
    conn.close()
    # Estimate dollars from config defaults — most clubs run $220/$260
    # tiers; the seed pegs both at the player level via overdue inputs.
    return int(n), float(n) * 220.0


def _wwcc_expiring(days: int = 60) -> list[dict]:
    until = (datetime.now().date() + timedelta(days=days)).isoformat()
    today = datetime.now().date().isoformat()
    conn = connect()
    rows = conn.execute(
        "SELECT id, name, role, wwcc_expiry FROM people "
        "WHERE wwcc_expiry IS NOT NULL "
        "AND wwcc_expiry <= ? "
        "ORDER BY wwcc_expiry", (until,)).fetchall()
    conn.close()
    out = []
    for r in rows:
        d = dict(r)
        d["expired"] = (d["wwcc_expiry"] or "") < today
        out.append(d)
    return out


def _fixture_changes_this_week() -> dict[str, list[dict]]:
    seven_days_ago = (datetime.now() - timedelta(days=7)).isoformat()
    conn = connect()
    rows = conn.execute(
        "SELECT id, team_id, status, status_reason, last_status_change "
        "FROM fixtures WHERE status IN ('cancelled','paused','completed') "
        "AND last_status_change >= ? "
        "ORDER BY last_status_change DESC", (seven_days_ago,)).fetchall()
    conn.close()
    grouped: dict[str, list[dict]] = {
        "cancelled": [], "paused": [], "completed": []}
    for r in rows:
        grouped.setdefault(r["status"], []).append(dict(r))
    return grouped


def _sponsor_obligations() -> list[dict]:
    conn = connect()
    rows = conn.execute(
        "SELECT id, name, tier, posts_owed, last_post_at, contract_end "
        "FROM sponsors WHERE posts_owed > 0 "
        "ORDER BY CASE tier WHEN 'major' THEN 0 ELSE 1 END, "
        "posts_owed DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _agent_health() -> dict:
    seven_days_ago = (datetime.now() - timedelta(days=7)).isoformat()
    conn = connect()
    cyc = conn.execute(
        "SELECT COUNT(*) AS n, COALESCE(SUM(broadcasts_sent),0) AS sent, "
        "COALESCE(SUM(decisions_opened),0) AS dec, "
        "COALESCE(SUM(policies_fired),0) AS pol "
        "FROM cycles WHERE started_at >= ?", (seven_days_ago,)).fetchone()
    open_breakers = conn.execute(
        "SELECT integration, last_error FROM breakers "
        "WHERE state IN ('open','half_open')").fetchall()
    conn.close()
    return {
        "cycles": cyc["n"] if cyc else 0,
        "broadcasts_sent": cyc["sent"] if cyc else 0,
        "decisions_opened": cyc["dec"] if cyc else 0,
        "policies_fired": cyc["pol"] if cyc else 0,
        "open_breakers": [dict(b) for b in open_breakers],
    }


def assemble_body() -> tuple[str, str]:
    today = datetime.now().date()
    incidents = _open_incidents()
    decisions = _pending_decisions()
    pay_n, pay_aud = _outstanding_payments()
    wwcc = _wwcc_expiring()
    fxc = _fixture_changes_this_week()
    sponsors = _sponsor_obligations()
    health = _agent_health()

    subject = f"Committee briefing — week of {today.isoformat()}"

    parts = [
        f"# Committee briefing — week of {today.isoformat()}",
        "",
        "Diane gets her Sunday back. Here's what the club did this week,",
        "what is sitting on her queue right now, and what the committee",
        "should weigh in on at Tuesday's meeting.",
        "",
        "## Open incidents",
    ]
    if incidents:
        for i in incidents:
            tag = ""
            if i["league_report_at"]:
                tag = f" (reported {i['league_report_at'][:16]})"
            elif i["league_report_due"]:
                try:
                    due = datetime.fromisoformat(i["league_report_due"])
                    delta_h = (due - datetime.now()).total_seconds() / 3600
                    if delta_h < 0:
                        tag = f" (LEAGUE REPORT OVERDUE by {abs(delta_h):.0f}h)"
                    elif delta_h < 12:
                        tag = f" ({delta_h:.0f}h to league deadline)"
                    else:
                        tag = f" (deadline {i['league_report_due'][:16]})"
                except Exception:
                    tag = ""
            parts.append(
                f"- [{i['severity'].upper()}] {i['kind']} — "
                f"{(i['summary'] or '')[:90]}{tag}")
    else:
        parts.append("- None open. Last week was clean.")

    parts += ["", "## Pending decisions on Diane's queue"]
    if decisions:
        for k, n in decisions.items():
            parts.append(f"- {k}: {n}")
    else:
        parts.append("- None pending. The agent has it covered.")

    parts += ["", "## Outstanding registrations"]
    if pay_n:
        parts.append(f"- {pay_n} players still pending — approx "
                     f"${pay_aud:,.0f} outstanding.")
        parts.append("- 3-stage chase is running automatically. "
                     "Stage 3 escalates to committee for hardship review.")
    else:
        parts.append("- All current. Nothing to chase.")

    parts += ["", "## WWCC — within 60 days"]
    if wwcc:
        for w in wwcc:
            flag = "EXPIRED" if w["expired"] else f"expires {w['wwcc_expiry']}"
            parts.append(f"- {w['name']} ({w['role']}) — {flag}")
    else:
        parts.append("- All coaches/managers current.")

    parts += ["", "## Fixture changes this week"]
    if any(fxc.values()):
        if fxc.get("cancelled"):
            parts.append(f"- Cancelled: {len(fxc['cancelled'])} "
                         "(see status_reason — typically heat policy)")
        if fxc.get("paused"):
            parts.append(f"- Paused: {len(fxc['paused'])} "
                         "(typically lightning policy)")
        if fxc.get("completed"):
            parts.append(f"- Completed: {len(fxc['completed'])}")
    else:
        parts.append("- No status changes this week.")

    parts += ["", "## Sponsor obligations remaining"]
    if sponsors:
        for s in sponsors:
            last = (s["last_post_at"] or "—")[:10]
            parts.append(
                f"- {s['name']} ({s['tier']}): {s['posts_owed']} posts "
                f"owed; last post {last}; contract end "
                f"{s['contract_end'] or '—'}")
    else:
        parts.append("- All sponsor obligations current.")

    parts += ["", "## Agent health (last 7 days)",
              f"- Cycles run: {health['cycles']}",
              f"- Broadcasts sent: {health['broadcasts_sent']}",
              f"- Decisions opened: {health['decisions_opened']}",
              f"- Policies fired: {health['policies_fired']}"]
    if health["open_breakers"]:
        parts.append("- **CIRCUIT BREAKERS OPEN:**")
        for b in health["open_breakers"]:
            parts.append(f"  - {b['integration']}: "
                         f"{(b['last_error'] or '')[:80]}")
    else:
        parts.append("- All integrations healthy.")

    parts += ["",
              "_Sent autonomously by clubrunner. Reply to this email to ",
              "raise an item; Diane will see it on her queue._",
              "",
              "secretary@henleygrangejfc.com.au"]

    return subject, "\n".join(parts)


def run(cycle_id: int, related_input: str | None = None) -> dict[str, Any]:
    recipients = _committee_recipients()
    reach = len(recipients)
    if reach == 0:
        log_action(cycle_id, "execute", "committee_brief_skipped",
                   None, None, True, "no committee recipients in state")
        return {"reach": 0, "result": {"status": "skipped"}}

    subject, body = assemble_body()
    today_iso = datetime.now().date().isoformat()

    result = broadcast.submit(
        cycle_id=cycle_id, playbook="committee_brief", channel="email",
        segment="committee", reach=reach, subject=subject, body=body,
        target_kind="committee_brief", target_id=today_iso,
        related_input=related_input)

    log_action(cycle_id, "execute", "committee_brief_run",
               "committee_brief", today_iso, True,
               f"reach={reach} status={result['status']}")
    return {"reach": reach, "result": result, "subject": subject,
            "recipients": [r["id"] for r in recipients]}

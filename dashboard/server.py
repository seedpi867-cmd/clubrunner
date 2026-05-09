"""Dashboard server.

Plain stdlib http.server — no Flask, no FastAPI. Two endpoints:
  GET  /            -> HTML dashboard, live state from state.db
  POST /decide      -> resolve a decision (approve/decline/hold/ack/...)

This is Diane's decision queue and situational awareness. Every value
on the page is read fresh from the database on each request — no
caching, no stale view.
"""
from __future__ import annotations

import json
import sys
import urllib.parse
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from edge.state.db import connect, init_db
from edge.health import breaker
from edge.collectors import sms_inbound, facebook_page
from tools import broadcast as broadcast_mod
import config as cfg_mod


def _q(conn, sql, params=()):
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _snapshot() -> dict:
    conn = connect()
    cycle = conn.execute(
        "SELECT * FROM cycles ORDER BY id DESC LIMIT 1").fetchone()
    cycle = dict(cycle) if cycle else None
    last_5 = _q(conn,
        "SELECT id, started_at, ended_at, items_in, items_classified, "
        "drafts_created, broadcasts_sent, decisions_opened, policies_fired "
        "FROM cycles ORDER BY id DESC LIMIT 5")
    pending = _q(conn,
        "SELECT id, kind, summary, options_json, default_option, target_kind, "
        "target_id, created_at, context FROM decisions WHERE status='pending' "
        "ORDER BY created_at DESC LIMIT 25")
    for d in pending:
        try:
            d["options"] = json.loads(d.pop("options_json") or "[]")
        except Exception:
            d["options"] = []
    inputs_24h = _q(conn,
        "SELECT classification, COUNT(*) AS n FROM inputs "
        "WHERE received_at > datetime('now','-24 hours') "
        "GROUP BY classification ORDER BY n DESC")
    bc_today = _q(conn,
        "SELECT playbook, channel, status, COUNT(*) AS n FROM broadcasts "
        "WHERE created_at > datetime('now','-24 hours') "
        "GROUP BY playbook, channel, status ORDER BY n DESC")
    paused_fixtures = _q(conn,
        "SELECT id, team_id, ground_id, kickoff, status_reason "
        "FROM fixtures WHERE status='paused' ORDER BY kickoff")
    cancelled_fixtures = _q(conn,
        "SELECT id, team_id, ground_id, kickoff, status_reason "
        "FROM fixtures WHERE status='cancelled' ORDER BY kickoff")
    # Forward visibility — what's coming up that Diane should know about.
    # Combines next-7-days scheduled fixtures with next-14-days events
    # (committee meetings, working bees, AGM). Without this, the
    # dashboard shows weather and exceptions but no "what's on this
    # week" — Diane has to keep that map in her head.
    upcoming_fixtures = _q(conn,
        "SELECT f.id, f.round, f.team_id, f.opponent, f.ground_id, "
        "f.kickoff, f.home_or_away, "
        "(SELECT COUNT(*) FROM duties d "
        " WHERE d.fixture_id=f.id AND d.confirmed=0) AS unconfirmed_duties "
        "FROM fixtures f "
        "WHERE f.status='scheduled' "
        "AND f.kickoff > datetime('now') "
        "AND f.kickoff < datetime('now','+8 days') "
        "ORDER BY f.kickoff LIMIT 30")
    upcoming_events = _q(conn,
        "SELECT id, summary, audience, start_at, location, status "
        "FROM events WHERE status='scheduled' "
        "AND start_at > datetime('now') "
        "AND start_at < datetime('now','+14 days') "
        "ORDER BY start_at LIMIT 20")
    weather = _q(conn,
        "SELECT g.name AS ground, ws.forecast_max_c, ws.lightning_km, "
        "ws.observed_at FROM weather_state ws JOIN grounds g "
        "ON ws.ground_id=g.id WHERE ws.observed_at = "
        "(SELECT MAX(observed_at) FROM weather_state ws2 "
        "WHERE ws2.ground_id=ws.ground_id) "
        "ORDER BY g.name")
    breakers = _q(conn,
        "SELECT integration, state, consecutive_fail, opened_at, "
        "last_error, last_success_at FROM breakers ORDER BY integration")
    overdue = _q(conn,
        "SELECT id, first_name, last_name, team_id, rego_status FROM players "
        "WHERE rego_status='pending' ORDER BY last_name LIMIT 10")
    open_incidents = _q(conn,
        "SELECT id, kind, severity, summary, status, detected_at, "
        "league_report_due, league_report_at, player_id, notes "
        "FROM incidents WHERE status IN ('open','reported') "
        "ORDER BY CASE severity WHEN 'critical' THEN 0 "
        "WHEN 'serious' THEN 1 WHEN 'moderate' THEN 2 ELSE 3 END, "
        "detected_at DESC LIMIT 20")
    recent_actions = _q(conn,
        "SELECT ts, phase, action, target_kind, target_id, ok, detail "
        "FROM actions ORDER BY id DESC LIMIT 30")
    # The agent talks to ~480 parents/coaches/sponsors in Diane's name.
    # Without a way to read what was actually said, autonomy turns into
    # a black box: a parent rings Monday saying "you sent me X at 7pm
    # Friday" and the dashboard has nothing to show. This pulls the last
    # 24h of broadcasts in any non-drafted state — sent, gated awaiting
    # her, and cancelled — so she can audit and learn what went out.
    recent_broadcasts = _q(conn,
        "SELECT id, playbook, channel, segment, reach, subject, status, "
        "blocked_by, created_at, sent_at, decided_at, related_input "
        "FROM broadcasts "
        "WHERE created_at > datetime('now','-24 hours') "
        "AND status IN ('sent','gated','cancelled','skipped','drafted') "
        "ORDER BY COALESCE(sent_at, decided_at, created_at) DESC LIMIT 40")
    # Coach reliability — only worth showing rows where we have data to
    # report against. A coach with 0 reports_due is either pre-season or
    # has never had a fixture come due; surfacing them as 100% would
    # imply we're tracking what they don't owe us yet.
    coach_reliability = _q(conn,
        "SELECT cm.coach_id, p.name, cm.reports_due, "
        "       cm.reports_submitted, cm.reliability "
        "FROM coach_memory cm "
        "JOIN people p ON p.id = cm.coach_id "
        "WHERE cm.reports_due > 0 OR cm.reports_submitted > 0 "
        "ORDER BY cm.reliability ASC, cm.reports_due DESC")
    conn.close()
    return {
        "cycle": cycle, "last_5": last_5, "pending": pending,
        "inputs_24h": inputs_24h, "bc_today": bc_today,
        "paused_fixtures": paused_fixtures,
        "cancelled_fixtures": cancelled_fixtures,
        "weather": weather, "breakers": breakers,
        "overdue": overdue,
        "open_incidents": open_incidents,
        "recent_actions": recent_actions,
        "recent_broadcasts": recent_broadcasts,
        "coach_reliability": coach_reliability,
        "upcoming_fixtures": upcoming_fixtures,
        "upcoming_events": upcoming_events,
    }


def _safe(s) -> str:
    if s is None:
        return ""
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def _render(snap: dict) -> str:
    cfg = cfg_mod.get()
    club = cfg.get("club", {})
    cycle = snap["cycle"]
    pending = snap["pending"]
    age_min = ""
    if cycle and cycle.get("started_at"):
        try:
            dt = datetime.fromisoformat(cycle["started_at"])
            age_min = f"{(datetime.now() - dt).total_seconds() / 60:.0f}m ago"
        except Exception:
            pass

    def _decision_card(d):
        opts = "".join(
            f'<form method="post" action="/decide" style="display:inline">'
            f'<input type="hidden" name="decision_id" value="{_safe(d["id"])}">'
            f'<input type="hidden" name="choice" value="{_safe(o["key"])}">'
            f'<button type="submit" class="opt'
            f'{" default" if o["key"] == d["default_option"] else ""}">'
            f'{_safe(o["label"])}</button></form>'
            for o in d["options"])
        ctx = (d.get("context") or "")[:600]
        return (
            f'<div class="card decision">'
            f'<div class="kind">{_safe(d["kind"])}</div>'
            f'<div class="summary">{_safe(d["summary"])}</div>'
            f'<pre class="ctx">{_safe(ctx)}</pre>'
            f'<div class="opts">{opts}</div>'
            f'<div class="meta">{_safe(d["created_at"])} · '
            f'target {_safe(d["target_kind"])}/{_safe(d["target_id"])}</div>'
            f'</div>')

    decisions_html = ("".join(_decision_card(d) for d in pending) or
                      '<div class="empty">No items pending. The agent has '
                      'this covered.</div>')

    def _due_label(due_iso: str | None, reported_at: str | None) -> str:
        if reported_at:
            return f'<span class="ok">reported {_safe(reported_at[:16])}</span>'
        if not due_iso:
            return "—"
        try:
            due = datetime.fromisoformat(due_iso)
        except Exception:
            return _safe(due_iso)
        delta = due - datetime.now()
        hrs = delta.total_seconds() / 3600
        if hrs < 0:
            return (f'<span class="overdue">OVERDUE by '
                    f'{abs(hrs):.0f}h</span>')
        if hrs < 6:
            return f'<span class="urgent">{hrs:.0f}h to go</span>'
        return f'{hrs:.0f}h to go'

    incident_rows = "".join(
        f'<tr><td>{_safe(i["id"])}</td>'
        f'<td class="sev-{_safe(i["severity"])}">{_safe(i["severity"])}</td>'
        f'<td>{_safe(i["status"])}</td>'
        f'<td>{_safe(i["player_id"] or "—")}</td>'
        f'<td>{_safe(i["detected_at"])}</td>'
        f'<td>{_due_label(i["league_report_due"], i["league_report_at"])}</td>'
        f'<td>{_safe((i["summary"] or "")[:90])}</td>'
        f'<td>{_safe((i["notes"] or "")[:120])}</td></tr>'
        for i in snap["open_incidents"])

    weather_rows = "".join(
        f'<tr><td>{_safe(w["ground"])}</td>'
        f'<td>{w["forecast_max_c"] if w["forecast_max_c"] is not None else "—"}°C</td>'
        f'<td>{round(w["lightning_km"], 1) if w["lightning_km"] is not None else "—"}km</td>'
        f'<td>{_safe(w["observed_at"])}</td></tr>'
        for w in snap["weather"])

    paused_rows = "".join(
        f'<tr><td>{_safe(f["id"])}</td><td>{_safe(f["team_id"])}</td>'
        f'<td>{_safe(f["ground_id"])}</td><td>{_safe(f["kickoff"])}</td>'
        f'<td>{_safe(f["status_reason"])}</td></tr>'
        for f in snap["paused_fixtures"])

    cancelled_rows = "".join(
        f'<tr><td>{_safe(f["id"])}</td><td>{_safe(f["team_id"])}</td>'
        f'<td>{_safe(f["ground_id"])}</td><td>{_safe(f["kickoff"])}</td>'
        f'<td>{_safe(f["status_reason"])}</td></tr>'
        for f in snap["cancelled_fixtures"])

    cycles_rows = "".join(
        f'<tr><td>{c["id"]}</td><td>{_safe(c["started_at"])}</td>'
        f'<td>{c["items_in"]}</td><td>{c["items_classified"]}</td>'
        f'<td>{c["drafts_created"]}</td><td>{c["broadcasts_sent"]}</td>'
        f'<td>{c["decisions_opened"]}</td><td>{c["policies_fired"]}</td></tr>'
        for c in snap["last_5"])

    inputs_rows = "".join(
        f'<tr><td>{_safe(i["classification"])}</td><td>{i["n"]}</td></tr>'
        for i in snap["inputs_24h"])

    bc_rows = "".join(
        f'<tr><td>{_safe(b["playbook"])}</td><td>{_safe(b["channel"])}</td>'
        f'<td>{_safe(b["status"])}</td><td>{b["n"]}</td></tr>'
        for b in snap["bc_today"])

    breaker_rows = "".join(
        f'<tr><td>{_safe(b["integration"])}</td>'
        f'<td class="state-{_safe(b["state"])}">{_safe(b["state"])}</td>'
        f'<td>{b["consecutive_fail"]}</td>'
        f'<td>{_safe(b["last_error"] or "")[:80]}</td>'
        f'<td>{_safe(b["last_success_at"] or "")}</td></tr>'
        for b in snap["breakers"])

    actions_rows = "".join(
        f'<tr><td>{_safe(a["ts"])}</td><td>{_safe(a["phase"])}</td>'
        f'<td>{_safe(a["action"])}</td><td>{_safe(a["target_kind"])}</td>'
        f'<td>{_safe(a["target_id"])}</td>'
        f'<td>{"ok" if a["ok"] else "FAIL"}</td>'
        f'<td>{_safe((a["detail"] or "")[:120])}</td></tr>'
        for a in snap["recent_actions"])

    # Players whose rego is unpaid — same set the payment_chase playbook
    # works on. Surfacing them lets Diane spot a kid who's been chased
    # three times and still not paid (escalation candidate).
    overdue_rows = "".join(
        f'<tr><td>{_safe(p["id"])}</td>'
        f'<td>{_safe(p["first_name"])} {_safe(p["last_name"])}</td>'
        f'<td>{_safe(p["team_id"])}</td>'
        f'<td>{_safe(p["rego_status"])}</td></tr>'
        for p in snap["overdue"])

    # Coach reliability — match-report submission rate. Only rows with
    # any due-or-submitted history; pre-season coaches with nothing to
    # owe yet stay invisible. Reliability < 0.5 is the threshold the
    # coach_reliability playbook uses to open a quiet decision.
    def _rel_class(rel):
        if rel is None:
            return ""
        if rel < 0.5:
            return "rel-low"
        if rel < 0.8:
            return "rel-mid"
        return "rel-ok"

    coach_rows = "".join(
        f'<tr><td>{_safe(c["coach_id"])}</td>'
        f'<td>{_safe(c["name"])}</td>'
        f'<td>{c["reports_due"]}</td>'
        f'<td>{c["reports_submitted"]}</td>'
        f'<td class="{_rel_class(c["reliability"])}">'
        f'{round(c["reliability"], 2) if c["reliability"] is not None else "—"}'
        f'</td></tr>'
        for c in snap["coach_reliability"])

    def _bc_when(b):
        # Show the moment the broadcast last transitioned, since that's
        # what Diane is asking ("when did this go out?"). For sent rows
        # that's sent_at; for gated/cancelled the decision moment.
        return (b["sent_at"] or b["decided_at"] or b["created_at"] or "")[:16]

    # Upcoming fixtures + events combined. The two come from different
    # tables but Diane's question is the same: "what's on this week, and
    # are we ready for it?" Sort merged by datetime. The unconfirmed-
    # duties column is the one number she's probably scanning for —
    # zero means coverage is locked, anything >0 is a chase candidate.
    def _when_label(iso: str) -> str:
        try:
            dt = datetime.fromisoformat(iso)
        except ValueError:
            return iso[:16]
        delta = dt - datetime.now()
        days = int(delta.total_seconds() // 86400)
        # Below the day boundary, give hours; otherwise weekday + time.
        if 0 <= delta.total_seconds() < 86400:
            hrs = delta.total_seconds() / 3600
            return f"{dt.strftime('%a %H:%M')} (in {hrs:.0f}h)"
        if days < 0:
            return dt.strftime('%a %H:%M')
        return dt.strftime('%a %d %b %H:%M')

    upcoming_items: list[tuple[str, str, str, str]] = []
    for f in snap["upcoming_fixtures"]:
        unc = f["unconfirmed_duties"] or 0
        unc_html = (f'<span class="urgent">{unc} unconfirmed</span>'
                    if unc else '<span class="ok">all confirmed</span>')
        what = (f'R{f["round"]} {_safe(f["team_id"])} vs '
                f'{_safe(f["opponent"])} @ {_safe(f["ground_id"])} '
                f'({_safe(f["home_or_away"])})')
        upcoming_items.append(
            (f["kickoff"], "fixture",
             f'<tr><td>{_safe(_when_label(f["kickoff"]))}</td>'
             f'<td><span class="tag-fixture">match</span></td>'
             f'<td>{what}</td><td>{unc_html}</td></tr>',
             f["kickoff"]))
    for e in snap["upcoming_events"]:
        loc = _safe(e["location"] or "")
        what = _safe(e["summary"])
        if loc:
            what += f' <small>({loc})</small>'
        upcoming_items.append(
            (e["start_at"], "event",
             f'<tr><td>{_safe(_when_label(e["start_at"]))}</td>'
             f'<td><span class="tag-event">{_safe(e["audience"])}</span></td>'
             f'<td>{what}</td><td>—</td></tr>',
             e["start_at"]))
    upcoming_items.sort(key=lambda x: x[0])
    upcoming_rows = "".join(row for _, _, row, _ in upcoming_items)

    bc_recent_rows = "".join(
        f'<tr class="bc-{_safe(b["status"])}">'
        f'<td>{_safe(_bc_when(b))}</td>'
        f'<td>{_safe(b["status"])}</td>'
        f'<td>{_safe(b["playbook"])}</td>'
        f'<td>{_safe(b["channel"])}</td>'
        f'<td>{_safe(b["segment"])}</td>'
        f'<td>{b["reach"]}</td>'
        f'<td>{_safe((b["subject"] or "")[:80])}</td>'
        f'<td><a href="/broadcast/{_safe(b["id"])}">read →</a></td>'
        f'</tr>'
        for b in snap["recent_broadcasts"])

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>clubrunner — {_safe(club.get("short_name", ""))}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="20">
<style>
* {{ box-sizing: border-box; }}
body {{ font: 14px/1.4 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,
            sans-serif; margin: 0; padding: 12px;
       background: #f5f5f7; color: #1d1d1f; }}
header {{ display: flex; justify-content: space-between; align-items: baseline;
         padding: 8px 0 16px; border-bottom: 1px solid #ddd; margin-bottom: 16px; }}
h1 {{ margin: 0; font-size: 20px; }}
h2 {{ font-size: 15px; margin: 24px 0 8px; color: #444;
      text-transform: uppercase; letter-spacing: 0.06em; }}
.meta {{ color: #666; font-size: 12px; }}
.grid {{ display: grid; gap: 12px;
        grid-template-columns: repeat(auto-fit,minmax(280px,1fr)); }}
.card {{ background: #fff; padding: 12px; border-radius: 8px;
        box-shadow: 0 1px 2px rgba(0,0,0,.08); }}
.decision {{ border-left: 3px solid #ff9500; }}
.decision .kind {{ font-size: 11px; color: #888; text-transform: uppercase;
                  letter-spacing: 0.04em; }}
.decision .summary {{ font-weight: 600; margin: 4px 0 8px; }}
.decision .ctx {{ background: #f7f7f9; padding: 8px; border-radius: 4px;
                 font: 12px/1.35 ui-monospace,Menlo,monospace; max-height: 180px;
                 overflow: auto; white-space: pre-wrap; word-wrap: break-word; }}
.decision .opts {{ margin: 8px 0 4px; display: flex; gap: 6px; flex-wrap: wrap; }}
button.opt {{ padding: 8px 12px; border: 1px solid #ccc; background: #fff;
             border-radius: 6px; cursor: pointer; font: inherit; }}
button.opt.default {{ background: #007aff; color: #fff; border-color: #007aff; }}
button.opt:hover {{ background: #f0f0f0; }}
button.opt.default:hover {{ background: #005ec4; }}
.empty {{ padding: 20px; background: #fff; border-radius: 8px;
         color: #6c6; font-weight: 500; }}
table {{ width: 100%; border-collapse: collapse; background: #fff;
        border-radius: 8px; overflow: hidden;
        box-shadow: 0 1px 2px rgba(0,0,0,.06); }}
th, td {{ padding: 6px 10px; text-align: left; border-bottom: 1px solid #eee;
         font-size: 13px; }}
th {{ background: #fafafa; font-weight: 600; font-size: 12px;
      text-transform: uppercase; letter-spacing: 0.04em; color: #666; }}
.stats {{ display: flex; gap: 12px; flex-wrap: wrap; margin: 8px 0; }}
.stat {{ background: #fff; padding: 8px 12px; border-radius: 6px;
        box-shadow: 0 1px 2px rgba(0,0,0,.06); }}
.stat .v {{ font-size: 18px; font-weight: 600; }}
.stat .k {{ color: #666; font-size: 11px;
            text-transform: uppercase; letter-spacing: 0.04em; }}
.state-closed {{ color: #2a8; }}
.state-open {{ color: #d33; font-weight: 600; }}
.state-half_open {{ color: #d80; }}
.sev-critical {{ color: #fff; background: #c00;
                  padding: 1px 6px; border-radius: 3px; font-weight: 600; }}
.sev-serious {{ color: #c00; font-weight: 600; }}
.sev-moderate {{ color: #c80; }}
.sev-minor {{ color: #888; }}
.overdue {{ color: #fff; background: #c00; padding: 2px 6px;
            border-radius: 3px; font-weight: 600; }}
.urgent {{ color: #c80; font-weight: 600; }}
.ok {{ color: #2a8; }}
small {{ color: #888; }}
.flags {{ margin-top: 4px; }}
.flag {{ display: inline-block; padding: 2px 6px; border-radius: 3px;
        font-size: 11px; margin-right: 4px; }}
.flag.heat {{ background: #ffeac4; color: #8a4f00; }}
.flag.lightning {{ background: #e2d6ff; color: #4a2dad; }}
.flag.match-day {{ background: #c4e9ff; color: #08527a; }}
.tag-fixture {{ display: inline-block; padding: 1px 6px; border-radius: 3px;
                background: #c4e9ff; color: #08527a; font-size: 11px;
                font-weight: 600; }}
.tag-event {{ display: inline-block; padding: 1px 6px; border-radius: 3px;
              background: #efe4ff; color: #432a8a; font-size: 11px;
              font-weight: 600; }}
tr.bc-sent td {{ color: #1d1d1f; }}
tr.bc-gated td {{ background: #fff7e6; }}
tr.bc-cancelled td {{ color: #888; text-decoration: line-through; }}
tr.bc-skipped td {{ color: #999; font-style: italic; }}
tr.bc-drafted td {{ background: #f0f7ff; }}
.rel-low {{ color: #fff; background: #c00; padding: 1px 6px;
            border-radius: 3px; font-weight: 600; }}
.rel-mid {{ color: #c80; font-weight: 600; }}
.rel-ok {{ color: #2a8; }}
.body-pre {{ background: #f7f7f9; padding: 12px; border-radius: 6px;
             font: 13px/1.4 ui-monospace,Menlo,monospace; white-space: pre-wrap;
             word-wrap: break-word; }}
.kv {{ display: grid; grid-template-columns: 140px 1fr; gap: 4px 12px;
       font-size: 13px; margin: 8px 0; }}
.kv .k {{ color: #666; }}
</style></head>
<body>
<header>
  <div>
    <h1>clubrunner — {_safe(club.get("short_name", ""))}</h1>
    <small>{_safe(club.get("name", ""))} · Secretary: {_safe(club.get("secretary_name", ""))}</small>
  </div>
  <div class="meta">cycle #{cycle["id"] if cycle else "—"} ·
       {_safe(age_min)} · auto-refresh 20s</div>
</header>

<div class="stats">
  <div class="stat"><div class="v">{cycle["items_in"] if cycle else 0}</div>
       <div class="k">items in</div></div>
  <div class="stat"><div class="v">{cycle["items_classified"] if cycle else 0}</div>
       <div class="k">classified</div></div>
  <div class="stat"><div class="v">{cycle["broadcasts_sent"] if cycle else 0}</div>
       <div class="k">sent</div></div>
  <div class="stat"><div class="v">{cycle["policies_fired"] if cycle else 0}</div>
       <div class="k">policies fired</div></div>
  <div class="stat"><div class="v">{len(pending)}</div>
       <div class="k">awaiting Diane</div></div>
</div>

<h2>Awaiting your decision ({len(pending)})</h2>
<div class="grid">
  {decisions_html}
</div>

<h2>Coming up — next 7 days ({len(upcoming_items)})</h2>
<table>
  <tr><th>when</th><th>kind</th><th>what</th><th>readiness</th></tr>
  {upcoming_rows or "<tr><td colspan='4'>Nothing on the calendar in the next 7 days.</td></tr>"}
</table>

<h2>Open incidents ({len(snap["open_incidents"])})</h2>
<table>
  <tr><th>id</th><th>severity</th><th>status</th><th>player</th>
      <th>detected</th><th>league deadline</th>
      <th>summary</th><th>notes</th></tr>
  {incident_rows or "<tr><td colspan='8'>No open incidents.</td></tr>"}
</table>

<h2>Weather (latest per ground)</h2>
<table>
  <tr><th>Ground</th><th>Forecast max</th><th>Nearest lightning</th><th>Observed</th></tr>
  {weather_rows or "<tr><td colspan='4'>No observations.</td></tr>"}
</table>

<h2>Fixtures paused ({len(snap["paused_fixtures"])}) / cancelled ({len(snap["cancelled_fixtures"])})</h2>
<table>
  <tr><th>id</th><th>team</th><th>ground</th><th>kickoff</th><th>reason</th></tr>
  {paused_rows}{cancelled_rows or
                "<tr><td colspan='5'>No fixture interruptions.</td></tr>"}
</table>

<h2>Last 5 cycles</h2>
<table>
  <tr><th>#</th><th>started</th><th>in</th><th>cls</th>
      <th>drafts</th><th>sent</th><th>decisions</th><th>policies</th></tr>
  {cycles_rows}
</table>

<h2>Inputs by classification (24h)</h2>
<table><tr><th>classification</th><th>n</th></tr>
  {inputs_rows or "<tr><td colspan='2'>—</td></tr>"}</table>

<h2>Broadcasts by playbook (24h)</h2>
<table><tr><th>playbook</th><th>channel</th><th>status</th><th>n</th></tr>
  {bc_rows or "<tr><td colspan='4'>—</td></tr>"}</table>

<h2>Health — circuit breakers</h2>
<table>
  <tr><th>integration</th><th>state</th><th>fails</th>
      <th>last error</th><th>last success</th></tr>
  {breaker_rows or "<tr><td colspan='5'>—</td></tr>"}
</table>

<h2>Overdue registrations ({len(snap["overdue"])})</h2>
<table>
  <tr><th>id</th><th>player</th><th>team</th><th>status</th></tr>
  {overdue_rows or "<tr><td colspan='4'>All paid up.</td></tr>"}
</table>

<h2>Coach reliability — match reports ({len(snap["coach_reliability"])})</h2>
<table>
  <tr><th>id</th><th>coach</th><th>due</th><th>submitted</th><th>reliability</th></tr>
  {coach_rows or "<tr><td colspan='5'>No fixtures due yet — pre-season.</td></tr>"}
</table>

<h2>Sent in your name — last 24h ({len(snap["recent_broadcasts"])})</h2>
<table>
  <tr><th>when</th><th>status</th><th>playbook</th><th>channel</th>
      <th>segment</th><th>reach</th><th>subject</th><th></th></tr>
  {bc_recent_rows or "<tr><td colspan='8'>Quiet — nothing went out yet.</td></tr>"}
</table>

<h2>Recent actions (last 30)</h2>
<table>
  <tr><th>ts</th><th>phase</th><th>action</th><th>target_kind</th>
      <th>target_id</th><th>ok</th><th>detail</th></tr>
  {actions_rows}
</table>
</body></html>"""


def _render_broadcast(bid: str) -> tuple[int, str]:
    """Detail view for a single broadcast — what the agent actually said.

    Pulls the broadcast row, its meta, and any decision still tied to it
    so Diane can see (a) the body, (b) why it was held / who approved,
    (c) the audit timestamps. Returns (status_code, html)."""
    conn = connect()
    bc = conn.execute(
        "SELECT id, cycle_id, playbook, channel, segment, reach, subject, "
        "body, status, blocked_by, created_at, decided_at, sent_at, "
        "related_input, meta_json FROM broadcasts WHERE id=?",
        (bid,)).fetchone()
    if not bc:
        conn.close()
        return 404, (
            "<!doctype html><body style='font-family:sans-serif;padding:20px'>"
            "<h1>Not found</h1><p>No broadcast with that id.</p>"
            "<p><a href='/'>← back</a></p></body>")
    bc = dict(bc)
    # For approval-gated broadcasts (>50 reach, sponsor-tagged) the
    # decision points straight at the broadcast row. For brain-drafted
    # replies the broadcast was held without a gate fire, so the
    # decision sits on the input — follow related_input to find it.
    decision = conn.execute(
        "SELECT id, kind, status, chosen, resolved_at, resolved_note "
        "FROM decisions WHERE target_kind='broadcast' AND target_id=? "
        "ORDER BY created_at DESC LIMIT 1", (bid,)).fetchone()
    if not decision and bc["related_input"]:
        decision = conn.execute(
            "SELECT id, kind, status, chosen, resolved_at, resolved_note "
            "FROM decisions WHERE target_kind='input' AND target_id=? "
            "ORDER BY created_at DESC LIMIT 1",
            (bc["related_input"],)).fetchone()
    decision = dict(decision) if decision else None
    related_input = None
    if bc["related_input"]:
        ri = conn.execute(
            "SELECT id, sender, subject, classification, received_at "
            "FROM inputs WHERE id=?", (bc["related_input"],)).fetchone()
        related_input = dict(ri) if ri else None
    conn.close()

    meta = {}
    if bc["meta_json"]:
        try:
            meta = json.loads(bc["meta_json"])
        except json.JSONDecodeError:
            meta = {}

    def _row(k, v):
        return (f'<div class="k">{_safe(k)}</div>'
                f'<div>{_safe(v) if v not in (None, "") else "—"}</div>')

    kv_html = (
        _row("id", bc["id"])
        + _row("playbook", bc["playbook"])
        + _row("channel", bc["channel"])
        + _row("segment", bc["segment"])
        + _row("reach", bc["reach"])
        + _row("status", bc["status"])
        + _row("blocked by", bc["blocked_by"])
        + _row("created", bc["created_at"])
        + _row("decided", bc["decided_at"])
        + _row("sent", bc["sent_at"])
        + _row("cycle", bc["cycle_id"]))

    if related_input:
        ri_html = (
            f'<h2>Triggered by</h2>'
            f'<div class="kv">'
            + _row("input id", related_input["id"])
            + _row("from", related_input["sender"])
            + _row("subject", related_input["subject"])
            + _row("classified as", related_input["classification"])
            + _row("received", related_input["received_at"])
            + f'</div>')
    else:
        ri_html = ""

    if decision:
        dec_html = (
            f'<h2>Decision trail</h2>'
            f'<div class="kv">'
            + _row("decision id", decision["id"])
            + _row("kind", decision["kind"])
            + _row("status", decision["status"])
            + _row("you chose", decision["chosen"])
            + _row("resolved at", decision["resolved_at"])
            + _row("note", decision["resolved_note"])
            + f'</div>')
    else:
        dec_html = ""

    meta_html = ""
    if meta:
        meta_html = (
            f'<h2>Domain side-effects</h2>'
            f'<pre class="body-pre">{_safe(json.dumps(meta, indent=2))}</pre>')

    subj = bc["subject"] or "(no subject)"
    body = bc["body"] or ""
    return 200, f"""<!doctype html>
<html><head><meta charset="utf-8"><title>broadcast {_safe(bc["id"])}</title>
<style>
body {{ font: 14px/1.4 -apple-system,sans-serif; margin: 0; padding: 16px;
       background: #f5f5f7; color: #1d1d1f; max-width: 860px; }}
a {{ color: #0a64c4; }}
h1 {{ font-size: 18px; margin: 0 0 6px; }}
h2 {{ font-size: 14px; margin: 18px 0 6px; color: #444;
      text-transform: uppercase; letter-spacing: 0.06em; }}
.kv {{ display: grid; grid-template-columns: 140px 1fr; gap: 4px 12px;
       font-size: 13px; margin: 8px 0; background: #fff; padding: 12px;
       border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,.06); }}
.kv .k {{ color: #666; }}
.body-pre {{ background: #fff; padding: 12px; border-radius: 8px;
            box-shadow: 0 1px 2px rgba(0,0,0,.06);
            font: 13px/1.45 ui-monospace,Menlo,monospace;
            white-space: pre-wrap; word-wrap: break-word; }}
</style></head><body>
<p><a href="/">← back to dashboard</a></p>
<h1>{_safe(subj)}</h1>
<div class="kv">{kv_html}</div>
{ri_html}
{dec_html}
{meta_html}
<h2>Body</h2>
<pre class="body-pre">{_safe(body)}</pre>
</body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            html = _render(_snapshot())
            payload = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path == "/snapshot.json":
            data = json.dumps(_snapshot(), default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if self.path.startswith("/broadcast/"):
            bid = urllib.parse.unquote(self.path.split("/", 2)[2])
            code, html = _render_broadcast(bid)
            payload = html.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self.send_response(404)
        self.end_headers()

    def _read_body(self) -> tuple[bytes, dict]:
        """Pull the request body and parse it as form-encoded or JSON.
        Twilio MO sends form-encoded; FB Graph webhook sends JSON. We
        accept both so we don't have to fork the route per provider."""
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b""
        ctype = (self.headers.get("Content-Type") or "").lower()
        if "application/json" in ctype:
            try:
                return raw, json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                return raw, {}
        if "application/x-www-form-urlencoded" in ctype or not ctype:
            params = urllib.parse.parse_qs(raw.decode("utf-8"))
            # Flatten single-value lists
            return raw, {k: (v[0] if v else "") for k, v in params.items()}
        return raw, {}

    def do_POST(self):
        if self.path == "/decide":
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length).decode("utf-8")
            params = urllib.parse.parse_qs(body)
            decision_id = (params.get("decision_id") or [""])[0]
            choice = (params.get("choice") or [""])[0]
            note = (params.get("note") or [""])[0]
            if not decision_id or not choice:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"missing fields")
                return
            result = broadcast_mod.approve_decision(decision_id, choice, note)
            self.send_response(303)
            self.send_header("Location", "/")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        if self.path == "/sms/inbound":
            _, payload = self._read_body()
            res = sms_inbound.webhook_append(payload)
            code = 200 if res.get("ok") else 400
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(res).encode("utf-8"))
            return

        if self.path == "/fb/webhook":
            _, payload = self._read_body()
            res = facebook_page.webhook_append(payload)
            code = 200 if res.get("ok") else 400
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(res).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()


def main():
    init_db()
    cfg = cfg_mod.get().get("dashboard", {})
    bind = cfg.get("bind", "127.0.0.1")
    port = int(cfg.get("port", 8089))
    server = HTTPServer((bind, port), Handler)
    print(f"dashboard: http://{bind}:{port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

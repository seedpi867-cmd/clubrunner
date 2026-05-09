"""Brain executive — Claude CLI for the items where rules aren't enough.

Inputs to the brain have already been classified and routed. Each one
is novel, has negative sentiment, is a sponsor/council dispute, or
carries a safeguarding signal. The brain returns a structured Decision:

  {
    "kind": "complaint_review|incident_brief|sponsor_dispute|"
            "council_brief|safeguarding_brief|generic_brief|"
            "drafted_reply",
    "summary": "<one line>",
    "context": "<paragraph for Diane's queue>",
    "draft_reply": "<optional drafted reply body>",
    "options": [{"key":"...","label":"..."}, ...],
    "default": "ack",
    "channel": "email|sms|null",
    "segment": "<optional>",
    "reach": <int>
  }

Behaviour:
  - if dry_run = True: brain runs deterministic offline templates, no
    Claude call, but the path is identical.
  - if dry_run = False: shells out to `claude` CLI with the prompt;
    expects JSON on stdout.
"""
from __future__ import annotations

import json
import shlex
import subprocess
from typing import Any

from edge.state.db import connect, log_action, now_iso
import config as cfg_mod
from tools.broadcast import _open_decision, insert_drafted
from tools import incidents as incidents_mod
from tools import coach_no_show as coach_no_show_mod


SYSTEM = (
    "You are clubrunner's brain — the deputy of Diane Pasquale, "
    "Secretary of Henley & Grange Junior Football Club. You decide what "
    "to do with one tricky inbound item at a time. You have access to "
    "context the agent already gathered. Return ONE JSON object only, "
    "no prose, no fences. Schema:\n"
    "{\"kind\":\"complaint_review|incident_brief|sponsor_dispute|"
    "council_brief|safeguarding_brief|generic_brief|drafted_reply\","
    "\"summary\":\"<one line for queue>\",\"context\":\"<longer brief>\","
    "\"draft_reply\":\"<optional reply>\",\"options\":[{\"key\":\"...\","
    "\"label\":\"...\"}],\"default\":\"<key>\","
    "\"channel\":\"email|sms|null\",\"segment\":\"\",\"reach\":1}\n"
    "Escalate anything safeguarding-flagged with options that DO NOT "
    "include 'autosend'. Never propose autosend for negative-sentiment "
    "complaints. For sponsor and council items, propose a reply Diane "
    "can edit."
)


def _gather_context(input_id: str) -> dict:
    conn = connect()
    inp = conn.execute(
        "SELECT id, source, sender, subject, body, classification, "
        "sentiment, safeguarding, priority FROM inputs WHERE id=?",
        (input_id,)).fetchone()
    parent_hist = None
    if inp:
        parent_hist = conn.execute(
            "SELECT complaint_count, last_complaint_at, sentiment_trend "
            "FROM parent_memory WHERE parent_id IN "
            "(SELECT id FROM people WHERE LOWER(email)=LOWER(?))",
            (inp["sender"] or "",)).fetchone()
    conn.close()
    return {
        "input": dict(inp) if inp else None,
        "parent_history": dict(parent_hist) if parent_hist else None,
    }


def _offline_brief(ctx: dict) -> dict:
    """Deterministic offline equivalent — picks a plausible kind based
    on classification + sentiment + safeguarding flag, drafts a brief,
    suggests options."""
    inp = ctx["input"] or {}
    cls = (inp.get("classification") or "").lower()
    sg = bool(inp.get("safeguarding"))
    sent = inp.get("sentiment") or "neu"
    sender = inp.get("sender", "unknown")
    subj = inp.get("subject", "")
    body = (inp.get("body") or "").strip()
    excerpt = body[:300] + ("…" if len(body) > 300 else "")

    if sg:
        return {
            "kind": "safeguarding_brief",
            "summary": "Safeguarding signal — call president immediately",
            "context": (
                f"Inbound from {sender}: \"{subj}\"\n\n"
                f"{excerpt}\n\n"
                "This is flagged because the message contains language that "
                "matches the club's child-safeguarding patterns. Do NOT "
                "reply directly. Ring Geoff (President) and brief him; he "
                "will decide whether this requires a Member Protection "
                "Information Officer (MPIO) referral or a police call. "
                "Do not discuss with the parent further until briefed."),
            "draft_reply": "",
            "options": [
                {"key": "call_pres", "label": "Calling Geoff now"},
                {"key": "queue_mpio", "label": "Open MPIO referral"},
                {"key": "ack", "label": "Acknowledged — handling"},
            ],
            "default": "call_pres",
            "channel": None,
            "segment": "",
            "reach": 0,
        }

    if cls == "incident":
        # Open the incident row (idempotent on input id) and assemble
        # the brief from the agent's already-collected context. Diane
        # never sees a bare "injury reported — over to you" — she sees
        # severity, the 24h league deadline, who the player is, and
        # what we did the last three times.
        cyc = ctx.get("_cycle_id")
        opened = incidents_mod.open_incident(
            cycle_id=cyc or 0, input_id=inp.get("id") or "",
            sender=sender, subject=subj, body=body)
        history = incidents_mod._recent_incidents(3)
        history_lines = "\n".join(
            f"- {h['detected_at'][:10]} {h['kind']}/{h['severity']}: "
            f"{(h['summary'] or '')[:80]} [{h['status']}]"
            for h in history) or "- (no prior incidents on file)"
        player = opened.get("player")
        if player and not opened.get("multi_child"):
            player_line = (
                f"Player: {player['first_name']} {player['last_name']} "
                f"({player['team_name']}; rego={player['rego_status']})")
        elif opened.get("multi_child"):
            sibs = ", ".join(
                f"{s['first_name']} ({s['team_name']})"
                for s in opened.get("siblings", []))
            player_line = (
                f"Player: AMBIGUOUS — sender has multiple kids "
                f"registered: {sibs}. Confirm which one before reporting.")
        else:
            player_line = (
                "Player: unknown — sender's email isn't in the parents "
                "table. Either someone else's contact or an unregistered "
                "child. Confirm details before reporting.")
        sev = opened.get("severity", "moderate")
        due = opened.get("league_report_due", "")
        return {
            "kind": "incident_brief",
            "summary": (
                f"Injury reported — {sev} (league deadline {due[:16]})"),
            "context": (
                f"From {sender}: {subj}\n\n{excerpt}\n\n"
                f"{player_line}\n"
                f"Severity: {sev} (auto-classified from message text — "
                f"verify when you call the parent)\n"
                f"League incident report due by: {due}\n"
                f"Insurance: per club policy, registered active players "
                f"are covered. Confirm rego status above before "
                f"submitting the league report.\n\n"
                f"Recent incidents:\n{history_lines}\n\n"
                f"Suggested calls: parent first (welfare check + "
                f"verify what happened), then president, then league. "
                f"Don't reply by email — phone."),
            "draft_reply": "",
            "options": [
                {"key": "ack",            "label": "Acknowledged — I'm on it"},
                {"key": "call_pres",      "label": "Calling Geoff (President)"},
                {"key": "report_league",  "label": "League report submitted"},
                {"key": "close_incident", "label": "Close incident (resolved)"},
            ],
            "default": "ack",
            "channel": None,
            "segment": "",
            "reach": 0,
            "_incident_id": opened.get("id"),
        }

    if cls == "coach_no_show":
        # The brain receives the report after `think()` has already
        # resolved sender → manager → team → coach and (when the
        # reporter is the manager) bumped coach_memory.no_shows. The
        # resolved data is on ctx['coach_no_show']; the offline brief
        # frames it.
        cns = ctx.get("coach_no_show") or {}
        coach = cns.get("coach")
        team = cns.get("team")
        manager = cns.get("manager")
        record = cns.get("record") or {}
        fixtures_rows = cns.get("fixtures") or []
        reporter_role = cns.get("reporter_role", "unknown")
        ambiguous = bool(cns.get("ambiguous"))
        candidates = cns.get("candidate_teams") or []
        ns = int(record.get("no_shows") or 0)
        rd = int(record.get("reports_due") or 0)
        rs = int(record.get("reports_submitted") or 0)
        rel = record.get("reliability")
        if team:
            team_line = f"Team: {team['name']} ({team['age_grade']})"
        elif ambiguous:
            cand = ", ".join(
                f"{t['name']}" for t in candidates) or "—"
            team_line = (
                f"Team: ambiguous — {manager['name'] if manager else 'sender'} "
                f"manages multiple teams ({cand}) and the report didn't name "
                "one. no_shows counter NOT incremented; phone the manager "
                "to confirm which team before acting.")
        else:
            team_line = (
                "Team: not auto-resolved — sender isn't on file as a "
                "current manager or coach. Verify the report before "
                "acting.")
        coach_line = (
            f"Coach: {coach['name']} ({coach.get('email') or '—'}, "
            f"{coach.get('sms') or '—'})" if coach else
            "Coach: not on file for this team — check the team contact list.")
        manager_line = (
            f"Team Manager: {manager['name']} "
            f"({manager.get('email') or '—'}, "
            f"{manager.get('sms') or '—'})" if manager else
            "Team Manager: not on file.")
        if reporter_role == "manager" and ambiguous:
            reporter_line = (
                "Reporter: team manager — credible, but the team they "
                "mean isn't pinned down. Counter held until you confirm.")
        else:
            reporter_line = {
                "manager": ("Reporter: team manager — credible source. "
                            "no_shows counter incremented."),
                "coach":   ("Reporter: another coach (assistant or peer) — "
                            "no counter increment until manager confirms."),
                "unknown": ("Reporter: not on file as a manager or coach. "
                            "Treat as unverified; phone the manager to confirm."),
            }[reporter_role]
        # Framing: first/few no-shows reads as welfare (call them, are
        # they OK?). A pattern (>=3 historical, or >=2 with weak report
        # record) reads as recruiting. We never label the coach — we
        # frame Diane's call.
        if ns >= 3 or (ns >= 2 and (rel or 1.0) < 0.6):
            framing = (
                "Pattern: this is the {n}th recorded no-show. Combined "
                "with the report record below, the team is operating "
                "without reliable coaching. Recruiting an assistant or a "
                "co-coach is the conversation to have — phone the manager "
                "first, then the coach. Don't post anything until both "
                "calls are done."
            ).format(n=ns)
        elif ns == 1 and reporter_role == "manager":
            framing = (
                "First recorded no-show. Most likely a one-off — illness, "
                "family. Phone the coach for a welfare check before "
                "anything else; the manager has covered tonight already.")
        else:
            framing = (
                "Recurring no-show watch — {n} on file. Phone the coach "
                "for a welfare check; if there's a real reason "
                "(injury / family), agree on cover for the next match.").format(
                    n=ns or 0)
        if rd > 0 or rs > 0 or ns > 0:
            label = ("strong record" if (rel or 0) >= 0.85 else
                     "patchy record" if (rel or 0) >= 0.5 else
                     "weak record")
            history_line = (
                f"Coach record: {rs}/{rd} match reports submitted, "
                f"{ns} no-shows ({label}, reliability "
                f"{round(rel, 2) if rel is not None else '—'}).")
        else:
            history_line = (
                "Coach record: no prior history on file — pre-season or "
                "new appointment.")
        if fixtures_rows:
            fx = "\n".join(
                f"- R{r['round']} vs {r['opponent']} — "
                f"{r['kickoff'][:16].replace('T', ' ')} ({r['home_or_away']})"
                for r in fixtures_rows)
        else:
            fx = "- (no scheduled fixtures in the database for this team)"
        team_label = team["name"] if team else "team unknown"
        return {
            "kind": "coach_no_show",
            "summary": (f"Coach no-show — {team_label} (×{ns or '?'})"),
            "context": (
                f"From {sender}: {subj}\n\n{excerpt}\n\n"
                f"{team_line}\n{coach_line}\n{manager_line}\n"
                f"{reporter_line}\n{history_line}\n\n"
                f"Next fixtures:\n{fx}\n\n"
                f"{framing}"),
            "draft_reply": "",
            "options": [
                {"key": "ack",       "label": "Phoning the coach now"},
                {"key": "manager",   "label": "Phoning the team manager"},
                {"key": "call_pres", "label": "Looping in the President"},
                {"key": "discard",   "label": "Misreport — not a no-show"},
            ],
            "default": "ack",
            "channel": None,
            "segment": "",
            "reach": 0,
        }

    if cls == "coach_admin":
        # A coach pulling out is a recruiting problem with a deadline:
        # the team's next fixture. Surface the team, the manager (Diane's
        # likely fallback contact), the next three fixtures, the coach's
        # report-submission record (so a chronic no-reporter resigning
        # reads differently to a 100%-reliable coach burning out), and
        # any assistant the coach has — Diane has to make the ask, but
        # the brief saves her the lookup.
        from edge.state.db import connect as _connect
        conn = _connect()
        coach_row = conn.execute(
            "SELECT id, name FROM people WHERE LOWER(email)=LOWER(?) "
            "AND role='coach'", (sender or "",)).fetchone()
        team_row = None
        manager_row = None
        fixtures_rows: list = []
        reliability_row = None
        if coach_row:
            team_row = conn.execute(
                "SELECT id, name, age_grade FROM teams WHERE coach_id=?",
                (coach_row["id"],)).fetchone()
            reliability_row = conn.execute(
                "SELECT reports_due, reports_submitted, no_shows, reliability "
                "FROM coach_memory WHERE coach_id=?",
                (coach_row["id"],)).fetchone()
            if team_row:
                manager_row = conn.execute(
                    "SELECT p.name, p.email, p.sms FROM teams t "
                    "JOIN people p ON t.manager_id=p.id WHERE t.id=?",
                    (team_row["id"],)).fetchone()
                fixtures_rows = conn.execute(
                    "SELECT round, opponent, kickoff, home_or_away "
                    "FROM fixtures WHERE team_id=? AND status='scheduled' "
                    "AND kickoff > ? ORDER BY kickoff LIMIT 3",
                    (team_row["id"], now_iso())).fetchall()
        conn.close()
        team_line = (
            f"Team: {team_row['name']} ({team_row['age_grade']})"
            if team_row else
            "Team: not auto-resolved — sender email isn't on file as a "
            "current head coach. Check manually before responding.")
        manager_line = (
            f"Team Manager: {manager_row['name']} ({manager_row['email']}, "
            f"{manager_row['sms']})" if manager_row else
            "Team Manager: not on file.")
        # Reliability framing — not a judgement, just data Diane should
        # have before the call. A 0.4 coach resigning means recruitment
        # AND it solves an existing problem; a 1.0 coach resigning is
        # pure loss. The line stays absent when there's no history.
        if reliability_row and (
                (reliability_row["reports_due"] or 0) > 0 or
                (reliability_row["no_shows"] or 0) > 0):
            rel = reliability_row["reliability"]
            rd = reliability_row["reports_due"] or 0
            rs = reliability_row["reports_submitted"] or 0
            ns = reliability_row["no_shows"] or 0
            label = ("strong record" if (rel or 0) >= 0.85 else
                     "patchy record" if (rel or 0) >= 0.5 else
                     "weak record")
            reliability_line = (
                f"Coach record: {rs}/{rd} match reports submitted, "
                f"{ns} no-shows ({label}, reliability "
                f"{round(rel, 2) if rel is not None else '—'}).")
        else:
            reliability_line = (
                "Coach record: no reporting history yet — pre-season "
                "or new appointment.")
        if fixtures_rows:
            fx = "\n".join(
                f"- R{r['round']} vs {r['opponent']} — "
                f"{r['kickoff'][:16].replace('T', ' ')} ({r['home_or_away']})"
                for r in fixtures_rows)
        else:
            fx = "- (no scheduled fixtures in the database for this team)"
        return {
            "kind": "coach_admin",
            "summary": (f"Coach stepping back — "
                        f"{team_row['name'] if team_row else 'team unknown'}"),
            "context": (
                f"From {sender}: {subj}\n\n{excerpt}\n\n"
                f"{team_line}\n{manager_line}\n{reliability_line}\n\n"
                f"Next fixtures:\n{fx}\n\n"
                "Recruitment deadline is the next match — assistant coaches "
                "or the team manager are usually the fastest fill. Don't "
                "post publicly until you've spoken to the coach (they may "
                "still want to finish the round). Do the call, then update "
                "the team contact list."),
            "draft_reply": "",
            "options": [
                {"key": "ack",       "label": "Calling the coach now"},
                {"key": "phone",     "label": "Phoning the team manager"},
                {"key": "call_pres", "label": "Looping in the President"},
                {"key": "discard",   "label": "Not a real resignation"},
            ],
            "default": "ack",
            "channel": None,
            "segment": "",
            "reach": 0,
        }

    if cls == "refund_request":
        return {
            "kind": "complaint_review",
            "summary": "Refund request — needs your call",
            "context": (
                f"From {sender}: {subj}\n\n{excerpt}\n\n"
                "Refunds over $200 require committee sign-off per "
                "club policy. The committee usually approves "
                "injury-related refunds at full amount, behaviour-related "
                "at 50%, and change-of-mind cases case-by-case. The "
                "treasurer needs the player's payment record (PlayHQ) "
                "and a committee email thread."),
            "draft_reply": (
                "Hi,\n\nThanks for letting me know — sorry to hear that. "
                "Refunds need to go past the committee, which we'll do "
                "this week. I'll come back to you by Sunday with a "
                "confirmation and the process. In the meantime, please "
                "send through the medical certificate (if injury) so we "
                "can move quickly.\n\nDiane\n"),
            "options": [
                {"key": "send_draft", "label": "Send drafted reply"},
                {"key": "edit", "label": "Edit before send"},
                {"key": "decline", "label": "Decline reply"},
            ],
            "default": "edit",
            "channel": "email",
            "segment": f"sender:{sender}",
            "reach": 1,
        }

    if cls == "complaint" or sent == "neg":
        history = ctx.get("parent_history") or {}
        repeat = (history.get("complaint_count") or 0) > 0
        return {
            "kind": "complaint_review",
            "summary": ("Complaint" + (" — repeat sender" if repeat else "")),
            "context": (
                f"From {sender}: {subj}\n\n{excerpt}\n\n"
                + ("This sender has complained before — handle with extra "
                   "care, copy the President.\n" if repeat else "")
                + "Tone-sensitive — we draft, you decide."),
            "draft_reply": (
                "Hi,\n\nThanks for raising this. I've read your message "
                "carefully — I want to make sure we get this right. Can I "
                "give you a call later today to talk through it?\n\n"
                "Diane\n"),
            "options": [
                {"key": "send_draft", "label": "Send drafted reply"},
                {"key": "edit", "label": "Edit before send"},
                {"key": "phone", "label": "Phone instead"},
            ],
            "default": "edit",
            "channel": "email",
            "segment": f"sender:{sender}",
            "reach": 1,
        }

    if cls == "sponsor":
        return {
            "kind": "sponsor_dispute",
            "summary": f"Sponsor inbound — {sender}",
            "context": (
                f"From {sender}: {subj}\n\n{excerpt}\n\n"
                "Sponsor relationship is contractual; treat the response "
                "carefully. Treasurer (Robin) holds the contracts."),
            "draft_reply": (
                "Hi,\n\nThanks for the update — I've recorded the "
                "change and will get it actioned in the next "
                "newsletter and on the page banners. Anything else you "
                "need from us this season?\n\nDiane\n"),
            "options": [
                {"key": "send_draft", "label": "Send drafted reply"},
                {"key": "edit", "label": "Edit before send"},
                {"key": "loop_treasurer", "label": "Loop in Treasurer"},
            ],
            "default": "send_draft",
            "channel": "email",
            "segment": f"sender:{sender}",
            "reach": 1,
        }

    if cls in ("council", "council_inbound"):
        return {
            "kind": "council_brief",
            "summary": "Council notice — requires response",
            "context": (
                f"From {sender}: {subj}\n\n{excerpt}\n\n"
                "Multi-stakeholder — council, ground booking, possibly "
                "the league. Worth a phone call rather than email "
                "ping-pong."),
            "draft_reply": "",
            "options": [
                {"key": "phone", "label": "Phone the contact"},
                {"key": "draft", "label": "Draft a reply"},
                {"key": "ack", "label": "Acknowledge for now"},
            ],
            "default": "phone",
            "channel": None,
            "segment": "",
            "reach": 0,
        }

    return {
        "kind": "generic_brief",
        "summary": subj or "Item for your review",
        "context": (
            f"From {sender}: {subj}\n\n{excerpt}\n\n"
            "Didn't match an automation — over to you."),
        "draft_reply": "",
        "options": [
            {"key": "ack", "label": "Acknowledge — handling"},
            {"key": "discard", "label": "Discard"},
        ],
        "default": "ack",
        "channel": None,
        "segment": "",
        "reach": 0,
    }


def _claude_brief(prompt: str, max_seconds: int) -> dict:
    cli = cfg_mod.get().get("brain", {}).get("cli_path", "claude")
    cmd = [cli, "-p", prompt, "--output-format", "stream-json"]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True,
                             timeout=max_seconds, check=False)
    except (subprocess.TimeoutExpired, FileNotFoundError) as ex:
        return {"_error": f"claude cli failed: {ex}"}
    text = out.stdout.strip()
    # Try JSON-parse the last line / a JSON block
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"_error": f"could not parse: {text[:200]}"}


def think(cycle_id: int, input_id: str) -> dict:
    cfg = cfg_mod.get().get("brain", {})
    dry = bool(cfg.get("dry_run", True))
    ctx = _gather_context(input_id)
    if not ctx["input"]:
        return {"ok": False, "reason": "input not found"}

    ctx["_cycle_id"] = cycle_id

    # Incident classification: open the row + enrich ctx BEFORE either
    # path runs. The offline path looks the row up via uuid5(input_id);
    # the Claude path gets severity/deadline/recent-history baked into
    # ctx so its brief is grounded in the world model. Without this
    # hoist, dry_run=False would silently skip creating the incidents
    # row — the league deadline would never start ticking and Diane's
    # dashboard would say "no open incidents" while a kid is in
    # hospital.
    inp_cls = (ctx["input"].get("classification") or "").lower()
    if inp_cls == "incident":
        opened = incidents_mod.open_incident(
            cycle_id=cycle_id,
            input_id=ctx["input"]["id"],
            sender=ctx["input"].get("sender"),
            subject=ctx["input"].get("subject"),
            body=ctx["input"].get("body"))
        ctx["incident"] = opened
        ctx["incident_history"] = incidents_mod._recent_incidents(3)
    elif inp_cls == "coach_no_show":
        # Resolve sender → manager → team → coach and bump
        # coach_memory.no_shows (idempotent on input_id+coach_id) BEFORE
        # the dry/online brief fork. Following the recurring lesson:
        # domain-table side effects must happen outside config-flag
        # branches, otherwise dry_run flips silently desync state.
        ctx["coach_no_show"] = coach_no_show_mod.record_world_model(
            cycle_id=cycle_id,
            input_id=ctx["input"]["id"],
            sender=ctx["input"].get("sender"),
            subject=ctx["input"].get("subject"),
            body=ctx["input"].get("body"))

    if dry:
        brief = _offline_brief(ctx)
    else:
        prompt = (SYSTEM + "\n\nContext:\n" +
                  json.dumps(ctx, default=str, indent=2))
        max_s = int(cfg.get("max_seconds", 90))
        brief = _claude_brief(prompt, max_s)
        if "_error" in brief:
            log_action(cycle_id, "brain", "claude_failed",
                       "input", input_id, False, brief["_error"])
            brief = _offline_brief(ctx)

    # Open the decision. If brief includes a draft_reply, append the
    # full drafted text to the context so Diane can read what she'd
    # be sending before tapping "Send drafted reply". Without this,
    # the decision card shows the inbound but hides the outbound —
    # the option labels become guesswork.
    options = brief.get("options", [])
    decision_context = brief.get("context", "")
    if brief.get("draft_reply"):
        decision_context = (
            decision_context.rstrip()
            + "\n\n--- DRAFTED REPLY (held; sends only if you tap "
            + "'Send drafted reply') ---\n"
            + brief["draft_reply"].rstrip())
    did = _open_decision(
        cycle_id, brief.get("kind", "generic_brief"),
        "input", input_id,
        brief.get("summary", "Item for review"),
        decision_context,
        options,
        brief.get("default", "ack"))

    # Stash the drafted reply on the broadcast table as 'drafted'.
    # Critically NOT through submit() — submit() runs gates and
    # autosends anything under the 50-reach threshold. A 1-recipient
    # refund or sponsor reply would clear every gate and go out
    # before Diane saw it, defeating the entire point of escalating
    # to the brain. insert_drafted holds the broadcast in 'drafted'
    # state; approve_decision picks the linked draft up by
    # related_input and sends it (or cancels it) when Diane chooses.
    bid = None
    if brief.get("draft_reply") and brief.get("channel"):
        bid = insert_drafted(
            cycle_id=cycle_id, playbook="brain_drafted_reply",
            channel=brief["channel"],
            segment=brief.get("segment", f"sender:{ctx['input']['sender']}"),
            reach=int(brief.get("reach", 1)),
            subject=f"Re: {ctx['input']['subject']}",
            body=brief["draft_reply"],
            target_kind="input", target_id=input_id,
            related_input=input_id)

    # Mark input as escalated/done
    conn = connect()
    conn.execute(
        "UPDATE inputs SET status='escalated' WHERE id=?", (input_id,))
    conn.close()
    log_action(cycle_id, "brain", "thought",
               "input", input_id, True,
               f"kind={brief.get('kind')} draft_id={bid} dec={did}")
    return {"ok": True, "decision_id": did, "draft_broadcast_id": bid,
            "brief": brief}

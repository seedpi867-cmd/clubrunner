"""Event reminder playbook — fired when gcal mints a reminder tick.

Audience routing:
  committee  -> 7 portfolio members, channel=email (low-reach, autosend)
  coaches    -> coach + manager rows from people, channel=email
  all_club   -> all parents on file, channel=email (high reach → gate)
  team:<tid> -> that team's parents, channel=email
  diane      -> personal — only Diane. Goes to her decision queue as a
                soft 'reminder' card, not a broadcast.

Body composition is deliberately bare: title, start, location, the
description Diane already wrote. We don't paraphrase or summarise — the
event description is the source of truth, and a reminder that subtly
edits Diane's wording is a reminder she stops trusting.

Idempotency:
  The reminder *input* is unique per (event_id, window) at upsert_input
  time. The *broadcast* additionally goes through the dedup gate keyed
  on playbook + segment + window — same window for the same event won't
  fire a second broadcast even if the input row gets re-classified.
"""
from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from typing import Any

from edge.state.db import connect, log_action, now_iso
from tools import broadcast


_REMINDER_RE = re.compile(
    r"^reminder:(?P<eid>[a-z0-9_]+):(?P<window>[a-z0-9]+)$",
    re.IGNORECASE)


def _parse_input_id(input_id: str) -> tuple[str | None, str | None]:
    # input.id is "gcal:reminder:evt_xxx:7d"; the source_id is everything
    # after the first colon.
    if not input_id.startswith("gcal:"):
        return None, None
    m = _REMINDER_RE.match(input_id.split(":", 1)[1])
    if not m:
        return None, None
    return m.group("eid"), m.group("window")


def _load_event(event_id: str) -> dict | None:
    conn = connect()
    row = conn.execute(
        "SELECT id, summary, description, location, start_at, end_at, "
        "audience, team_id FROM events WHERE id=?", (event_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def _audience_recipients(audience: str, team_id: str | None) -> dict:
    """Return {'segment', 'reach', 'channel', 'recipients_summary'}."""
    conn = connect()
    if audience == "committee":
        rows = conn.execute(
            "SELECT id FROM people WHERE role='committee'").fetchall()
        conn.close()
        return {"segment": "committee", "reach": len(rows),
                "channel": "email"}
    if audience == "coaches":
        rows = conn.execute(
            "SELECT id FROM people WHERE role IN ('coach','manager')"
        ).fetchall()
        conn.close()
        return {"segment": "coaches_and_managers", "reach": len(rows),
                "channel": "email"}
    if audience == "team" and team_id:
        rows = conn.execute(
            "SELECT DISTINCT pl.parent_id FROM players pl "
            "WHERE pl.team_id=? AND pl.parent_id IS NOT NULL",
            (team_id,)).fetchall()
        conn.close()
        return {"segment": f"team:{team_id}", "reach": len(rows),
                "channel": "email"}
    if audience == "all_club":
        rows = conn.execute(
            "SELECT id FROM people WHERE role='parent'").fetchall()
        conn.close()
        return {"segment": "all_parents", "reach": len(rows),
                "channel": "email"}
    if audience == "diane":
        conn.close()
        return {"segment": "diane", "reach": 1, "channel": "email"}
    conn.close()
    # Unknown — fall back to committee, the safe default.
    return {"segment": "committee", "reach": 7, "channel": "email"}


def _delta_phrase(delta: timedelta) -> str:
    """Phrase the delta the recipient actually faces, not the window
    label. The 2h window now fires for anything in (0, 2h15m] (so the
    agent doesn't go silent on late-add events), which means the
    body's time phrase has to be computed, not hardcoded — telling a
    recipient "in about 2 hours" when the event is 20 minutes away
    is the kind of small lie that makes Diane stop trusting the agent.
    """
    secs = int(delta.total_seconds())
    if secs <= 0:
        return "now"
    minutes = secs // 60
    if minutes < 60:
        return f"in about {minutes} minute{'s' if minutes != 1 else ''}"
    hours = minutes / 60
    if hours < 4:
        rounded = round(hours * 2) / 2  # nearest half hour
        if rounded == int(rounded):
            return f"in about {int(rounded)} hour{'s' if rounded != 1 else ''}"
        return f"in about {rounded} hours"
    days = secs // 86400
    if days <= 0:
        return f"later today"
    if days == 1:
        return "tomorrow"
    if days < 7:
        return f"in {days} days"
    return "one week away"


def _format_when(start_at: str, window: str) -> str:
    try:
        dt = datetime.fromisoformat(start_at)
        when = dt.strftime("%a %d %b, %I:%M%p").replace(" 0", " ")
        delta = dt - datetime.now()
    except ValueError:
        return start_at
    return f"{when} ({_delta_phrase(delta)})"


def _compose_body(event: dict, window: str) -> tuple[str, str]:
    """Return (subject, body)."""
    when_line = _format_when(event["start_at"], window)
    audience = event["audience"]

    salutation = {
        "committee": "Hi committee,",
        "coaches": "Hi coaches and managers,",
        "all_club": "Hi all,",
        "team": "Hi team,",
        "diane": "Diane,",
    }.get(audience, "Hi all,")

    desc = (event.get("description") or "").strip()
    location = event.get("location") or ""
    title = event.get("summary") or "Club event"

    try:
        dt = datetime.fromisoformat(event["start_at"])
        when_phrase = _delta_phrase(dt - datetime.now()).capitalize()
    except (ValueError, KeyError):
        when_phrase = when_line

    lines = [
        salutation,
        "",
        f"Quick reminder — {when_phrase}: {title}.",
        f"When: {when_line}",
    ]
    if location:
        lines.append(f"Where: {location}")
    if desc:
        lines += ["", desc]
    lines += ["", "Diane"]
    body = "\n".join(lines)

    subject = f"Reminder ({window}) — {title}"
    return subject, body


def _diane_decision(cycle_id: int, event: dict, window: str,
                    related_input: str | None) -> dict:
    """Diane-tagged events stay off the wire. Drop a low-priority card
    in her queue so she's reminded without it counting against the
    broadcast budget."""
    did = f"dec_{uuid.uuid4().hex[:12]}"
    when = _format_when(event["start_at"], window)
    summary = f"Personal reminder ({window}) — {event['summary']}"
    ctx = (f"Event: {event['summary']}\n"
           f"When: {when}\n"
           f"Where: {event.get('location', '') or ''}\n"
           f"{event.get('description', '') or ''}")
    options = [
        {"key": "ack", "label": "Got it"},
        {"key": "snooze", "label": "Remind me again later"},
    ]
    conn = connect()
    conn.execute(
        "INSERT INTO decisions(id, cycle_id, kind, target_kind, target_id, "
        "summary, context, options_json, default_option, created_at) "
        "VALUES(?,?, 'personal_reminder','event',?,?,?,?,?,?)",
        (did, cycle_id, event["id"], summary[:200], ctx[:4000],
         json.dumps(options), "ack", now_iso()))
    conn.execute("UPDATE inputs SET status='done' WHERE id=?",
                 (related_input,)) if related_input else None
    conn.close()
    log_action(cycle_id, "execute", "personal_reminder_opened",
               "decision", did, True,
               f"event={event['id']} window={window}")
    return {"ok": True, "kind": "personal", "decision_id": did}


def run(cycle_id: int, related_input: str | None = None) -> dict[str, Any]:
    if not related_input:
        return {"ok": False, "reason": "event_reminder needs an input"}

    event_id, window = _parse_input_id(related_input)
    if not event_id or not window:
        log_action(cycle_id, "execute", "event_reminder_unparseable",
                   "input", related_input, False, "no event_id/window")
        return {"ok": False, "reason": "input id not a reminder"}

    event = _load_event(event_id)
    if not event:
        log_action(cycle_id, "execute", "event_reminder_no_event",
                   "input", related_input, False, event_id)
        return {"ok": False, "reason": f"event {event_id} not found"}

    audience = event["audience"]
    if audience == "diane":
        return _diane_decision(cycle_id, event, window, related_input)

    aud = _audience_recipients(audience, event["team_id"])
    subject, body = _compose_body(event, window)

    res = broadcast.submit(
        cycle_id=cycle_id,
        playbook="event_reminder",
        channel=aud["channel"],
        # Window in segment is what makes the dedup key per-window:
        # a 7d send and a 1d send are different segments and don't
        # collide in the dedup gate.
        segment=f"{aud['segment']}:{window}",
        reach=aud["reach"],
        subject=subject,
        body=body,
        target_kind="event",
        target_id=event["id"],
        related_input=related_input,
    )
    log_action(cycle_id, "execute", "event_reminder",
               "event", event["id"], True,
               f"audience={audience} window={window} reach={aud['reach']} "
               f"status={res['status']}")
    return {"ok": True, "kind": "broadcast", "result": res}

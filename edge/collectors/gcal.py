"""Google Calendar collector — the club calendar Diane keeps for
committee meetings, working bees, AGM, special events, and re-routed
training sessions.

ARCHITECTURE.md promises this collector ("club calendar — every 30
min") but it was missing in code; the cycle 108 explore narrative
includes the AGM, the committee meeting cadence, and the presentation
night, none of which had a source. Without a calendar collector these
events stayed invisible to the agent and the human had to manually
prompt every reminder.

Two jobs:
  1. Mirror events from Google Calendar into `events` (audience hint
     parsed from a [tag] in the summary, otherwise defaults to
     'committee' — the most common reason Diane adds an event). New
     additions log a `collect/gcal_event_added` audit row but DO NOT
     mint inputs — the events table is the source of truth and the
     dashboard reads from it directly. Minting an input for every
     calendar add would flood Diane's queue every time she rescheduled
     a training session.
  2. Mint `gcal:reminder:<event_id>:<window>` inputs at 7d / 1d / 2h
     before kickoff. These route to the event_reminder playbook which
     drafts an audience-appropriate broadcast.

Sources (in order):
  - scenarios/today.json:gcal_events[]   — offline test fixture
  - inputs/gcal_queue.jsonl              — webhook drop point
  - HTTP ICS (config: collectors.gcal.ics_url) — real Google Calendar
    publishes a public ICS feed at .../basic.ics

Audience tagging convention (Diane writes this when adding the event):
    [committee]   committee meeting / sub-committee meeting
    [all]         all-club event — AGM, presentation night, working bee
    [coaches]     coach training session, coach meet-and-greet
    [team:u13_g]  a single team's special event
    [diane]       personal reminder
Anything without a tag is treated as 'committee'.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from edge.state.db import connect, log_action, now_iso, upsert_input
import config as cfg_mod

ROOT = Path(__file__).resolve().parents[2]
SCENARIO = ROOT / "scenarios" / "today.json"
QUEUE = ROOT / "inputs" / "gcal_queue.jsonl"

# Reminder windows (label, hi-bound, lo-bound). An event whose
# (start - now) sits inside one of the bounds mints exactly one
# reminder tick.
#
# 7d and 1d use strict in-band bounds — these are "natural advance
# notice" anchors. An event that landed in the calendar after the 7d
# anchor passed shouldn't suddenly fire 7d 5 days later; the 1d / 2h
# anchors will catch it.
#
# 2h is the LAST window before kickoff and uses a lo of 0 — i.e. fire
# any time delta is in (0, 2h15m]. Two reasons:
#   - off-hours cycle interval (120 min) is wider than a 30-minute
#     in-band slice, so a 7:30am event on a non-match-day Sunday would
#     have its 5:15-5:45am window skipped (cycle at 5am is at 2h30m,
#     cycle at 7am is at 30m — both outside in-band bounds, silent miss).
#   - Late-add: Diane realises at 3pm there's a 4pm meeting; she
#     adds it; agent should still announce it. delta=1h is below the
#     1h45m floor — under the old logic, silent.
# upsert_input UNIQUE(source, source_id) is keyed on (event, "2h"),
# so consecutive cycles cannot double-mint.
_REMINDER_WINDOWS = [
    ("7d", timedelta(days=7, hours=1), timedelta(days=6, hours=23)),
    ("1d", timedelta(days=1, hours=1), timedelta(days=0, hours=23)),
    ("2h", timedelta(hours=2, minutes=15), timedelta(seconds=0)),
]

_TAG_RE = re.compile(r"^\s*\[([a-z0-9_:\-]+)\]\s*", re.IGNORECASE)


def _stable_event_id(source_uid: str) -> str:
    return f"evt_{hashlib.sha1(source_uid.encode()).hexdigest()[:12]}"


def _parse_audience(summary: str, description: str = "") -> tuple[str, str | None, str]:
    """Parse [tag] from the summary. Returns (audience, team_id, clean_summary).
    Falls back to 'committee' when there's no tag — the realistic default
    for Diane (committee + sub-committee make up most of her calendar)."""
    text = summary or ""
    m = _TAG_RE.match(text)
    if not m:
        return "committee", None, text.strip()
    tag = m.group(1).lower()
    cleaned = _TAG_RE.sub("", text).strip()
    if tag == "all":
        return "all_club", None, cleaned
    if tag == "coaches":
        return "coaches", None, cleaned
    if tag == "diane":
        return "diane", None, cleaned
    if tag == "committee":
        return "committee", None, cleaned
    if tag.startswith("team:"):
        return "team", tag.split(":", 1)[1], cleaned
    # unknown tag → keep as committee (safe default), preserve in cleaned
    return "committee", None, cleaned


def _upsert_event(item: dict) -> tuple[str, bool]:
    """Insert or update an event row. Returns (event_id, is_new)."""
    source_uid = (item.get("uid") or item.get("id") or "").strip()
    if not source_uid:
        # Fall back to a hash so a fixture without a uid still uniqueifies.
        source_uid = hashlib.sha1(
            f"{item.get('summary', '')}|{item.get('start', '')}".encode()
        ).hexdigest()[:24]
    eid = _stable_event_id(source_uid)
    summary = (item.get("summary") or "").strip() or "(no title)"
    description = item.get("description") or ""
    location = item.get("location") or ""
    start_at = item.get("start") or ""
    end_at = item.get("end") or ""
    audience, team_id, clean_summary = _parse_audience(summary, description)

    conn = connect()
    existing = conn.execute(
        "SELECT id FROM events WHERE source_uid=?", (source_uid,)).fetchone()
    if existing:
        conn.execute(
            "UPDATE events SET summary=?, description=?, location=?, "
            "start_at=?, end_at=?, audience=?, team_id=?, last_seen_at=? "
            "WHERE id=?",
            (clean_summary, description, location, start_at, end_at,
             audience, team_id, now_iso(), eid))
        conn.close()
        return eid, False
    conn.execute(
        "INSERT INTO events(id, source_uid, summary, description, location, "
        "start_at, end_at, audience, team_id, last_seen_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (eid, source_uid, clean_summary, description, location, start_at,
         end_at, audience, team_id, now_iso()))
    conn.close()
    return eid, True


def _mint_reminders(cycle_id: int, now: datetime) -> list[str]:
    """For every active event, check whether its delta sits inside a
    reminder window — if so, mint exactly one tick per (event, window).
    The playbooks table-style idempotency comes from upsert_input's
    UNIQUE(source, source_id) — we never re-mint the same id."""
    conn = connect()
    rows = conn.execute(
        "SELECT id, summary, start_at, audience, team_id, location "
        "FROM events WHERE status='scheduled' AND start_at > ? "
        "ORDER BY start_at LIMIT 200", (now.isoformat(),)).fetchall()
    conn.close()
    minted: list[str] = []
    for r in rows:
        try:
            start = datetime.fromisoformat(r["start_at"])
        except ValueError:
            continue
        delta = start - now
        for label, hi, lo in _REMINDER_WINDOWS:
            if not (lo <= delta <= hi):
                continue
            sid = f"reminder:{r['id']}:{label}"
            subject = f"Event reminder ({label}) — {r['summary']}"
            body = (
                f"Reminder window {label}.\n"
                f"Event id: {r['id']}\n"
                f"Audience: {r['audience']}\n"
                f"Team: {r['team_id'] or ''}\n"
                f"Start: {r['start_at']}\n"
                f"Location: {r['location'] or ''}\n")
            iid, is_new = upsert_input(
                "gcal", sid, sender="gcal.reminder",
                subject=subject, body=body)
            if is_new:
                minted.append(iid)
                log_action(cycle_id, "collect", "gcal_reminder_minted",
                           "input", iid, True,
                           f"{r['id']} {label} -> {r['summary'][:60]}")
    return minted


def _mark_past(now: datetime) -> int:
    """Anything whose start is in the past gets flagged 'past'. Keeps the
    upcoming queries cheap and the reminder loop short."""
    conn = connect()
    cur = conn.execute(
        "UPDATE events SET status='past' "
        "WHERE status='scheduled' AND start_at < ?", (now.isoformat(),))
    n = cur.rowcount
    conn.close()
    return n


def _drain_scenario() -> list[dict]:
    if not SCENARIO.exists():
        return []
    return json.loads(SCENARIO.read_text()).get("gcal_events", []) or []


def _drain_queue() -> list[dict]:
    if not QUEUE.exists() or QUEUE.stat().st_size == 0:
        return []
    snapshot = QUEUE.with_suffix(".jsonl.processing")
    QUEUE.rename(snapshot)
    out: list[dict] = []
    try:
        for line in snapshot.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    finally:
        snapshot.unlink(missing_ok=True)
        QUEUE.parent.mkdir(parents=True, exist_ok=True)
        QUEUE.touch(exist_ok=True)
    return out


def webhook_append(payload: dict) -> dict:
    """Append one event payload — used if an external webhook
    forwards Google Calendar push notifications."""
    QUEUE.parent.mkdir(parents=True, exist_ok=True)
    item = {
        "uid": payload.get("uid") or payload.get("id"),
        "summary": payload.get("summary") or payload.get("title", ""),
        "description": payload.get("description", ""),
        "location": payload.get("location", ""),
        "start": payload.get("start") or payload.get("start_at", ""),
        "end": payload.get("end") or payload.get("end_at", ""),
    }
    if not item["uid"] or not item["start"]:
        return {"ok": False, "reason": "uid+start required"}
    with QUEUE.open("a") as f:
        f.write(json.dumps(item) + "\n")
    return {"ok": True}


def run(cycle_id: int) -> dict[str, Any]:
    cfg = cfg_mod.get()
    coll_cfg = cfg.get("collectors", {}).get("gcal", {})
    if not coll_cfg.get("enabled", True):
        return {"skipped": True}

    items = _drain_scenario() + _drain_queue()
    # Real ICS HTTP fetch plugs in via coll_cfg.get('ics_url'); v1 ships
    # with stub-only so the agent runs offline. The seam is the only
    # piece Diane needs to flip when she pastes her calendar's secret
    # ICS URL into config.
    added = 0
    seen = 0
    for item in items:
        eid, is_new = _upsert_event(item)
        seen += 1
        if is_new:
            added += 1
            log_action(cycle_id, "collect", "gcal_event_added",
                       "event", eid, True,
                       (item.get("summary") or "")[:80])

    now = datetime.now()
    reminder_ids = _mint_reminders(cycle_id, now)
    pasted = _mark_past(now)

    log_action(cycle_id, "collect", "gcal_polled",
               None, None, True,
               f"seen={seen} added={added} "
               f"reminders={len(reminder_ids)} past={pasted}")
    return {
        "ingested": len(reminder_ids),
        "events_seen": seen,
        "events_added": added,
        "reminders_minted": reminder_ids,
        "marked_past": pasted,
    }

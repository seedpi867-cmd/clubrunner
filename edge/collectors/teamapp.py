"""TeamApp collector — team chat + availability + manager-post inbound.

Architecture and config.yaml both promised TeamApp on a 15-min poll, but
no collector existed. Without it, parents using the team's TeamApp chat
(the dominant channel for U10-U17 teams at this club, because Diane has
been pushing parents off SMS to save Twilio cost) had no way to reach
the agent. Their messages stayed inside TeamApp and Diane only saw
them when a coach forwarded a screenshot two days later.

Sources of truth:
  - scenarios/today.json:teamapp_inbound[] — offline test fixture
  - inputs/teamapp_queue.jsonl — the dashboard webhook + any TeamApp-
    side scraper appends one JSON line per item; collector drains here

Inbound shapes:
  kind='availability'   — structured RSVP: yes/no/maybe to a poll
  kind='team_chat'      — free-text message in a team chat thread
  kind='manager_post'   — a team manager posted something requiring
                          club-level review (e.g. proposed fixture
                          forfeit, training time change)

Each minted input has source='teamapp' so:
  - parent_faq / player_availability body_regex rules cross-apply
  - safeguarding patterns are checked exactly as on every channel
  - structured availability=no/maybe gets the body it would have had
    from SMS ("X won't make Saturday") so the existing availability
    rule routes it to the same ack playbook
  - everything else falls through to teamapp_unsorted -> escalate
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from edge.state.db import upsert_input, log_action
import config as cfg_mod

ROOT = Path(__file__).resolve().parents[2]
SCENARIO = ROOT / "scenarios" / "today.json"
QUEUE = ROOT / "inputs" / "teamapp_queue.jsonl"


def _stable_id(item: dict) -> str:
    """Prefer TeamApp's own message_id when present so re-polls are
    idempotent. Fall back to a content hash so scenario fixtures
    without an id still uniqueify."""
    ta_id = (item.get("teamapp_id") or item.get("message_id")
             or item.get("post_id") or item.get("rsvp_id"))
    if ta_id:
        return f"{item.get('kind', 'teamapp')}:{ta_id}"
    h = hashlib.sha1(
        f"{item.get('kind', '')}|{item.get('from', '')}|"
        f"{item.get('team_id', '')}|"
        f"{item.get('body', '')}|{item.get('ts', '')}".encode()
    ).hexdigest()
    return h[:16]


def _availability_to_body(item: dict) -> str:
    """Reshape a structured RSVP into a sentence the existing
    player_availability body_regex can match.

    TeamApp availability replies arrive as {response: 'yes'|'no'|'maybe',
    player_name: 'Liam', match_id: '...'}. The classifier rules don't
    speak that shape — they look for natural-language phrases like
    'won't make Saturday' or 'is sick'. So we lower the structured
    reply to the same English the existing rule already matches,
    rather than inventing a teamapp-only rule branch.

    'yes' replies need no agent action — they confirm the default
    expectation and wouldn't fire any playbook anyway, but we still
    record them so the manager view shows a complete RSVP state.
    """
    resp = (item.get("response") or "").lower()
    name = item.get("player_name") or "the player"
    if resp == "no":
        return f"{name} won't make this Saturday — RSVP via TeamApp."
    if resp == "maybe":
        return f"{name} is uncertain about this Saturday — RSVP via TeamApp."
    if resp == "yes":
        return f"{name} confirmed in for this Saturday — RSVP via TeamApp."
    return item.get("body") or "(empty availability response)"


def _ingest_one(cycle_id: int, item: dict) -> tuple[str, bool]:
    sid = _stable_id(item)
    sender = item.get("from") or item.get("sender") or "unknown"
    kind = item.get("kind") or "team_chat"
    team_id = item.get("team_id") or ""

    if kind == "availability":
        body = _availability_to_body(item)
        subject = f"TeamApp availability ({team_id})" if team_id else \
                  "TeamApp availability"
    elif kind == "manager_post":
        body = (item.get("body") or "").strip()
        subject = (f"TeamApp manager post ({team_id})" if team_id
                   else "TeamApp manager post")
    else:
        body = (item.get("body") or "").strip()
        subject = f"TeamApp message ({team_id})" if team_id else \
                  "TeamApp message"

    if not body:
        return ("", False)

    iid, is_new = upsert_input(
        "teamapp", sid, sender=sender, subject=subject,
        body=body, raw_json=json.dumps(item))
    if is_new:
        log_action(cycle_id, "collect", "teamapp_ingested",
                   "input", iid, True,
                   f"{kind} from={sender} team={team_id} "
                   f"body={body[:80]}")
    return iid, is_new


def _drain_queue(cycle_id: int) -> int:
    if not QUEUE.exists() or QUEUE.stat().st_size == 0:
        return 0
    snapshot = QUEUE.with_suffix(".jsonl.processing")
    QUEUE.rename(snapshot)
    n = 0
    try:
        for line in snapshot.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as ex:
                log_action(cycle_id, "collect", "teamapp_queue_malformed",
                           "input", None, False, str(ex)[:200])
                continue
            _, is_new = _ingest_one(cycle_id, item)
            if is_new:
                n += 1
    finally:
        snapshot.unlink(missing_ok=True)
        QUEUE.parent.mkdir(parents=True, exist_ok=True)
        QUEUE.touch(exist_ok=True)
    return n


def _drain_scenario(cycle_id: int) -> int:
    if not SCENARIO.exists():
        return 0
    drop = json.loads(SCENARIO.read_text()).get("teamapp_inbound", [])
    n = 0
    for item in drop:
        _, is_new = _ingest_one(cycle_id, item)
        if is_new:
            n += 1
    return n


def webhook_append(payload: dict) -> dict:
    """Append one TeamApp-shaped item from the dashboard webhook.

    Accepts either our internal shape or a TeamApp-style payload.
    """
    QUEUE.parent.mkdir(parents=True, exist_ok=True)
    item = {
        "kind": payload.get("kind") or "team_chat",
        "from": payload.get("from") or payload.get("sender") or "unknown",
        "team_id": payload.get("team_id") or "",
        "body": payload.get("body") or payload.get("message") or "",
        "response": payload.get("response"),
        "player_name": payload.get("player_name"),
        "ts": payload.get("ts") or datetime.now().isoformat(),
        "teamapp_id": (payload.get("teamapp_id")
                       or payload.get("message_id")
                       or payload.get("post_id")
                       or payload.get("rsvp_id")),
    }
    if item["kind"] != "availability" and not item["body"]:
        return {"ok": False, "reason": "empty body"}
    if item["kind"] == "availability" and not item.get("response"):
        return {"ok": False, "reason": "missing availability response"}
    with QUEUE.open("a") as f:
        f.write(json.dumps(item) + "\n")
    return {"ok": True, "queued_at": item["ts"]}


def run(cycle_id: int) -> dict[str, Any]:
    """Like facebook_page: drain scenario+queue every cycle. The real
    TeamApp poll plugs in only when `teamapp.enabled` is true and a
    token is configured — left as a no-op in v1, per architecture."""
    cfg = cfg_mod.get()
    coll_cfg = cfg.get("collectors", {}).get("teamapp", {})
    n = _drain_scenario(cycle_id) + _drain_queue(cycle_id)
    if coll_cfg.get("enabled", False) and coll_cfg.get("api_token"):
        # Real TeamApp poll plugs in here. Left as a no-op in v1.
        pass
    log_action(cycle_id, "collect", "teamapp_polled",
               None, None, True, f"minted={n}")
    return {"ingested": n}

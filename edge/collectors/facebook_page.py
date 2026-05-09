"""Facebook Page collector — page comments + page-message inbound.

Architecture promised the FB Page Graph API on a 5-min poll for inbound
messages and post comments, but no collector existed. Without it, the
Friday-night three-Facebook-replies scenario from the explore doc had
nowhere to land — the rant, the FAQ, and the thanks all stayed
invisible to the agent.

Sources of truth:
  - scenarios/today.json:fb_inbound[] — offline test fixture
  - inputs/fb_queue.jsonl — anything the dashboard webhook (or a poller
    you script later) appends here gets minted next cycle

Each item is one of:
  - kind='comment'  on a page post (carries post_id)
  - kind='message'  via Messenger to the page
  - kind='mention'  the page was tagged elsewhere

The minted input has source='fb' so:
  - existing parent_faq body_regex rules cross-apply
  - safeguarding patterns are checked exactly as on email
  - everything else falls through to fb_unsorted (escalates) — Diane
    sees the rant in her queue tagged 'tone-sensitive'
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
QUEUE = ROOT / "inputs" / "fb_queue.jsonl"


def _stable_id(item: dict) -> str:
    """Prefer FB's own id when present (comment_id / message_id /
    mention_id) so re-polls are idempotent. Fall back to a content
    hash so scenario fixtures without an id still uniqueify."""
    fb_id = (item.get("fb_id") or item.get("comment_id")
             or item.get("message_id") or item.get("mention_id"))
    if fb_id:
        return f"{item.get('kind', 'fb')}:{fb_id}"
    h = hashlib.sha1(
        f"{item.get('kind', '')}|{item.get('from', '')}|"
        f"{item.get('body', '')}|{item.get('ts', '')}".encode()
    ).hexdigest()
    return h[:16]


def _ingest_one(cycle_id: int, item: dict) -> tuple[str, bool]:
    sid = _stable_id(item)
    sender = item.get("from") or item.get("sender") or "unknown"
    body = (item.get("body") or item.get("message") or "").strip()
    kind = item.get("kind") or "comment"
    post_id = item.get("post_id") or ""
    subject = f"FB {kind}"
    if post_id:
        subject = f"FB {kind} on {post_id}"
    iid, is_new = upsert_input(
        "fb", sid, sender=sender, subject=subject,
        body=body, raw_json=json.dumps(item))
    if is_new:
        log_action(cycle_id, "collect", "fb_ingested",
                   "input", iid, True,
                   f"{kind} from={sender} body={body[:80]}")
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
                log_action(cycle_id, "collect", "fb_queue_malformed",
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
    drop = json.loads(SCENARIO.read_text()).get("fb_inbound", [])
    n = 0
    for item in drop:
        _, is_new = _ingest_one(cycle_id, item)
        if is_new:
            n += 1
    return n


def webhook_append(payload: dict) -> dict:
    """Append one FB-shaped item from the webhook handler.

    The dashboard's /fb/webhook route can normalise Graph API JSON
    into our internal shape and forward it here.
    """
    QUEUE.parent.mkdir(parents=True, exist_ok=True)
    item = {
        "kind": payload.get("kind") or "comment",
        "from": payload.get("from") or payload.get("sender") or "unknown",
        "body": payload.get("body") or payload.get("message") or "",
        "ts": payload.get("ts") or datetime.now().isoformat(),
        "post_id": payload.get("post_id"),
        "fb_id": payload.get("fb_id") or payload.get("comment_id"),
    }
    if not item["body"]:
        return {"ok": False, "reason": "empty body"}
    with QUEUE.open("a") as f:
        f.write(json.dumps(item) + "\n")
    return {"ok": True, "queued_at": item["ts"]}


def run(cycle_id: int) -> dict[str, Any]:
    """When `facebook.enabled` is false (default — Diane hasn't supplied
    a token yet), we still drain the local queue + scenarios. The Graph
    API call is the only thing gated by `enabled`, and it isn't built
    here in v1; the architecture is explicit that the wire is config
    only. The collector is real, the wire is plug-in."""
    cfg = cfg_mod.get()
    coll_cfg = cfg.get("collectors", {}).get("facebook", {})
    n = _drain_scenario(cycle_id) + _drain_queue(cycle_id)
    if coll_cfg.get("enabled", False) and coll_cfg.get("page_token"):
        # Real Graph API poll plugs in here. Left as a no-op in v1.
        pass
    log_action(cycle_id, "collect", "fb_polled",
               None, None, True, f"minted={n}")
    return {"ingested": n}

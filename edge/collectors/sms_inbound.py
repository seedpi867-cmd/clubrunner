"""SMS inbound collector — Twilio webhook + offline queue.

The architecture promised SMS inbound (Twilio MO webhooks → state.db)
but no collector minted them, so an SMS asking "when is training?"
sat invisible to triage. This fixes that hole.

How it runs:
  - The dashboard server (or any HTTP listener) accepts POSTs at the
    configured webhook path and writes one JSON line per message into
    inputs/sms_queue.jsonl. The collector drains that file every
    cycle, minting one input per line.
  - In stub/offline mode it also reads scenarios/today.json:sms_inbound
    so the test harness has a fixture to drive the cycle.

Each minted input has source='sms' so triage rules.yaml's body_regex
patterns (parent_faq, player_availability, incident keywords) apply
identically to email and SMS — a parent texting "when is training"
gets the same FAQ playbook as one emailing it. Messages that don't
match any rule fall through to sms_unsorted and escalate to the
brain (Diane sees it on the queue).
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from edge.state.db import upsert_input, log_action
import config as cfg_mod

ROOT = Path(__file__).resolve().parents[2]
SCENARIO = ROOT / "scenarios" / "today.json"
QUEUE = ROOT / "inputs" / "sms_queue.jsonl"


def _stable_id(from_num: str, body: str, ts: str) -> str:
    """A given (from, body, ts) tuple is the same SMS no matter how many
    times the webhook is replayed. Stable id keeps upsert_input
    idempotent across retries (Twilio's at-least-once delivery)."""
    h = hashlib.sha1(f"{from_num}|{body}|{ts}".encode()).hexdigest()
    return h[:16]


def _normalise_number(n: str) -> str:
    """Strip whitespace, parens, dashes. Keep leading + if present."""
    if not n:
        return ""
    cleaned = re.sub(r"[^\d+]", "", n)
    return cleaned


def _ingest_one(cycle_id: int, msg: dict) -> tuple[str, bool]:
    sender = _normalise_number(msg.get("from", ""))
    body = (msg.get("body") or "").strip()
    ts = msg.get("ts") or datetime.now().isoformat()
    sid = _stable_id(sender, body, ts)
    iid, is_new = upsert_input(
        "sms", sid, sender=sender, subject="(sms)",
        body=body, raw_json=json.dumps(msg))
    if is_new:
        log_action(cycle_id, "collect", "sms_ingested",
                   "input", iid, True,
                   f"from={sender} body={body[:80]}")
    return iid, is_new


def _drain_queue(cycle_id: int) -> int:
    """Read every line in the queue file, mint inputs, then truncate.
    A short-lived swap file avoids losing late-arriving lines: we rename
    the queue first, then process the snapshot. Anything written by the
    webhook in between lives in the new (now-empty) queue and is picked
    up next cycle."""
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
                msg = json.loads(line)
            except json.JSONDecodeError as ex:
                log_action(cycle_id, "collect", "sms_queue_malformed",
                           "input", None, False, str(ex)[:200])
                continue
            _, is_new = _ingest_one(cycle_id, msg)
            if is_new:
                n += 1
    finally:
        snapshot.unlink(missing_ok=True)
        # Touch the queue back into existence so the webhook's append
        # never has to mkdir/touch.
        QUEUE.parent.mkdir(parents=True, exist_ok=True)
        QUEUE.touch(exist_ok=True)
    return n


def _drain_scenario(cycle_id: int) -> int:
    if not SCENARIO.exists():
        return 0
    drop = json.loads(SCENARIO.read_text()).get("sms_inbound", [])
    n = 0
    for msg in drop:
        _, is_new = _ingest_one(cycle_id, msg)
        if is_new:
            n += 1
    return n


def webhook_append(payload: dict) -> dict:
    """Called by the dashboard /sms/inbound POST handler.

    Twilio MO posts form-encoded fields like From, Body, MessageSid,
    NumMedia. We accept either Twilio-style upper-case keys or
    lowercase {from, body, ts} so scenario tests and live webhooks
    share the same path.
    """
    QUEUE.parent.mkdir(parents=True, exist_ok=True)
    msg = {
        "from": payload.get("from") or payload.get("From") or "",
        "body": payload.get("body") or payload.get("Body") or "",
        "ts": payload.get("ts") or datetime.now().isoformat(),
        "provider_id": payload.get("MessageSid") or payload.get("provider_id"),
    }
    if not msg["from"] or not msg["body"]:
        return {"ok": False, "reason": "missing from/body"}
    with QUEUE.open("a") as f:
        f.write(json.dumps(msg) + "\n")
    return {"ok": True, "queued_at": msg["ts"]}


def run(cycle_id: int) -> dict[str, Any]:
    cfg = cfg_mod.get()
    coll_cfg = cfg.get("collectors", {}).get("twilio_inbound", {})
    if not coll_cfg.get("enabled", True):
        return {"skipped": True}
    n = _drain_scenario(cycle_id) + _drain_queue(cycle_id)
    log_action(cycle_id, "collect", "sms_polled",
               None, None, True, f"minted={n}")
    return {"ingested": n}

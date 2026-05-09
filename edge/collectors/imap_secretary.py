"""IMAP collector for the secretary inbox.

Real mode: imaplib2 IDLE on secretary@henleygrangejfc.com.au.
Stub mode: reads scenarios/today.json[imap_drop] AND any .eml files
dropped into inputs/maildir/new/. The maildir path is so Diane
(or testing) can drop emails as plain files and have them ingested
exactly like real IMAP fetch.

Note: 'maildir' is a SOURCE we watch, not the agent's primary input
mode. It exists for offline operation and as a graceful fallback when
IMAP credentials aren't configured. The collector watches it
continuously, the user does not 'feed' the agent.
"""
from __future__ import annotations

import email
import hashlib
import imaplib
import json
import socket
from datetime import datetime
from email.header import decode_header, make_header
from email.utils import parseaddr, parsedate_to_datetime
from pathlib import Path
from typing import Any


def _hdr(msg, key: str, default: str = "") -> str:
    """Decode an email header to a plain str, even if RFC 2047 encoded."""
    raw = msg.get(key)
    if raw is None:
        return default
    try:
        return str(make_header(decode_header(str(raw))))
    except (UnicodeDecodeError, ValueError, LookupError):
        return str(raw)

from edge.state.db import upsert_input, log_action
from edge.health import breaker
import config as cfg_mod

ROOT = Path(__file__).resolve().parents[2]
SCENARIO = ROOT / "scenarios" / "today.json"
MAILDIR_NEW = ROOT / "inputs" / "maildir" / "new"
MAILDIR_CUR = ROOT / "inputs" / "maildir" / "cur"


def _stable_id(sender: str, subject: str, body: str, ts: str) -> str:
    h = hashlib.sha1(f"{sender}|{subject}|{ts}|{body[:80]}".encode()).hexdigest()
    return h[:16]


def _ingest_scenario(cycle_id: int) -> int:
    if not SCENARIO.exists():
        return 0
    drop = json.loads(SCENARIO.read_text()).get("imap_drop", [])
    minted = 0
    for msg in drop:
        sid = _stable_id(msg["from"], msg["subject"], msg["body"], msg["ts"])
        iid, is_new = upsert_input(
            "imap", sid, sender=msg["from"],
            subject=msg["subject"], body=msg["body"],
            raw_json=json.dumps(msg))
        if is_new:
            minted += 1
            log_action(cycle_id, "collect", "imap_ingested",
                       "input", iid, True, msg["subject"][:80])
    return minted


def _ingest_maildir(cycle_id: int) -> int:
    MAILDIR_NEW.mkdir(parents=True, exist_ok=True)
    MAILDIR_CUR.mkdir(parents=True, exist_ok=True)
    n = 0
    for path in sorted(MAILDIR_NEW.glob("*.eml")):
        try:
            msg = email.message_from_bytes(path.read_bytes())
            _, sender = parseaddr(_hdr(msg, "From"))
            subject = _hdr(msg, "Subject", "(no subject)")
            ts = _hdr(msg, "Date") or datetime.now().isoformat()
            try:
                ts_iso = parsedate_to_datetime(ts).isoformat()
            except (TypeError, ValueError):
                ts_iso = datetime.now().isoformat()
            body = ""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        body = part.get_payload(decode=True).decode(
                            errors="replace")
                        break
            else:
                body = msg.get_payload(decode=True).decode(errors="replace")
            sid = _stable_id(sender, subject, body, ts_iso)
            iid, is_new = upsert_input(
                "imap", sid, sender=sender, subject=subject,
                body=body, raw_json=path.name)
            target = MAILDIR_CUR / path.name
            path.rename(target)
            if is_new:
                n += 1
                log_action(cycle_id, "collect", "imap_maildir_ingested",
                           "input", iid, True, subject[:80])
        except Exception as ex:
            # Broad on purpose — a malformed eml shouldn't tank the cycle,
            # and earlier we lost a sqlite3.ProgrammingError to a too-narrow
            # except. Fail the file, keep collecting.
            log_action(cycle_id, "collect", "imap_maildir_failed",
                       "input", path.name, False,
                       f"{type(ex).__name__}: {ex}"[:200])
    return n


def _ingest_live(cycle_id: int, host: str, user: str, pwd: str,
                 inbox: str, timeout: float) -> int:
    socket.setdefaulttimeout(timeout)
    try:
        with imaplib.IMAP4_SSL(host) as M:
            M.login(user, pwd)
            M.select(inbox)
            typ, data = M.search(None, "UNSEEN")
            n = 0
            for num in data[0].split():
                typ, msg_data = M.fetch(num, "(RFC822)")
                msg = email.message_from_bytes(msg_data[0][1])
                _, sender = parseaddr(_hdr(msg, "From"))
                subject = _hdr(msg, "Subject", "(no subject)")
                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body = part.get_payload(decode=True).decode(
                                errors="replace")
                            break
                else:
                    body = msg.get_payload(decode=True).decode(
                        errors="replace")
                sid = msg.get("Message-ID", "") or _stable_id(
                    sender, subject, body, datetime.now().isoformat())
                _, is_new = upsert_input(
                    "imap", sid, sender=sender,
                    subject=subject, body=body)
                if is_new:
                    n += 1
            breaker.record_success("imap")
            return n
    except (imaplib.IMAP4.error, socket.error, OSError) as ex:
        breaker.record_failure("imap", str(ex))
        log_action(cycle_id, "collect", "imap_live_failed",
                   None, None, False, str(ex)[:200])
        return 0


def run(cycle_id: int) -> dict[str, Any]:
    cfg = cfg_mod.get()
    coll_cfg = cfg.get("collectors", {}).get("imap", {})
    if not coll_cfg.get("enabled", False):
        return {"skipped": True}

    n = 0
    # Always pick up scenario + maildir drops first (they are real sources
    # to us — IMAP is just one of several email-flavoured channels).
    n += _ingest_scenario(cycle_id)
    n += _ingest_maildir(cycle_id)

    # If real IMAP is configured and breaker isn't open, hit it too.
    if coll_cfg.get("host", "").startswith(("imap.", "mail.")):
        if not breaker.is_open("imap"):
            host = coll_cfg["host"]
            user = coll_cfg.get("user")
            pwd = coll_cfg.get("password")
            timeout = float(cfg.get("health", {}).get(
                "fail_fast_external_seconds", 8))
            if user and pwd:
                n += _ingest_live(cycle_id, host, user, pwd,
                                  coll_cfg.get("inbox", "INBOX"), timeout)

    log_action(cycle_id, "collect", "imap_polled",
               None, None, True, f"minted={n}")
    return {"ingested": n}

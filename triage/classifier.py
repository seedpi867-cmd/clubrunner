"""Deterministic-first classifier.

Walks rules.yaml in order, applies the first match. Two cross-cutting
checks ALWAYS run before rules:
  1. Safeguarding patterns (config.safeguarding.trigger_patterns) —
     forces category=safeguarding, escalate=true, safeguarding=1,
     priority=100. Never autosent. Ever.
  2. Negative sentiment lexicon — bumps priority +10 when matched.

Items not matching any rule fall through to category 'novel' and are
sent to the brain.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from edge.state.db import connect, log_action, now_iso
import config as cfg_mod

ROOT = Path(__file__).resolve().parent
RULES_PATH = ROOT / "rules.yaml"


_NEG_LEX = (
    "angry", "disgusted", "outraged", "ridiculous", "shameful",
    "incompetent", "appalled", "furious", "lawyer", "ombudsman",
    "ban this", "ban him", "ban her",
)

# Short positive acknowledgements (FB comments, SMS, TeamApp posts)
# arrive constantly during the season — "Thanks team!", "Cheers Diane",
# "Awesome work this weekend". The fb/sms/teamapp catch-alls escalate
# them all to the brain, where the brain opens a generic_brief for
# Diane: queue noise that buries the actual judgment items.
#
# A pre-rule check here catches the obvious-positive case so the
# router can silent-record it. It runs AFTER safeguarding (a "thanks
# but my coach has been messaging my kid privately" message still
# escalates) and is conservative: short body, no question mark, no
# complaint/refund/cancel triggers, an explicit positive phrase. The
# safeguarding pre-check already runs first, so its triggers always win.
_POSITIVE_PHRASES = (
    "thanks", "thank you", "cheers", "awesome", "great work",
    "great job", "appreciate", "amazing job", "fantastic",
    "love what you", "you guys are", "well done", "champion effort",
    "legends", "kudos",
)
_POSITIVE_BLOCKERS = (
    # Anything that suggests the message is more than a standalone
    # positive — a question, a complaint, a follow-up that needs work.
    "but ", " but,", "however", "refund", "cancel", "complain",
    "angry", "unacceptable", "disappointed", "sick of", "fed up",
    "never again", "why was", "why is", "why are", "why did",
    "where is", "where are", "when is", "when does", "when do",
    "how do", "how can", "what time", "broken", "injur",
    "ambulance", "hospital", "outraged",
)
_POSITIVE_MAX_LEN = 240


def _is_positive_ack(subject: str, body: str) -> bool:
    text = f"{subject or ''} {body or ''}".strip().lower()
    if not text or len(text) > _POSITIVE_MAX_LEN:
        return False
    if "?" in text:
        return False
    if any(b in text for b in _POSITIVE_BLOCKERS):
        return False
    return any(p in text for p in _POSITIVE_PHRASES)


def _load_rules() -> list[dict]:
    text = RULES_PATH.read_text()
    try:
        import yaml  # type: ignore
        data = yaml.safe_load(text) or {}
    except ImportError:
        # Use mini-yaml from config module — same parser
        data = cfg_mod._load_yaml(text)
    return data.get("rules", [])


def _domain(addr: str | None) -> str:
    if not addr:
        return ""
    if "@" in addr:
        return addr.split("@", 1)[1].lower().strip("> ")
    return addr.lower()


def _match(rule: dict, inp: dict) -> bool:
    m = rule.get("match", {})
    src = (inp.get("source") or "").lower()
    sender = (inp.get("sender") or "").lower()
    subj = inp.get("subject") or ""
    body = inp.get("body") or ""

    if "source" in m and src != m["source"].lower():
        return False
    if "sender_exact" in m and sender != m["sender_exact"].lower():
        return False
    if "sender_domain" in m and _domain(sender) != m["sender_domain"].lower():
        return False
    if "subject_regex" in m and not re.search(m["subject_regex"], subj):
        return False
    if "body_regex" in m and not re.search(m["body_regex"], body):
        return False
    return True


def _has_safeguarding(text: str, patterns: list[str]) -> bool:
    t = text.lower()
    return any(p.lower() in t for p in patterns)


def _has_negative_sentiment(text: str) -> bool:
    t = text.lower()
    return any(p in t for p in _NEG_LEX)


def classify_one(inp: dict) -> dict[str, Any]:
    cfg = cfg_mod.get()
    sg_patterns = cfg.get("safeguarding", {}).get("trigger_patterns", [])
    full_text = f"{inp.get('subject','')}\n{inp.get('body','')}"

    if _has_safeguarding(full_text, sg_patterns):
        return {
            "classification": "safeguarding",
            "sentiment": "alarm",
            "safeguarding": 1,
            "priority": 100,
            "escalate": True,
            "playbook": "safeguarding_brief",
            "rule_hit": "safeguarding_pattern",
        }

    # Only apply the pre-rule positive-ack check to inbound parent
    # channels (fb / sms / teamapp). League fixture confirmations or
    # council emails can contain "thanks" boilerplate but still need
    # full rule processing.
    if (inp.get("source") in ("fb", "sms", "teamapp")
            and _is_positive_ack(inp.get("subject", ""),
                                 inp.get("body", ""))):
        return {
            "classification": "positive_ack",
            "sentiment": "pos",
            "safeguarding": 0,
            "priority": 10,
            "escalate": False,
            "playbook": None,
            "rule_hit": "positive_ack_prerule",
        }

    rules = _load_rules()
    for r in rules:
        if _match(r, inp):
            s = r.get("set", {})
            sentiment = s.get("sentiment", "neu")
            priority = int(s.get("priority", 50))
            if _has_negative_sentiment(full_text):
                priority = min(95, priority + 10)
                if sentiment == "neu":
                    sentiment = "neg"
            return {
                "classification": s.get("category", "unsorted"),
                "sentiment": sentiment,
                "safeguarding": 0,
                "priority": priority,
                "escalate": bool(s.get("escalate", False)),
                "playbook": s.get("playbook"),
                "rule_hit": str(r.get("match")),
            }

    # No rule hit — novel; route to brain.
    return {
        "classification": "novel",
        "sentiment": "neg" if _has_negative_sentiment(full_text) else "neu",
        "safeguarding": 0,
        "priority": 60,
        "escalate": True,
        "playbook": None,
        "rule_hit": "none",
    }


def run(cycle_id: int) -> dict[str, Any]:
    """Classify all inputs in 'new' status."""
    conn = connect()
    rows = [dict(r) for r in conn.execute(
        "SELECT id, source, sender, subject, body FROM inputs "
        "WHERE status='new' ORDER BY received_at LIMIT 200")]
    conn.close()

    n_class, n_escalate, n_sg = 0, 0, 0
    for inp in rows:
        v = classify_one(inp)
        conn = connect()
        conn.execute(
            "UPDATE inputs SET classification=?, sentiment=?, "
            "safeguarding=?, priority=?, status='classified' "
            "WHERE id=?",
            (v["classification"], v["sentiment"], v["safeguarding"],
             v["priority"], inp["id"]))
        conn.close()
        log_action(cycle_id, "triage", "classified",
                   "input", inp["id"], True,
                   f"{v['classification']} pri={v['priority']} "
                   f"esc={v['escalate']} pb={v.get('playbook')}")
        n_class += 1
        if v["escalate"]:
            n_escalate += 1
        if v["safeguarding"]:
            n_sg += 1

    return {"classified": n_class, "to_escalate": n_escalate,
            "safeguarding": n_sg}


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    print(run(0))

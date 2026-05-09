"""Small autoresponder playbooks.

These are intentionally simple — single recipient, no broadcast,
deterministic body. They handle the high-volume long-tail of routine
parent comms so the brain only sees novel things.

  - availability_ack: parent says "Liam is sick this Saturday" → ack
                      with "thanks, recorded — coach is informed"
  - faq_training:     parent asks training time → looks up team, replies
  - faq_register:     parent asks how to register → standard reply
  - council_clash_resolved: clash resolved upstream → no comms needed,
                            just record + close
  - fixtures_ingest:  league fixtures email → parse and store (the
                      orchestrator records the league email, the brain
                      reviews, in v1 we just acknowledge receipt)
"""
from __future__ import annotations

from edge.state.db import connect, log_action, now_iso
from tools import broadcast


def _input(input_id: str) -> dict | None:
    conn = connect()
    row = conn.execute(
        "SELECT id, source, sender, subject, body FROM inputs WHERE id=?",
        (input_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


# Map the input source to the reply channel. A parent SMSing the club
# number doesn't have a known email, and a FB-comment reply by email
# would arrive in a stranger's inbox a day later — useless. Reply on
# the channel they used. teamapp/gcal would never trigger an ack
# playbook so we don't map them here.
_REPLY_CHANNEL = {
    "imap": "email",
    "sms": "sms",
    "fb": "fb_comment",
    "teamapp": "teamapp_post",
}


def _reply_channel(source: str | None) -> str:
    return _REPLY_CHANNEL.get(source or "", "email")


def _reply_subject(source: str | None, base_subject: str) -> str:
    """SMS and FB comments don't carry a subject. The 'Re: (sms)' that
    you'd otherwise get on the dashboard is noise. For non-email
    channels, drop the subject prefix."""
    if source in ("sms", "fb", "teamapp"):
        return ""
    return f"Re: {base_subject}"


def _close_input(input_id: str) -> None:
    conn = connect()
    conn.execute("UPDATE inputs SET status='done' WHERE id=?", (input_id,))
    conn.close()


def _person_by_email(email: str) -> dict | None:
    if not email:
        return None
    conn = connect()
    row = conn.execute(
        "SELECT id, name, sms, preferred_channel FROM people "
        "WHERE LOWER(email)=LOWER(?)", (email,)).fetchone()
    conn.close()
    return dict(row) if row else None


def _short(body: str, limit: int = 320) -> str:
    """SMS bodies must be tight. Trim to N chars, breaking on the last
    sentence boundary inside that window so we don't slice mid-word."""
    if len(body) <= limit:
        return body
    cut = body[:limit]
    for sep in ("\n\n", ". ", "\n"):
        idx = cut.rfind(sep)
        if idx >= limit // 2:
            return cut[:idx + len(sep)].rstrip()
    return cut.rstrip()


def availability_ack(cycle_id: int, input_id: str) -> dict:
    inp = _input(input_id)
    if not inp:
        return {"ok": False}
    chan = _reply_channel(inp.get("source"))
    long_body = (
        f"Hi,\n\nThanks for letting us know. I've recorded "
        f"\"{inp['subject']}\" — your coach will be in the loop.\n\n"
        f"Get well / safe travels,\nDiane\n")
    sms_body = ("Got it — coach will be informed. Get well. — HGJFC")
    body = sms_body if chan == "sms" else long_body
    res = broadcast.submit(
        cycle_id=cycle_id, playbook="availability_ack", channel=chan,
        segment=f"sender:{inp['sender']}", reach=1,
        subject=_reply_subject(inp.get("source"), inp["subject"]),
        body=body, target_kind="input", target_id=input_id,
        related_input=input_id)
    _close_input(input_id)
    log_action(cycle_id, "execute", "availability_ack",
               "input", input_id, True, res["status"])
    return {"ok": True, "result": res}


def faq_training(cycle_id: int, input_id: str) -> dict:
    inp = _input(input_id)
    if not inp:
        return {"ok": False}
    chan = _reply_channel(inp.get("source"))
    long_body = (
        "Hi,\n\nClub training schedule — most teams train at their home "
        "ground on weeknights:\n\n"
        "- Auskick: Saturday 9-10am at Henley Oval\n"
        "- U8/U9: Tuesday 4:30-5:30pm at Henley Oval\n"
        "- U10-U12: Wednesday 5-6pm at Grange Reserve / Fulham North\n"
        "- U13-U17: Thursday 5:30-6:45pm (check team manager)\n\n"
        "Your team manager will confirm the specifics — they have the "
        "current week's plan.\n\nDiane\n")
    sms_body = ("Training: Auskick Sat 9-10am Henley; U8/9 Tue 4:30-5:30pm "
                "Henley; U10-12 Wed 5-6pm Grange/Fulham; U13+ Thu 5:30pm "
                "(team mgr confirms). — HGJFC")
    body = _short(sms_body, 320) if chan == "sms" else long_body
    res = broadcast.submit(
        cycle_id=cycle_id, playbook="faq_training", channel=chan,
        segment=f"sender:{inp['sender']}", reach=1,
        subject=_reply_subject(inp.get("source"), inp["subject"]),
        body=body, target_kind="input", target_id=input_id,
        related_input=input_id)
    _close_input(input_id)
    log_action(cycle_id, "execute", "faq_training",
               "input", input_id, True, res["status"])
    return {"ok": True, "result": res}


def faq_register(cycle_id: int, input_id: str) -> dict:
    inp = _input(input_id)
    if not inp:
        return {"ok": False}
    chan = _reply_channel(inp.get("source"))
    long_body = (
        "Hi,\n\nThanks for reaching out — registration is via PlayHQ:\n"
        "  https://www.playhq.com/aus/hgjfc\n\n"
        "Steps:\n"
        " 1. Create a PlayHQ account\n"
        " 2. Search for Henley & Grange JFC\n"
        " 3. Choose your child's age grade\n"
        " 4. Pay (rego is $220, includes insurance + jumper hire)\n\n"
        "Once registered the team manager will be in touch with training "
        "info. If you hit any snag, reply and I'll sort it.\n\nDiane\n")
    sms_body = ("Rego is via PlayHQ: playhq.com/aus/hgjfc — search Henley "
                "& Grange JFC, pick age grade, pay $220 (incl insurance & "
                "jumper). Team mgr will then be in touch. — HGJFC")
    body = _short(sms_body, 320) if chan == "sms" else long_body
    res = broadcast.submit(
        cycle_id=cycle_id, playbook="faq_register", channel=chan,
        segment=f"sender:{inp['sender']}", reach=1,
        subject=_reply_subject(inp.get("source"), inp["subject"]),
        body=body, target_kind="input", target_id=input_id,
        related_input=input_id)
    _close_input(input_id)
    log_action(cycle_id, "execute", "faq_register",
               "input", input_id, True, res["status"])
    return {"ok": True, "result": res}


def council_clash_resolved(cycle_id: int, input_id: str) -> dict:
    """Clash resolved upstream — close the new resolved-input AND any
    council_brief decision still pending on the matching 'pending'
    input for the same physical clash window.

    The council_portal collector keys source_id as
    `clash_{ground}_{window_start}:{status}`. A resolution mints a
    fresh input but leaves the prior 'pending' input (and the
    council_brief decision Diane is still looking at) untouched. We
    walk the pending sibling, mark its status='done', and resolve any
    open decision pointing at it. Without this hop, the agent sees
    the resolution but Diane's queue still shows the stale
    council_brief."""
    inp = _input(input_id)
    if not inp:
        return {"ok": False}
    iid = inp["id"]
    sibling_input_id: str | None = None
    sibling_decisions: list[str] = []
    if iid.startswith("council:") and iid.endswith(":resolved"):
        # Strip the trailing ':resolved' off the source_id portion to
        # find the corresponding ':pending' twin. iid is the full row
        # primary key 'source:source_id'; the source_id itself ends
        # with ':<status>'.
        window_key = iid[: -len(":resolved")]
        pending_iid = f"{window_key}:pending"
        conn = connect()
        sib = conn.execute(
            "SELECT id, status FROM inputs WHERE id=?",
            (pending_iid,)).fetchone()
        if sib:
            sibling_input_id = sib["id"]
            conn.execute(
                "UPDATE inputs SET status='done' WHERE id=?",
                (pending_iid,))
            dec_rows = conn.execute(
                "SELECT id FROM decisions "
                "WHERE target_kind='input' AND target_id=? "
                "AND status='pending'", (pending_iid,)).fetchall()
            for d in dec_rows:
                conn.execute(
                    "UPDATE decisions SET status='resolved', "
                    "chosen='auto_resolved_upstream', "
                    "resolved_at=?, resolved_note=? WHERE id=?",
                    (now_iso(),
                     "Council confirmed clash resolved — auto-closed",
                     d["id"]))
                sibling_decisions.append(d["id"])
        conn.close()
    _close_input(input_id)
    detail = "resolved upstream"
    if sibling_input_id:
        detail += f"; closed pending sibling {sibling_input_id}"
    if sibling_decisions:
        detail += f"; auto-resolved {len(sibling_decisions)} decision(s)"
    log_action(cycle_id, "execute", "council_clash_resolved",
               "input", input_id, True, detail)
    return {"ok": True, "noop": True,
            "closed_pending_input": sibling_input_id,
            "auto_resolved_decisions": sibling_decisions}


def fixtures_ingest(cycle_id: int, input_id: str) -> dict:
    """League fixtures email landed — in v1 we just close and let
    PlayHQ collector pick up the actual fixture data. The agent's
    response is to know it arrived (recorded) and not bother Diane."""
    _close_input(input_id)
    log_action(cycle_id, "execute", "fixtures_ingest",
               "input", input_id, True,
               "league fixtures email recorded; PlayHQ will sync data")
    return {"ok": True, "noop": True}

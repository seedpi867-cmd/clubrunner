"""Broadcast helper — draft, gate, send (or escalate).

The single point through which any outbound message goes. Playbooks
build a draft and call submit() — gates run, the broadcast is recorded
as drafted/gated/approved/sent, and if blocked it appears in Diane's
decision queue as an approval_required item.

Real wire-out (Twilio/Graph/IMAP-send) is intentionally a stub here —
the orchestration is real, the wire is config-only. send_provider is
the seam.
"""
from __future__ import annotations

import json
import uuid
from typing import Any

from edge.state.db import connect, log_action, now_iso
from edge.gates import run_all
import config as cfg_mod


def _new_id() -> str:
    return f"bc_{uuid.uuid4().hex[:12]}"


def _record_policy_run(playbook: str, target_kind: str | None,
                       target_id: str | None, cycle_id: int,
                       outcome: str, detail: str,
                       broadcast_id: str | None = None) -> None:
    """Record one policy fire into the audit log.

    Uses broadcast_id (when present) as the disambiguator in the
    primary key. Without it, two broadcasts in the same second on the
    same playbook+target (e.g. email + sms variants of a heat
    cancellation) collide on a second-precision timestamp key and one
    silently overwrites the other under INSERT OR REPLACE — masking
    half the audit log.
    """
    if not playbook:
        return
    pid = (f"{playbook}:{target_kind or '_'}:{target_id or '_'}:"
           f"{broadcast_id or now_iso()}")[:128]
    conn = connect()
    conn.execute(
        "INSERT OR REPLACE INTO policy_runs(id, playbook, target_kind, "
        "target_id, cycle_id, fired_at, outcome, detail) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (pid, playbook, target_kind, target_id, cycle_id, now_iso(),
         outcome, detail[:500]))
    conn.close()


def _open_decision(cycle_id: int, kind: str, target_kind: str | None,
                   target_id: str | None, summary: str, context: str,
                   options: list[dict], default: str) -> str:
    did = f"dec_{uuid.uuid4().hex[:12]}"
    conn = connect()
    conn.execute(
        "INSERT INTO decisions(id, cycle_id, kind, target_kind, target_id, "
        "summary, context, options_json, default_option, created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (did, cycle_id, kind, target_kind, target_id, summary[:200],
         context[:4000], json.dumps(options), default, now_iso()))
    conn.close()
    log_action(cycle_id, "execute", "decision_opened",
               "decision", did, True, kind)
    return did


def submit(*, cycle_id: int, playbook: str | None, channel: str,
           segment: str, reach: int, body: str, subject: str = "",
           target_kind: str | None = None,
           target_id: str | None = None,
           related_input: str | None = None,
           policy_override_threshold: bool = False,
           override_business_hours: bool = False,
           force_approval: bool = False,
           recipient_id: str | None = None,
           meta: dict | None = None) -> dict[str, Any]:
    """Draft a broadcast, run gates, decide outcome.

    force_approval=True makes broadcast_threshold fail regardless of
    reach. Heat cancellations use this: the architecture mandates Diane
    confirms heat cancels because forecasts are noisy, and we cannot
    rely on the >50-reach threshold to catch them — a small club with a
    single U8 team has reach=3 and would otherwise autosend.

    Returns: {id, status, blocked_by, decision_id?}
    """
    bid = _new_id()
    action = {
        "playbook": playbook,
        "channel": channel,
        "segment": segment,
        "reach": reach,
        "subject": subject,
        "body": body,
        "target_kind": target_kind,
        "target_id": target_id,
        "related_input": related_input,
        "policy_override_threshold": policy_override_threshold,
        "override_business_hours": override_business_hours,
        "force_approval": force_approval,
        "recipient_id": recipient_id,
        # meta is what gates like sponsor_post inspect — without it the
        # gate can't see sponsor_credit and a small-reach sponsor-tagged
        # post (e.g. brain-drafted FB credit) would auto-send without
        # Diane ever seeing her sponsor's name go out.
        "meta": meta or {},
    }
    meta_blob = json.dumps(meta) if meta else None

    conn = connect()
    conn.execute(
        "INSERT INTO broadcasts(id, cycle_id, playbook, channel, segment, "
        "reach, subject, body, status, created_at, related_input, meta_json) "
        "VALUES(?,?,?,?,?,?,?,?, 'drafted', ?, ?, ?)",
        (bid, cycle_id, playbook, channel, segment, reach, subject,
         body, now_iso(), related_input, meta_blob))
    conn.close()

    results = run_all(action)
    blocking = [r for r in results if not r.allow]

    if not blocking:
        # Auto-approve and send.
        sent = _wire_send(channel, segment, body)
        conn = connect()
        conn.execute(
            "UPDATE broadcasts SET status='sent', decided_at=?, sent_at=? "
            "WHERE id=?", (now_iso(), now_iso(), bid))
        conn.close()
        _apply_meta_on_send(bid)
        _record_policy_run(playbook, target_kind, target_id, cycle_id,
                           "sent", f"reach={reach} via {channel}",
                           broadcast_id=bid)
        log_action(cycle_id, "execute", "broadcast_sent",
                   "broadcast", bid, True,
                   f"{playbook}/{channel}/{segment}/reach={reach}")
        return {"id": bid, "status": "sent", "wire_id": sent}

    # Blocked. Categorise the blocking gates so we only escalate the ones
    # that genuinely need Diane.
    #
    # SILENT_GATES — re-fireable, non-judgement: dedup says "we already
    #   sent this, don't double-send"; rate_limit says "this recipient is
    #   capped today"; hours says "wait until business hours". The agent
    #   just doesn't send, no decision needed. Adding these to the
    #   decision queue is exactly the kind of noise that ruins the queue
    #   as a signal.
    # APPROVAL_GATES — need a human via the one-tap approve flow:
    #   - broadcast_threshold: >50-reach broadcasts
    #   - sponsor_post: any sponsor-tagged broadcast (regardless of reach)
    # BRIEF_GATES — escalate as a "blocked_brief" decision, not approve flow:
    #   - safeguarding: child-safeguarding always-escalate
    #   - sms_cost: hit a spend cap, Diane should know
    SILENT_GATES = {"dedup", "rate_limit", "hours"}
    APPROVAL_GATES = {"broadcast_threshold", "sponsor_post"}

    blocked_by = ",".join(b.name for b in blocking)
    silent_only = all(b.name in SILENT_GATES for b in blocking)

    # When a silent gate AND only approval-style gates also block, the
    # silent gate wins: the prior dedup peer (or rate-limited peer) is
    # what already opened — or chose not to open — the human decision.
    # Opening a SECOND approval card for the same logical broadcast
    # floods Diane's queue inside one dedup window. Safeguarding/cost
    # are NOT silenceable — they always escalate as a brief regardless
    # of dedup.
    silent_present = any(b.name in SILENT_GATES for b in blocking)
    nonsilent = [b for b in blocking if b.name not in SILENT_GATES]
    approval_only_nonsilent = (
        silent_present and bool(nonsilent)
        and all(b.name in APPROVAL_GATES for b in nonsilent))
    treat_as_silent = silent_only or approval_only_nonsilent

    conn = connect()
    new_status = "skipped" if treat_as_silent else "gated"
    conn.execute(
        "UPDATE broadcasts SET status=?, blocked_by=?, decided_at=? "
        "WHERE id=?", (new_status, blocked_by, now_iso(), bid))
    conn.close()
    _record_policy_run(playbook, target_kind, target_id, cycle_id,
                       "skipped_dedup" if treat_as_silent else "gated",
                       blocked_by, broadcast_id=bid)
    log_action(cycle_id, "execute",
               "broadcast_skipped" if treat_as_silent else "broadcast_gated",
               "broadcast", bid, True,
               f"{playbook}/{channel}/{segment}/{blocked_by}")

    if treat_as_silent:
        return {"id": bid, "status": "skipped", "blocked_by": blocked_by}

    # If the only blocking gates are approval-style (threshold and/or
    # sponsor_post), route to the one-tap-approve flow. Both gates ask
    # the same question: should this go out as drafted? Diane gets one
    # card, with the reasons listed in context. Sponsor-tagged means
    # the body mentions a partner she's vouching for — the sponsor row
    # also gets surfaced so she can see how many posts are owed.
    if all(b.name in APPROVAL_GATES for b in blocking):
        kind = "approval_required"
        sponsor_id = (meta or {}).get("sponsor_credit")
        sponsor_note = ""
        if sponsor_id:
            conn = connect()
            srow = conn.execute(
                "SELECT name, tier, posts_owed FROM sponsors WHERE id=?",
                (sponsor_id,)).fetchone()
            conn.close()
            if srow:
                sponsor_note = (f"\nSponsor credit: {srow['name']} "
                                f"({srow['tier']}, {srow['posts_owed']} "
                                f"posts owed)")
        gate_reasons = "; ".join(f"{b.name}: {b.reason}" for b in blocking)
        summary = f"Approve {playbook} broadcast ({reach} recipients)"
        ctx = (f"Channel: {channel}\nSegment: {segment}\n"
               f"Reach: {reach}\nSubject: {subject}{sponsor_note}\n"
               f"Gates: {gate_reasons}\n\n{body}")
        options = [
            {"key": "approve", "label": "Approve & send"},
            {"key": "decline", "label": "Decline (do not send)"},
            {"key": "hold", "label": "Hold for me to edit first"},
        ]
        did = _open_decision(cycle_id, kind, "broadcast", bid,
                             summary, ctx, options, "approve")
        return {"id": bid, "status": "gated", "blocked_by": blocked_by,
                "decision_id": did}

    # Safeguarding / cost — never autosend, escalate as a brief.
    kind = "blocked_brief"
    summary = (f"{playbook or 'broadcast'} blocked: {blocked_by}")
    ctx = (f"Channel: {channel}\nSegment: {segment}\nReach: {reach}\n"
           f"Subject: {subject}\n\n{body}\n\n"
           f"Blocked by: {blocked_by}\n"
           + "\n".join(f"- {b.name}: {b.reason}" for b in blocking))
    options = [
        {"key": "ack", "label": "Acknowledge — handle manually"},
        {"key": "discard", "label": "Discard"},
    ]
    did = _open_decision(cycle_id, kind, "broadcast", bid,
                         summary, ctx, options, "ack")
    return {"id": bid, "status": "gated", "blocked_by": blocked_by,
            "decision_id": did}


def _wire_send(channel: str, segment: str, body: str) -> str:
    """Stub provider. Real Twilio/Graph/SMTP plugs in here without any
    upstream change. Returns a fake provider id so audit logs are
    consistent."""
    return f"stub:{channel}:{uuid.uuid4().hex[:8]}"


def _apply_meta_on_send(broadcast_id: str) -> None:
    """Apply the broadcast's recorded domain side-effects.

    Fires whenever a broadcast transitions to 'sent' — both auto-send
    (under threshold) and post-approval (>50-reach gate). Without this
    second path, anything credit-tracked by playbooks (sponsor posts,
    duty confirmations, etc.) silently desyncs whenever the gate kicks
    in: the credit only ever applied at draft time when status='gated',
    not when Diane finally tapped approve.
    """
    conn = connect()
    row = conn.execute(
        "SELECT meta_json FROM broadcasts WHERE id=?",
        (broadcast_id,)).fetchone()
    conn.close()
    if not row or not row["meta_json"]:
        return
    try:
        meta = json.loads(row["meta_json"])
    except json.JSONDecodeError:
        return
    sponsor_id = meta.get("sponsor_credit")
    if sponsor_id:
        conn = connect()
        conn.execute(
            "UPDATE sponsors SET posts_owed=MAX(0, posts_owed-1), "
            "last_post_at=? WHERE id=?",
            (now_iso(), sponsor_id))
        conn.close()
        log_action(None, "execute", "sponsor_credit_applied",
                   "sponsor", sponsor_id, True,
                   f"broadcast={broadcast_id}")


def insert_drafted(*, cycle_id: int, playbook: str, channel: str,
                   segment: str, reach: int, body: str, subject: str = "",
                   target_kind: str | None = None,
                   target_id: str | None = None,
                   related_input: str | None = None,
                   meta: dict | None = None) -> str:
    """Insert a broadcast in 'drafted' state — no gates, no send.

    Used by the brain when it produces a reply for an inbound that
    needs Diane's explicit choice (send_draft / edit / decline /
    phone). Bypassing submit() is deliberate: a low-reach
    refund/sponsor/safeguarding reply would clear every gate and
    autosend, which would defeat the entire point of escalating to
    the brain. The decision the brain opens points back to this
    broadcast via related_input, and approve_decision flips it to
    sent or cancelled when Diane resolves.
    """
    bid = _new_id()
    meta_blob = json.dumps(meta) if meta else None
    conn = connect()
    conn.execute(
        "INSERT INTO broadcasts(id, cycle_id, playbook, channel, segment, "
        "reach, subject, body, status, created_at, related_input, meta_json) "
        "VALUES(?,?,?,?,?,?,?,?, 'drafted', ?, ?, ?)",
        (bid, cycle_id, playbook, channel, segment, reach, subject,
         body, now_iso(), related_input, meta_blob))
    conn.close()
    log_action(cycle_id, "execute", "broadcast_drafted",
               "broadcast", bid, True,
               f"{playbook}/{channel}/{segment}/reach={reach}/awaiting_human")
    _record_policy_run(playbook, target_kind, target_id, cycle_id,
                       "drafted", f"reach={reach} via {channel} (held)",
                       broadcast_id=bid)
    return bid


# Decision options whose semantics are "send the drafted reply".
_SEND_OPTIONS = {"approve", "send_draft", "send"}
# Decision options whose semantics are "do not send" — Diane will
# handle by phone/in person, or the draft is no longer wanted.
_CANCEL_OPTIONS = {"decline", "discard", "phone", "loop_treasurer",
                   "edit", "ack", "call_pres", "queue_mpio", "hold"}


def approve_decision(decision_id: str, choice: str,
                     note: str = "") -> dict[str, Any]:
    """Apply the human's resolution.

    Two flavours of decision:
      1. target_kind='broadcast' (approval_required, >50-reach gate).
         The broadcast itself is the target; approve→send, decline→cancel.
      2. target_kind='input' (brain brief: complaint/sponsor/etc).
         Any drafted brain_drafted_reply broadcast linked to that input
         is sent (if the option is a send-option) or cancelled
         (everything else, including 'edit'/'phone' — Diane is taking
         it from here manually). Without this branch, the decision
         options were theatre: the broadcast had already auto-sent
         when the brain ran.
    """
    conn = connect()
    row = conn.execute(
        "SELECT id, kind, target_kind, target_id, status "
        "FROM decisions WHERE id=?", (decision_id,)).fetchone()
    if not row:
        conn.close()
        return {"ok": False, "reason": "not found"}
    if row["status"] != "pending":
        conn.close()
        return {"ok": False, "reason": "already resolved"}

    target_kind, target_id = row["target_kind"], row["target_id"]
    affected_broadcasts: list[str] = []

    sent_broadcasts: list[str] = []
    if (row["kind"] == "approval_required" and target_kind == "broadcast"
            and choice == "approve"):
        bc = conn.execute(
            "SELECT channel, segment, body FROM broadcasts WHERE id=?",
            (target_id,)).fetchone()
        if bc:
            _wire_send(bc["channel"], bc["segment"], bc["body"])
            conn.execute(
                "UPDATE broadcasts SET status='sent', sent_at=? WHERE id=?",
                (now_iso(), target_id))
            affected_broadcasts.append(target_id)
            sent_broadcasts.append(target_id)
    elif row["kind"] == "approval_required" and choice == "decline":
        conn.execute(
            "UPDATE broadcasts SET status='cancelled' WHERE id=?",
            (target_id,))
        affected_broadcasts.append(target_id)
    elif target_kind == "input":
        drafts = conn.execute(
            "SELECT id, channel, segment, body FROM broadcasts "
            "WHERE related_input=? AND status='drafted' "
            "AND playbook='brain_drafted_reply'",
            (target_id,)).fetchall()
        if choice in _SEND_OPTIONS:
            for d in drafts:
                _wire_send(d["channel"], d["segment"], d["body"])
                conn.execute(
                    "UPDATE broadcasts SET status='sent', sent_at=? "
                    "WHERE id=?", (now_iso(), d["id"]))
                affected_broadcasts.append(d["id"])
                sent_broadcasts.append(d["id"])
        else:
            for d in drafts:
                conn.execute(
                    "UPDATE broadcasts SET status='cancelled' WHERE id=?",
                    (d["id"],))
                affected_broadcasts.append(d["id"])

        # Incident-brief resolutions: incident_id is derived from input_id
        # (deterministic uuid5 in tools.incidents), so we don't need to
        # carry it on the decision row. report_league → reported,
        # close_incident → closed. Other choices (ack/call_pres) leave
        # the incident open since the work isn't done yet.
        if row["kind"] == "incident_brief":
            import uuid as _uuid
            iid_hash = _uuid.uuid5(
                _uuid.NAMESPACE_URL,
                f"clubrunner:incident:{target_id}")
            incident_id = f"inc_{iid_hash.hex[:12]}"
            if choice == "report_league":
                conn.execute(
                    "UPDATE incidents SET status='reported', "
                    "league_report_at=? WHERE id=? AND status='open'",
                    (now_iso(), incident_id))
            elif choice == "close_incident":
                conn.execute(
                    "UPDATE incidents SET status='closed' "
                    "WHERE id=? AND status IN ('open','reported')",
                    (incident_id,))

    # Duty-escalation resolutions update the duties table directly so
    # the next cadence cycle doesn't re-fire on a duty Diane has already
    # closed. backfill:<pid> reassigns the rostered person — subsequent
    # reminders/escalations chase the new person, not the old.
    # phone_rostered: no DB change. Diane will phone; the duty either
    # gets confirmed inbound (parent texts back Y) or stays open and
    # re-escalates on the next 2h tick.
    if row["kind"] == "duty_escalation" and target_kind == "duty":
        if choice == "close_duty":
            conn.execute(
                "UPDATE duties SET confirmed=1 WHERE id=?", (target_id,))
        elif choice.startswith("backfill:"):
            new_pid = choice.split(":", 1)[1]
            conn.execute(
                "UPDATE duties SET person_id=?, confirmed=0, "
                "last_reminded_at=NULL WHERE id=?", (new_pid, target_id))

    conn.execute(
        "UPDATE decisions SET status='resolved', chosen=?, "
        "resolved_at=?, resolved_note=? WHERE id=?",
        (choice, now_iso(), note[:500], decision_id))
    conn.close()
    # Apply any recorded domain side-effects AFTER the broadcasts table is
    # in its committed 'sent' state. _apply_meta_on_send re-opens the conn,
    # so it must run after this connection's writes have been flushed.
    for bid in sent_broadcasts:
        _apply_meta_on_send(bid)
    return {"ok": True, "choice": choice,
            "broadcasts": affected_broadcasts}

"""Public liability insurance — quarterly forward + expiry watch.

Council requires a current Certificate of Currency on file for any
ground booking. Two jobs:

  1. Quarterly forward — every `forward_cadence_days` (90 by default) we
     email the latest Certificate of Currency to council.bookings. The
     daily schedule tick fires this playbook; the playbook itself
     no-ops if the last forward was inside the cadence window.

  2. Expiry watch — when days-to-expiry hits one of the configured
     reminder thresholds (60/30/14/7/1) we open a decision card for
     Diane. Renewal is a broker phone call — the agent can't do it.
     If we are inside <7 days OR already expired, we ALSO escalate
     to the treasurer + president because ground bookings are at risk.

Inbound council requests for proof of insurance are routed here too
(category=insurance_request). In that path we forward the cert
immediately, regardless of cadence — the council asked for it.
"""
from __future__ import annotations

from datetime import datetime, date
from typing import Any

from edge.state.db import connect, log_action, now_iso
import config as cfg_mod
from tools import broadcast


def _days_to_expiry(expiry_iso: str | None) -> int | None:
    if not expiry_iso:
        return None
    try:
        d = datetime.fromisoformat(expiry_iso).date()
    except ValueError:
        return None
    return (d - date.today()).days


def _last_forward_at() -> datetime | None:
    """When did we last successfully forward (sent or gated)?

    'Gated' counts: a >50-recipient gate isn't relevant here (council is
    one address), but if a future change ever inflates reach we still
    don't want to spam-forward inside one week. The cadence is about
    council inbox hygiene, not delivery proof.
    """
    conn = connect()
    row = conn.execute(
        "SELECT MAX(fired_at) AS last FROM policy_runs "
        "WHERE playbook='insurance_forward' "
        "AND outcome IN ('sent','gated','drafted')").fetchone()
    conn.close()
    if not row or not row["last"]:
        return None
    try:
        return datetime.fromisoformat(row["last"])
    except ValueError:
        return None


def _build_forward_email(cfg: dict, days_to_expiry: int | None,
                         requested_by: str | None = None) -> tuple[str, str]:
    insurer = cfg.get("insurer", "")
    pol = cfg.get("policy_number", "")
    cover = cfg.get("cover_aud") or 0
    expiry = cfg.get("expiry", "")
    cert_path = cfg.get("certificate_path", "")
    cover_m = f"${int(cover/1_000_000)}M" if cover else "—"
    expiry_note = (f" (renewing — {days_to_expiry} days remaining)"
                   if days_to_expiry is not None and days_to_expiry <= 60
                   else "")
    salutation = ("Hi council bookings,\n\n"
                  if not requested_by else
                  f"Hi {requested_by},\n\n")
    subject = (f"HGJFC Certificate of Currency — {insurer} ({pol})"
               + (" [as requested]" if requested_by else ""))
    body = (
        salutation
        + ("Thanks for the request — attached is our current "
           "Certificate of Currency.\n\n" if requested_by else
           "Quarterly courtesy forward of our current Certificate of "
           "Currency, for your bookings file.\n\n")
        + f"Insurer: {insurer}\n"
        + f"Policy: {pol}\n"
        + f"Public liability cover: {cover_m}\n"
        + f"Expiry: {expiry}{expiry_note}\n"
        + f"Attached: {cert_path}\n\n"
        + "If anything else is needed for the season's bookings let me "
        + "know.\n\n"
        + "Diane Pasquale\n"
        + "Secretary, Henley & Grange Junior Football Club\n")
    return subject, body


def _forward(cycle_id: int, related_input: str | None,
             requested_by: str | None = None) -> dict:
    """Send (or gate) a Certificate of Currency forward to council."""
    cfg = cfg_mod.get().get("insurance", {})
    council_email = cfg.get("council_email")
    if not council_email:
        return {"ok": False, "reason": "no council_email configured"}
    days = _days_to_expiry(cfg.get("expiry"))
    subject, body = _build_forward_email(cfg, days, requested_by)
    # target_id encodes the policy number + ISO week — without a stable
    # target the dedup gate would let two forwards inside one week through
    # if the schedule tick re-minted (e.g. daily tick + an inbound
    # council request on the same day). Inbound requests get a distinct
    # target_id (requested_by suffix) so the council's explicit ask
    # always lands.
    iso = date.today().isocalendar()
    suffix = f"req:{requested_by}" if requested_by else f"week:{iso[0]}W{iso[1]:02d}"
    target_id = f"{cfg.get('policy_number','POL')}::{suffix}"
    return broadcast.submit(
        cycle_id=cycle_id, playbook="insurance_forward",
        channel="email", segment=f"council:{council_email}",
        reach=1, recipient_id=council_email,
        subject=subject, body=body,
        target_kind="council_compliance",
        target_id=target_id,
        related_input=related_input)


def _expiry_decision(cycle_id: int, days: int) -> str:
    """Open a decision card for Diane to chase the broker.

    Inside 7 days (or expired) the brief widens to include the
    treasurer + president, because all ground bookings are at risk if
    the policy lapses. Diane can dispatch with one tap.
    """
    cfg = cfg_mod.get().get("insurance", {})
    insurer = cfg.get("insurer", "")
    pol = cfg.get("policy_number", "")
    cover = cfg.get("cover_aud") or 0
    cover_m = f"${int(cover/1_000_000)}M" if cover else "—"
    expiry = cfg.get("expiry", "")
    state = "EXPIRED" if days < 0 else f"{days} days remaining"
    urgency = (
        "URGENT — bookings at risk" if days < 7
        else "Renewal approaching")
    summary = f"Insurance: {state} ({pol})"
    context = (
        f"Policy: {pol} — {insurer}\n"
        f"Cover: {cover_m}\n"
        f"Expiry: {expiry} ({state})\n\n"
        f"{urgency}. Renewal is a broker call — agent can't do it. "
        f"Once the new certificate is on file the agent will resume "
        f"quarterly forwards to council automatically.\n\n"
        f"If <7 days, suggest looping in treasurer + president now: "
        f"council bookings require a current CoC on file and any "
        f"lapse pauses ground access.")
    options = [
        {"key": "ack", "label": "I'll call the broker"},
        {"key": "draft_brief",
         "label": "Draft brief to treasurer + president"},
        {"key": "snooze", "label": "Snooze 7 days"},
    ]
    from tools.broadcast import _open_decision
    return _open_decision(
        cycle_id, "insurance_renewal_required",
        "insurance", pol, summary, context, options, "ack")


def run(cycle_id: int, related_input: str | None = None,
        requested_by: str | None = None) -> dict[str, Any]:
    """Daily insurance check.

    Inbound council requests skip the cadence test — they always
    forward. The schedule tick path runs the cadence test plus the
    expiry-window check.
    """
    cfg = cfg_mod.get().get("insurance", {})
    if not cfg.get("enabled", False):
        log_action(cycle_id, "execute", "insurance_skipped",
                   None, None, True, "disabled in config")
        return {"skipped": True, "reason": "disabled"}

    out: dict[str, Any] = {"forwarded": None, "escalated": None,
                           "skipped": False}
    days = _days_to_expiry(cfg.get("expiry"))

    # Inbound council request: bypass cadence; route is "the council
    # asked, send it now".
    if requested_by:
        result = _forward(cycle_id, related_input, requested_by)
        out["forwarded"] = result
        out["mode"] = "inbound_request"
        log_action(cycle_id, "execute", "insurance_forwarded_on_request",
                   "council_compliance", cfg.get("policy_number"),
                   True, f"requested_by={requested_by} status={result.get('status')}")
        return out

    # Expiry escalation — fires once per reminder day per cycle. The
    # decision card itself is bounded by the standard one-decision-per-
    # logical-target convention; reopening the same insurance renewal
    # card every cycle in the 14-day window is exactly the queue-noise
    # the silent_gates feedback prohibits, so we check the open queue.
    reminder_days = sorted(set(int(d) for d in cfg.get(
        "expiry_reminder_days", [60, 30, 14, 7, 1])), reverse=True)
    if days is not None and (days in reminder_days or days < 0):
        conn = connect()
        already = conn.execute(
            "SELECT id FROM decisions "
            "WHERE kind='insurance_renewal_required' "
            "AND target_id=? AND status='pending'",
            (cfg.get("policy_number"),)).fetchone()
        conn.close()
        if not already:
            did = _expiry_decision(cycle_id, days)
            out["escalated"] = {"decision_id": did, "days": days}
            log_action(cycle_id, "execute", "insurance_renewal_decision",
                       "insurance", cfg.get("policy_number"),
                       True, f"days={days} dec={did}")
        else:
            out["escalated"] = {"decision_id": already["id"],
                                "days": days, "reused": True}

    # Quarterly forward — only if we're not already past expiry. Sending
    # a stale CoC to council is worse than not sending one.
    if days is not None and days < 0:
        out["mode"] = "expired_no_forward"
        log_action(cycle_id, "execute", "insurance_forward_blocked",
                   None, None, True, "policy expired, withholding forward")
        return out

    last = _last_forward_at()
    cadence = int(cfg.get("forward_cadence_days", 90))
    if last is None:
        due = True
        days_since = None
    else:
        days_since = (datetime.now() - last).days
        due = days_since >= cadence

    if due:
        result = _forward(cycle_id, related_input)
        out["forwarded"] = result
        out["mode"] = "cadence_due"
        log_action(cycle_id, "execute", "insurance_forwarded_cadence",
                   "council_compliance", cfg.get("policy_number"),
                   True, f"days_since={days_since} status={result.get('status')}")
    else:
        out["mode"] = "cadence_skip"
        out["days_since_last_forward"] = days_since
        log_action(cycle_id, "execute", "insurance_cadence_skip",
                   None, None, True,
                   f"days_since={days_since} cadence={cadence}")
    return out

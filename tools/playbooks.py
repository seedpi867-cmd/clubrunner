"""Playbook registry — the orchestrator dispatches by name.

A playbook receives (cycle_id, input_id_or_None) and is responsible
for: drafting any broadcasts, calling the broadcast helper, and closing
the originating input if applicable.
"""
from __future__ import annotations

from typing import Callable

from tools import (heat_policy, lightning_policy, duty_reminder,
                   payment_chase, wwcc_check, newsletter, simple_acks,
                   committee_brief, match_report, insurance_check,
                   event_reminder, coach_reliability, match_day_brief,
                   sponsor_obligations, ground_drainage)
from edge.state.db import connect


def _close(input_id: str | None) -> None:
    if not input_id:
        return
    conn = connect()
    conn.execute("UPDATE inputs SET status='done' WHERE id=?",
                 (input_id,))
    conn.close()


def _heat(cycle_id: int, input_id: str | None) -> dict:
    out = heat_policy.run(cycle_id, related_input=input_id)
    _close(input_id)
    return out


def _lightning(cycle_id: int, input_id: str | None) -> dict:
    out = lightning_policy.run(cycle_id, related_input=input_id)
    _close(input_id)
    return out


def _duty(cycle_id: int, input_id: str | None) -> dict:
    out = duty_reminder.run(cycle_id, related_input=input_id)
    _close(input_id)
    return out


def _payment(cycle_id: int, input_id: str | None) -> dict:
    out = payment_chase.run(cycle_id, related_input=input_id)
    _close(input_id)
    return out


def _wwcc(cycle_id: int, input_id: str | None) -> dict:
    out = wwcc_check.run(cycle_id, related_input=input_id)
    _close(input_id)
    return out


def _newsletter(cycle_id: int, input_id: str | None) -> dict:
    out = newsletter.run(cycle_id, related_input=input_id)
    _close(input_id)
    return out


def _committee_brief(cycle_id: int, input_id: str | None) -> dict:
    out = committee_brief.run(cycle_id, related_input=input_id)
    _close(input_id)
    return out


def _match_day_brief(cycle_id: int, input_id: str | None) -> dict:
    out = match_day_brief.run(cycle_id, related_input=input_id)
    _close(input_id)
    return out


def _match_report(cycle_id: int, input_id: str | None) -> dict:
    out = match_report.run(cycle_id, related_input=input_id)
    _close(input_id)
    return out


def _insurance(cycle_id: int, input_id: str | None) -> dict:
    out = insurance_check.run(cycle_id, related_input=input_id)
    _close(input_id)
    return out


def _event_reminder(cycle_id: int, input_id: str | None) -> dict:
    out = event_reminder.run(cycle_id, related_input=input_id)
    _close(input_id)
    return out


def _coach_reliability(cycle_id: int, input_id: str | None) -> dict:
    # Pure background sweep — never tied to an input. We still accept the
    # signature so the orchestrator can dispatch through fire().
    return coach_reliability.run(cycle_id, related_input=input_id)


def _sponsor_obligations(cycle_id: int, input_id: str | None) -> dict:
    # Pure background sweep — checks each sponsor's deliverability vs
    # weeks left in the season; opens a quiet decision when a contract
    # cannot be honoured even if every remaining newsletter is theirs.
    return sponsor_obligations.run(cycle_id, related_input=input_id)


def _ground_drainage(cycle_id: int, input_id: str | None) -> dict:
    # Pure background sweep — reads the latest rain_24h_mm per ground
    # and fires a closure broadcast when a ground's threshold is
    # crossed. Idempotent within a day via policy_runs.
    return ground_drainage.run(cycle_id, related_input=input_id)


REGISTRY: dict[str, Callable[[int, str | None], dict]] = {
    "heat_policy": _heat,
    "lightning_policy": _lightning,
    "duty_reminder": _duty,
    "payment_chase": _payment,
    "wwcc_check": _wwcc,
    "newsletter": _newsletter,
    "committee_brief": _committee_brief,
    "match_day_brief": _match_day_brief,
    "match_report": _match_report,
    "insurance_check": _insurance,
    "event_reminder": _event_reminder,
    "coach_reliability": _coach_reliability,
    "sponsor_obligations": _sponsor_obligations,
    "ground_drainage": _ground_drainage,
    "availability_ack": lambda c, i: simple_acks.availability_ack(c, i),
    "faq_training": lambda c, i: simple_acks.faq_training(c, i),
    "faq_register": lambda c, i: simple_acks.faq_register(c, i),
    "council_clash_resolved": lambda c, i: simple_acks.council_clash_resolved(c, i),
    "fixtures_ingest": lambda c, i: simple_acks.fixtures_ingest(c, i),
}


def fire(playbook: str, cycle_id: int,
         input_id: str | None = None) -> dict:
    fn = REGISTRY.get(playbook)
    if not fn:
        return {"ok": False, "reason": f"unknown playbook {playbook}"}
    return fn(cycle_id, input_id)


def fire_with(playbook: str, cycle_id: int, input_id: str | None,
              **kwargs) -> dict:
    """Fire a playbook with extra keyword arguments.

    Specifically used for the inbound-council-insurance-request path,
    which carries `requested_by` so the playbook bypasses the cadence
    test. Restricted to playbooks that opt in via `accepts_kwargs`.
    """
    if playbook == "insurance_check":
        out = insurance_check.run(cycle_id, related_input=input_id,
                                  **kwargs)
        _close(input_id)
        return out
    # Fallback to plain fire — drop kwargs rather than raise.
    return fire(playbook, cycle_id, input_id)

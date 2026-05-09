"""Triage router — decides what each classified input does next.

After classifier.py has labelled inputs, the router splits them into:
  - playbook_targets: items with a playbook hint, no escalation, no
    safeguarding flag — these go through EXECUTE directly via the
    matched playbook
  - brain_targets:    items flagged escalate=True OR classification='novel'
                      OR safeguarding=1 (special-cased: brain gets a
                      pre-filled brief but never autosends)
  - ignored:          junk

The router only reads + writes input.status.
"""
from __future__ import annotations

from edge.state.db import connect, log_action
import config as cfg_mod


def run(cycle_id: int) -> dict:
    cfg = cfg_mod.get()
    max_brain = int(cfg.get("brain", {}).get("max_items_per_cycle", 6))

    conn = connect()
    rows = [dict(r) for r in conn.execute(
        "SELECT id, classification, safeguarding, priority "
        "FROM inputs WHERE status='classified' "
        "ORDER BY priority DESC, received_at ASC")]
    conn.close()

    playbook_ids: list[str] = []
    brain_ids: list[str] = []
    ignored: list[str] = []

    # Categories whose rule explicitly named a playbook AND does not
    # escalate — these route directly to EXECUTE. Adding a new playbook-
    # bound classification means appending the category here AND the
    # cls_to_pb dispatch table in orchestrator._phase_execute. Without
    # that pairing the input falls through to brain and the playbook
    # never fires (committee_tick used to silently do this).
    PLAYBOOK_CATEGORIES = {
        "newsletter_tick", "duty_tick", "payment_tick", "wwcc_tick",
        "committee_tick", "insurance_tick", "insurance_request",
        "match_day_tick",
        "weather_observation", "council_resolved", "fixture_change",
        "payment_overdue", "player_availability",
        "parent_faq_training", "parent_faq_register",
        "match_report_inbound", "gcal_reminder",
    }
    JUNK = {"junk"}
    # Inputs that should be recorded for audit but require no agent
    # action — e.g. a TeamApp 'yes' RSVP confirms the default
    # expectation. Routing these to the brain would burn cycles on the
    # most common case (most parents are in); routing to a playbook
    # adds noise. Silent-close: input stays in the table for the
    # manager view, but no brain, no broadcast, no decision.
    # Categories the agent records but never acts on. Routing these to
    # the brain would burn cycles on the most common "no action needed"
    # cases — a TeamApp 'yes' RSVP confirms the default expectation, and
    # a short FB/SMS thanks ("great work this weekend!") is queue noise
    # not a judgment item. Silent-close keeps the audit trail for the
    # manager view without filling Diane's queue.
    SILENT_RECORD = {"availability_yes", "positive_ack"}
    silent_ids: list[str] = []

    for r in rows:
        cls = r["classification"]
        if r["safeguarding"]:
            brain_ids.append(r["id"])
        elif cls in JUNK:
            ignored.append(r["id"])
        elif cls in SILENT_RECORD:
            silent_ids.append(r["id"])
        elif cls in PLAYBOOK_CATEGORIES:
            playbook_ids.append(r["id"])
        else:
            brain_ids.append(r["id"])

    # Cap brain items per cycle to control cost
    overflow = brain_ids[max_brain:]
    brain_ids = brain_ids[:max_brain]

    conn = connect()
    for iid in playbook_ids:
        conn.execute("UPDATE inputs SET status='routed_playbook' "
                     "WHERE id=?", (iid,))
    for iid in brain_ids:
        conn.execute("UPDATE inputs SET status='routed_brain' WHERE id=?",
                     (iid,))
    for iid in ignored:
        conn.execute("UPDATE inputs SET status='ignored' WHERE id=?", (iid,))
    for iid in silent_ids:
        conn.execute("UPDATE inputs SET status='done' WHERE id=?", (iid,))
    # Overflow stays as 'classified' — picked up next cycle.
    conn.close()

    log_action(cycle_id, "triage", "routed", None, None, True,
               f"playbook={len(playbook_ids)} brain={len(brain_ids)} "
               f"ignored={len(ignored)} silent={len(silent_ids)} "
               f"overflow={len(overflow)}")
    return {"playbook": playbook_ids, "brain": brain_ids,
            "ignored": ignored, "silent": silent_ids,
            "overflow": overflow}

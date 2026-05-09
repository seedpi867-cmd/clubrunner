"""clubrunner orchestrator.

Five phases, every cycle:

  COLLECT  → run all enabled collectors; mint inputs into state.db
  TRIAGE   → classify all 'new' inputs, route to playbook or brain
  BRAIN    → for each routed_brain input, build a brief + draft + open
             a decision on Diane's queue
  EXECUTE  → for each routed_playbook input, fire the playbook (which
             goes through gates and either sends or queues an approval)
  REFLECT  → update memory snapshots, finalise cycle row, log summary

Run modes:
  python -m orchestrator             # one cycle, exit
  python -m orchestrator --loop      # forever, sleeping per config
  python -m orchestrator --once-then-exit  (alias of no-args)
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from edge.state.db import connect, init_db, log_action, now_iso
from edge.state.seed import seed
from edge.collectors import (bom_heat, bom_lightning, bom_rain,
                             imap_secretary, playhq, council_portal,
                             schedule, sms_inbound, facebook_page, gcal,
                             teamapp)
from triage import classifier, router
from tools import playbooks
from brain import executive
from memory import working as memory
import config as cfg_mod


def _start_cycle() -> int:
    conn = connect()
    cur = conn.execute(
        "INSERT INTO cycles(started_at) VALUES(?)", (now_iso(),))
    cid = cur.lastrowid
    conn.close()
    return cid


def _end_cycle(cid: int, stats: dict) -> None:
    conn = connect()
    conn.execute(
        "UPDATE cycles SET ended_at=?, items_in=?, items_classified=?, "
        "drafts_created=?, broadcasts_sent=?, decisions_opened=?, "
        "policies_fired=?, notes=? WHERE id=?",
        (now_iso(),
         int(stats.get("items_in", 0)),
         int(stats.get("classified", 0)),
         int(stats.get("drafts_created", 0)),
         int(stats.get("broadcasts_sent", 0)),
         int(stats.get("decisions_opened", 0)),
         int(stats.get("policies_fired", 0)),
         stats.get("notes", "")[:500],
         cid))
    conn.close()


def _phase_collect(cid: int) -> dict:
    out = {}
    for mod in (schedule, bom_heat, bom_lightning, bom_rain,
                council_portal, playhq, imap_secretary, sms_inbound,
                facebook_page, teamapp, gcal):
        try:
            r = mod.run(cid) or {}
        except Exception as ex:
            log_action(cid, "collect", f"{mod.__name__}_failed",
                       None, None, False, str(ex)[:200])
            r = {"error": str(ex)[:200]}
        out[mod.__name__.split(".")[-1]] = r
    return out


def _phase_triage(cid: int) -> dict:
    cls_out = classifier.run(cid)
    rt_out = router.run(cid)
    return {"classify": cls_out, "route": rt_out}


def _phase_brain(cid: int, brain_input_ids: list[str]) -> dict:
    results = []
    for iid in brain_input_ids:
        results.append(executive.think(cid, iid))
    return {"thoughts": results}


def _phase_execute(cid: int, playbook_input_ids: list[str]) -> dict:
    """Fire input-driven playbooks AND any time-based 'tick' playbooks
    that classifier marked through the schedule collector.

    Weather policies (heat/lightning) are evaluated EVERY cycle whether or
    not an input triggered them — the BOM collectors only update
    weather_state, they do not mint inputs. Idempotency is enforced by the
    dedup gate against policy_runs.
    """
    results = []
    conn = connect()
    rows = conn.execute(
        "SELECT id, classification FROM inputs "
        "WHERE id IN (" + ",".join("?" * len(playbook_input_ids)) + ")",
        playbook_input_ids).fetchall() if playbook_input_ids else []
    conn.close()

    # Use rules.yaml playbook map: classification -> playbook
    cls_to_pb = {
        "newsletter_tick": "newsletter",
        "duty_tick": "duty_reminder",
        "payment_tick": "payment_chase",
        "wwcc_tick": "wwcc_check",
        "committee_tick": "committee_brief",
        "insurance_tick": "insurance_check",
        "match_day_tick": "match_day_brief",
        "insurance_request": "_insurance_request",  # branched below
        "weather_observation": "_weather",  # branched below
        "council_resolved": "council_clash_resolved",
        "fixture_change": "fixtures_ingest",
        "payment_overdue": "payment_chase",
        "player_availability": "availability_ack",
        "parent_faq_training": "faq_training",
        "parent_faq_register": "faq_register",
        "match_report_inbound": "match_report",
        "gcal_reminder": "event_reminder",
    }

    fired_weather = False
    for r in rows:
        cls = r["classification"]
        iid = r["id"]
        pb = cls_to_pb.get(cls)
        if pb == "_weather":
            # Both heat + lightning evaluate latest state regardless
            if not fired_weather:
                results.append(playbooks.fire("heat_policy", cid, iid))
                results.append(playbooks.fire("lightning_policy", cid, iid))
                fired_weather = True
            else:
                # close the input — already handled this cycle
                conn = connect()
                conn.execute("UPDATE inputs SET status='done' WHERE id=?",
                             (iid,))
                conn.close()
        elif pb == "_insurance_request":
            # Inbound council request: pass requested_by so the playbook
            # bypasses the cadence test and forwards immediately.
            conn = connect()
            row = conn.execute(
                "SELECT sender FROM inputs WHERE id=?", (iid,)).fetchone()
            conn.close()
            requested_by = row["sender"] if row else None
            results.append(playbooks.fire_with(
                "insurance_check", cid, iid,
                requested_by=requested_by))
        elif pb:
            results.append(playbooks.fire(pb, cid, iid))

    # Always-on weather evaluation — heat + lightning each cycle, dedup-
    # gated. This is how the agent stays in sync with BOM observations
    # without needing a synthetic input row.
    if not fired_weather:
        results.append(playbooks.fire("heat_policy", cid, None))
        results.append(playbooks.fire("lightning_policy", cid, None))

    # Drainage policy is also always-on — same model as heat. Rain
    # observations come from bom_rain into weather_state, the playbook
    # cancels fixtures and drafts the closure broadcast, and policy_runs
    # ensures one fire per ground per day.
    results.append(playbooks.fire("ground_drainage", cid, None))

    # Coach reliability sweep — registers report obligations once per
    # fixture as deadlines pass, recomputes reliability, opens a quiet
    # decision when a coach drops below threshold. Pure background work,
    # idempotent via policy_runs.
    results.append(playbooks.fire("coach_reliability", cid, None))

    # Sponsor obligation sweep — closes the architecture-promised
    # "rotation per contract spec" gap by detecting sponsors whose
    # posts_owed cannot fit in the remaining newsletter weeks. Quiet
    # decision; cooldown by sponsor + shortfall severity.
    results.append(playbooks.fire("sponsor_obligations", cid, None))

    return {"playbooks_fired": results}


def _phase_reflect(cid: int) -> dict:
    mem = memory.update_long_term()
    return {"memory": mem}


def _stats(cid: int) -> dict:
    conn = connect()
    # items_in counts inputs whose received_at falls inside this cycle's
    # wall-clock window — i.e. genuinely new mints. Counting collect actions
    # over-reports because re-seen items still log poll events.
    inp = conn.execute(
        "SELECT COUNT(*) AS n FROM inputs "
        "WHERE received_at >= (SELECT started_at FROM cycles WHERE id=?)",
        (cid,)).fetchone()
    classified = conn.execute(
        "SELECT COUNT(*) AS n FROM actions WHERE cycle_id=? "
        "AND phase='triage' AND action='classified'", (cid,)).fetchone()
    drafts = conn.execute(
        "SELECT COUNT(*) AS n FROM broadcasts WHERE cycle_id=?",
        (cid,)).fetchone()
    sent = conn.execute(
        "SELECT COUNT(*) AS n FROM broadcasts "
        "WHERE cycle_id=? AND status='sent'", (cid,)).fetchone()
    decisions = conn.execute(
        "SELECT COUNT(*) AS n FROM decisions WHERE cycle_id=?",
        (cid,)).fetchone()
    policies = conn.execute(
        "SELECT COUNT(*) AS n FROM policy_runs WHERE cycle_id=?",
        (cid,)).fetchone()
    conn.close()
    return {
        "items_in": inp["n"], "classified": classified["n"],
        "drafts_created": drafts["n"], "broadcasts_sent": sent["n"],
        "decisions_opened": decisions["n"], "policies_fired": policies["n"],
    }


def _cycle_once() -> dict:
    init_db()
    seed()
    cid = _start_cycle()
    t0 = time.time()
    log_action(cid, "orchestrator", "cycle_start", None, None, True, "")

    coll = _phase_collect(cid)
    triage_out = _phase_triage(cid)
    brain_ids = triage_out["route"]["brain"]
    pb_ids = triage_out["route"]["playbook"]
    brain_out = _phase_brain(cid, brain_ids)
    exec_out = _phase_execute(cid, pb_ids)
    reflect_out = _phase_reflect(cid)

    stats = _stats(cid)
    stats["notes"] = (f"collect={sum(1 for k,v in coll.items() if v) }; "
                      f"playbooks={len(pb_ids)}; "
                      f"brain={len(brain_ids)}")
    _end_cycle(cid, stats)

    snapshot = {
        "cycle_id": cid,
        "started_at": now_iso(),
        "duration_s": round(time.time() - t0, 2),
        "collect": coll,
        "triage": triage_out,
        "brain": brain_out,
        "execute": exec_out,
        "reflect": reflect_out,
        "stats": stats,
    }
    memory.write_working(snapshot)
    memory.append_short({"cycle": cid, "stats": stats,
                         "ended_at": now_iso()})
    log_action(cid, "orchestrator", "cycle_end", None, None, True,
               str(stats))
    return snapshot


def _interval() -> int:
    cfg = cfg_mod.get().get("cycle", {})
    weekday = datetime.now().weekday()
    if weekday in cfg.get("match_days", [5]):
        return int(cfg.get("match_day_interval_minutes", 10))
    hour = datetime.now().hour
    if hour < 7 or hour >= 21:
        return int(cfg.get("off_hours_interval_minutes", 120))
    return int(cfg.get("interval_minutes", 30))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", action="store_true", help="run forever")
    args = ap.parse_args()

    if not args.loop:
        snap = _cycle_once()
        print(f"cycle {snap['cycle_id']} done in {snap['duration_s']}s — "
              f"{snap['stats']}")
        return

    while True:
        try:
            snap = _cycle_once()
            print(f"[{datetime.now().isoformat(timespec='seconds')}] "
                  f"cycle {snap['cycle_id']} {snap['stats']}")
        except KeyboardInterrupt:
            return
        except Exception as ex:
            print(f"cycle failed: {ex}")
        sleep_s = _interval() * 60
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()

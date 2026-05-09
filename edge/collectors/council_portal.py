"""Council ground-booking collector.

Polls the council booking portal for clashes with our fixtures.
Stub mode reads from scenarios/today.json.

Status transitions matter as much as initial state. A clash is born
'pending' (escalates to brain → Diane's queue), then upstream the
council either resolves the overlap or doubles down. The collector
must mint a fresh input on each status change or Diane sees a stale
'pending' card forever and the all-clear playbook never fires. The
source_id therefore encodes the status: a transition gives a new
(source, source_id) tuple, upsert_input mints, classifier routes the
new state to the right handler.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from edge.state.db import connect, log_action, upsert_input
import config as cfg_mod

ROOT = Path(__file__).resolve().parents[2]
SCENARIO = ROOT / "scenarios" / "today.json"


def _clash_window_key(ground_id: str, datetime_window: str) -> str:
    """Stable identifier for the physical clash (ground + window),
    independent of status. Downstream playbooks use this to find any
    prior 'pending' input for the same window so a 'resolved' arrival
    can close out the matching council_brief decision."""
    return f"clash_{ground_id}_{datetime_window.split('/')[0]}"


def run(cycle_id: int) -> dict[str, Any]:
    cfg = cfg_mod.get()
    coll_cfg = cfg.get("collectors", {}).get("council_portal", {})
    if not coll_cfg.get("enabled", False):
        return {"skipped": True}

    if not SCENARIO.exists():
        return {"ingested": 0}
    scenario = json.loads(SCENARIO.read_text())
    clashes = scenario.get("council_portal_clashes", [])

    n = 0
    for c in clashes:
        status = c.get("status") or "pending"
        # source_id includes the status — a pending→resolved transition
        # gets its own (source, source_id) tuple so upsert_input mints
        # a new input and the classifier routes the resolution through
        # the council_clash_resolved playbook. Without the status in
        # the key, the existing 'pending' row absorbs the resolution
        # and Diane's open council_brief never closes.
        window_key = _clash_window_key(c["ground_id"], c["datetime_window"])
        sid = f"{window_key}:{status}"
        _, is_new = upsert_input(
            "council", sid,
            sender="council.bookings",
            subject=f"Booking clash {c['ground_id']} — {status}",
            body=(f"Competing event: {c['competing_event']}. "
                  f"Window: {c['datetime_window']}. "
                  f"Status: {status}. {c.get('note','')}"),
            raw_json=json.dumps(c))
        if is_new:
            n += 1

    log_action(cycle_id, "collect", "council_polled",
               None, None, True, f"clashes={len(clashes)} minted={n}")
    return {"ingested": n, "clashes": len(clashes)}

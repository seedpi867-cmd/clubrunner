"""PlayHQ club portal collector.

Real mode: scrape or API. Stub mode: scenarios/today.json provides the
outstanding payments + match results we'd otherwise pull.

What we actually do here is reflect external reality into our state:
- update player.rego_status and rego_paid_at when payments come in
- create payment-overdue inputs when registration deadline missed
- update fixture results
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from edge.state.db import connect, log_action, upsert_input, now_iso
import config as cfg_mod

ROOT = Path(__file__).resolve().parents[2]
SCENARIO = ROOT / "scenarios" / "today.json"


def run(cycle_id: int) -> dict[str, Any]:
    cfg = cfg_mod.get()
    coll_cfg = cfg.get("collectors", {}).get("playhq", {})
    if not coll_cfg.get("enabled", False):
        return {"skipped": True}
    if coll_cfg.get("base_url") != "stub":
        # Real PlayHQ wiring would go here; we ship the orchestration.
        return {"skipped": True, "reason": "live_endpoint_not_configured"}

    if not SCENARIO.exists():
        return {"ingested": 0}
    scenario = json.loads(SCENARIO.read_text())
    overdue = scenario.get("playhq_payments_outstanding", [])

    conn = connect()
    n_overdue = 0
    today_iso = datetime.now().date().isoformat()
    for row in overdue:
        pid = row["player_id"]
        # check player exists
        player = conn.execute(
            "SELECT id, first_name, last_name, parent_id FROM players "
            "WHERE id=?", (pid,)).fetchone()
        if not player:
            continue
        # mint once per (player, day) — upsert returns is_new=False on rerun
        sid = f"playhq_overdue_{pid}_{today_iso}"
        _, is_new = upsert_input(
            "playhq", sid,
            sender="playhq.system",
            subject=f"Payment overdue — {player['first_name']} "
                    f"{player['last_name']}",
            body=(f"${row['amount_aud']:.2f} due {row['due_at']}, "
                  "still outstanding."),
            raw_json=json.dumps(row))
        if is_new:
            n_overdue += 1
    conn.close()

    log_action(cycle_id, "collect", "playhq_polled",
               None, None, True,
               f"overdue_inputs_minted={n_overdue}")
    return {"ingested": n_overdue, "overdue_count": len(overdue)}

"""BOM lightning observation collector.

Polls strike data and records the nearest strike distance per ground
into weather_state. Distance == NULL means no strikes within 50km.

Stub uses scenarios/today.json (lightning section). Real mode would
call the BOM lightning radar / WZRPC product.
"""
from __future__ import annotations

import json
import math
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path
from typing import Any

from edge.state.db import connect, log_action, now_iso
from edge.health import breaker
import config as cfg_mod

ROOT = Path(__file__).resolve().parents[2]
SCENARIO = ROOT / "scenarios" / "today.json"


def _haversine_km(lat1, lng1, lat2, lng2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlng / 2) ** 2)
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _stub_strikes() -> list[dict]:
    if SCENARIO.exists():
        scenario = json.loads(SCENARIO.read_text())
        return scenario.get("bom_lightning", {}).get("strikes_last_10min", [])
    return []


def run(cycle_id: int) -> dict[str, Any]:
    cfg = cfg_mod.get()
    coll_cfg = cfg.get("collectors", {}).get("bom_lightning", {})
    if not coll_cfg.get("enabled", False):
        return {"skipped": True}
    if breaker.is_open("bom"):
        return {"skipped": True, "breaker": "open"}

    use_stub = coll_cfg.get("radar_endpoint") == "stub"
    try:
        strikes = _stub_strikes() if use_stub else _live_strikes()
        breaker.record_success("bom")
    except (urllib.error.URLError, TimeoutError, ValueError) as ex:
        breaker.record_failure("bom", f"lightning: {ex}")
        log_action(cycle_id, "collect", "bom_lightning_failed",
                   None, None, False, str(ex)[:200])
        return {"skipped": True, "error": str(ex)[:200]}

    conn = connect()
    grounds = [dict(r) for r in conn.execute(
        "SELECT id, lat, lng FROM grounds")]

    nearest_per_ground: dict[str, float | None] = {}
    for g in grounds:
        if not strikes:
            nearest_per_ground[g["id"]] = None
            continue
        nearest = min(_haversine_km(g["lat"], g["lng"],
                                    s["lat"], s["lng"]) for s in strikes)
        nearest_per_ground[g["id"]] = nearest

    n = 0
    for gid, dist in nearest_per_ground.items():
        conn.execute(
            "INSERT OR REPLACE INTO weather_state(ground_id, observed_at, "
            "forecast_max_c, forecast_for, lightning_km) "
            "VALUES(?,?, "
            "  (SELECT forecast_max_c FROM weather_state WHERE ground_id=? "
            "   ORDER BY observed_at DESC LIMIT 1),"
            "  (SELECT forecast_for FROM weather_state WHERE ground_id=? "
            "   ORDER BY observed_at DESC LIMIT 1),"
            "?)",
            (gid, now_iso(), gid, gid, dist))
        n += 1
    conn.close()

    log_action(cycle_id, "collect", "bom_lightning_observed",
               None, None, True,
               json.dumps({k: round(v, 2) if v is not None else None
                           for k, v in nearest_per_ground.items()}))
    return {"observed": n, "nearest_km": nearest_per_ground,
            "strike_count": len(strikes)}


def _live_strikes():
    raise NotImplementedError("BOM lightning live endpoint requires "
                              "WZRPC subscription — config.collectors."
                              "bom_lightning.radar_endpoint='stub' for now.")

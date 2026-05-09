"""BOM heat-forecast collector.

Polls the forecast for each ground postcode. Writes to weather_state.
The heat policy itself runs in the EXECUTE phase via the playbook —
this collector ONLY observes and records.

Stub mode (config.collectors.bom_heat.forecast_endpoint == 'stub') uses
a deterministic fixture: scenarios/today.json. Real mode uses
api.weather.bom.gov.au — wire-compatible response shape.
"""
from __future__ import annotations

import json
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


def _stub_forecasts(grounds: list[dict]) -> dict[str, float]:
    """Read a fixture or fall back to a defaulted realistic Adelaide-summer
    forecast that fires the heat policy (matches cycle 108 explore: 38°C)."""
    if SCENARIO.exists():
        scenario = json.loads(SCENARIO.read_text())
        forecasts = scenario.get("bom_heat", {})
        return {g["id"]: float(forecasts.get(g["postcode"], 38.0))
                for g in grounds}
    # Default scenario: extreme heat across all club grounds.
    return {g["id"]: 38.0 for g in grounds}


def _live_forecast(postcode: str, timeout: float) -> float:
    url = (f"https://api.weather.bom.gov.au/v1/locations/{postcode}/forecasts/daily")
    req = urllib.request.Request(url, headers={"User-Agent": "clubrunner/1"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read())
    today = data["data"][0]
    return float(today["temp_max"])


def run(cycle_id: int) -> dict[str, Any]:
    cfg = cfg_mod.get()
    coll_cfg = cfg.get("collectors", {}).get("bom_heat", {})
    if not coll_cfg.get("enabled", False):
        return {"skipped": True}
    if breaker.is_open("bom"):
        log_action(cycle_id, "collect", "bom_heat_breaker_open",
                   None, None, False, "skipped: breaker open")
        return {"skipped": True, "breaker": "open"}

    timeout = float(cfg.get("health", {}).get(
        "fail_fast_external_seconds", 8))
    use_stub = coll_cfg.get("forecast_endpoint") == "stub"

    conn = connect()
    grounds = [dict(r) for r in conn.execute(
        "SELECT id, postcode FROM grounds")]
    conn.close()

    try:
        if use_stub:
            results = _stub_forecasts(grounds)
        else:
            results = {}
            for g in grounds:
                results[g["id"]] = _live_forecast(g["postcode"], timeout)
        breaker.record_success("bom")
    except (urllib.error.URLError, TimeoutError, ValueError, KeyError) as ex:
        breaker.record_failure("bom", f"heat: {ex}")
        log_action(cycle_id, "collect", "bom_heat_failed",
                   None, None, False, str(ex)[:200])
        return {"skipped": True, "error": str(ex)[:200]}

    today = datetime.now().date().isoformat()
    conn = connect()
    n = 0
    for ground_id, temp_c in results.items():
        conn.execute(
            "INSERT OR REPLACE INTO weather_state(ground_id, observed_at, "
            "forecast_max_c, forecast_for, lightning_km) "
            "VALUES(?,?,?,?, NULL)",
            (ground_id, now_iso(), temp_c, today))
        n += 1
    conn.close()

    log_action(cycle_id, "collect", "bom_heat_observed",
               None, None, True, json.dumps(results))
    return {"observed": n, "forecasts": results}


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from edge.state.db import init_db
    init_db()
    print(run(0))

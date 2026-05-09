"""BOM rainfall observations collector.

Rolling 24h rain accumulation per ground postcode. The drainage policy
runs in EXECUTE; this collector ONLY observes and records, the same way
bom_heat and bom_lightning do.

Stub mode (config.collectors.bom_rain.endpoint == 'stub') reads
scenarios/today.json's bom_rain map ({postcode: mm}). Real mode hits
the BOM observations API and sums the last 24h of point readings.

We update the SAME weather_state row that bom_heat just wrote (keyed on
ground_id + observed_at by ISO minute) — that way the dashboard, the
brain context, and the drainage playbook all see one row per ground per
observation, not three.
"""
from __future__ import annotations

import json
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from edge.state.db import connect, log_action, now_iso
from edge.health import breaker
import config as cfg_mod

ROOT = Path(__file__).resolve().parents[2]
SCENARIO = ROOT / "scenarios" / "today.json"


def _stub_rain(grounds: list[dict]) -> dict[str, float]:
    """Read scenarios/today.json's bom_rain map; default to 0mm so dry
    seasons are the assumption rather than a default-soaking that would
    re-fire the closure policy on every cycle."""
    if SCENARIO.exists():
        try:
            scenario = json.loads(SCENARIO.read_text())
            rain_map = scenario.get("bom_rain", {})
            return {g["id"]: float(rain_map.get(g["postcode"], 0.0))
                    for g in grounds}
        except (json.JSONDecodeError, ValueError):
            pass
    return {g["id"]: 0.0 for g in grounds}


def _live_rain_24h(postcode: str, timeout: float) -> float:
    """Sum the last 24h of point observations for a postcode. The BOM
    observations endpoint returns 30-minute samples; we sum them."""
    url = (f"https://api.weather.bom.gov.au/v1/locations/{postcode}"
           f"/observations")
    req = urllib.request.Request(url, headers={"User-Agent": "clubrunner/1"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read())
    cutoff = datetime.now() - timedelta(hours=24)
    total = 0.0
    for obs in data.get("data", []):
        try:
            ts = datetime.fromisoformat(
                obs["observation_time"].replace("Z", "+00:00")).replace(
                tzinfo=None)
            if ts >= cutoff:
                total += float(obs.get("rain_since_9am_mm", 0.0) or 0.0)
        except (KeyError, ValueError):
            continue
    return total


def run(cycle_id: int) -> dict[str, Any]:
    cfg = cfg_mod.get()
    coll_cfg = cfg.get("collectors", {}).get("bom_rain", {})
    if not coll_cfg.get("enabled", True):
        return {"skipped": True}
    if breaker.is_open("bom"):
        log_action(cycle_id, "collect", "bom_rain_breaker_open",
                   None, None, False, "skipped: breaker open")
        return {"skipped": True, "breaker": "open"}

    timeout = float(cfg.get("health", {}).get(
        "fail_fast_external_seconds", 8))
    use_stub = coll_cfg.get("endpoint", "stub") == "stub"

    conn = connect()
    grounds = [dict(r) for r in conn.execute(
        "SELECT id, postcode FROM grounds")]
    conn.close()

    try:
        if use_stub:
            results = _stub_rain(grounds)
        else:
            results = {g["id"]: _live_rain_24h(g["postcode"], timeout)
                       for g in grounds}
        breaker.record_success("bom")
    except (urllib.error.URLError, TimeoutError, ValueError, KeyError) as ex:
        breaker.record_failure("bom", f"rain: {ex}")
        log_action(cycle_id, "collect", "bom_rain_failed",
                   None, None, False, str(ex)[:200])
        return {"skipped": True, "error": str(ex)[:200]}

    # Merge into the same weather_state row keyed at this minute. If
    # bom_heat ran first this cycle it stamped (ground, now_iso) without
    # rain. We want one row per minute per ground, so update-by-key
    # rather than inserting a second nearly-identical row.
    obs_at = now_iso()
    conn = connect()
    n = 0
    for ground_id, mm in results.items():
        existing = conn.execute(
            "SELECT 1 FROM weather_state WHERE ground_id=? AND observed_at=?",
            (ground_id, obs_at)).fetchone()
        if existing:
            conn.execute(
                "UPDATE weather_state SET rain_24h_mm=? "
                "WHERE ground_id=? AND observed_at=?",
                (mm, ground_id, obs_at))
        else:
            conn.execute(
                "INSERT INTO weather_state(ground_id, observed_at, "
                "rain_24h_mm) VALUES(?,?,?)",
                (ground_id, obs_at, mm))
        n += 1
    conn.close()

    log_action(cycle_id, "collect", "bom_rain_observed",
               None, None, True, json.dumps(results))
    return {"observed": n, "rain_24h_mm": results}


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from edge.state.db import init_db
    init_db()
    print(run(0))

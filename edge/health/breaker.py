"""Circuit breakers per external integration.

Trip after N consecutive failures, cool down for M cycles.

We deliberately do NOT block the cycle inside a single call retry
loop — the breaker carries cooldown state across cycles instead.
(Memory: long external waits must fail fast; cooldown is the
breaker's job, not the caller's.)
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from edge.state.db import connect, now_iso
import config as cfg_mod


def _settings() -> tuple[int, int]:
    h = cfg_mod.get().get("health", {})
    return (int(h.get("breaker_trip_after_failures", 3)),
            int(h.get("breaker_cooldown_cycles", 10)))


def is_open(integration: str) -> bool:
    conn = connect()
    row = conn.execute(
        "SELECT state, cooldown_until FROM breakers WHERE integration=?",
        (integration,)).fetchone()
    conn.close()
    if not row:
        return False
    if row["state"] != "open":
        return False
    until = row["cooldown_until"]
    if until and until <= now_iso():
        # Half-open: let the next call try
        conn = connect()
        conn.execute(
            "UPDATE breakers SET state='half_open' WHERE integration=?",
            (integration,))
        conn.close()
        return False
    return True


def record_success(integration: str) -> None:
    conn = connect()
    conn.execute(
        "UPDATE breakers SET state='closed', consecutive_fail=0, "
        "last_success_at=?, opened_at=NULL, cooldown_until=NULL "
        "WHERE integration=?", (now_iso(), integration))
    conn.close()


def record_failure(integration: str, err: str,
                   cycle_minutes: int = 30) -> bool:
    """Returns True if breaker is now open."""
    trip_after, cooldown_cycles = _settings()
    conn = connect()
    row = conn.execute(
        "SELECT consecutive_fail FROM breakers WHERE integration=?",
        (integration,)).fetchone()
    fails = (row["consecutive_fail"] if row else 0) + 1
    if fails >= trip_after:
        until = (datetime.now() +
                 timedelta(minutes=cooldown_cycles * cycle_minutes)
                 ).isoformat(timespec="seconds")
        conn.execute(
            "INSERT INTO breakers(integration, state, consecutive_fail, "
            "opened_at, cooldown_until, last_error) "
            "VALUES(?, 'open', ?, ?, ?, ?) "
            "ON CONFLICT(integration) DO UPDATE SET "
            "state='open', consecutive_fail=excluded.consecutive_fail, "
            "opened_at=excluded.opened_at, "
            "cooldown_until=excluded.cooldown_until, "
            "last_error=excluded.last_error",
            (integration, fails, now_iso(), until, err[:500]))
        conn.close()
        return True
    conn.execute(
        "INSERT INTO breakers(integration, consecutive_fail, last_error) "
        "VALUES(?, ?, ?) "
        "ON CONFLICT(integration) DO UPDATE SET "
        "consecutive_fail=excluded.consecutive_fail, "
        "last_error=excluded.last_error",
        (integration, fails, err[:500]))
    conn.close()
    return False

"""Shared test helpers — db isolation.

Tests must NOT share state.db with production runs or with each other.
A leftover broadcast row from a prior test poisons the dedup gate for
the next run, and a leftover 'sent' broadcast on a brain-target input
falsely fails the held-draft assertion.

use_temp_db() points the CLUBRUNNER_DB env var at a fresh tmp file for
the lifetime of this test process. Modules that already imported db.py
still go through _db_path(), which resolves the env var at call time.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path


_INSTALLED = False


def use_temp_db() -> Path:
    global _INSTALLED
    if _INSTALLED:
        return Path(os.environ["CLUBRUNNER_DB"])
    tmp = Path(tempfile.mkdtemp(prefix="clubrunner_test_")) / "state.db"
    os.environ["CLUBRUNNER_DB"] = str(tmp)
    _INSTALLED = True
    return tmp


def reset_volatile() -> None:
    """Wipe transactional tables between tests inside one process.

    Keeps the seed (teams, grounds, people, players, fixtures, sponsors,
    breakers) so we don't pay the seed cost on every test, but clears
    everything that accumulates across tests and would cross-contaminate.
    Mutable seed columns (sponsor obligations, fixture status, breaker
    state) get re-stamped to seed values so tests that exercise rotation
    or status-flip start from a known baseline.
    """
    from edge.state.db import connect
    conn = connect()
    for tbl in ("actions", "inputs", "broadcasts", "decisions",
                "policy_runs", "cycles", "weather_state",
                "incidents", "duties", "events",
                # Per-test memory rows leak across tests too: a coach_memory
                # row inserted by an earlier test triggers UNIQUE-constraint
                # failures on a sibling test that inserts for the same coach.
                # parent_memory and match_reports follow the same pattern.
                "coach_memory", "parent_memory", "match_reports"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    # Sponsors carry mutable state (posts_owed, last_post_at) that earlier
    # tests may have decremented. Re-seed those columns to the values in
    # edge.state.seed.SPONSORS so rotation tests are deterministic.
    from edge.state.seed import SPONSORS
    for sid, _name, _tier, _val, _end, posts_owed, _rep in SPONSORS:
        conn.execute(
            "UPDATE sponsors SET posts_owed=?, last_post_at=NULL WHERE id=?",
            (posts_owed, sid))
    # Fixtures: reset any cancelled/paused/completed back to scheduled so
    # heat/lightning playbooks have something to act on between tests AND
    # so that match_report's "stamp score onto fixture" step doesn't see
    # a sibling test's already-completed fixture for the same team. Also
    # clear the scores that an upstream test stamped, so an unordered
    # SELECT in a later test doesn't return stale numbers.
    conn.execute(
        "UPDATE fixtures SET status='scheduled', status_reason=NULL, "
        "home_score=NULL, away_score=NULL "
        "WHERE status IN ('cancelled','paused','completed')")
    conn.close()

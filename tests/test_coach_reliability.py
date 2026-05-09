"""Coach reliability — every fixture creates a report obligation; the
sweep counts unsubmitted ones and surfaces persistently low coaches.

Without this sweep, reports_due never moved and reliability sat at 1.0
for everyone — the dashboard's coach-reliability column was theatre.

Tests:
 1. A completed-and-past-deadline fixture registers exactly one
    reports_due bump per coach. Re-running the sweep is a no-op.
 2. A fixture inside the grace window (kickoff < 72h ago) does not
    register — coaches aren't penalised mid-game.
 3. Cancelled/forfeit fixtures don't register — there was no game to
    report on.
 4. A submission via the match_report playbook + the sweep produces
    reliability = submitted / due (clamped to 1.0).
 5. A coach with reliability < threshold AND ≥ alert_min_due
    obligations opens a coach_reliability decision exactly once
    (cooldown suppresses the second).
 6. End-to-end: orchestrator runs the sweep without an explicit input.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from _helpers import use_temp_db, reset_volatile
use_temp_db()

from edge.state.db import init_db, connect, now_iso, upsert_input
from edge.state.seed import seed
from tools import coach_reliability, playbooks, match_report


def _passed(name): print(f"  \N{check mark} {name}")


def _start_cycle() -> int:
    c = connect()
    cur = c.execute("INSERT INTO cycles(started_at) VALUES(?)", (now_iso(),))
    cid = cur.lastrowid
    c.close()
    return cid


def _make_past_fixture(fid: str, team_id: str, hours_ago: int,
                       status: str = "completed",
                       opponent: str = "Old Opponent",
                       round_n: int = 5) -> None:
    """Insert a synthetic past fixture. The seed only plants this-Saturday
    fixtures; the reliability sweep needs historical kickoffs."""
    c = connect()
    kick = (datetime.now() - timedelta(hours=hours_ago)).isoformat(
        timespec="seconds")
    c.execute(
        "INSERT INTO fixtures(id, round, team_id, opponent, ground_id, "
        "kickoff, home_or_away, status, last_status_change) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        (fid, round_n, team_id, opponent, "henley_oval", kick, "home",
         status, now_iso()))
    c.close()


def _coach_memory(coach_id: str) -> dict:
    c = connect()
    row = c.execute(
        "SELECT reports_due, reports_submitted, reliability "
        "FROM coach_memory WHERE coach_id=?", (coach_id,)).fetchone()
    c.close()
    return dict(row) if row else {}


def setup():
    init_db()
    seed()
    reset_volatile()
    # Reset coach_memory between tests — seed leaves it at zeros, but
    # earlier tests in this file may have bumped reports_due/submitted.
    c = connect()
    c.execute("UPDATE coach_memory SET reports_due=0, "
              "reports_submitted=0, reliability=1.0")
    c.execute("DELETE FROM match_reports")
    c.execute("DELETE FROM fixtures WHERE id LIKE 'fx_test_%'")
    c.close()


# ---------------------------------------------------------------------
# 1. Past completed fixture registers exactly one bump; re-runs no-op.
# ---------------------------------------------------------------------
def test_idempotent_due_registration():
    setup()
    # u13_girls -> coach_helen
    _make_past_fixture("fx_test_helen_1", "u13_girls", hours_ago=200)
    cid = _start_cycle()
    out1 = coach_reliability.run(cid)
    assert out1["newly_registered"] == 1, out1
    assert _coach_memory("coach_helen")["reports_due"] == 1

    # Second cycle — same fixture, no new bump.
    cid2 = _start_cycle()
    out2 = coach_reliability.run(cid2)
    assert out2["newly_registered"] == 0, out2
    assert _coach_memory("coach_helen")["reports_due"] == 1
    _passed("idempotent: one fixture → one due bump, re-runs are no-ops")


# ---------------------------------------------------------------------
# 2. Inside grace window (e.g. 12h post-kickoff) does not register.
# ---------------------------------------------------------------------
def test_grace_window():
    setup()
    _make_past_fixture("fx_test_grace", "u13_girls", hours_ago=12)
    out = coach_reliability.run(_start_cycle())
    assert out["newly_registered"] == 0, out
    assert _coach_memory("coach_helen").get("reports_due", 0) == 0
    _passed("grace window: kickoff <72h ago does not bump reports_due")


# ---------------------------------------------------------------------
# 3. Cancelled fixtures don't register.
# ---------------------------------------------------------------------
def test_cancelled_excluded():
    setup()
    _make_past_fixture("fx_test_cancelled", "u13_girls", hours_ago=200,
                       status="cancelled")
    _make_past_fixture("fx_test_forfeit", "u13_girls", hours_ago=200,
                       status="forfeit")
    out = coach_reliability.run(_start_cycle())
    assert out["newly_registered"] == 0, out
    _passed("cancelled/forfeit fixtures excluded from due count")


# ---------------------------------------------------------------------
# 4. Submission + sweep → reliability = submitted/due.
# ---------------------------------------------------------------------
def test_reliability_math():
    setup()
    # Plant 4 past-deadline fixtures for u13_girls (coach_helen).
    for i, h in enumerate([200, 224, 248, 272]):
        _make_past_fixture(f"fx_test_helen_n{i}", "u13_girls", hours_ago=h,
                           round_n=10 - i)

    # Helen submits two match reports — bump reports_submitted twice.
    cid = _start_cycle()
    iid1, _ = upsert_input("imap", "mr1_test",
                           sender="helen.w@example.com",
                           subject="U13 Girls — Round 9 match report",
                           body="We won 30-22.")
    c = connect()
    c.execute("UPDATE inputs SET status='routed_playbook', "
              "classification='match_report_inbound' WHERE id=?", (iid1,))
    c.close()
    match_report.run(cid, related_input=iid1)
    iid2, _ = upsert_input("imap", "mr2_test",
                           sender="helen.w@example.com",
                           subject="U13 Girls — Round 8 match report",
                           body="We won 25-12.")
    c = connect()
    c.execute("UPDATE inputs SET status='routed_playbook', "
              "classification='match_report_inbound' WHERE id=?", (iid2,))
    c.close()
    match_report.run(cid, related_input=iid2)

    # Sweep registers all 4 obligations.
    out = coach_reliability.run(cid)
    assert out["newly_registered"] == 4, out
    cm = _coach_memory("coach_helen")
    assert cm["reports_due"] == 4
    assert cm["reports_submitted"] == 2
    # 2/4 = 0.5
    assert abs(cm["reliability"] - 0.5) < 1e-6, cm
    _passed("reliability = 2 submitted / 4 due → 0.5")


# ---------------------------------------------------------------------
# 5. Below-threshold coach gets one decision, then cooldown suppresses
#    the next sweep.
# ---------------------------------------------------------------------
def test_alert_decision_and_cooldown():
    setup()
    # Plant 5 past-deadline fixtures for u13_girls — Helen submits zero.
    for i in range(5):
        _make_past_fixture(f"fx_test_helen_a{i}", "u13_girls",
                           hours_ago=200 + i * 24, round_n=12 - i)

    cid = _start_cycle()
    out1 = coach_reliability.run(cid)
    assert out1["newly_registered"] == 5, out1
    assert len(out1["alerts"]) == 1, out1
    assert out1["alerts"][0]["coach"] == "coach_helen"

    c = connect()
    n_decisions = c.execute(
        "SELECT COUNT(*) AS n FROM decisions "
        "WHERE kind='coach_reliability' AND target_id='coach_helen'"
    ).fetchone()["n"]
    c.close()
    assert n_decisions == 1, f"expected 1 decision, got {n_decisions}"

    # Second sweep — cooldown should suppress.
    cid2 = _start_cycle()
    out2 = coach_reliability.run(cid2)
    assert len(out2["alerts"]) == 0, out2
    c = connect()
    n_decisions = c.execute(
        "SELECT COUNT(*) AS n FROM decisions "
        "WHERE kind='coach_reliability' AND target_id='coach_helen'"
    ).fetchone()["n"]
    c.close()
    assert n_decisions == 1, f"cooldown failed — got {n_decisions} decisions"
    _passed("low-reliability alert fires once, cooldown suppresses re-fire")


# ---------------------------------------------------------------------
# 6. Below alert_min_due → no alert even if reliability is 0.
# ---------------------------------------------------------------------
def test_alert_min_due_floor():
    setup()
    # Only 2 obligations, both unsubmitted — reliability=0 but min_due=4.
    _make_past_fixture("fx_test_min1", "u13_girls", hours_ago=200)
    _make_past_fixture("fx_test_min2", "u13_girls", hours_ago=224)
    out = coach_reliability.run(_start_cycle())
    assert out["newly_registered"] == 2
    assert len(out["alerts"]) == 0, out
    _passed("min_due floor protects coaches with too few chances yet")


# ---------------------------------------------------------------------
# 7. Orchestrator dispatch — sweep runs every cycle without an input.
# ---------------------------------------------------------------------
def test_orchestrator_runs_sweep():
    setup()
    _make_past_fixture("fx_test_orch", "u13_girls", hours_ago=200)
    # Use the playbook registry the orchestrator dispatches through.
    out = playbooks.fire("coach_reliability", _start_cycle(), None)
    assert out.get("ok") is True
    assert out.get("newly_registered") == 1
    _passed("playbooks.fire('coach_reliability', cid, None) runs the sweep")


def main():
    tests = [
        test_idempotent_due_registration,
        test_grace_window,
        test_cancelled_excluded,
        test_reliability_math,
        test_alert_decision_and_cooldown,
        test_alert_min_due_floor,
        test_orchestrator_runs_sweep,
    ]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failed += 1
            print(f"  ✗ {t.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"  ✗ {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

"""Match-day briefing — Diane's 6am personal scan.

Locks down the autonomy claim from the WITH-clubrunner scenario:
> "Saturday 6am — Briefing on her phone."

The brief must:
  - fire only on configured match days, not other weekdays
  - be auto-sendable (reach=1 → Diane), no approval gate
  - dedup so the second cycle inside the same Saturday morning doesn't
    re-send
  - actually pull live state (today's fixtures, live weather flags,
    unconfirmed duty slots, last-round outstanding match reports,
    pending decisions) — not just template filler
  - escape gracefully on a quiet Saturday (no fixtures, no weather, no
    open decisions) without spamming junk
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

from edge.state.db import connect, init_db, now_iso
from edge.state.seed import seed
from edge.collectors import schedule as schedule_col
from triage import classifier, router
from tools import match_day_brief
from tools import playbooks
import config as cfg_mod


def _new_cycle() -> int:
    init_db()
    seed()
    reset_volatile()
    conn = connect()
    cur = conn.execute(
        "INSERT INTO cycles(started_at) VALUES(datetime('now'))")
    cid = cur.lastrowid
    conn.close()
    return cid


def _force_today_fixtures(n: int = 2) -> list[str]:
    """Make sure the seeded round-12 fixtures land in today's window
    so the brief has something to render. Returns the fixture ids that
    were re-stamped."""
    today = datetime.now()
    ko1 = today.replace(hour=9, minute=0, second=0, microsecond=0)
    ko2 = today.replace(hour=10, minute=30, second=0, microsecond=0)
    conn = connect()
    rows = conn.execute(
        "SELECT id FROM fixtures ORDER BY kickoff LIMIT ?", (n,)
    ).fetchall()
    ids = [r["id"] for r in rows]
    if len(ids) >= 1:
        conn.execute("UPDATE fixtures SET kickoff=?, status='scheduled' "
                     "WHERE id=?", (ko1.isoformat(), ids[0]))
    if len(ids) >= 2:
        conn.execute("UPDATE fixtures SET kickoff=?, status='scheduled' "
                     "WHERE id=?", (ko2.isoformat(), ids[1]))
    conn.close()
    return ids


def _seed_weather(ground_id: str, max_c: float, lkm: float | None) -> None:
    conn = connect()
    conn.execute(
        "INSERT OR REPLACE INTO weather_state(ground_id, observed_at, "
        "forecast_max_c, forecast_for, lightning_km) VALUES(?,?,?,?,?)",
        (ground_id, now_iso(), max_c,
         datetime.now().isoformat(timespec="minutes"), lkm))
    conn.close()


def _seed_duty(fixture_id: str, role: str, person_id: str,
               confirmed: int = 0) -> None:
    conn = connect()
    duty_id = f"duty_{fixture_id}_{role}"
    conn.execute(
        "INSERT OR REPLACE INTO duties(id, fixture_id, role, person_id, "
        "confirmed) VALUES(?,?,?,?,?)",
        (duty_id, fixture_id, role, person_id, confirmed))
    conn.close()


def test_assemble_body_renders_known_facts():
    """The brief must reference today's fixtures, the heat flag for a
    37°C ground, an unconfirmed duty assignment, and a known parent."""
    _new_cycle()
    fids = _force_today_fixtures(n=2)
    assert fids, "seed didn't produce fixtures"

    # Inspect the first fixture's ground so we know what to flag.
    conn = connect()
    fx = conn.execute(
        "SELECT ground_id FROM fixtures WHERE id=?", (fids[0],)).fetchone()
    parent = conn.execute(
        "SELECT id, name FROM people WHERE role='parent' LIMIT 1"
    ).fetchone()
    conn.close()
    gid = fx["ground_id"]
    assert gid, "fixture missing ground"

    _seed_weather(gid, max_c=38.0, lkm=None)        # heat-cancel territory
    _seed_duty(fids[0], role="canteen",
               person_id=parent["id"], confirmed=0)

    subject, body = match_day_brief.assemble_body()

    today_iso = datetime.now().date().isoformat()
    assert today_iso in subject, f"date missing from subject: {subject}"
    assert "Match-day briefing" in subject

    assert "Today: 2 fixtures" in body or "Today: " in body, body[:300]
    assert "heat-cancel" in body, "heat flag missing"
    assert parent["name"] in body, "duty assignee not surfaced"
    assert "[pending]" in body, "duty status not surfaced"
    print(f"  ✓ brief renders fixtures + heat flag + duty assignee")


def test_clear_morning_does_not_spam():
    """A Saturday with no fixtures, no weather alerts, no unconfirmed
    duties, no pending decisions: the brief still sends (Diane wants the
    confirmation that things are quiet) but the body must say so
    directly, not invent items."""
    _new_cycle()
    # Cancel everything in the seed set today
    conn = connect()
    conn.execute("UPDATE fixtures SET kickoff='2099-01-01T09:00:00' "
                 "WHERE 1=1")
    conn.close()

    subject, body = match_day_brief.assemble_body()
    assert "0 fixtures live" in subject or "fixtures live" in subject
    assert "No fixtures scheduled today." in body, body[:400]
    assert ("All duty slots confirmed" in body
            or "No fixtures scheduled today." in body)
    assert "Clean. Nothing escalated overnight." in body, (
        "quiet-state line missing")
    print(f"  ✓ quiet morning → confirms quietly, no fabricated items")


def test_run_autosends_with_reach_1():
    """Reach=1 (Diane), auto-send, no decision opens. Result row says
    sent."""
    cid = _new_cycle()
    _force_today_fixtures(n=1)

    out = match_day_brief.run(cycle_id=cid, related_input=None)
    assert out["reach"] == 1
    assert out["result"]["status"] == "sent", (
        f"expected sent, got {out['result']['status']} "
        f"blocked_by={out['result'].get('blocked_by')}")

    # No decision should have opened — under threshold + no draft hold.
    conn = connect()
    n_decisions = conn.execute(
        "SELECT COUNT(*) AS n FROM decisions "
        "WHERE target_kind='match_day_brief' AND cycle_id=?",
        (cid,)).fetchone()["n"]
    conn.close()
    assert n_decisions == 0, (
        f"unexpected decisions opened for match_day_brief: {n_decisions}")
    print(f"  ✓ run(reach=1) → autosent, no decision queue noise")


def test_dedup_blocks_second_run_same_day():
    """A second cycle on the same Saturday morning (e.g. 06:30 after
    06:00) must not re-send. Dedup window 1200m covers the day."""
    cid = _new_cycle()
    _force_today_fixtures(n=1)
    first = match_day_brief.run(cid, None)
    assert first["result"]["status"] == "sent"
    # Same day, second invocation — must be a real cycle row to satisfy
    # the FK on broadcasts.cycle_id.
    conn = connect()
    cur = conn.execute(
        "INSERT INTO cycles(started_at) VALUES(datetime('now'))")
    cid2 = cur.lastrowid
    conn.close()
    second = match_day_brief.run(cid2, None)
    assert second["result"]["status"] == "skipped", (
        f"expected dedup-skipped, got {second['result']['status']} "
        f"blocked_by={second['result'].get('blocked_by')}")
    print("  ✓ second run same Saturday → dedup-skipped")


def test_schedule_only_mints_on_match_days():
    """The schedule collector must NOT mint a match_day_brief tick on a
    non-match-day. Force config match_days=[<today's weekday + 1>] so
    today is NOT a match day."""
    cid = _new_cycle()
    cfg = cfg_mod.get()
    today_weekday = datetime.now().weekday()
    not_today = (today_weekday + 1) % 7
    # Stash original then override
    original_match_days = cfg["cycle"]["match_days"]
    cfg["cycle"]["match_days"] = [not_today]
    try:
        schedule_col.run(cid)
        conn = connect()
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM inputs "
            "WHERE source='schedule' AND id LIKE 'schedule:match_day_brief_%'"
        ).fetchone()
        conn.close()
        assert row["n"] == 0, (
            f"non-match-day still minted match_day_brief tick: {row['n']}")
        print("  ✓ schedule respects match_days config")
    finally:
        cfg["cycle"]["match_days"] = original_match_days


def test_schedule_mints_on_match_day_after_ready_time():
    """Force today to count as a match day AND the ready time to be in
    the past, then confirm one tick is minted exactly once."""
    cid = _new_cycle()
    cfg = cfg_mod.get()
    today_weekday = datetime.now().weekday()
    original_match_days = cfg["cycle"]["match_days"]
    original_send_at = cfg.get("match_day_brief", {}).get("send_at")
    cfg["cycle"]["match_days"] = [today_weekday]
    cfg.setdefault("match_day_brief", {})["enabled"] = True
    # 00:01 — we're certainly past it
    cfg["match_day_brief"]["send_at"] = "00:01"
    try:
        schedule_col.run(cid)
        schedule_col.run(cid)   # second tick same calendar day
        conn = connect()
        rows = conn.execute(
            "SELECT id FROM inputs "
            "WHERE source='schedule' AND id LIKE 'schedule:match_day_brief_%'"
        ).fetchall()
        conn.close()
        assert len(rows) == 1, (
            f"expected exactly 1 match_day_brief tick, got "
            f"{[r['id'] for r in rows]}")
        print("  ✓ schedule mints exactly once per match-day morning")
    finally:
        cfg["cycle"]["match_days"] = original_match_days
        if original_send_at is not None:
            cfg["match_day_brief"]["send_at"] = original_send_at


def test_orchestrator_dispatch_through_playbooks_fire():
    """Mint a match_day_brief tick, run classifier→router→playbook
    fire, and assert the broadcast comes out the other end as 'sent'.
    This proves the wiring (rules.yaml → router → orchestrator
    cls_to_pb → playbooks.fire) is intact end-to-end."""
    cid = _new_cycle()
    _force_today_fixtures(n=1)

    cfg = cfg_mod.get()
    today_weekday = datetime.now().weekday()
    original_match_days = cfg["cycle"]["match_days"]
    cfg["cycle"]["match_days"] = [today_weekday]
    cfg.setdefault("match_day_brief", {})["enabled"] = True
    cfg["match_day_brief"]["send_at"] = "00:01"
    try:
        schedule_col.run(cid)
        classifier.run(cid)
        rt = router.run(cid)
        # Confirm the tick went into the playbook bucket, not brain.
        playbook_ids = rt["playbook"]
        assert any(iid.startswith("schedule:match_day_brief_")
                   for iid in playbook_ids), (
            f"match_day_brief tick not routed to playbook: {playbook_ids}")
        # Fire it directly through the registry (the orchestrator does
        # this via cls_to_pb dispatch).
        for iid in playbook_ids:
            if iid.startswith("schedule:match_day_brief_"):
                out = playbooks.fire("match_day_brief", cid, iid)
                assert out["result"]["status"] == "sent", (
                    f"playbook fire didn't send: {out}")
        conn = connect()
        sent = conn.execute(
            "SELECT id FROM broadcasts WHERE playbook='match_day_brief' "
            "AND status='sent'").fetchall()
        conn.close()
        assert len(sent) == 1, f"expected 1 sent broadcast, got {len(sent)}"
        print("  ✓ tick → triage → playbook → sent broadcast")
    finally:
        cfg["cycle"]["match_days"] = original_match_days


def main():
    tests = [
        ("brief renders known facts",
         test_assemble_body_renders_known_facts),
        ("clear morning does not spam",
         test_clear_morning_does_not_spam),
        ("run autosends reach=1",
         test_run_autosends_with_reach_1),
        ("dedup blocks second run same day",
         test_dedup_blocks_second_run_same_day),
        ("schedule respects match_days",
         test_schedule_only_mints_on_match_days),
        ("schedule mints once on match-day morning",
         test_schedule_mints_on_match_day_after_ready_time),
        ("orchestrator dispatch end-to-end",
         test_orchestrator_dispatch_through_playbooks_fire),
    ]
    passed = failed = 0
    for label, fn in tests:
        try:
            fn()
            passed += 1
        except AssertionError as ex:
            print(f"  ✗ {label}: {ex}")
            failed += 1
        except Exception as ex:
            print(f"  ✗ {label}: {type(ex).__name__}: {ex}")
            import traceback
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

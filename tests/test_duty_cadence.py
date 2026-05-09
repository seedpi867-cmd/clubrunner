"""duty_reminder cadence + T-2h escalation.

Tests the autonomy story:

 1. A schedule tick for fixture A's 3d window only reminds duties on
    fixture A — never reaches into fixture B's roster (the bug
    'input-driven playbooks scope to input' guards against).
 2. The 3 cadence labels (3d/1d/2h) each fire their own broadcast
    because the dedup segment key carries the window — a 1d reminder
    sent two days after a 3d reminder is independent.
 3. At the T-2h window, an unconfirmed duty opens a decision card on
    Diane's queue with backfill candidate options, in addition to the
    reminder. Confirmed duties at 2h do NOT escalate.
 4. A confirmed duty at any window: no broadcast, no decision.
 5. A garbled input id (no parseable fixture+window) is a no-op — no
    broadcasts, no escalations, no DB scan of every duty.
 6. End-to-end: schedule mints the tick, classifier marks duty_tick,
    orchestrator dispatches duty_reminder, the right scope happens.
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
from tools import playbooks
from triage import classifier, router
from edge.collectors import schedule


def _start_cycle() -> int:
    c = connect()
    cur = c.execute("INSERT INTO cycles(started_at) VALUES(?)",
                    (now_iso(),))
    cid = cur.lastrowid
    c.close()
    return cid


def _passed(name): print(f"  \N{check mark} {name}")


def _seed_duty(fixture_id: str, role: str, person_id: str,
               confirmed: int = 0) -> str:
    """Insert one duty row. Returns its id."""
    did = f"d_{fixture_id}_{role}"
    c = connect()
    c.execute(
        "INSERT INTO duties(id, fixture_id, role, person_id, confirmed) "
        "VALUES(?,?,?,?,?)",
        (did, fixture_id, role, person_id, confirmed))
    c.close()
    return did


def _set_kickoff(fixture_id: str, dt: datetime) -> None:
    c = connect()
    c.execute("UPDATE fixtures SET kickoff=?, status='scheduled' WHERE id=?",
              (dt.isoformat(timespec="seconds"), fixture_id))
    c.close()


# ---------------------------------------------------------------------


def test_3d_tick_scopes_to_its_fixture():
    """A 3d tick for fixture A must NOT remind duties on fixture B."""
    init_db(); seed(); reset_volatile()

    # Fixture A 3 days out, fixture B 1 day out — both with one
    # unconfirmed canteen duty assigned to a parent.
    now = datetime.now()
    _set_kickoff("fx_r12_u13_g", now + timedelta(days=3, hours=0))
    _set_kickoff("fx_r12_u14",   now + timedelta(days=1, hours=0))
    duty_a = _seed_duty("fx_r12_u13_g", "canteen", "p_amelia")
    duty_b = _seed_duty("fx_r12_u14",   "canteen", "p_marcus")

    cid = _start_cycle()
    iid, _ = upsert_input(
        "schedule", "duty_reminder_fx_r12_u13_g_3d",
        sender="schedule.duty",
        subject="Duty roster reminder (3d) — fx_r12_u13_g",
        body="fire duty for fixture fx_r12_u13_g")
    out = playbooks.fire("duty_reminder", cid, iid)

    c = connect()
    bcs = c.execute(
        "SELECT segment, status FROM broadcasts "
        "WHERE playbook='duty_reminder' ORDER BY segment").fetchall()
    c.close()
    segs = [b["segment"] for b in bcs]
    assert len(segs) == 1, f"expected 1 broadcast, got {len(segs)}: {segs}"
    assert duty_a in segs[0], f"broadcast not scoped to A: {segs[0]}"
    assert duty_b not in segs[0], (
        f"broadcast spilled into B's duty: {segs[0]}")
    assert out["fixture"] == "fx_r12_u13_g"
    assert out["window"] == "3d"
    _passed("3d tick on fixture A scopes only to A's duty")


def test_cadence_segment_keys_dont_collide():
    """3d, 1d, 2h reminders for the same duty must each fire — segment
    carries the window so the dedup gate doesn't silently drop them."""
    init_db(); seed(); reset_volatile()

    now = datetime.now()
    _set_kickoff("fx_r12_u11", now + timedelta(hours=2))
    duty = _seed_duty("fx_r12_u11", "bbq", "p_amelia")

    cid = _start_cycle()
    for win in ("3d", "1d", "2h"):
        iid, _ = upsert_input(
            "schedule", f"duty_reminder_fx_r12_u11_{win}",
            sender="schedule.duty",
            subject=f"Duty roster reminder ({win}) — fx_r12_u11",
            body=f"fire {win} duty")
        playbooks.fire("duty_reminder", cid, iid)

    c = connect()
    bcs = c.execute(
        "SELECT segment, status FROM broadcasts "
        "WHERE playbook='duty_reminder' ORDER BY segment").fetchall()
    c.close()
    segs = [b["segment"] for b in bcs]
    assert len(segs) == 3, f"expected 3 broadcasts, got {len(segs)}: {segs}"
    assert any(s.endswith(":3d") for s in segs), f"missing :3d in {segs}"
    assert any(s.endswith(":1d") for s in segs), f"missing :1d in {segs}"
    assert any(s.endswith(":2h") for s in segs), f"missing :2h in {segs}"
    for b in bcs:
        assert b["status"] == "sent", f"reach=1 should auto-send: {b}"
    _passed(f"3 cadence variants → 3 distinct broadcasts: {segs}")


def test_2h_unconfirmed_opens_escalation_with_backfill():
    """T-2h on a still-unconfirmed duty: reminder + decision card with
    backfill candidates."""
    init_db(); seed(); reset_volatile()

    now = datetime.now()
    _set_kickoff("fx_r12_u15_g", now + timedelta(hours=2))
    duty = _seed_duty("fx_r12_u15_g", "gate", "p_holly", confirmed=0)

    cid = _start_cycle()
    iid, _ = upsert_input(
        "schedule", "duty_reminder_fx_r12_u15_g_2h",
        sender="schedule.duty",
        subject="Duty roster reminder (2h) — fx_r12_u15_g",
        body="fire 2h duty")
    out = playbooks.fire("duty_reminder", cid, iid)

    c = connect()
    decs = c.execute(
        "SELECT id, kind, target_kind, target_id, default_option, "
        "options_json, summary, context "
        "FROM decisions WHERE kind='duty_escalation'").fetchall()
    bcs = c.execute(
        "SELECT id, status FROM broadcasts "
        "WHERE playbook='duty_reminder'").fetchall()
    c.close()
    assert len(decs) == 1, (
        f"expected 1 escalation, got {len(decs)} ({[dict(d) for d in decs]})")
    d = decs[0]
    assert d["target_kind"] == "duty"
    assert d["target_id"] == duty
    assert "Holly Underhill" in d["context"], (
        f"rostered name missing from context: {d['context'][:200]}")
    import json as _json
    opts = _json.loads(d["options_json"])
    keys = [o["key"] for o in opts]
    backfill_keys = [k for k in keys if k.startswith("backfill:")]
    assert len(backfill_keys) >= 1, (
        f"expected backfill options, got {keys}")
    assert "phone_rostered" in keys, f"missing phone_rostered in {keys}"
    assert "close_duty" in keys, f"missing close_duty in {keys}"
    # The rostered person must NOT appear as their own backfill option.
    assert "backfill:p_holly" not in keys, (
        f"rostered person became their own backfill: {keys}")
    assert len(bcs) == 1 and bcs[0]["status"] == "sent", (
        f"reminder should still go out alongside escalation: "
        f"{[dict(b) for b in bcs]}")
    _passed(f"2h unconfirmed → reminder + escalation {d['id']}, "
            f"{len(backfill_keys)} backfill options")


def test_2h_confirmed_no_escalation():
    """Confirmed duties at 2h: no reminder, no escalation. Diane should
    not see a card for a duty already locked in."""
    init_db(); seed(); reset_volatile()

    now = datetime.now()
    _set_kickoff("fx_r12_u16", now + timedelta(hours=2))
    _seed_duty("fx_r12_u16", "scoreboard", "p_kev", confirmed=1)

    cid = _start_cycle()
    iid, _ = upsert_input(
        "schedule", "duty_reminder_fx_r12_u16_2h",
        sender="schedule.duty",
        subject="Duty roster reminder (2h) — fx_r12_u16",
        body="fire 2h duty")
    out = playbooks.fire("duty_reminder", cid, iid)

    c = connect()
    decs = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE kind='duty_escalation'"
    ).fetchone()[0]
    bcs = c.execute(
        "SELECT COUNT(*) FROM broadcasts "
        "WHERE playbook='duty_reminder'").fetchone()[0]
    c.close()
    assert decs == 0, f"confirmed duty should not escalate, got {decs}"
    assert bcs == 0, f"confirmed duty should not remind, got {bcs}"
    _passed("confirmed 2h duty → no reminder, no escalation")


def test_garbled_input_is_noop():
    """Without a parseable fixture+window, the playbook must NOT fall
    back to iterating every unconfirmed duty in the DB. That was the
    pre-fix bug."""
    init_db(); seed(); reset_volatile()

    # Pre-load a duty that the buggy version would have spammed.
    now = datetime.now()
    _set_kickoff("fx_r12_u13_b", now + timedelta(hours=4))
    _seed_duty("fx_r12_u13_b", "canteen", "p_gita")

    cid = _start_cycle()
    out = playbooks.fire("duty_reminder", cid,
                         "schedule:not_a_duty_tick")

    c = connect()
    bcs = c.execute(
        "SELECT COUNT(*) FROM broadcasts "
        "WHERE playbook='duty_reminder'").fetchone()[0]
    decs = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE kind='duty_escalation'"
    ).fetchone()[0]
    c.close()
    assert bcs == 0, f"garbled input should not broadcast, got {bcs}"
    assert decs == 0, f"garbled input should not escalate, got {decs}"
    assert out["fixture"] is None
    _passed("garbled input → 0 broadcasts, 0 escalations (no DB scan)")


def test_escalation_resolution_updates_duty():
    """close_duty marks the duty confirmed (so next cycle no-ops);
    backfill:<pid> reassigns the rostered person (so next cycle's
    reminder goes to the new person)."""
    init_db(); seed(); reset_volatile()
    from tools.broadcast import approve_decision

    now = datetime.now()
    _set_kickoff("fx_r12_u11", now + timedelta(hours=2))
    _set_kickoff("fx_r12_u12", now + timedelta(hours=2))
    duty_close = _seed_duty("fx_r12_u11", "canteen", "p_holly")
    duty_back = _seed_duty("fx_r12_u12", "gate", "p_kev")

    cid = _start_cycle()
    for fid in ("fx_r12_u11", "fx_r12_u12"):
        iid, _ = upsert_input(
            "schedule", f"duty_reminder_{fid}_2h",
            sender="schedule.duty",
            subject=f"Duty roster reminder (2h) — {fid}",
            body=f"fire {fid}")
        playbooks.fire("duty_reminder", cid, iid)

    c = connect()
    decs = {d["target_id"]: d for d in c.execute(
        "SELECT id, target_id, options_json FROM decisions "
        "WHERE kind='duty_escalation'").fetchall()}
    c.close()
    assert duty_close in decs and duty_back in decs

    # close_duty path
    r = approve_decision(decs[duty_close]["id"], "close_duty")
    assert r["ok"]
    c = connect()
    confirmed = c.execute(
        "SELECT confirmed FROM duties WHERE id=?",
        (duty_close,)).fetchone()["confirmed"]
    c.close()
    assert confirmed == 1, f"close_duty didn't confirm: {confirmed}"

    # backfill path — pick a real candidate id from the options
    import json as _json
    opts = _json.loads(decs[duty_back]["options_json"])
    backfill_keys = [o["key"] for o in opts
                     if o["key"].startswith("backfill:")]
    assert backfill_keys, "no backfill option to test"
    pick = backfill_keys[0]
    new_pid = pick.split(":", 1)[1]
    r = approve_decision(decs[duty_back]["id"], pick)
    assert r["ok"]
    c = connect()
    row = c.execute(
        "SELECT person_id, confirmed, last_reminded_at FROM duties "
        "WHERE id=?", (duty_back,)).fetchone()
    c.close()
    assert row["person_id"] == new_pid, (
        f"backfill didn't reassign: {row['person_id']} != {new_pid}")
    assert row["confirmed"] == 0
    assert row["last_reminded_at"] is None
    _passed(f"close_duty → confirmed=1; backfill:{new_pid} → reassigned")


def test_orchestrator_dispatch_end_to_end():
    """schedule mint -> classifier -> router -> orchestrator dispatch.
    Mints a 1d tick for a fixture 1 day out and confirms the broadcast
    fires through the full pipeline."""
    init_db(); seed(); reset_volatile()

    now = datetime.now()
    # Set fixture exactly 1 day out so the schedule collector mints a 1d
    # tick, but no others.
    _set_kickoff("fx_r12_u17_g", now + timedelta(days=1))
    _seed_duty("fx_r12_u17_g", "first_aid", "p_dimitri")

    # Block the schedule collector from minting a fresh duty tick on an
    # unrelated future fixture by setting the rest to past status.
    c = connect()
    c.execute(
        "UPDATE fixtures SET status='completed' "
        "WHERE id != 'fx_r12_u17_g'")
    c.close()

    cid = _start_cycle()
    schedule.run(cid)
    classifier.run(cid)
    routed = router.run(cid)
    duty_inputs = [i for i in routed["playbook"]
                   if "duty_reminder_" in i]
    assert duty_inputs, f"router didn't surface duty tick: {routed}"

    # Dispatch the matched playbook the way orchestrator does.
    for iid in duty_inputs:
        playbooks.fire("duty_reminder", cid, iid)

    c = connect()
    bcs = c.execute(
        "SELECT segment, status FROM broadcasts "
        "WHERE playbook='duty_reminder'").fetchall()
    c.close()
    assert any(s["segment"].endswith(":1d") for s in bcs), (
        f"expected 1d segment broadcast, got {[dict(b) for b in bcs]}")
    _passed(f"end-to-end: schedule->triage->dispatch fired "
            f"{len(bcs)} duty broadcast(s)")


# ---------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_3d_tick_scopes_to_its_fixture,
        test_cadence_segment_keys_dont_collide,
        test_2h_unconfirmed_opens_escalation_with_backfill,
        test_2h_confirmed_no_escalation,
        test_garbled_input_is_noop,
        test_escalation_resolution_updates_duty,
        test_orchestrator_dispatch_end_to_end,
    ]
    passed = 0
    failed = 0
    for fn in tests:
        try:
            fn()
            passed += 1
        except AssertionError as e:
            print(f"  \N{ballot x} {fn.__name__}: {e}")
            failed += 1
    print()
    print(f"{passed} passed, {failed} failed")
    if failed:
        sys.exit(1)

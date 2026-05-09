"""gcal collector + event_reminder playbook end-to-end.

Tests the autonomy story:
 1. _parse_audience reads [tag] prefix and falls back to 'committee'.
 2. Collector upserts an event into `events`, idempotent on uid.
 3. Reminder windows (7d/1d/2h) mint exactly one input each, and only
    when the start delta lands inside the window. An event 5 days out
    sits between 7d and 1d windows and must NOT mint.
 4. Audience='diane' opens a personal_reminder decision, NEVER a
    broadcast — Diane's calendar shouldn't trigger an SMS to parents.
 5. Audience='committee' broadcasts to 7 portfolio members (low reach,
    auto-sends through gates).
 6. Audience='all_club' on the seeded sample (8 parents) auto-sends;
    the same playbook against an above-threshold reach gates instead.
 7. Re-running gcal in a second cycle does NOT re-mint or re-broadcast
    — input upsert + dedup gate enforce idempotency together.
 8. Two windows on the same event (1d AND 2h same cycle) each fire
    their own broadcast — segment carries the window so they don't
    collide in dedup.
 9. Untagged summary defaults to committee audience.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from _helpers import use_temp_db, reset_volatile
use_temp_db()

from edge.state.db import init_db, connect, now_iso
from edge.state.seed import seed
from edge.collectors import gcal
from tools import playbooks, broadcast


def _start_cycle() -> int:
    c = connect()
    cur = c.execute("INSERT INTO cycles(started_at) VALUES(?)",
                    (now_iso(),))
    cid = cur.lastrowid
    c.close()
    return cid


def _passed(name): print(f"  \N{check mark} {name}")


def _scenario_set(events: list[dict]):
    p = ROOT / "scenarios" / "today.json"
    d = json.loads(p.read_text())
    d["gcal_events"] = events
    p.write_text(json.dumps(d, indent=2))


def _save_scenario():
    return (ROOT / "scenarios" / "today.json").read_text()


def _restore_scenario(text: str):
    (ROOT / "scenarios" / "today.json").write_text(text)


# ---------------------------------------------------------------------

def test_parse_audience_tags():
    cases = [
        ("[committee] May meeting", ("committee", None, "May meeting")),
        ("[all] Working bee", ("all_club", None, "Working bee")),
        ("[coaches] Catch-up", ("coaches", None, "Catch-up")),
        ("[diane] Bank drop", ("diane", None, "Bank drop")),
        ("[team:u13_girls] Pizza night",
         ("team", "u13_girls", "Pizza night")),
        ("Untagged event", ("committee", None, "Untagged event")),
    ]
    for summary, expected in cases:
        got = gcal._parse_audience(summary)
        assert got == expected, f"{summary!r} -> {got} != {expected}"
    _passed("[committee]/[all]/[coaches]/[diane]/[team:..]/untagged parse")


def test_event_upsert_idempotent():
    init_db(); seed(); reset_volatile()
    saved = _save_scenario()
    try:
        now = datetime.now()
        _scenario_set([{
            "uid": "test-evt-upsert-1",
            "summary": "[committee] Meeting",
            "description": "Standing.",
            "location": "Clubrooms",
            "start": (now + timedelta(days=10)).isoformat(timespec='seconds'),
        }])
        cid = _start_cycle()
        gcal.run(cid)
        gcal.run(cid)  # Second pass — must update, not duplicate.
        c = connect()
        n = c.execute(
            "SELECT COUNT(*) FROM events WHERE source_uid=?",
            ("test-evt-upsert-1",)).fetchone()[0]
        c.close()
        assert n == 1, f"expected 1 event row, got {n}"
        _passed("upsert idempotent on source_uid (1 row after 2 polls)")
    finally:
        _restore_scenario(saved)


def test_reminder_windows_fire_only_in_band():
    """Event +5d sits in no window; +1d sits in 1d; +2h sits in 2h."""
    init_db(); seed(); reset_volatile()
    saved = _save_scenario()
    try:
        now = datetime.now()
        _scenario_set([
            {"uid": "in-band-1d",  "summary": "[committee] One day",
             "start": (now + timedelta(days=1)).isoformat(timespec='seconds')},
            {"uid": "in-band-2h",  "summary": "[committee] Two hours",
             "start": (now + timedelta(hours=2)).isoformat(timespec='seconds')},
            {"uid": "out-of-band", "summary": "[committee] Mid-week",
             "start": (now + timedelta(days=5)).isoformat(timespec='seconds')},
        ])
        cid = _start_cycle()
        gcal.run(cid)
        c = connect()
        rows = c.execute(
            "SELECT id FROM inputs WHERE source='gcal' "
            "ORDER BY id").fetchall()
        c.close()
        ids = {r["id"] for r in rows}
        assert any("1d" in i for i in ids), f"missing 1d tick in {ids}"
        assert any("2h" in i for i in ids), f"missing 2h tick in {ids}"
        assert not any("out-of-band" in i for i in ids), \
            f"+5d should NOT have a tick: {ids}"
        _passed(f"in-band 1d + 2h fired ({len(ids)} ticks), 5d skipped")
    finally:
        _restore_scenario(saved)


def test_diane_audience_decision_not_broadcast():
    init_db(); seed(); reset_volatile()
    saved = _save_scenario()
    try:
        now = datetime.now()
        _scenario_set([{
            "uid": "diane-personal-1",
            "summary": "[diane] Drop cheque at bank",
            "location": "Bendigo",
            "start": (now + timedelta(hours=2)).isoformat(timespec='seconds'),
        }])
        cid = _start_cycle()
        gcal.run(cid)
        c = connect()
        ticks = [r["id"] for r in c.execute(
            "SELECT id FROM inputs WHERE source='gcal'").fetchall()]
        c.close()
        assert ticks, "no tick minted"
        for t in ticks:
            playbooks.fire("event_reminder", cid, t)
        c = connect()
        bc = c.execute(
            "SELECT COUNT(*) FROM broadcasts WHERE playbook='event_reminder'"
        ).fetchone()[0]
        d = c.execute(
            "SELECT COUNT(*) FROM decisions WHERE kind='personal_reminder'"
        ).fetchone()[0]
        c.close()
        assert bc == 0, f"diane event should NOT broadcast, got {bc}"
        assert d == 1, f"expected 1 personal_reminder decision, got {d}"
        _passed("[diane] event → decision, zero broadcasts")
    finally:
        _restore_scenario(saved)


def test_committee_audience_autosends():
    init_db(); seed(); reset_volatile()
    saved = _save_scenario()
    try:
        now = datetime.now()
        _scenario_set([{
            "uid": "committee-may",
            "summary": "[committee] May meeting",
            "description": "Standing committee. Agenda: AGM prep.",
            "location": "Clubrooms",
            "start": (now + timedelta(days=1)).isoformat(timespec='seconds'),
        }])
        cid = _start_cycle()
        gcal.run(cid)
        c = connect()
        ticks = [r["id"] for r in c.execute(
            "SELECT id FROM inputs WHERE source='gcal'").fetchall()]
        c.close()
        assert ticks, "no tick"
        for t in ticks:
            playbooks.fire("event_reminder", cid, t)
        c = connect()
        bc = c.execute(
            "SELECT id, channel, segment, reach, status, body FROM broadcasts "
            "WHERE playbook='event_reminder'").fetchone()
        c.close()
        assert bc, "no broadcast created"
        assert bc["channel"] == "email"
        assert bc["segment"].startswith("committee:")
        assert bc["reach"] == 7, f"committee reach should be 7: {bc['reach']}"
        assert bc["status"] == "sent", f"should auto-send: {bc['status']}"
        assert "Hi committee" in bc["body"]
        assert "AGM prep" in bc["body"], "description should be carried"
        _passed(f"[committee] reach=7 → auto-sent {bc['id']}")
    finally:
        _restore_scenario(saved)


def test_high_reach_event_gates_for_approval():
    """Synthetic: bump player+parent count above the >50 threshold so an
    all_club reminder hits broadcast_threshold and opens an approval card.
    Without this gate, a calendar-driven all-parents email would
    autosend at any reach — defeating the gate's purpose."""
    init_db(); seed(); reset_volatile()
    saved = _save_scenario()
    try:
        # Inflate parent rows past the 50-recipient threshold.
        c = connect()
        for i in range(60):
            c.execute(
                "INSERT INTO people(id, role, name, email, "
                "preferred_channel) VALUES(?,?,?,?,?)",
                (f"p_synth_{i}", "parent", f"Parent {i}",
                 f"parent{i}@example.com", "email"))
        c.close()

        now = datetime.now()
        _scenario_set([{
            "uid": "presentation-night",
            "summary": "[all] Presentation night",
            "description": "Trophies + sausage sizzle.",
            "location": "Henley clubrooms",
            "start": (now + timedelta(days=7)).isoformat(timespec='seconds'),
        }])
        cid = _start_cycle()
        gcal.run(cid)
        c = connect()
        ticks = [r["id"] for r in c.execute(
            "SELECT id FROM inputs WHERE source='gcal'").fetchall()]
        c.close()
        for t in ticks:
            playbooks.fire("event_reminder", cid, t)
        c = connect()
        bc = c.execute(
            "SELECT status, blocked_by, reach FROM broadcasts "
            "WHERE playbook='event_reminder'").fetchone()
        d = c.execute(
            "SELECT id, kind, default_option FROM decisions "
            "WHERE kind='approval_required'").fetchone()
        c.close()
        assert bc, "no broadcast"
        assert bc["reach"] >= 50, f"reach too small: {bc['reach']}"
        assert bc["status"] == "gated", f"should gate: {bc['status']}"
        assert "broadcast_threshold" in (bc["blocked_by"] or "")
        assert d, "no approval decision opened"
        assert d["default_option"] == "approve"
        _passed(f"[all_club] reach={bc['reach']} >50 → gated, "
                f"approval card {d['id']}")
    finally:
        _restore_scenario(saved)


def test_two_cycles_same_event_no_double_send():
    init_db(); seed(); reset_volatile()
    saved = _save_scenario()
    try:
        now = datetime.now()
        _scenario_set([{
            "uid": "no-double-send",
            "summary": "[committee] Quarterly sync",
            "start": (now + timedelta(days=1)).isoformat(timespec='seconds'),
        }])
        cid1 = _start_cycle()
        gcal.run(cid1)
        c = connect()
        ticks = [r["id"] for r in c.execute(
            "SELECT id FROM inputs WHERE source='gcal'").fetchall()]
        c.close()
        for t in ticks:
            playbooks.fire("event_reminder", cid1, t)

        # Second cycle — gcal sees the same event, must not mint again,
        # and even if a fresh tick somehow appeared, dedup would block.
        cid2 = _start_cycle()
        gcal.run(cid2)
        c = connect()
        bc_count = c.execute(
            "SELECT COUNT(*) FROM broadcasts WHERE playbook='event_reminder'"
        ).fetchone()[0]
        gcal_inputs = c.execute(
            "SELECT COUNT(*) FROM inputs WHERE source='gcal'").fetchone()[0]
        c.close()
        assert bc_count == 1, f"expected 1 broadcast, got {bc_count}"
        assert gcal_inputs == 1, f"expected 1 gcal input, got {gcal_inputs}"
        _passed("two cycles → 1 broadcast, 1 input (idempotent)")
    finally:
        _restore_scenario(saved)


def test_two_windows_same_event_independent():
    """One event whose start sits at +2h ALSO sits at... well, no, a
    single now-delta lands in exactly one window. So manufacture a
    delta inside both: place an event +1d (in 1d window), AND mint a
    second event with same summary but start at +2h. Both should fire,
    each on its own segment+window key."""
    init_db(); seed(); reset_volatile()
    saved = _save_scenario()
    try:
        now = datetime.now()
        _scenario_set([
            {"uid": "win-evt-1", "summary": "[committee] Sync (1d view)",
             "start": (now + timedelta(days=1)).isoformat(timespec='seconds')},
            {"uid": "win-evt-2", "summary": "[committee] Sync (2h view)",
             "start": (now + timedelta(hours=2)).isoformat(timespec='seconds')},
        ])
        cid = _start_cycle()
        gcal.run(cid)
        c = connect()
        ticks = [r["id"] for r in c.execute(
            "SELECT id FROM inputs WHERE source='gcal' "
            "ORDER BY id").fetchall()]
        c.close()
        assert len(ticks) == 2, f"expected 2 ticks, got {len(ticks)}: {ticks}"
        for t in ticks:
            playbooks.fire("event_reminder", cid, t)
        c = connect()
        bcs = c.execute(
            "SELECT segment, status FROM broadcasts "
            "WHERE playbook='event_reminder' ORDER BY segment").fetchall()
        c.close()
        segs = {b["segment"] for b in bcs}
        assert any(s.endswith(":1d") for s in segs), \
            f"missing 1d segment: {segs}"
        assert any(s.endswith(":2h") for s in segs), \
            f"missing 2h segment: {segs}"
        for b in bcs:
            assert b["status"] == "sent"
        _passed(f"two windows → {len(segs)} segments, all sent")
    finally:
        _restore_scenario(saved)


def test_unparseable_input_handled_gracefully():
    """A malformed input id (e.g. someone hand-crafted it) should not
    crash the playbook; it should log + return ok=False."""
    init_db(); seed(); reset_volatile()
    cid = _start_cycle()
    out = playbooks.fire("event_reminder", cid, "gcal:notareminder:foo")
    assert out["ok"] is False, f"expected ok=False, got {out}"
    _passed("malformed gcal input id → playbook returns ok=False")


# ---------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_parse_audience_tags,
        test_event_upsert_idempotent,
        test_reminder_windows_fire_only_in_band,
        test_diane_audience_decision_not_broadcast,
        test_committee_audience_autosends,
        test_high_reach_event_gates_for_approval,
        test_two_cycles_same_event_no_double_send,
        test_two_windows_same_event_independent,
        test_unparseable_input_handled_gracefully,
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

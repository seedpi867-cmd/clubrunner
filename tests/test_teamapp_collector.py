"""TeamApp collector — end-to-end inbound coverage.

Architecture and config.yaml both promised TeamApp on a 15-min poll, but
no collector existed. Without it parents using the team chat (the
default channel for U10-U17 at this club) had no path to the agent —
their messages stayed inside TeamApp until a coach forwarded a
screenshot two days later.

These tests guard the collector + the routing decisions that make a
yes-RSVP a silent record (no broadcast, no decision) while a no-RSVP
gets the standard ack on the teamapp_post channel.

  1. Webhook → queue → collector mints exactly one input.
  2. Replays of the same teamapp_id idempotent (no double-mint).
  3. Structured availability=no reshapes into a body the existing
     player_availability rule recognises and acks on TeamApp.
  4. Structured availability=yes silent-records — input minted,
     status=done, NO broadcast, NO decision (so the brain doesn't
     burn cycles on the most common case).
  5. team_chat 'when is training' classifies as parent_faq and replies
     on the teamapp_post channel (not email — we don't have one).
  6. manager_post escalates to teamapp_unsorted; no autosend.
  7. Empty body items are skipped (collector never mints noise).
  8. Malformed queue line dropped, good line still minted.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from _helpers import use_temp_db, reset_volatile
use_temp_db()

from edge.state.db import connect, init_db, now_iso
from edge.state.seed import seed
from edge.collectors import teamapp
from triage import classifier, router
from tools import playbooks


_NULL_SCENARIO = Path("/tmp/clubrunner_test_teamapp_no_scenario.json")
if _NULL_SCENARIO.exists():
    _NULL_SCENARIO.unlink()
teamapp.SCENARIO = _NULL_SCENARIO


def _new_cycle() -> int:
    init_db()
    seed()
    reset_volatile()
    if teamapp.QUEUE.exists():
        teamapp.QUEUE.unlink()
    conn = connect()
    cur = conn.execute(
        "INSERT INTO cycles(started_at) VALUES(?)", (now_iso(),))
    cid = cur.lastrowid
    conn.close()
    return cid


def _drive_to_playbooks(cid: int) -> dict:
    """Tiny orchestrator slice — classify, route, fire the playbooks
    we exercise here. Skips the brain branch (which would Claude-call)."""
    classifier.run(cid)
    rt = router.run(cid)
    for iid in rt["playbook"]:
        conn = connect()
        row = conn.execute(
            "SELECT classification, body FROM inputs WHERE id=?",
            (iid,)).fetchone()
        conn.close()
        cls = row["classification"]
        if cls == "parent_faq_training":
            pb = "faq_training"
        elif cls == "parent_faq_register":
            pb = "faq_register"
        elif cls == "player_availability":
            pb = "availability_ack"
        else:
            continue
        playbooks.fire(pb, cid, iid)
    return rt


def test_webhook_appends_and_collector_drains():
    cid = _new_cycle()
    res = teamapp.webhook_append({
        "kind": "team_chat",
        "from": "Anita Mehta",
        "team_id": "u11_boys",
        "message_id": "ta_msg_88301",
        "body": "When is U11 training this week?",
    })
    assert res["ok"], res
    assert teamapp.QUEUE.exists() and teamapp.QUEUE.read_text().strip()

    out = teamapp.run(cid)
    assert out.get("ingested") == 1, out

    conn = connect()
    row = conn.execute(
        "SELECT source, sender, subject, body FROM inputs "
        "WHERE source='teamapp' LIMIT 1").fetchone()
    conn.close()
    assert row["sender"] == "Anita Mehta"
    assert "u11_boys" in row["subject"]
    # Queue must be truncated
    assert teamapp.QUEUE.read_text().strip() == ""
    print(f"  ✓ TeamApp webhook → queue → collector → input "
          f"({row['subject']})")


def test_replay_idempotent():
    """TeamApp's API may resend the same message_id on retry. Stable
    id is `kind:teamapp_id`, so two appends collapse into one input."""
    cid = _new_cycle()
    payload = {"kind": "team_chat", "from": "Sam",
               "team_id": "u10_mixed", "message_id": "ta_msg_replay_1",
               "body": "training tomorrow?"}
    teamapp.webhook_append(payload)
    teamapp.webhook_append(payload)
    teamapp.run(cid)
    conn = connect()
    n = conn.execute(
        "SELECT COUNT(*) AS n FROM inputs "
        "WHERE source='teamapp'").fetchone()["n"]
    conn.close()
    assert n == 1, f"replay double-minted: count={n}"
    print(f"  ✓ replayed TeamApp message minted exactly one input")


def test_availability_no_reshapes_and_acks_on_teamapp():
    """Structured RSVP=no → reshape body to natural language → existing
    body_regex catches → availability_ack fires on the teamapp_post
    channel (not email — we have no parent email from a TeamApp RSVP)."""
    cid = _new_cycle()
    teamapp.webhook_append({
        "kind": "availability",
        "from": "Bel Chen",
        "team_id": "u9_mixed",
        "player_name": "Liam",
        "response": "no",
        "rsvp_id": "ta_rsvp_avail_no_1",
    })
    teamapp.run(cid)
    rt = _drive_to_playbooks(cid)

    conn = connect()
    inp = dict(conn.execute(
        "SELECT classification, status FROM inputs "
        "WHERE source='teamapp' LIMIT 1").fetchone())
    bcs = [dict(r) for r in conn.execute(
        "SELECT playbook, channel, segment, status FROM broadcasts "
        "WHERE related_input LIKE 'teamapp:%'")]
    conn.close()

    assert inp["classification"] == "player_availability", (
        f"no-RSVP must classify as player_availability, got {inp}")
    assert len(bcs) == 1, (
        f"expected 1 ack broadcast, got {len(bcs)}: {bcs}")
    bc = bcs[0]
    assert bc["playbook"] == "availability_ack"
    assert bc["channel"] == "teamapp_post", (
        f"reply must come back via teamapp_post, got {bc['channel']}")
    assert bc["status"] == "sent"
    assert bc["segment"] == "sender:Bel Chen"
    print(f"  ✓ RSVP=no → availability_ack on teamapp_post")


def test_availability_yes_silent_recorded():
    """RSVP=yes is the high-volume case. The collector mints the input
    so the manager view shows a complete RSVP roster, but routing
    silent-records it: no broadcast, no decision, no brain call.
    Without this rule, every confirmed-in burns a brain cycle."""
    cid = _new_cycle()
    teamapp.webhook_append({
        "kind": "availability",
        "from": "Steve Whitcombe",
        "team_id": "u9_mixed",
        "player_name": "Tom",
        "response": "yes",
        "rsvp_id": "ta_rsvp_avail_yes_1",
    })
    teamapp.run(cid)
    rt = _drive_to_playbooks(cid)

    conn = connect()
    inp = dict(conn.execute(
        "SELECT classification, status FROM inputs "
        "WHERE source='teamapp' LIMIT 1").fetchone())
    n_bcs = conn.execute(
        "SELECT COUNT(*) AS n FROM broadcasts "
        "WHERE related_input LIKE 'teamapp:%'").fetchone()["n"]
    n_dec = conn.execute(
        "SELECT COUNT(*) AS n FROM decisions "
        "WHERE target_id LIKE 'teamapp:%'").fetchone()["n"]
    conn.close()

    assert inp["classification"] == "availability_yes", inp
    assert inp["status"] == "done", (
        f"silent-record must close the input (status=done), got {inp}")
    assert n_bcs == 0, (
        f"yes-RSVP must NOT produce a broadcast, got {n_bcs}")
    assert n_dec == 0, (
        f"yes-RSVP must NOT open a decision, got {n_dec}")
    iid = "teamapp:availability:ta_rsvp_avail_yes_1"
    assert iid not in rt["brain"], (
        f"yes-RSVP must not route to brain, got {rt['brain']}")
    assert iid in rt["silent"], (
        f"yes-RSVP must land in silent bucket, got {rt}")
    print(f"  ✓ RSVP=yes silent-recorded (0 broadcasts, 0 decisions, "
          f"status=done)")


def test_team_chat_faq_replies_on_teamapp():
    """Free-text 'when is training' on team chat → parent_faq → reply
    must come back via teamapp_post (not email)."""
    cid = _new_cycle()
    teamapp.webhook_append({
        "kind": "team_chat",
        "from": "Anita Mehta",
        "team_id": "u11_boys",
        "message_id": "ta_msg_faq_1",
        "body": "When is U11 training this week? I keep losing track.",
    })
    teamapp.run(cid)
    _drive_to_playbooks(cid)

    conn = connect()
    bc = dict(conn.execute(
        "SELECT playbook, channel, segment, status FROM broadcasts "
        "WHERE related_input LIKE 'teamapp:%' LIMIT 1").fetchone())
    conn.close()
    assert bc["playbook"] == "faq_training"
    assert bc["channel"] == "teamapp_post"
    assert bc["segment"] == "sender:Anita Mehta"
    assert bc["status"] == "sent"
    print(f"  ✓ TeamApp FAQ → faq_training on teamapp_post")


def test_manager_post_escalates_no_autosend():
    """A manager post asking the club to approve a fixture forfeit is
    novel — no autosend, must escalate to brain (which will draft an
    options brief for Diane's queue)."""
    cid = _new_cycle()
    teamapp.webhook_append({
        "kind": "manager_post",
        "from": "Coach Phil (U14)",
        "team_id": "u14_boys",
        "post_id": "ta_post_escalate_1",
        "body": ("Brighton coach asked us to forfeit Saturday — they're "
                 "short 4 players. Can the club approve?"),
    })
    teamapp.run(cid)
    classifier.run(cid)
    rt = router.run(cid)

    conn = connect()
    cls = dict(conn.execute(
        "SELECT classification FROM inputs "
        "WHERE source='teamapp' LIMIT 1").fetchone())["classification"]
    n_bcs = conn.execute(
        "SELECT COUNT(*) FROM broadcasts "
        "WHERE related_input LIKE 'teamapp:%'").fetchone()[0]
    conn.close()

    assert cls == "teamapp_unsorted", (
        f"manager_post should fall through to teamapp_unsorted, got {cls}")
    iid = "teamapp:manager_post:ta_post_escalate_1"
    assert iid in rt["brain"], (
        f"manager_post should route to brain, got {rt}")
    assert n_bcs == 0, (
        f"manager_post must NOT autosend, got {n_bcs} broadcast(s)")
    print(f"  ✓ TeamApp manager_post → teamapp_unsorted → brain (no autosend)")


def test_empty_body_items_skipped():
    """A team_chat or manager_post with no body is noise. The collector
    must drop it — minting an empty input would route to brain and
    waste a Claude call on nothing."""
    cid = _new_cycle()
    res = teamapp.webhook_append({
        "kind": "team_chat",
        "from": "Some Parent",
        "team_id": "u9_mixed",
        "body": "",
    })
    assert not res["ok"], (
        f"empty body should reject at webhook, got {res}")

    teamapp.QUEUE.parent.mkdir(parents=True, exist_ok=True)
    with teamapp.QUEUE.open("a") as f:
        f.write(json.dumps({
            "kind": "team_chat", "from": "Some Parent",
            "team_id": "u9_mixed", "message_id": "ta_msg_empty_1",
            "body": ""
        }) + "\n")
    out = teamapp.run(cid)
    assert out.get("ingested") == 0, (
        f"empty-body line should be skipped, got {out}")
    conn = connect()
    n = conn.execute(
        "SELECT COUNT(*) FROM inputs "
        "WHERE source='teamapp'").fetchone()[0]
    conn.close()
    assert n == 0, f"empty body minted {n} input(s)"
    print(f"  ✓ empty-body items dropped at webhook + drain")


def test_malformed_queue_line_does_not_kill_collector():
    cid = _new_cycle()
    teamapp.QUEUE.parent.mkdir(parents=True, exist_ok=True)
    with teamapp.QUEUE.open("a") as f:
        f.write("{not valid json\n")
        f.write(json.dumps({
            "kind": "team_chat", "from": "Sam",
            "team_id": "u10_mixed", "message_id": "ta_msg_malformed_good",
            "body": "still works",
        }) + "\n")
    out = teamapp.run(cid)
    assert out.get("ingested") == 1, out
    conn = connect()
    n = conn.execute(
        "SELECT COUNT(*) FROM inputs "
        "WHERE source='teamapp'").fetchone()[0]
    conn.close()
    assert n == 1
    print(f"  ✓ malformed line skipped, good line minted")


def main():
    tests = [
        ("webhook → queue → collector → input",
         test_webhook_appends_and_collector_drains),
        ("replay idempotent (TeamApp retries)",
         test_replay_idempotent),
        ("RSVP=no → availability_ack on teamapp_post",
         test_availability_no_reshapes_and_acks_on_teamapp),
        ("RSVP=yes silent-recorded (no broadcast, no decision)",
         test_availability_yes_silent_recorded),
        ("team_chat FAQ → faq_training on teamapp_post",
         test_team_chat_faq_replies_on_teamapp),
        ("manager_post → teamapp_unsorted → brain (no autosend)",
         test_manager_post_escalates_no_autosend),
        ("empty body skipped at webhook + drain",
         test_empty_body_items_skipped),
        ("malformed queue line skipped",
         test_malformed_queue_line_does_not_kill_collector),
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

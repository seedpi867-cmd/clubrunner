"""Inbound parent channels — SMS + Facebook end-to-end.

The agent only deserves the title 'autonomous' if a parent can reach
the club via SMS or a Facebook comment and have it handled without
Diane lifting a finger. Before this build, both channels were
architecture-promised but had no collector — every parent SMS sat
invisible to triage, and Friday-night FB comments were the
explore-doc's lived example of Diane's overload.

These tests guard:

  1. Webhook-shaped payloads queue cleanly.
  2. Scenario-shaped payloads queue cleanly.
  3. SMS 'when is training' reaches the SMS-channel auto-replier (the
     reply must NOT come back by email — the parent's number is all
     we know).
  4. SMS 'Liam is sick' reaches availability_ack on the SMS channel.
  5. FB 'how do I register' reaches faq_register on the fb_comment
     channel.
  6. FB rant ('Why was U9 cancelled??') escalates to a decision and
     does NOT autosend a reply.
  7. Replays of the same Twilio MessageSid don't double-mint the
     input (Twilio retries on timeout).
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
from edge.collectors import sms_inbound, facebook_page
from triage import classifier, router
from tools import playbooks


# Point the collectors at non-existent scenario paths so tests don't
# inherit the today.json fixture used by the live cycle. Each test
# drives the inbound channels via webhook_append — that is exactly
# the path Twilio / the FB Graph webhook will take in production.
_NULL_SCENARIO = Path("/tmp/clubrunner_test_no_scenario.json")
if _NULL_SCENARIO.exists():
    _NULL_SCENARIO.unlink()
sms_inbound.SCENARIO = _NULL_SCENARIO
facebook_page.SCENARIO = _NULL_SCENARIO


def _new_cycle() -> int:
    init_db()
    seed()
    reset_volatile()
    # Wipe the on-disk queue files between tests so a stale line from
    # the prior test doesn't bleed into this one.
    for q in (sms_inbound.QUEUE, facebook_page.QUEUE):
        if q.exists():
            q.unlink()
    conn = connect()
    cur = conn.execute(
        "INSERT INTO cycles(started_at) VALUES(?)", (now_iso(),))
    cid = cur.lastrowid
    conn.close()
    return cid


def _drive_one(cid: int, brain_input_ids=None,
               playbook_input_ids=None) -> None:
    """Tiny orchestrator slice — classify, route, fire playbooks. We
    don't run the full orchestrator because we don't want the brain
    branch (which would reach for Claude on escalations)."""
    classifier.run(cid)
    rt = router.run(cid)
    for iid in rt["playbook"]:
        # Mirror the orchestrator's class→playbook map for the cases
        # we exercise here.
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


def test_sms_webhook_appends_to_queue_and_collector_drains():
    """The dashboard webhook handler calls webhook_append. Next cycle
    the collector finds the line, mints an input, and truncates."""
    cid = _new_cycle()
    res = sms_inbound.webhook_append({
        "From": "+61 400 555 666",
        "Body": "When is U10 training?",
        "MessageSid": "SM_test_3001",
    })
    assert res["ok"], res
    assert sms_inbound.QUEUE.exists()
    assert sms_inbound.QUEUE.read_text().strip()  # non-empty

    out = sms_inbound.run(cid)
    assert out.get("ingested") == 1, out

    conn = connect()
    row = conn.execute(
        "SELECT source, sender, body FROM inputs "
        "WHERE source='sms' ORDER BY received_at DESC LIMIT 1").fetchone()
    conn.close()
    assert row["sender"] == "+61400555666", (
        f"webhook number must be normalised, got {row['sender']}")
    assert "U10" in row["body"]
    # Queue file must be empty after drain — otherwise next cycle
    # re-mints, which would noisy the dashboard.
    assert sms_inbound.QUEUE.read_text().strip() == "", (
        "queue not truncated after drain")
    print(f"  ✓ SMS webhook → queue → collector → input "
          f"(sender={row['sender']})")


def test_sms_replay_idempotent():
    """Twilio's at-least-once delivery means the same MessageSid can
    arrive twice. We hash on (from, body, ts) so identical replays
    upsert into the same input row."""
    cid = _new_cycle()
    payload = {"from": "+61400111222",
               "body": "Liam sick, won't be at U9",
               "ts": "2026-05-09T10:00:00",
               "provider_id": "SM_replay_001"}
    sms_inbound.webhook_append(payload)
    sms_inbound.webhook_append(payload)
    sms_inbound.run(cid)

    conn = connect()
    n = conn.execute(
        "SELECT COUNT(*) AS n FROM inputs WHERE source='sms'").fetchone()["n"]
    conn.close()
    assert n == 1, f"replay double-minted: count={n}"
    print(f"  ✓ replayed SMS minted exactly one input")


def test_sms_training_question_autoreplies_on_sms():
    """End-to-end: parent texts 'when is U10 training', the FAQ
    classifier picks it up, and the reply lands on the SMS channel
    (not email — we don't have their email)."""
    cid = _new_cycle()
    sms_inbound.webhook_append({
        "from": "+61400112233",
        "body": "hi when is U10 training this week?",
        "ts": "2026-05-09T17:00:00",
    })
    sms_inbound.run(cid)
    _drive_one(cid)

    conn = connect()
    bcs = conn.execute(
        "SELECT playbook, channel, segment, body, status "
        "FROM broadcasts WHERE related_input LIKE 'sms:%'"
    ).fetchall()
    conn.close()
    assert len(bcs) == 1, f"expected 1 reply, got {len(bcs)}"
    bc = dict(bcs[0])
    assert bc["channel"] == "sms", (
        f"FAQ reply to an SMS must come back on SMS, got {bc['channel']}")
    assert bc["segment"] == "sender:+61400112233"
    assert bc["status"] == "sent"
    assert "Training" in bc["body"] or "training" in bc["body"]
    # SMS body should be tight — under ~320 chars to fit two segments.
    assert len(bc["body"]) <= 320, (
        f"SMS body too long ({len(bc['body'])} chars) — will fragment "
        "expensively")
    print(f"  ✓ SMS FAQ → SMS reply ({len(bc['body'])} chars, "
          f"channel={bc['channel']})")


def test_sms_player_availability_autoreplies_on_sms():
    """Parent SMSes 'Mira sick, won't make Saturday'. The body-anchored
    availability rule (added because SMS has no useful subject) catches
    it and acks on SMS."""
    cid = _new_cycle()
    sms_inbound.webhook_append({
        "from": "+61400998877",
        "body": "Mira sick, won't make Saturday — Liz",
        "ts": "2026-05-09T18:00:00",
    })
    sms_inbound.run(cid)
    _drive_one(cid)

    conn = connect()
    cls = conn.execute(
        "SELECT classification FROM inputs "
        "WHERE source='sms' LIMIT 1").fetchone()["classification"]
    bc = conn.execute(
        "SELECT playbook, channel, status FROM broadcasts "
        "WHERE related_input LIKE 'sms:%' LIMIT 1").fetchone()
    conn.close()
    assert cls == "player_availability", (
        f"expected player_availability, got {cls}")
    assert bc["playbook"] == "availability_ack"
    assert bc["channel"] == "sms"
    assert bc["status"] == "sent"
    print(f"  ✓ SMS 'Mira sick' → availability_ack on sms (cls={cls})")


def test_fb_comment_register_autoreplies_on_fb_comment():
    """FB comment 'how do I register my son' — the broadened FAQ rule
    catches it, the reply goes back as an fb_comment (not email)."""
    cid = _new_cycle()
    facebook_page.webhook_append({
        "kind": "comment",
        "from": "Rita McMahon",
        "fb_id": "comment_8821",
        "post_id": "post_991",
        "body": "How do I register my son for next season?",
    })
    facebook_page.run(cid)
    _drive_one(cid)

    conn = connect()
    bc = conn.execute(
        "SELECT playbook, channel, segment, status FROM broadcasts "
        "WHERE related_input LIKE 'fb:%' LIMIT 1").fetchone()
    conn.close()
    assert bc, "no broadcast drafted for fb register question"
    assert bc["playbook"] == "faq_register"
    assert bc["channel"] == "fb_comment", (
        f"FB-sourced reply must use fb_comment channel, got {bc['channel']}")
    assert bc["status"] == "sent"
    assert bc["segment"] == "sender:Rita McMahon"
    print(f"  ✓ FB register Q → faq_register on fb_comment")


def test_training_question_with_register_keyword_routes_to_training():
    """Regression: a single SMS like 'where is U10 training? Do I
    register at the same place?' previously misrouted.

    The classifier correctly hit the training rule (it appears first in
    rules.yaml, and 'where is U10 training' matches its regex) — but
    both training and register rules collapsed into category
    'parent_faq', and the orchestrator re-discriminated by checking
    'register' / 'sign up' as a substring of the body. That heuristic
    flipped this message to faq_register because the keyword is in a
    follow-up clause, sending the parent a registration link instead
    of training times.

    Now each rule maps to its own category (parent_faq_training /
    parent_faq_register) and the orchestrator routes off the
    classification directly. The training rule wins and the parent
    gets the right reply."""
    cid = _new_cycle()
    sms_inbound.webhook_append({
        "From": "+61 400 555 777",
        "Body": ("where is U10 training? Do I register them at "
                 "PlayHQ also?"),
        "MessageSid": "SM_test_faq_split",
    })
    sms_inbound.run(cid)
    _drive_one(cid)

    conn = connect()
    cls = conn.execute(
        "SELECT classification FROM inputs "
        "WHERE source='sms' LIMIT 1").fetchone()["classification"]
    bc = conn.execute(
        "SELECT playbook, channel FROM broadcasts "
        "WHERE related_input LIKE 'sms:%' LIMIT 1").fetchone()
    conn.close()
    assert cls == "parent_faq_training", (
        f"training-with-register-mention must classify as "
        f"parent_faq_training, got {cls}")
    assert bc, "no broadcast drafted for SMS training question"
    assert bc["playbook"] == "faq_training", (
        f"must route to faq_training (the question is about training), "
        f"got {bc['playbook']}")
    assert bc["channel"] == "sms"
    print("  ✓ training Q with 'register' clause → faq_training (not register)")


def test_fb_rant_escalates_no_autoreply():
    """An angry FB comment must NOT auto-respond. It hits fb_unsorted,
    the router escalates it to brain, and no broadcast is drafted by
    the playbook layer. (Brain itself will produce a held draft +
    decision, but the playbook layer alone — what this test exercises
    — must not autosend.)"""
    cid = _new_cycle()
    facebook_page.webhook_append({
        "kind": "comment",
        "from": "Angry Dad",
        "fb_id": "comment_8822",
        "body": ("Why was U9 cancelled?? We drove from Whyalla just "
                 "for this. Absolutely unacceptable."),
    })
    facebook_page.run(cid)
    classifier.run(cid)
    rt = router.run(cid)

    conn = connect()
    cls = conn.execute(
        "SELECT classification, status FROM inputs "
        "WHERE source='fb' LIMIT 1").fetchone()
    n_bcs = conn.execute(
        "SELECT COUNT(*) FROM broadcasts "
        "WHERE related_input LIKE 'fb:%'").fetchone()[0]
    conn.close()
    assert cls["classification"] == "fb_unsorted", (
        f"angry rant should fall through to fb_unsorted, got "
        f"{cls['classification']}")
    iid = f"fb:comment:comment_8822"
    assert iid in rt["brain"], (
        f"angry rant should escalate to brain, got route={rt}")
    assert n_bcs == 0, (
        f"angry rant produced {n_bcs} autosent broadcasts — must "
        "always escalate, never auto-reply")
    print(f"  ✓ FB rant → fb_unsorted → brain (no autosend)")


def test_collector_skips_when_no_input():
    """A cycle with no SMS or FB activity must NOT log audit noise
    on every run. (We log one collect event per cycle either way —
    that's expected — but no per-line errors.)"""
    cid = _new_cycle()
    sms_out = sms_inbound.run(cid)
    fb_out = facebook_page.run(cid)
    assert sms_out.get("ingested") == 0, sms_out
    assert fb_out.get("ingested") == 0, fb_out
    print(f"  ✓ no-op cycle clean (sms={sms_out}, fb={fb_out})")


def test_malformed_queue_line_does_not_kill_collector():
    """A corrupt line in the queue file (something writes garbage)
    must not stop the collector from processing the rest. The bad
    line should be logged and dropped."""
    cid = _new_cycle()
    sms_inbound.QUEUE.parent.mkdir(parents=True, exist_ok=True)
    with sms_inbound.QUEUE.open("a") as f:
        f.write("{not valid json\n")
        f.write(json.dumps({
            "from": "+61400000000",
            "body": "still works",
            "ts": "2026-05-09T19:00:00",
        }) + "\n")
    out = sms_inbound.run(cid)
    assert out.get("ingested") == 1, (
        f"good line should have minted: {out}")
    conn = connect()
    n = conn.execute(
        "SELECT COUNT(*) FROM inputs WHERE source='sms'").fetchone()[0]
    conn.close()
    assert n == 1
    print(f"  ✓ malformed line skipped, good line minted")


def main():
    tests = [
        ("SMS webhook → queue → collector → input",
         test_sms_webhook_appends_to_queue_and_collector_drains),
        ("SMS replay idempotent (Twilio retries)",
         test_sms_replay_idempotent),
        ("SMS 'when is training' → SMS auto-reply",
         test_sms_training_question_autoreplies_on_sms),
        ("SMS 'Mira sick' → availability_ack on SMS",
         test_sms_player_availability_autoreplies_on_sms),
        ("FB 'how do I register' → fb_comment auto-reply",
         test_fb_comment_register_autoreplies_on_fb_comment),
        ("FB rant → escalation, no autosend",
         test_fb_rant_escalates_no_autoreply),
        ("collector no-op cycle clean",
         test_collector_skips_when_no_input),
        ("malformed queue line skipped, good line minted",
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

"""Sponsor delivery shortfall watch — closes a hole the architecture promised.

ARCHITECTURE.md lists "sponsor banner rotation per contract spec" under
autonomous behaviours. Rotation itself works (newsletter picks the
longest-since-last-post sponsor and decrements posts_owed on send), but
nothing notices when a sponsor cannot physically be delivered against
its contract before the season runs out — e.g. Bendigo with 8 posts
owed and only 5 weekly newsletters remaining before contract_end. Diane
finds out at the AGM when the sponsor renewal conversation lands and
the bank's marketing manager has counted four banners, not eight. By
then it's a renewal-killer, not an oversight.

This sweep runs each cycle, pure background work, and surfaces a quiet
decision the moment a sponsor is mathematically impossible to deliver
against the rotation schedule. It does NOT:

  - chase, post, or contact the sponsor directly (relationship work is
    Diane's call — the agent stops at "you should know")
  - re-flag the same shortfall every cycle (cooldown by sponsor + by
    severity tier — a worsening shortfall reopens; a steady one does
    not)

What it does flag is impossibility. The rotation is round-robin
oldest-first; a sponsor with posts_owed=K needs at least K weekly
newsletter slots between now and the earliest of (their contract_end,
season_end) for delivery to be feasible at all. If weeks_remaining
drops below posts_owed + buffer, raise the flag.

Why "posts_owed + buffer" rather than equality:
  - one newsletter could be cancelled by a heat-driven Sunday rebuild
    where Diane prioritises another section
  - the rotation can stall when both sponsors are equal-oldest and a
    later cycle picks the wrong one
  - the buffer is the difference between "I'll squeeze it in" and
    "we missed".

Decision options give Diane real remedies, not just acknowledgement:
  - book a mid-week social post (raises capacity by N for the season)
  - reduce contracted obligations for next year (kick to renewal)
  - email the sponsor now to pre-empt an awkward AGM moment
  - tolerate (sometimes a sponsor's rep is on long service leave and
    won't notice; Diane's call)

Idempotent. Safe to run every cycle.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from typing import Any

from edge.state.db import connect, log_action, now_iso
import config as cfg_mod


PLAYBOOK_NAME = "sponsor_obligations"
ALERT_KEY_PREFIX = "sponsor_shortfall"


def _weeks_between(start: datetime, end_iso: str | None) -> int:
    """Whole newsletter weeks between now and end. Newsletter is weekly,
    so each remaining week == one rotation slot. Returns 0 if end is
    in the past or unparseable; the caller treats 0 as "no slots left".
    """
    if not end_iso:
        return 0
    try:
        # contract_end is stored as date or ISO datetime; normalise.
        end = datetime.fromisoformat(end_iso[:19]) if "T" in end_iso \
            else datetime.fromisoformat(end_iso + "T23:59:59")
    except ValueError:
        return 0
    delta = end - start
    return max(0, delta.days // 7)


def _alert_recently_fired(sponsor_id: str, shortfall: int,
                          cooldown_days: int) -> bool:
    """Suppress duplicate alerts for the same sponsor at the same or
    lower shortfall. A worsening shortfall (more posts at risk) bypasses
    the cooldown — Diane should hear about a 5-post hole even if she
    saw a 2-post hole last week and chose 'tolerate'.
    """
    cutoff = (datetime.now() - timedelta(days=cooldown_days)).isoformat(
        timespec="seconds")
    conn = connect()
    rows = conn.execute(
        "SELECT detail, fired_at FROM policy_runs "
        "WHERE id LIKE ? AND fired_at > ? "
        "ORDER BY fired_at DESC",
        (f"{ALERT_KEY_PREFIX}:{sponsor_id}:%", cutoff)).fetchall()
    conn.close()
    for r in rows:
        # detail format: "shortfall=N owed=O weeks=W"
        try:
            prev = int((r["detail"] or "").split("shortfall=")[1].split()[0])
        except (IndexError, ValueError):
            continue
        if shortfall <= prev:
            return True
    return False


def _open_shortfall_decision(cycle_id: int, sponsor_id: str,
                             sponsor_name: str, tier: str,
                             posts_owed: int, weeks_remaining: int,
                             contract_end: str | None,
                             shortfall: int) -> str:
    did = f"dec_{uuid.uuid4().hex[:12]}"
    end_label = (contract_end or "")[:10] if contract_end else "no end on file"
    summary = (f"{sponsor_name} ({tier}) — {posts_owed} posts owed, "
               f"only {weeks_remaining} newsletter weeks left "
               f"(short {shortfall})")
    context = (
        f"Sponsor: {sponsor_name} ({tier} tier, contract end {end_label})\n"
        f"Posts contracted but not yet delivered: {posts_owed}\n"
        f"Newsletter weeks remaining before contract end: "
        f"{weeks_remaining}\n"
        f"Mathematical shortfall: {shortfall} posts cannot fit even if "
        "every remaining newsletter spotlights this sponsor.\n\n"
        "The rotation is one sponsor per Sunday newsletter. With more "
        "than one active sponsor, this sponsor's effective share is "
        "smaller still, so the real shortfall is at least this number.\n\n"
        "Why you're seeing this now: the rotation will keep doing its "
        "job until the contract ends, and then the sponsor will tally "
        "what they actually got versus what they paid for. Renewal "
        "conversations are easier when a 'we're behind, here's the fix' "
        "email beats us to the bank's marketing review.\n\n"
        "Realistic remedies:\n"
        "  - Book a mid-week social-only post (Insta + FB) — raises "
        "capacity outside the newsletter rotation.\n"
        "  - Reduce next-year contracted obligations and fold the "
        "shortfall into a renewal credit.\n"
        "  - Tell the sponsor first — most sponsors prefer a heads-up "
        "over a discovery.")
    options = [
        {"key": "midweek_post",  "label": "Schedule mid-week social posts"},
        {"key": "email_sponsor", "label": "Email sponsor now (pre-empt)"},
        {"key": "renewal_credit", "label": "Roll into renewal credit"},
        {"key": "tolerate",      "label": "Tolerate — known about"},
    ]
    conn = connect()
    conn.execute(
        "INSERT INTO decisions(id, cycle_id, kind, target_kind, target_id, "
        "summary, context, options_json, default_option, created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (did, cycle_id, "sponsor_shortfall", "sponsor", sponsor_id,
         summary[:200], context[:4000],
         json.dumps(options), "midweek_post", now_iso()))
    # Include uuid so a worsening shortfall reopened inside the same
    # second (fast tests, or rapid contract_end edits) does not collide
    # on the second-precision now_iso() key.
    pid = (f"{ALERT_KEY_PREFIX}:{sponsor_id}:"
           f"{now_iso()}:{uuid.uuid4().hex[:8]}")[:128]
    conn.execute(
        "INSERT INTO policy_runs(id, playbook, target_kind, target_id, "
        "cycle_id, fired_at, outcome, detail) VALUES(?,?,?,?,?,?,?,?)",
        (pid, PLAYBOOK_NAME, "sponsor", sponsor_id, cycle_id,
         now_iso(), "alert_opened",
         f"shortfall={shortfall} owed={posts_owed} "
         f"weeks={weeks_remaining}"))
    conn.close()
    log_action(cycle_id, "execute", "sponsor_shortfall_alert",
               "sponsor", sponsor_id, True,
               f"shortfall={shortfall} decision={did}")
    return did


def run(cycle_id: int, related_input: str | None = None) -> dict[str, Any]:
    cfg = cfg_mod.get().get("sponsor_obligations", {})
    if not cfg.get("enabled", True):
        return {"ok": True, "skipped": "disabled"}

    buffer_weeks = int(cfg.get("weeks_buffer", 2))
    cooldown_days = int(cfg.get("alert_cooldown_days", 14))
    season_end = cfg.get("season_end")  # optional override

    now = datetime.now()
    season_end_dt = None
    if season_end:
        try:
            season_end_dt = datetime.fromisoformat(season_end + "T23:59:59")
        except ValueError:
            season_end_dt = None

    conn = connect()
    sponsors = conn.execute(
        "SELECT id, name, tier, posts_owed, contract_end "
        "FROM sponsors WHERE posts_owed > 0").fetchall()
    conn.close()

    alerts = []
    for s in sponsors:
        # Effective end: earlier of contract_end and season_end
        end_iso = s["contract_end"]
        if season_end_dt and end_iso:
            try:
                ce = datetime.fromisoformat(end_iso[:10] + "T23:59:59")
                if season_end_dt < ce:
                    end_iso = season_end_dt.isoformat(timespec="seconds")
            except ValueError:
                pass
        elif season_end_dt and not end_iso:
            end_iso = season_end_dt.isoformat(timespec="seconds")

        weeks = _weeks_between(now, end_iso)
        capacity = max(0, weeks - buffer_weeks)
        shortfall = (s["posts_owed"] or 0) - capacity
        if shortfall <= 0:
            continue
        if _alert_recently_fired(s["id"], shortfall, cooldown_days):
            continue
        did = _open_shortfall_decision(
            cycle_id, s["id"], s["name"], s["tier"],
            s["posts_owed"], weeks, s["contract_end"], shortfall)
        alerts.append({"sponsor": s["id"], "shortfall": shortfall,
                       "decision": did})

    log_action(cycle_id, "execute", "sponsor_obligations_run",
               None, None, True,
               f"sponsors_scanned={len(sponsors)} alerts={len(alerts)}")
    return {"ok": True, "sponsors_scanned": len(sponsors),
            "alerts": alerts}

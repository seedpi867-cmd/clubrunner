"""Sponsor post gate — anything tagged with a sponsor goes through Diane.

Architecture rule: "anything tagged with a sponsor goes to approval gate."

Why this exists at all (and isn't just covered by broadcast_threshold):
the threshold gate only catches >50-reach broadcasts. A brain-drafted
Facebook post crediting Bendigo Bank for the U13 girls' premiership win
might reach 12 followers in a comment thread, or a small-segment SMS to
the 8 sponsor-bound club VIPs. Both clear threshold trivially. Both put
the sponsor's name in front of a public audience under the club's voice.

The whole sponsor relationship is built on Diane vouching for content
that mentions them — the bank manager has called twice over four years
about post wording. So: any broadcast carrying meta.sponsor_credit has
to be Diane's call, regardless of reach.

The agent has one bypass: when the playbook itself IS the sponsor
contract delivery (the Sunday newsletter's spotlight rotation, today),
broadcast_threshold already catches it because reach is the full parent
list. The newsletter sets meta.sponsor_credit so posts_owed decrements
on send — it does NOT need a second approval card on top of the
threshold one. Stacked gates collapse to a single approval flow in
broadcast.submit.
"""
from __future__ import annotations

from . import GateResult


def check(action: dict) -> GateResult:
    meta = action.get("meta") or {}
    sponsor_id = meta.get("sponsor_credit")
    if not sponsor_id:
        return GateResult("sponsor_post", True)
    if action.get("policy_override_sponsor_post"):
        return GateResult("sponsor_post", True,
                          f"override (sponsor={sponsor_id})")
    return GateResult(
        "sponsor_post", False,
        f"sponsor-tagged ({sponsor_id}) — Diane approves all sponsor posts")

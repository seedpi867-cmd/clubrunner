"""Safeguarding gate — anything carrying safeguarding signal is BLOCKED
from autosend forever and forced to escalate.

Pattern matching is done at triage time (input.safeguarding=1). This
gate just refuses to send any broadcast that originated from a
safeguarding-flagged input."""
from __future__ import annotations

from edge.state.db import connect
from . import GateResult


def check(action: dict) -> GateResult:
    related = action.get("related_input")
    if not related:
        return GateResult("safeguarding", True)
    conn = connect()
    row = conn.execute(
        "SELECT safeguarding FROM inputs WHERE id=?", (related,)).fetchone()
    conn.close()
    if row and row["safeguarding"]:
        return GateResult("safeguarding", False,
                          "safeguarding flag — never autosend, escalate only")
    return GateResult("safeguarding", True)

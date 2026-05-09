"""SQLite access for clubrunner. WAL mode, single state.db, all writes audited."""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = ROOT / "edge" / "state" / "state.db"
SCHEMA_PATH = ROOT / "edge" / "state" / "schema.sql"


def _db_path() -> Path:
    """Resolve the db path at call time so tests can swap via env var.
    Without this, importing db.py freezes DB_PATH before the test sets
    CLUBRUNNER_DB."""
    override = os.environ.get("CLUBRUNNER_DB")
    return Path(override) if override else DEFAULT_DB_PATH


# Back-compat for any caller still reading db.DB_PATH directly.
DB_PATH = DEFAULT_DB_PATH


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_db_path()), isolation_level=None,
                           timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    p = _db_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = connect()
    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())
    # Idempotent column adds for state.db files that pre-date a schema
    # change. CREATE TABLE IF NOT EXISTS doesn't migrate columns onto an
    # existing table, so any new column must be ALTER-added here. Each
    # entry: (table, column, type+default).
    _migrations: list[tuple[str, str, str]] = [
        ("broadcasts", "meta_json", "TEXT"),
        ("weather_state", "rain_24h_mm", "REAL"),
        ("grounds", "drainage_threshold_mm", "REAL"),
    ]
    for table, col, decl in _migrations:
        cols = {r["name"] for r in conn.execute(
            f"PRAGMA table_info({table})").fetchall()}
        if col not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
    conn.close()


@contextmanager
def txn():
    conn = connect()
    try:
        conn.execute("BEGIN")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def log_action(cycle_id: int | None, phase: str, action: str,
               target_kind: str | None, target_id: str | None,
               ok: bool, detail: str | None) -> None:
    cid = cycle_id if (cycle_id and cycle_id > 0) else None
    conn = connect()
    conn.execute(
        "INSERT INTO actions (cycle_id, ts, phase, action, target_kind, "
        "target_id, ok, detail) VALUES (?,?,?,?,?,?,?,?)",
        (cid, now_iso(), phase, action, target_kind, target_id,
         1 if ok else 0, (detail or "")[:1000]),
    )
    conn.close()


def upsert_input(source: str, source_id: str, *, sender: str = "",
                 subject: str = "", body: str = "",
                 raw_json: str = "") -> tuple[str, bool]:
    """Upsert an input row. Returns (iid, is_new). The is_new flag lets
    collectors log audit events only when an input is genuinely minted —
    re-seen items would otherwise inflate the action log every cycle."""
    iid = f"{source}:{source_id}"
    conn = connect()
    existing = conn.execute(
        "SELECT id FROM inputs WHERE id=?", (iid,)).fetchone()
    if existing:
        conn.close()
        return iid, False
    conn.execute(
        "INSERT INTO inputs(id, source, source_id, received_at, sender, "
        "subject, body, raw_json, status) "
        "VALUES (?,?,?,?,?,?,?,?, 'new')",
        (iid, source, source_id, now_iso(), sender, subject, body, raw_json),
    )
    conn.close()
    return iid, True

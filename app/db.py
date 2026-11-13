"""SQLite persistence for dependencies and progress.

Five tables:

* ``daily_versions``  - one row per uploaded station-day (full-replacement).
* ``monthly_generations`` - one row per station-month Parquet build attempt.
* ``monthly_dependencies`` - which daily version ids went into a monthly file.
* ``jobs`` - durable work queue; everything the worker does survives restarts.
* ``reclaim_items`` - two-phase deletion records for reclaimed old files.

The ``reclaimed_at`` / ``reclaim_reason`` columns on the two file tables are
*logical* tombstones: once set they are never cleared, so a reclaimed link can
never become downloadable again even if the physical ``unlink`` had to be
retried (or failed permanently).

WAL + a single writer (the worker thread / request handlers only enqueue)
keeps this safe inside the one and only service instance.
"""
from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from typing import Any, Iterator

from . import config

STATION_OK = re.compile(rf"^{config.STATION_RE}$")
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MONTH_RE = re.compile(r"^\d{4}-\d{2}$")

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=10000;

CREATE TABLE IF NOT EXISTS daily_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    station     TEXT NOT NULL,
    day         TEXT NOT NULL,
    csv_path    TEXT NOT NULL,
    parquet_path TEXT,
    row_count   INTEGER NOT NULL DEFAULT 0,
    status      TEXT NOT NULL CHECK (status IN ('building','ready','failed')),
    error       TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    is_current  INTEGER NOT NULL DEFAULT 0,
    superseded_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_daily_lookup
    ON daily_versions(station, day, is_current);

CREATE TABLE IF NOT EXISTS monthly_generations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    station     TEXT NOT NULL,
    month       TEXT NOT NULL,
    parquet_path TEXT,
    signature   TEXT,
    row_count   INTEGER,
    status      TEXT NOT NULL CHECK (status IN
                    ('building','current','superseded','failed','obsolete')),
    error       TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    published_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_monthly_lookup
    ON monthly_generations(station, month, status);

CREATE TABLE IF NOT EXISTS monthly_dependencies (
    generation_id INTEGER NOT NULL
        REFERENCES monthly_generations(id) ON DELETE CASCADE,
    daily_id      INTEGER NOT NULL
        REFERENCES daily_versions(id) ON DELETE CASCADE,
    day           TEXT NOT NULL,
    PRIMARY KEY (generation_id, daily_id)
);

CREATE TABLE IF NOT EXISTS jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL CHECK (kind IN ('daily_build','monthly_build')),
    ref_id      INTEGER NOT NULL,
    status      TEXT NOT NULL CHECK (status IN
                    ('pending','running','done','failed','obsolete')),
    error       TEXT,
    attempts    INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_jobs_pick ON jobs(status, id);

-- One row per file selected for reclamation. The row is committed (together
-- with the logical tombstone on the owning version) *before* the physical
-- unlink is attempted, so an interrupted or failing delete is always retried
-- after a restart instead of leaking an untracked file.
CREATE TABLE IF NOT EXISTS reclaim_items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    station     TEXT NOT NULL,
    month       TEXT NOT NULL,
    kind        TEXT NOT NULL CHECK (kind IN
                    ('csv','daily_parquet','monthly_parquet')),
    table_name  TEXT NOT NULL CHECK (table_name IN
                    ('daily_versions','monthly_generations')),
    row_id      INTEGER NOT NULL,
    day         TEXT,
    path        TEXT NOT NULL,
    size_bytes  INTEGER,
    status      TEXT NOT NULL CHECK (status IN ('pending','deleted')),
    reason      TEXT,
    error       TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    deleted_at  TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_reclaim_once
    ON reclaim_items(table_name, row_id, kind);
CREATE INDEX IF NOT EXISTS idx_reclaim_status ON reclaim_items(status, id);
"""

# Columns added after the first deployment: applied in place on existing
# volumes so an upgrade never requires a manual migration.
MIGRATIONS = {
    "daily_versions": (
        ("reclaimed_at", "TEXT"),
        ("reclaim_reason", "TEXT"),
    ),
    "monthly_generations": (
        ("reclaimed_at", "TEXT"),
        ("reclaim_reason", "TEXT"),
    ),
}


def get_conn() -> sqlite3.Connection:
    config.ensure_dirs()
    conn = sqlite3.connect(
        config.DB_PATH, timeout=15, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        _migrate(conn)


def _migrate(conn: sqlite3.Connection) -> None:
    """Add columns introduced after the initial deployment (idempotent)."""
    for table, columns in MIGRATIONS.items():
        existing = {
            row["name"] for row in conn.execute(f"PRAGMA table_info({table})")
        }
        for name, decl in columns:
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


@contextmanager
def tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Explicit BEGIN/COMMIT; isolation_level=None means autocommit otherwise."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def validate_station(station: str) -> None:
    if not STATION_OK.match(station):
        raise ValueError("站号只能包含字母、数字、._- 且长度 1-32")


def validate_day(day: str) -> None:
    if not DAY_RE.match(day):
        raise ValueError("日期格式应为 YYYY-MM-DD")
    # Reject e.g. 2026-13-40; calendar check.
    import datetime as _dt

    try:
        _dt.date.fromisoformat(day)
    except ValueError as exc:
        raise ValueError(f"非法日历日期: {day}") from exc


def validate_month(month: str) -> None:
    if not MONTH_RE.match(month):
        raise ValueError("月份格式应为 YYYY-MM")
    import datetime as _dt

    try:
        _dt.date.fromisoformat(month + "-01")
    except ValueError as exc:
        raise ValueError(f"非法月份: {month}") from exc


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None

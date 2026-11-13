"""Reclaim old station-month files.

Retention contract (per station-month, triggered from the web page)
-------------------------------------------------------------------
* every **current** daily file is kept;
* the **last successfully published** monthly file of that station-month and
  the daily files it was built from are kept;
* inputs needed by queued / running / retryable-failed jobs are kept;
* everything else **registered by this service** is reclaimed: older CSV
  uploads, superseded daily Parquet versions, superseded / obsolete monthly
  generations. Files the service never registered are left alone.

Two-phase deletion
------------------
1. One transaction marks the selected rows with a durable tombstone
   (``reclaimed_at`` + ``reclaim_reason``) and inserts one ``reclaim_items``
   row per path. This commit is the point of no return: from then on every
   download entry refuses the link even if the physical delete has not
   happened yet (or fails forever).
2. Each item is unlinked through :func:`downloads.unlink_if_idle`, which skips
   files still streaming to a client. Items that are busy, fail, or are
   interrupted by a crash stay ``pending`` in the database and are retried by
   the sweeper thread after a restart (and periodically while running).

The sweep never holds a database transaction while deleting, and the lease
lock is per file, so a slow download cannot block uploads or the reclamation
of other files.
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from . import config, db, downloads

_sweep_wakeup = threading.Event()
_sweeper_started = False


class ReclaimError(RuntimeError):
    """User-facing refusal to start a reclamation."""


def normalize_month(arg: str) -> str:
    """Accept ``2026-11`` or ``2026-11-10``; return the month."""
    text = arg.strip()
    if len(text) == 10:
        text = text[:7]
    db.validate_month(text)
    return text


def _size(path: str | None) -> int:
    if not path:
        return 0
    try:
        return Path(path).stat().st_size
    except OSError:
        return 0


def _last_published_id(conn: sqlite3.Connection, station: str, month: str):
    """Id of the newest monthly generation that ever got published."""
    row = conn.execute(
        "SELECT MAX(id) AS id FROM monthly_generations "
        "WHERE station=? AND month=? AND status IN ('current','superseded') "
        "AND parquet_path IS NOT NULL",
        (station, month),
    ).fetchone()
    return row["id"] if row and row["id"] is not None else None


# --------------------------------------------------------------------------- #
# Planning (read-only): what is kept (and why) vs what would be reclaimed
# --------------------------------------------------------------------------- #
def build_plan(conn: sqlite3.Connection, station: str, month: str) -> dict:
    db.validate_station(station)
    db.validate_month(month)
    last_pub = _last_published_id(conn, station, month)

    last_pub_deps: set[int] = set()
    if last_pub is not None:
        last_pub_deps = {
            r["daily_id"] for r in conn.execute(
                "SELECT daily_id FROM monthly_dependencies WHERE generation_id=?",
                (last_pub,),
            )
        }

    # A daily version must survive while a queued/running job, or a failed
    # (hence retryable) daily job, still needs its CSV as input.
    job_protected_daily = {
        r["ref_id"] for r in conn.execute(
            "SELECT j.ref_id FROM jobs j JOIN daily_versions d ON d.id=j.ref_id "
            "WHERE j.kind='daily_build' AND d.station=? AND substr(d.day,1,7)=? "
            "AND j.status IN ('pending','running','failed')",
            (station, month),
        )
    }
    # Retryable failed monthly jobs re-read the current daily files, which are
    # protected anyway; record them so the UI can explain it.
    job_monthly = {
        r["ref_id"] for r in conn.execute(
            "SELECT j.ref_id FROM jobs j JOIN monthly_generations g ON g.id=j.ref_id "
            "WHERE j.kind='monthly_build' AND g.station=? AND g.month=? "
            "AND j.status IN ('pending','running')",
            (station, month),
        )
    }
    for gen_id in job_monthly:
        last_pub_deps |= {
            r["daily_id"] for r in conn.execute(
                "SELECT daily_id FROM monthly_dependencies WHERE generation_id=?",
                (gen_id,),
            )
        }

    daily_kept: list[dict] = []
    daily_candidates: list[dict] = []
    for row in conn.execute(
        "SELECT * FROM daily_versions WHERE station=? AND substr(day,1,7)=? "
        "ORDER BY day, id",
        (station, month),
    ).fetchall():
        entry = {
            "id": row["id"],
            "day": row["day"],
            "status": row["status"],
            "is_current": bool(row["is_current"]),
            "row_count": row["row_count"],
            "csv_path": row["csv_path"],
            "parquet_path": row["parquet_path"],
            "reclaimed_at": row["reclaimed_at"],
            "reclaim_reason": row["reclaim_reason"],
        }
        if row["reclaimed_at"]:
            entry["why"] = f"已回收：{row['reclaim_reason']}"
            daily_kept.append(entry)
        elif row["is_current"] and row["status"] == "ready":
            entry["why"] = "当前日档"
            daily_kept.append(entry)
        elif row["id"] in last_pub_deps:
            entry["why"] = "最后成功发布的月文件所依赖"
            daily_kept.append(entry)
        elif row["id"] in job_protected_daily:
            entry["why"] = "排队/运行/待重试作业的输入"
            daily_kept.append(entry)
        elif row["status"] in ("building", "failed"):
            # Unfinished or retryable correction: keep its CSV input.
            entry["why"] = ("转写中的日档" if row["status"] == "building"
                            else "失败待重试的日档")
            daily_kept.append(entry)
        else:
            entry["why"] = "旧版本日档（已被当前版本取代）"
            daily_candidates.append(entry)

    monthly_kept: list[dict] = []
    monthly_candidates: list[dict] = []
    for row in conn.execute(
        "SELECT * FROM monthly_generations WHERE station=? AND month=? ORDER BY id",
        (station, month),
    ).fetchall():
        entry = {
            "id": row["id"],
            "status": row["status"],
            "parquet_path": row["parquet_path"],
            "row_count": row["row_count"],
            "created_at": row["created_at"],
            "published_at": row["published_at"],
            "reclaimed_at": row["reclaimed_at"],
            "reclaim_reason": row["reclaim_reason"],
        }
        if row["reclaimed_at"]:
            entry["why"] = f"已回收：{row['reclaim_reason']}"
            monthly_kept.append(entry)
        elif row["status"] == "current":
            entry["why"] = "当前最新月文件"
            monthly_kept.append(entry)
        elif row["id"] == last_pub:
            entry["why"] = "该站月最后成功发布的月文件"
            monthly_kept.append(entry)
        elif row["status"] in ("building", "failed"):
            entry["why"] = ("补算中的月档" if row["status"] == "building"
                            else "失败待重试的月档")
            monthly_kept.append(entry)
        elif not row["parquet_path"]:
            entry["why"] = "无产物文件（作废/未发布）"
            monthly_kept.append(entry)
        else:
            entry["why"] = ("已过期月文件（非最后发布版本）"
                            if row["status"] == "superseded"
                            else "已作废月文件")
            monthly_candidates.append(entry)

    candidate_bytes = sum(
        _size(e["parquet_path"]) + _size(e["csv_path"]) for e in daily_candidates
    ) + sum(_size(e["parquet_path"]) for e in monthly_candidates)
    return {
        "station": station,
        "month": month,
        "last_published_id": last_pub,
        "daily_kept": daily_kept,
        "daily_candidates": daily_candidates,
        "monthly_kept": monthly_kept,
        "monthly_candidates": monthly_candidates,
        "bytes": candidate_bytes,
    }


# --------------------------------------------------------------------------- #
# Phase 1: durable tombstone + pending delete records
# --------------------------------------------------------------------------- #
def request_reclaim(conn: sqlite3.Connection, station: str, month: str) -> dict:
    """Tombstone old files of a station-month. Returns a small summary."""
    db.validate_station(station)
    db.validate_month(month)
    with db.tx(conn):
        # Re-plan inside the write transaction: a publication or upload that
        # committed since the page was rendered must be respected.
        p = build_plan(conn, station, month)
        now_sql = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"
        for row in p["daily_candidates"]:
            conn.execute(
                f"UPDATE daily_versions SET reclaimed_at={now_sql}, "
                "reclaim_reason=? WHERE id=? AND reclaimed_at IS NULL",
                (row["why"], row["id"]),
            )
            for kind, path in (("daily_parquet", row["parquet_path"]),
                               ("csv", row["csv_path"])):
                if path:
                    _add_item(conn, station, month, kind, "daily_versions",
                              row["id"], row["day"], path, row["why"])
        for row in p["monthly_candidates"]:
            conn.execute(
                f"UPDATE monthly_generations SET reclaimed_at={now_sql}, "
                "reclaim_reason=? WHERE id=? AND reclaimed_at IS NULL",
                (row["why"], row["id"]),
            )
            if row["parquet_path"]:
                _add_item(conn, station, month, "monthly_parquet",
                          "monthly_generations", row["id"], None,
                          row["parquet_path"], row["why"])
        return {
            "station": station,
            "month": month,
            "daily": len(p["daily_candidates"]),
            "monthly": len(p["monthly_candidates"]),
            "bytes": p["bytes"],
        }


def _add_item(conn: sqlite3.Connection, station: str, month: str, kind: str,
              table_name: str, row_id: int, day: str | None, path: str,
              reason: str) -> None:
    try:
        conn.execute(
            "INSERT INTO reclaim_items(station, month, kind, table_name, row_id, "
            "day, path, size_bytes, status, reason) "
            "VALUES (?,?,?,?,?,?,?,?, 'pending', ?)",
            (station, month, kind, table_name, row_id, day, path,
             _size(path), reason),
        )
    except sqlite3.IntegrityError:
        # Already recorded by an earlier request; the sweeper keeps retrying it.
        pass


# --------------------------------------------------------------------------- #
# Phase 2: physical deletion (idempotent, resumable)
# --------------------------------------------------------------------------- #
def sweep(limit: int = 200) -> dict:
    """Process pending deletions once. Never holds a DB write lock per file."""
    summary = {"deleted": 0, "busy": 0, "failed": 0}
    with db.get_conn() as conn:
        items = conn.execute(
            "SELECT * FROM reclaim_items WHERE status='pending' ORDER BY id LIMIT ?",
            (limit,),
        ).fetchall()
        for item in items:
            outcome, error = downloads.unlink_if_idle(item["path"])
            if outcome == "deleted":
                conn.execute(
                    "UPDATE reclaim_items SET status='deleted', error=NULL, "
                    "deleted_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                    (item["id"],),
                )
                summary["deleted"] += 1
            elif outcome == "busy":
                # A download is streaming this file: retry on the next sweep.
                summary["busy"] += 1
            else:
                conn.execute(
                    "UPDATE reclaim_items SET error=? WHERE id=?",
                    (error, item["id"]),
                )
                summary["failed"] += 1
    return summary


def resume_interrupted() -> dict:
    """Startup hook: finish deletions left pending by a crash.

    Every ``pending`` row was committed before the unlink was attempted, so
    nothing is lost; a row whose path is already gone is completed.
    """
    return sweep(limit=100_000)


# --------------------------------------------------------------------------- #
# Periodic sweeper thread (retries busy/failed deletions too)
# --------------------------------------------------------------------------- #
def wakeup() -> None:
    _sweep_wakeup.set()


def _sweeper_loop(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            sweep()
        except Exception:
            pass
        _sweep_wakeup.wait(timeout=2.0)
        _sweep_wakeup.clear()


def start_sweeper() -> tuple[threading.Thread, threading.Event]:
    global _sweeper_started
    if _sweeper_started:
        raise RuntimeError("reclaim sweeper already started")
    _sweeper_started = True
    stop_event = threading.Event()
    thread = threading.Thread(target=_sweeper_loop, args=(stop_event,),
                              name="reclaim-sweeper", daemon=True)
    thread.start()
    return thread, stop_event

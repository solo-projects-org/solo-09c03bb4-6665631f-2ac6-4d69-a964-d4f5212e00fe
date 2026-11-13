"""Background worker: durable job queue, crash recovery and stale-job rules.

Guarantees implemented here
---------------------------
1. Daily Parquet is built and atomically published *before* any metadata says
   "current"; a failed replacement leaves the previous day file untouched.
2. A monthly job re-reads the set of current daily files both at dequeue time
   and again inside the publishing transaction. A job that was enqueued against
   an older state ("过期作业") is marked ``obsolete`` and never published; the
   newest pending job for that station-month continues the recomputation.
3. The last complete monthly file is only marked ``superseded`` inside the same
   transaction that publishes the new one. If the process dies at any earlier
   point (write failure, registration failure, SIGKILL), the old monthly file
   stays ``current`` and downloadable; on restart the interrupted job goes back
   to ``pending`` and resumes.
4. Nothing ever downloads a ``.tmp`` or a missing file: download paths are
   built only from registered rows and re-checked on disk.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

from . import config, db, fsutil, parquet

# One worker thread per process; the service is deployed as a single instance.
_wakeup = threading.Event()
_worker_started = False


def wakeup() -> None:
    _wakeup.set()


# --------------------------------------------------------------------------- #
# Enqueue helpers (called from request handlers)
# --------------------------------------------------------------------------- #
def enqueue_daily_build(conn: sqlite3.Connection, daily_id: int) -> int:
    cur = conn.execute(
        "INSERT INTO jobs(kind, ref_id, status) VALUES ('daily_build', ?, 'pending')",
        (daily_id,),
    )
    return int(cur.lastrowid)


def enqueue_monthly_build(conn: sqlite3.Connection, station: str, month: str) -> int:
    """Register a new monthly generation attempt and its job."""
    cur = conn.execute(
        "INSERT INTO monthly_generations(station, month, status) "
        "VALUES (?, ?, 'building')",
        (station, month),
    )
    gen_id = int(cur.lastrowid)
    conn.execute(
        "INSERT INTO jobs(kind, ref_id, status) VALUES ('monthly_build', ?, 'pending')",
        (gen_id,),
    )
    return gen_id


# --------------------------------------------------------------------------- #
# Startup recovery
# --------------------------------------------------------------------------- #
def recover() -> dict[str, object]:
    """Rebuild a consistent workable state after a clean start or a crash."""
    report: dict[str, object] = {"tmp_removed": [], "reset_jobs": [], "orphans_removed": []}
    report["tmp_removed"] = fsutil.cleanup_tmp_dir()

    with db.get_conn() as conn:
        with db.tx(conn):
            # 1. Jobs interrupted mid-flight go back to the queue.
            rows = conn.execute(
                "SELECT id, kind, ref_id FROM jobs WHERE status = 'running'"
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE jobs SET status='pending', updated_at="
                    "strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                    (row["id"],),
                )
                report["reset_jobs"].append(row["id"])

            # 2. Daily rows left in 'building' after a crash: the parquet was
            #    never registered, so the attempt becomes a retryable failure
            #    and any half-published file is removed.
            for row in conn.execute(
                "SELECT id, station, day, parquet_path FROM daily_versions "
                "WHERE status='building'"
            ).fetchall():
                stale_file = config.daily_parquet_path(
                    row["station"], row["day"], row["id"]
                )
                if stale_file.exists():
                    try:
                        stale_file.unlink()
                        report["orphans_removed"].append(str(stale_file))
                    except OSError:
                        pass
                conn.execute(
                    "UPDATE daily_versions SET status='failed', parquet_path=NULL, "
                    "error='进程在转写过程中退出，可重试' WHERE id=?",
                    (row["id"],),
                )
                conn.execute(
                    "UPDATE jobs SET status='failed', error='进程中途退出' "
                    "WHERE kind='daily_build' AND ref_id=? AND status IN ('pending','running')",
                    (row["id"],),
                )

            # 3. Monthly generations left in 'building': keep them as pending
            #    work (their job was reset above); remove any orphaned file.
            for row in conn.execute(
                "SELECT id, parquet_path FROM monthly_generations WHERE status='building'"
            ).fetchall():
                if row["parquet_path"]:
                    p = Path(row["parquet_path"])
                    if p.exists():
                        try:
                            p.unlink()
                        except OSError:
                            pass
                        report["orphans_removed"].append(str(p))
                    conn.execute(
                        "UPDATE monthly_generations SET parquet_path=NULL WHERE id=?",
                        (row["id"],),
                    )

            # 4. Database rows pointing at files that vanished from the volume.
            #    Reclaimed rows are skipped on purpose: their missing file is
            #    the expected end state, and the tombstone (not parquet_path)
            #    decides what a download says.
            for table in ("daily_versions", "monthly_generations"):
                for row in conn.execute(
                    f"SELECT id AS rid, parquet_path FROM {table} "
                    "WHERE parquet_path IS NOT NULL AND reclaimed_at IS NULL"
                ).fetchall():
                    if not Path(row["parquet_path"]).is_file():
                        conn.execute(
                            f"UPDATE {table} SET parquet_path=NULL, "
                            "error=COALESCE(error, '数据卷上的文件丢失') WHERE id=?",
                            (row["rid"],),
                        )

            # 5. Parquet files on disk that no registered row owns: they can
            #    never be reached through a download entry and are deleted.
            #    Reclaimed rows are excluded: their paths are deliberately
            #    pending deletion in reclaim_items (which retries them), and
            #    they might momentarily still exist on disk.
            registered = {
                str(r[0])
                for r in conn.execute(
                    "SELECT parquet_path FROM daily_versions "
                    "WHERE parquet_path IS NOT NULL AND reclaimed_at IS NULL "
                    "UNION ALL "
                    "SELECT parquet_path FROM monthly_generations "
                    "WHERE parquet_path IS NOT NULL AND reclaimed_at IS NULL "
                    "UNION ALL "
                    "SELECT path FROM reclaim_items WHERE status='pending'"
                ).fetchall()
            }
            for base in (config.DAILY_DIR, config.MONTHLY_DIR):
                if not base.exists():
                    continue
                for p in base.rglob("*.parquet"):
                    if str(p) not in registered:
                        try:
                            p.unlink()
                            report["orphans_removed"].append(str(p))
                        except OSError:
                            pass
    return report


# --------------------------------------------------------------------------- #
# Job selection
# --------------------------------------------------------------------------- #
def _pick_job(conn: sqlite3.Connection) -> sqlite3.Row | None:
    """Pick the oldest pending job, coalescing redundant monthly jobs.

    For a given station-month only the newest pending monthly job can ever be
    publishable; older ones are marked obsolete immediately (they are the
    "过期作业" that must not be published).
    """
    pending = conn.execute(
        "SELECT id, kind, ref_id FROM jobs WHERE status='pending' ORDER BY id"
    ).fetchall()
    if not pending:
        return None

    newest_monthly: dict[tuple[str, str], int] = {}
    monthly_rows: dict[int, sqlite3.Row] = {}
    for job in pending:
        if job["kind"] != "monthly_build":
            continue
        gen = conn.execute(
            "SELECT id, station, month FROM monthly_generations WHERE id=?",
            (job["ref_id"],),
        ).fetchone()
        if gen is None:
            continue
        monthly_rows[job["id"]] = job
        key = (gen["station"], gen["month"])
        if key not in newest_monthly or job["id"] > newest_monthly[key]:
            newest_monthly[key] = job["id"]

    for job_id, job in monthly_rows.items():
        gen = conn.execute(
            "SELECT station, month FROM monthly_generations WHERE id=?",
            (job["ref_id"],),
        ).fetchone()
        key = (gen["station"], gen["month"])
        if newest_monthly.get(key) != job_id:
            conn.execute(
                "UPDATE jobs SET status='obsolete', "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                (job_id,),
            )
            conn.execute(
                "UPDATE monthly_generations SET status='obsolete', "
                "error='被更新的补算作业取代' WHERE id=? AND status='building'",
                (job["ref_id"],),
            )

    # Daily builds always go first: a monthly merge whose daily inputs are
    # still pending would fail, and a newer correction may obsolete it anyway.
    chosen = conn.execute(
        "SELECT id, kind, ref_id FROM jobs WHERE status='pending' "
        "ORDER BY CASE kind WHEN 'daily_build' THEN 0 ELSE 1 END, id LIMIT 1"
    ).fetchone()
    return chosen


# --------------------------------------------------------------------------- #
# Daily build
# --------------------------------------------------------------------------- #
def _run_daily_build(conn: sqlite3.Connection, job_id: int, daily_id: int) -> None:
    row = conn.execute(
        "SELECT * FROM daily_versions WHERE id=?", (daily_id,)
    ).fetchone()
    if row is None:
        raise RuntimeError(f"日档记录 {daily_id} 不存在")

    csv_path = Path(row["csv_path"])
    if not csv_path.is_file():
        raise RuntimeError(f"原始 CSV 已丢失: {csv_path}")

    # Re-parse from the persisted CSV: the build must not depend on request
    # memory, and a re-run after restart takes exactly the same input.
    from .csv_validation import parse_csv

    parsed = parse_csv(csv_path.read_bytes())

    final_path = config.daily_parquet_path(parsed.station, parsed.day, daily_id)
    tmp_path = fsutil.tmp_target(final_path, f"daily{daily_id}")
    fsutil.discard_tmp(tmp_path)
    written = parquet.write_daily_parquet(parsed.rows, tmp_path)
    parquet.verify_daily_parquet(tmp_path, parsed.row_count)
    fsutil.atomic_publish(tmp_path, final_path)
    # From here the new Parquet is durably on disk; metadata transaction next.
    # If we crash between the rename and the commit, startup recovery treats
    # this version as a failed retry and never serves it as current. Its file
    # remains harmless (orphan-cleanup keeps files owned by registered rows).

    with db.tx(conn):
        conn.execute(
            "UPDATE daily_versions SET parquet_path=?, row_count=?, status='ready', "
            "error=NULL, is_current=1 WHERE id=?",
            (str(final_path), written, daily_id),
        )
        conn.execute(
            "UPDATE daily_versions SET is_current=0, superseded_at="
            "strftime('%Y-%m-%dT%H:%M:%fZ','now') "
            "WHERE station=? AND day=? AND id<>? AND is_current=1",
            (parsed.station, parsed.day, daily_id),
        )
        # The day changed: any monthly file covering it is now stale. It stays
        # downloadable, but is flagged until a fresh generation is published.
        month = parsed.day[:7]
        conn.execute(
            "UPDATE monthly_generations SET status='superseded', "
            "published_at=COALESCE(published_at, created_at) "
            "WHERE station=? AND month=? AND status='current'",
            (parsed.station, month),
        )
        enqueue_monthly_build(conn, parsed.station, month)
        conn.execute(
            "UPDATE jobs SET status='done', error=NULL, attempts=attempts+1, "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
            (job_id,),
        )


# --------------------------------------------------------------------------- #
# Monthly build
# --------------------------------------------------------------------------- #
def _current_daily_deps(conn: sqlite3.Connection, station: str, month: str):
    return conn.execute(
        "SELECT id, day, parquet_path, row_count FROM daily_versions "
        "WHERE station=? AND substr(day,1,7)=? AND is_current=1 AND status='ready' "
        "ORDER BY day",
        (station, month),
    ).fetchall()


def _month_signature(conn: sqlite3.Connection, station: str, month: str):
    """Canonical signature of *all current* daily files of a station-month.

    Computed through the same SQLite connection that holds the transaction, so
    the re-check under the write lock observes exactly the committed state.
    """
    deps = _current_daily_deps(conn, station, month)
    signature = ";".join(
        f"{r['day']}=v{r['id']}:rows{r['row_count']}" for r in deps
    )
    return signature, [dict(r) for r in deps]


def _obsolete_monthly(conn: sqlite3.Connection, job_id: int, gen_id: int,
                      reason: str) -> None:
    conn.execute(
        "UPDATE jobs SET status='obsolete', "
        "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
        (job_id,),
    )
    conn.execute(
        "UPDATE monthly_generations SET status='obsolete', error=? "
        "WHERE id=? AND status='building'",
        (reason, gen_id),
    )


def _run_monthly_build(conn: sqlite3.Connection, job_id: int, gen_id: int) -> None:
    gen = conn.execute(
        "SELECT * FROM monthly_generations WHERE id=?", (gen_id,)
    ).fetchone()
    if gen is None:
        raise RuntimeError(f"月生成记录 {gen_id} 不存在")
    station, month = gen["station"], gen["month"]

    # A newer pending monthly job for the same station-month means this one is
    # already stale even before it starts.
    newer = conn.execute(
        "SELECT j.id FROM jobs j JOIN monthly_generations g ON g.id=j.ref_id "
        "WHERE j.kind='monthly_build' AND j.status='pending' "
        "AND g.station=? AND g.month=? AND j.id<>? ORDER BY j.id DESC LIMIT 1",
        (station, month, job_id),
    ).fetchone()
    if newer is not None:
        _obsolete_monthly(conn, job_id, gen_id, "排队期间日档再次变更，由更新的作业接算")
        return

    # All current daily files of the month must be fully built first.
    pending_daily = conn.execute(
        "SELECT COUNT(*) AS c FROM jobs j JOIN daily_versions d ON d.id=j.ref_id "
        "WHERE j.kind='daily_build' AND j.status IN ('pending','running') "
        "AND d.station=? AND substr(d.day,1,7)=?",
        (station, month),
    ).fetchone()["c"]
    deps = _current_daily_deps(conn, station, month)
    if pending_daily or not deps or any(
        not Path(r["parquet_path"]).is_file() for r in deps
    ):
        raise RuntimeError("本月仍有日档未转写完成，月文件等待日档就绪")

    signature, dep_list = _month_signature(conn, station, month)
    daily_paths = [r["parquet_path"] for r in dep_list]

    final_path = config.monthly_parquet_path(station, month, gen_id)
    # A previous attempt of this same generation may have left an orphan file.
    if final_path.exists():
        final_path.unlink()
    tmp_path = fsutil.tmp_target(final_path, f"monthly{gen_id}")
    fsutil.discard_tmp(tmp_path)
    written = parquet.merge_monthly_parquet(daily_paths, tmp_path)
    if written != sum(r["row_count"] for r in dep_list):
        fsutil.discard_tmp(tmp_path)
        raise RuntimeError("月文件行数与日档合计不一致，拒绝发布")
    fsutil.atomic_publish(tmp_path, final_path)
    # New file is fully on disk. Publishing metadata transactionally next;
    # until this COMMIT, the previous monthly file remains 'current'.

    with db.tx(conn):
        # Re-check under the write lock: a correction committed while we were
        # merging makes this job stale and it must not be published.
        current_sig, current_deps = _month_signature(conn, station, month)
        if current_sig != signature:
            try:
                final_path.unlink()
            except OSError:
                pass
            _obsolete_monthly(
                conn, job_id, gen_id,
                "合并期间日档发生变更，本作业已过期，由后续作业接算",
            )
            return
        if any(not Path(r["parquet_path"]).is_file() for r in current_deps):
            try:
                final_path.unlink()
            except OSError:
                pass
            raise RuntimeError("发布前发现日档文件缺失，拒绝发布")

        conn.execute(
            "UPDATE monthly_generations SET status='superseded' "
            "WHERE station=? AND month=? AND status='current'",
            (station, month),
        )
        conn.execute(
            "UPDATE monthly_generations SET parquet_path=?, signature=?, row_count=?, "
            "status='current', error=NULL, "
            "published_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
            (str(final_path), signature, written, gen_id),
        )
        conn.execute("DELETE FROM monthly_dependencies WHERE generation_id=?", (gen_id,))
        conn.executemany(
            "INSERT INTO monthly_dependencies(generation_id, daily_id, day) "
            "VALUES (?, ?, ?)",
            [(gen_id, r["id"], r["day"]) for r in dep_list],
        )
        conn.execute(
            "UPDATE jobs SET status='done', error=NULL, attempts=attempts+1, "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
            (job_id,),
        )


# --------------------------------------------------------------------------- #
# Worker loop
# --------------------------------------------------------------------------- #
def _mark_failed(conn: sqlite3.Connection, job: sqlite3.Row, exc: Exception) -> None:
    msg = f"{type(exc).__name__}: {exc}"[:1000]
    with db.tx(conn):
        conn.execute(
            "UPDATE jobs SET status='failed', error=?, attempts=attempts+1, "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
            (msg, job["id"]),
        )
        if job["kind"] == "daily_build":
            conn.execute(
                "UPDATE daily_versions SET status='failed', error=? WHERE id=? "
                "AND status='building'",
                (msg, job["ref_id"]),
            )
        else:
            conn.execute(
                "UPDATE monthly_generations SET status='failed', error=? WHERE id=? "
                "AND status='building'",
                (msg, job["ref_id"]),
            )


def _process_one(conn: sqlite3.Connection) -> bool:
    with db.tx(conn):
        job = _pick_job(conn)
        if job is None:
            return False
        conn.execute(
            "UPDATE jobs SET status='running', "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
            (job["id"],),
        )
    try:
        if job["kind"] == "daily_build":
            _run_daily_build(conn, job["id"], job["ref_id"])
        else:
            _run_monthly_build(conn, job["id"], job["ref_id"])
    except Exception as exc:  # recorded as retryable failure, worker survives
        # The worker runs one task at a time, so every staging file left by
        # this attempt is safe to remove; the registered final paths are never
        # in tmp and therefore never touched.
        for child in config.TMP_DIR.glob("*.tmp"):
            fsutil.discard_tmp(child)
        _mark_failed(conn, job, exc)
    return True


def worker_loop(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            conn = db.get_conn()
            try:
                did_work = _process_one(conn)
            finally:
                conn.close()
        except Exception:
            # A SQLite hiccup must not kill the worker; retry after a pause.
            did_work = False
            time.sleep(1.0)
        if not did_work:
            _wakeup.wait(timeout=2.0)
            _wakeup.clear()


def start_worker() -> tuple[threading.Thread, threading.Event]:
    global _worker_started
    if _worker_started:
        raise RuntimeError("worker already started")
    _worker_started = True
    db.init_db()
    recover()
    stop_event = threading.Event()
    thread = threading.Thread(target=worker_loop, args=(stop_event,),
                              name="monthly-worker", daemon=True)
    thread.start()
    return thread, stop_event

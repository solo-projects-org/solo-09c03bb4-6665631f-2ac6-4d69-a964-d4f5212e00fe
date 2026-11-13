"""In-process unit tests for the worker's correctness rules.

No HTTP server here: jobs are processed one at a time via worker._process_one,
which makes failure injection and crash-window simulation deterministic.
"""
import sqlite3

import pytest

from app import config, db, fsutil, parquet, worker
from helpers import make_csv


@pytest.fixture()
def isolated(monkeypatch, tmp_path):
    vol = (tmp_path / "data").resolve()
    # config captured constants at import time; repoint them for this test.
    monkeypatch.setattr(config, "DATA_VOLUME", vol)
    monkeypatch.setattr(config, "DB_PATH", vol / "state.db")
    monkeypatch.setattr(config, "INBOX_DIR", vol / "inbox")
    monkeypatch.setattr(config, "DAILY_DIR", vol / "daily")
    monkeypatch.setattr(config, "MONTHLY_DIR", vol / "monthly")
    monkeypatch.setattr(config, "TMP_DIR", vol / "tmp")
    config.ensure_dirs()
    db.init_db()
    return vol


def _seed_upload(station: str, day: str, values=None) -> int:
    """Persist a validated upload exactly like the HTTP handler does."""
    raw = make_csv(station, day, values or {h: 10 for h in range(24)})
    with db.get_conn() as conn:
        with db.tx(conn):
            cur = conn.execute(
                "INSERT INTO daily_versions(station, day, csv_path, status) "
                "VALUES (?, ?, '', 'building')",
                (station, day),
            )
            daily_id = int(cur.lastrowid)
        csv_path = config.inbox_csv_path(station, day, daily_id)
        conn.execute("UPDATE daily_versions SET csv_path=? WHERE id=?",
                     (str(csv_path), daily_id))
    csv_path.write_bytes(raw)
    with db.get_conn() as conn:
        with db.tx(conn):
            worker.enqueue_daily_build(conn, daily_id)
    return daily_id


def _drain(conn, max_jobs: int = 50) -> None:
    for _ in range(max_jobs):
        if not worker._process_one(conn):
            return
    raise AssertionError("queue did not drain")


def _one(conn: sqlite3.Connection, sql: str, args=()):
    row = conn.execute(sql, args).fetchone()
    assert row is not None, f"no row: {sql} {args}"
    return row


def _latest_generation(conn, station: str, month: str = "2026-11"):
    return _one(conn,
        "SELECT * FROM monthly_generations WHERE station=? AND month=? "
        "ORDER BY id DESC LIMIT 1", (station, month))


def test_daily_failure_keeps_previous_day_and_monthly(isolated, monkeypatch):
    conn = db.get_conn()
    # First successful version + its monthly file.
    _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _drain(conn)
    gen1 = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='current'")["id"]
    old_monthly = config.monthly_parquet_path("S1", "2026-11", gen1)
    assert old_monthly.is_file()

    # Second version: force the Parquet write to fail.
    v2 = _seed_upload("S1", "2026-11-10", {h: 12 for h in range(24)})
    original_write = parquet.write_daily_parquet

    def boom(*_a, **_k):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(parquet, "write_daily_parquet", boom)
    _drain(conn)

    failed = _one(conn, "SELECT status, error FROM daily_versions WHERE id=?", (v2,))
    assert failed["status"] == "failed" and "disk failure" in failed["error"]
    # v1 remains current and its files are untouched; monthly gen1 still current.
    cur = _one(conn,
        "SELECT id, is_current, status FROM daily_versions WHERE station='S1' "
        "AND day='2026-11-10' AND is_current=1")
    assert cur["id"] != v2 and cur["status"] == "ready"
    m = _one(conn, "SELECT status FROM monthly_generations WHERE id=?", (gen1,))
    assert m["status"] == "current" and old_monthly.is_file()

    # Retry succeeds after the fault clears (restore the original function).
    monkeypatch.setattr(parquet, "write_daily_parquet", original_write)
    job = _one(conn, "SELECT id FROM jobs WHERE ref_id=? AND status='failed'", (v2,))
    conn.execute("UPDATE jobs SET status='pending' WHERE id=?", (job["id"],))
    conn.execute("UPDATE daily_versions SET status='building', error=NULL WHERE id=?", (v2,))
    _drain(conn)
    cur2 = _one(conn,
        "SELECT id FROM daily_versions WHERE station='S1' AND day='2026-11-10' AND is_current=1")
    assert cur2["id"] == v2
    latest = _latest_generation(conn, "S1")
    assert latest["status"] == "current" and latest["id"] > gen1
    # Old monthly still on disk and its row marked superseded.
    assert old_monthly.is_file()
    old = _one(conn, "SELECT status FROM monthly_generations WHERE id=?", (gen1,))
    assert old["status"] == "superseded"


def test_stale_monthly_job_is_never_published(isolated):
    conn = db.get_conn()
    _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _drain(conn)
    gen1 = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='current'")["id"]

    # Two corrections: let each daily build finish, but hold both monthly jobs
    # in the queue by only processing daily_build entries.
    v2 = _seed_upload("S1", "2026-11-10", {h: 11 for h in range(24)})
    worker._process_one(conn)  # v2 daily -> gen2 monthly job pending
    v3 = _seed_upload("S1", "2026-11-10", {h: 12 for h in range(24)})
    worker._process_one(conn)  # v3 daily -> gen3 monthly job pending
    pending_gens = conn.execute(
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='building' "
        "ORDER BY id").fetchall()
    assert len(pending_gens) == 2
    gen2, gen3 = pending_gens[0]["id"], pending_gens[1]["id"]

    # The picker must obsolete gen2's job in favour of the newest gen3 job.
    _drain(conn)
    states = {r["id"]: r["status"] for r in conn.execute(
        "SELECT id, status FROM monthly_generations WHERE station='S1'")}
    assert states[gen2] == "obsolete"
    assert states[gen3] == "current"
    assert states[gen1] == "superseded"
    # Published file corresponds exactly to the current (v3) daily set.
    gen3_row = _one(conn,
        "SELECT signature, parquet_path FROM monthly_generations WHERE id=?", (gen3,))
    deps = conn.execute(
        "SELECT daily_id FROM monthly_dependencies WHERE generation_id=?", (gen3,)).fetchall()
    assert [d["daily_id"] for d in deps] == [v3]
    assert f"v{v3}" in gen3_row["signature"]
    # The obsolete generation never got a downloadable file.
    g2 = _one(conn, "SELECT parquet_path FROM monthly_generations WHERE id=?", (gen2,))
    assert g2["parquet_path"] is None


def test_signature_recheck_rejects_publish_after_mid_merge_change(isolated, monkeypatch):
    conn = db.get_conn()
    _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _drain(conn)
    gen1 = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='current'")["id"]

    # Correction v2: build its daily so gen2's monthly job is queued.
    _seed_upload("S1", "2026-11-10", {h: 11 for h in range(24)})
    worker._process_one(conn)  # v2 daily -> gen2 pending
    gen2 = _latest_generation(conn, "S1")["id"]

    # A further correction (v4) is committed *during* gen2's merge window,
    # after the ready-check passed but before the publishing transaction.
    original_merge = parquet.merge_monthly_parquet
    call = {"n": 0}

    def merge_then_correct(daily_paths, tmp_path):
        result = original_merge(daily_paths, tmp_path)
        call["n"] += 1
        if call["n"] == 1:
            # Merge file is fully written but not yet registered; a correction
            # lands and its daily build commits a new current signature.
            _seed_upload("S1", "2026-11-10", {h: 12 for h in range(24)})
            worker._process_one(conn)  # v4 daily build -> new signature
        return result

    monkeypatch.setattr(parquet, "merge_monthly_parquet", merge_then_correct)
    worker._process_one(conn)  # gen2 merge -> publish re-check must reject it
    monkeypatch.setattr(parquet, "merge_monthly_parquet", original_merge)
    _drain(conn)

    g2 = _one(conn, "SELECT status, error FROM monthly_generations WHERE id=?", (gen2,))
    assert g2["status"] == "obsolete" and "过期" in (g2["error"] or "")
    # A final current generation exists and matches the newest daily version.
    latest = _latest_generation(conn, "S1")
    assert latest["status"] == "current"
    newest_daily = conn.execute(
        "SELECT max(id) AS id FROM daily_versions").fetchone()["id"]
    cur_daily = _one(conn,
        "SELECT id FROM daily_versions WHERE station='S1' AND is_current=1")
    assert cur_daily["id"] == newest_daily
    deps = conn.execute(
        "SELECT daily_id FROM monthly_dependencies WHERE generation_id=?",
        (latest["id"],)).fetchall()
    assert [d["daily_id"] for d in deps] == [newest_daily]
    # gen1 row: superseded, never lost; its file remains downloadable.
    assert _one(conn, "SELECT status FROM monthly_generations WHERE id=?",
                (gen1,))["status"] == "superseded"


def test_monthly_build_waits_for_pending_daily(isolated):
    conn = db.get_conn()
    _seed_upload("S1", "2026-11-10")
    # Build the first daily, but leave its monthly job pending.
    worker._process_one(conn)
    # A second correction: its daily job must run before any monthly publish.
    _seed_upload("S1", "2026-11-10", {h: 5 for h in range(24)})
    _drain(conn)
    latest = _latest_generation(conn, "S1")
    assert latest["status"] == "current"
    # The published monthly depends on the newest daily only.
    deps = conn.execute(
        "SELECT daily_id FROM monthly_dependencies WHERE generation_id=?",
        (latest["id"],)).fetchall()
    newest = conn.execute("SELECT max(id) AS id FROM daily_versions").fetchone()["id"]
    assert [d["daily_id"] for d in deps] == [newest]


def test_recovery_after_crash_resets_running_and_removes_orphans(isolated):
    conn = db.get_conn()
    _seed_upload("S1", "2026-11-10")
    _drain(conn)
    gen1 = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='current'")["id"]
    good = config.monthly_parquet_path("S1", "2026-11", gen1)

    # A correction in flight: daily v2 + its monthly generation, all 'running'.
    v2 = _seed_upload("S1", "2026-11-10", {h: 9 for h in range(24)})
    worker._process_one(conn)  # v2 daily builds and enqueues a new generation
    gen2 = _latest_generation(conn, "S1")["id"]
    assert gen2 > gen1
    conn.execute("UPDATE jobs SET status='running'")

    # Stray files a real crash could leave behind.
    stray_month = config.monthly_parquet_path("S1", "2026-11", gen2)
    stray_month.parent.mkdir(parents=True, exist_ok=True)
    stray_month.write_bytes(b"PAR1partial")
    stray_tmp = config.TMP_DIR / "leftover.tmp"
    stray_tmp.write_bytes(b"x")
    unregistered = config.DAILY_DIR / "S1" / "daily-2026-11-11-v999.parquet"
    unregistered.parent.mkdir(parents=True, exist_ok=True)
    unregistered.write_bytes(b"PAR1ghost")
    # Emulate the daily row being interrupted between parquet rename and commit.
    conn.execute("UPDATE daily_versions SET status='building' WHERE id=?", (v2,))
    conn.close()

    conn = db.get_conn()
    # Every interrupted ('running') job is reset; already-finished jobs stay.
    running = conn.execute("SELECT COUNT(*) AS c FROM jobs WHERE status='running'").fetchone()["c"]
    assert running > 0
    report = worker.recover()
    assert stray_tmp.name in [p.split("/")[-1] for p in report["tmp_removed"]]
    assert not stray_month.exists()
    assert not unregistered.exists()
    assert good.is_file()  # last complete monthly survives

    conn = db.get_conn()
    leftover_running = conn.execute(
        "SELECT COUNT(*) AS c FROM jobs WHERE status='running'").fetchone()["c"]
    assert leftover_running == 0
    # Every previously-running job is back in the queue.
    reset_rows = conn.execute(
        "SELECT status FROM jobs WHERE id IN (%s)" %
        ",".join(str(i) for i in range(1, 5))).fetchall()
    assert all(r["status"] in ("pending", "done", "obsolete", "failed") for r in reset_rows)
    assert _one(conn, "SELECT status FROM daily_versions WHERE id=?", (v2,))["status"] == "failed"
    gen2_row = _one(conn,
        "SELECT status, parquet_path FROM monthly_generations WHERE id=?", (gen2,))
    assert gen2_row["status"] == "building" and gen2_row["parquet_path"] is None

    # Retry after recovery converges and gen1 remains available throughout.
    conn.execute("UPDATE daily_versions SET status='building', error=NULL WHERE id=?", (v2,))
    conn.execute(
        "UPDATE jobs SET status='pending' WHERE kind='daily_build' AND ref_id=?", (v2,))
    _drain(conn)
    assert _latest_generation(conn, "S1")["status"] == "current"
    assert good.is_file()


def test_only_changed_month_is_rebuilt(isolated):
    conn = db.get_conn()
    _seed_upload("S1", "2026-10-31")
    _seed_upload("S1", "2026-11-10")
    _drain(conn)
    oct_gen = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND month='2026-10' "
        "AND status='current'")["id"]

    _seed_upload("S1", "2026-11-10", {h: 33 for h in range(24)})
    _drain(conn)
    # October generation untouched; November got a new one.
    assert _one(conn, "SELECT status FROM monthly_generations WHERE id=?",
                (oct_gen,))["status"] == "current"
    nov_gens = conn.execute(
        "SELECT id FROM monthly_generations WHERE station='S1' AND month='2026-11'").fetchall()
    assert len(nov_gens) == 2

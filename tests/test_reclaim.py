"""Reclaim tests: retention contract, tombstones, leases, crash resume.

Mix of in-process worker tests (deterministic protection/failure injection)
and real-HTTP tests (stream a file while it is being reclaimed, disconnect,
slow client does not block other work).
"""
import os
import sqlite3
import threading
import time
from pathlib import Path

import httpx
import pytest

from app import config, db, downloads, worker, reclaim, fsutil
from conftest import Server, free_port
from helpers import (dense_csv, hours_csv, make_csv, redirect_message, upload,
                     wait_for)


# --------------------------------------------------------------------------- #
# In-process helpers
# --------------------------------------------------------------------------- #
@pytest.fixture()
def isolated(monkeypatch, tmp_path):
    vol = (tmp_path / "data").resolve()
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
    raw = make_csv(station, day, values or {h: 10 for h in range(24)})
    with db.get_conn() as conn:
        with db.tx(conn):
            cur = conn.execute(
                "INSERT INTO daily_versions(station, day, csv_path, status) "
                "VALUES (?, ?, '', 'building')", (station, day),
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


def _one(conn, sql, args=()):
    row = conn.execute(sql, args).fetchone()
    assert row is not None, f"no row: {sql} {args}"
    return row


def _month_rows(conn, station="S1"):
    return {r["id"]: dict(r) for r in conn.execute(
        "SELECT * FROM monthly_generations WHERE station=?", (station,)).fetchall()}


# --------------------------------------------------------------------------- #
# Retention contract (in-process)
# --------------------------------------------------------------------------- #
def test_reclaim_keeps_current_last_published_and_their_inputs(isolated):
    conn = db.get_conn()
    # v1 -> gen1 published; v2 correction -> gen2 published (gen1 superseded);
    # v3 correction -> gen3 published; now gen2/gen1 are old.
    v1 = _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _drain(conn)
    v2 = _seed_upload("S1", "2026-11-10", {h: 11 for h in range(24)})
    _drain(conn)
    v3 = _seed_upload("S1", "2026-11-10", {h: 12 for h in range(24)})
    _drain(conn)
    gens = _month_rows(conn)
    assert gens[1]["status"] == "superseded"
    assert gens[2]["status"] == "superseded"
    assert gens[3]["status"] == "current"

    # Reclaim: gen3 (current) and its dep v3 stay; gen1, gen2, v1, v2 go.
    summary = reclaim.request_reclaim(conn, "S1", "2026-11")
    assert summary["monthly"] == 2 and summary["daily"] == 2
    reclaim.sweep()
    gens = _month_rows(conn)
    daily_rows = {r["id"]: dict(r) for r in conn.execute(
        "SELECT * FROM daily_versions WHERE station='S1'")}
    assert daily_rows[v1]["reclaimed_at"] and daily_rows[v2]["reclaimed_at"]
    assert not daily_rows[v3]["reclaimed_at"]
    assert not gens[3]["reclaimed_at"]
    assert gens[1]["reclaimed_at"] and gens[2]["reclaimed_at"]
    # Protected files are still on disk.
    assert config.daily_parquet_path("S1", "2026-11-10", v3).is_file()
    assert Path(gens[3]["parquet_path"]).is_file()
    # Reclaimed files are gone.
    assert not config.daily_parquet_path("S1", "2026-11-10", v1).exists()
    assert not Path(gens[1]["parquet_path"]).exists()
    # Their inputs (CSVs) are gone too.
    assert not config.inbox_csv_path("S1", "2026-11-10", v2).exists()

    # A second request is a no-op (nothing left to reclaim).
    again = reclaim.request_reclaim(conn, "S1", "2026-11")
    assert again["daily"] == 0 and again["monthly"] == 0
    # No duplicate delete records.
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM reclaim_items").fetchone()["c"] == 6


def test_last_successfully_published_month_kept_when_newer_build_failed(isolated, monkeypatch):
    """A failed rebuild must not destroy the last good monthly file or inputs."""
    conn = db.get_conn()
    v1 = _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _drain(conn)
    gen1 = _one(conn, "SELECT id FROM monthly_generations WHERE status='current'")["id"]

    # v2 daily builds, queues gen2; make the monthly merge fail.
    v2 = _seed_upload("S1", "2026-11-10", {h: 11 for h in range(24)})
    worker._process_one(conn)  # daily v2 ok -> gen2 pending
    from app import parquet
    original_merge = parquet.merge_monthly_parquet
    monkeypatch.setattr(parquet, "merge_monthly_parquet",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("boom")))
    _drain(conn)
    monkeypatch.setattr(parquet, "merge_monthly_parquet", original_merge)
    gens = _month_rows(conn)
    gen2 = max(gens)
    assert gens[gen2]["status"] == "failed"
    assert gens[gen1]["status"] == "superseded"  # still published & downloadable

    # v2 failed to publish: gen1 was the last successfully published month,
    # so its input v1 must be protected even though v2 is now current.
    plan = reclaim.build_plan(conn, "S1", "2026-11")
    kept_ids = {d["id"] for d in plan["daily_kept"]}
    assert v2 in kept_ids and v1 in kept_ids
    assert {m["id"] for m in plan["monthly_kept"]} >= {gen1, gen2}
    assert plan["monthly_candidates"] == []

    # Retrying the failed generation (same inputs) still works: inputs intact.
    conn.execute("UPDATE monthly_generations SET status='building' WHERE id=?", (gen2,))
    conn.execute("UPDATE jobs SET status='pending' WHERE kind='monthly_build' AND ref_id=?",
                 (gen2,))
    _drain(conn)
    gens = _month_rows(conn)
    assert gens[gen2]["status"] == "current"
    assert Path(gens[gen1]["parquet_path"]).is_file()


def test_inputs_of_queued_and_retryable_jobs_are_protected(isolated):
    conn = db.get_conn()
    # Daily job still pending (never drained): its CSV input must stay.
    v1 = _seed_upload("S1", "2026-11-10")
    v2 = _seed_upload("S1", "2026-11-11")
    plan = reclaim.build_plan(conn, "S1", "2026-11")
    assert plan["daily_candidates"] == []
    assert {d["id"] for d in plan["daily_kept"]} >= {v1, v2}
    assert "作业的输入" in " ".join(d["why"] for d in plan["daily_kept"])

    # A failed (retryable) daily job keeps its input version + CSV.
    conn.execute("UPDATE jobs SET status='failed' WHERE ref_id=?", (v1,))
    conn.execute("UPDATE daily_versions SET status='failed' WHERE id=?", (v1,))
    plan = reclaim.build_plan(conn, "S1", "2026-11")
    assert v1 in {d["id"] for d in plan["daily_kept"]}
    assert plan["daily_candidates"] == []


def test_tombstone_blocks_download_even_when_unlink_fails(isolated, monkeypatch):
    conn = db.get_conn()
    v1 = _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _drain(conn)
    v2 = _seed_upload("S1", "2026-11-10", {h: 11 for h in range(24)})
    _drain(conn)
    gen1 = _one(conn, "SELECT MIN(id) AS id FROM monthly_generations")["id"]

    # Make every unlink fail with EACCES.
    real_unlink = Path.unlink

    def denied(self, *a, **k):
        raise PermissionError(13, "denied", str(self))

    monkeypatch.setattr(Path, "unlink", denied)
    reclaim.request_reclaim(conn, "S1", "2026-11")
    summary = reclaim.sweep()
    monkeypatch.setattr(Path, "unlink", real_unlink)
    assert summary["failed"] > 0
    # Tombstones are committed even though nothing was physically removed.
    assert _one(conn, "SELECT reclaimed_at FROM daily_versions WHERE id=?", (v1,))[
        "reclaimed_at"]
    assert _one(conn, "SELECT reclaimed_at FROM monthly_generations WHERE id=?",
                (gen1,))["reclaimed_at"]
    # Files still exist, but pending records remain so a restart keeps trying.
    assert config.daily_parquet_path("S1", "2026-11-10", v1).is_file()
    pending = conn.execute(
        "SELECT COUNT(*) AS c FROM reclaim_items WHERE status='pending'").fetchone()["c"]
    assert pending == 3  # daily v1 parquet + csv + gen1 parquet

    # The download layer refuses from the durable tombstone.
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM daily_versions WHERE id=?", (v1,)).fetchall()]
    from app.main import _safe_file_response
    resp = _safe_file_response(rows[0]["parquet_path"], "x.parquet", rows[0])
    assert resp.status_code == 303
    assert "已回收" in redirect_message(resp)

    # A later sweep (e.g. after restart) completes the deletes.
    assert reclaim.sweep()["deleted"] == 3
    assert not config.daily_parquet_path("S1", "2026-11-10", v1).exists()


def test_interrupted_delete_is_resumed_after_restart(isolated, monkeypatch):
    """Rows committed before a crash stay pending and finish on restart."""
    conn = db.get_conn()
    v1 = _seed_upload("S1", "2026-11-10")
    _drain(conn)
    v2 = _seed_upload("S1", "2026-11-10", {h: 5 for h in range(24)})
    _drain(conn)

    # Simulate the process dying after phase 1 committed but before unlink.
    reclaim.request_reclaim(conn, "S1", "2026-11")
    conn.close()
    pending_before = None
    with db.get_conn() as c2:
        pending_before = c2.execute(
            "SELECT COUNT(*) AS c FROM reclaim_items WHERE status='pending'"
        ).fetchone()["c"]
    assert pending_before > 0
    reclaim.resume_interrupted()
    with db.get_conn() as c3:
        assert c3.execute(
            "SELECT COUNT(*) AS c FROM reclaim_items WHERE status='pending'"
        ).fetchone()["c"] == 0
    assert not config.daily_parquet_path("S1", "2026-11-10", v1).exists()
    assert config.daily_parquet_path("S1", "2026-11-10", v2).is_file()


def test_sweep_skips_files_held_by_a_download_lease(isolated):
    conn = db.get_conn()
    v1 = _seed_upload("S1", "2026-11-10")
    _drain(conn)
    v2 = _seed_upload("S1", "2026-11-10", {h: 7 for h in range(24)})
    _drain(conn)
    path = config.daily_parquet_path("S1", "2026-11-10", v1)
    reclaim.request_reclaim(conn, "S1", "2026-11")
    items = conn.execute("SELECT COUNT(*) AS c FROM reclaim_items").fetchone()["c"]

    lease = downloads.acquire(path)
    try:
        summary = reclaim.sweep()
        assert summary["busy"] >= 1
        assert path.is_file()  # still streaming: not deleted
        assert conn.execute(
            "SELECT status FROM reclaim_items WHERE path=? AND kind='daily_parquet'",
            (str(path),)).fetchone()["status"] == "pending"
    finally:
        downloads.release(lease)

    summary = reclaim.sweep()
    assert summary["deleted"] == 1
    assert not path.exists()
    with db.get_conn() as fresh:
        assert fresh.execute(
            "SELECT COUNT(*) AS c FROM reclaim_items WHERE status='pending'"
        ).fetchone()["c"] == 0
    # After completion the link remains closed (tombstone).
    assert conn.execute(
        "SELECT status FROM reclaim_items WHERE path=? AND kind='daily_parquet'",
        (str(path),)).fetchone()["status"] == "deleted"


def test_unregistered_files_are_not_touched(isolated):
    """Only files this service registered are eligible."""
    conn = db.get_conn()
    v1 = _seed_upload("S1", "2026-11-10")
    _drain(conn)
    v2 = _seed_upload("S1", "2026-11-10", {h: 9 for h in range(24)})
    _drain(conn)
    foreign = config.DAILY_DIR / "S1" / "manual-notes.parquet"
    foreign.write_bytes(b"PAR1 not ours")
    reclaim.request_reclaim(conn, "S1", "2026-11")
    reclaim.sweep()
    assert foreign.is_file()


def test_reclaim_other_month_left_alone(isolated):
    conn = db.get_conn()
    _seed_upload("S1", "2026-10-31")
    _seed_upload("S1", "2026-11-10")
    _drain(conn)
    oct_gen = _one(conn, "SELECT id FROM monthly_generations WHERE month='2026-10'")
    reclaim.request_reclaim(conn, "S1", "2026-11")
    reclaim.sweep()
    assert _one(conn, "SELECT status FROM monthly_generations WHERE id=?",
                (oct_gen["id"],))["status"] == "current"
    assert config.monthly_parquet_path("S1", "2026-10", oct_gen["id"]).is_file()


# --------------------------------------------------------------------------- #
# Reclamation over real HTTP
# --------------------------------------------------------------------------- #
def _status(client):
    return client.get("/api/status").json()


def _month(client, station="BIG", month="2026-11"):
    return next(m for m in _status(client)["months"]
                if m["station"] == station and m["month"] == month)


def _make_two_generations(client, dense=False):
    """Upload two versions of one day; return (v1, v2, gen1, gen2)."""
    payload = dense_csv("BIG", "2026-11-10") if dense else hours_csv("BIG", "2026-11-10")
    upload(client, payload)
    wait_for(client, ("BIG", "2026-11"))
    gen1 = _month(client)["download_id"]
    upload(client, hours_csv("BIG", "2026-11-10", pm=42) if dense
           else hours_csv("BIG", "2026-11-10", pm=42))
    wait_for(client, ("BIG", "2026-11"), after_gen=gen1)
    m = _month(client)
    return gen1, m["download_id"]


def test_http_reclaimed_links_closed_and_protected_still_served(server, volume):
    client = server.client()
    gen1, gen2 = _make_two_generations(client)

    # Before reclaim both the old generation and the current one download.
    assert client.get(f"/download/generation/{gen1}").status_code == 200
    assert client.get("/download/daily/version/1").status_code == 200

    resp = client.post("/reclaim", data={"station": "BIG", "month": "2026-11"},
                       follow_redirects=False)
    assert resp.status_code == 303
    assert "已回收" in redirect_message(resp)

    # Old links inform "已回收" instead of 404/streaming.
    old_gen = client.get(f"/download/generation/{gen1}", follow_redirects=False)
    assert old_gen.status_code == 303
    assert "已回收" in redirect_message(old_gen)
    old_daily = client.get("/download/daily/version/1", follow_redirects=False)
    assert old_daily.status_code == 303 and "已回收" in redirect_message(old_daily)

    # Protected current files are untouched and downloadable.
    assert client.get(f"/download/generation/{gen2}").status_code == 200
    assert client.get("/download/monthly/BIG/2026-11").status_code == 200
    assert client.get("/download/daily/BIG/2026-11-10").status_code == 200
    daily_files = list((volume / "daily").rglob("*.parquet"))
    monthly_files = list((volume / "monthly").rglob("*.parquet"))
    assert len(daily_files) == 1 and len(monthly_files) == 1

    # The page lists deleted, protected and pending items with reasons.
    html = client.get("/").text
    assert "已删除" in html and "待清理" in html
    assert "旧版本日档" in html  # reason recorded on the deleted records
    preview = client.get("/reclaim?station=BIG&month=2026-11").text
    assert "受保护、不会回收的文件及原因" in preview
    assert "当前最新月文件" in preview
    assert "没有可回收的旧文件" in preview


SLOW_ENV = {"DOWNLOAD_CHUNK_SIZE_BYTES": "64",
            "DOWNLOAD_CHUNK_DELAY_SECONDS": "0.05"}


def test_reclaim_during_inflight_download_finishes_it(make_server, volume):
    """A download already in flight completes in full; the file goes after."""
    client = make_server(SLOW_ENV).client(timeout=60)
    gen1, gen2 = _make_two_generations(client, dense=True)
    path = volume / "monthly" / "BIG" / f"monthly-2026-11-gen{gen1}.parquet"
    expected = path.stat().st_size

    received = {"bytes": 0}

    def slow_stream():
        with client.stream("GET", f"/download/generation/{gen1}") as resp:
            assert resp.status_code == 200
            for chunk in resp.iter_bytes(chunk_size=4096):
                received["bytes"] += len(chunk)
                time.sleep(0.002)

    thread = threading.Thread(target=slow_stream)
    thread.start()
    # Wait until the download has really started consuming bytes.
    deadline = time.time() + 15
    while received["bytes"] < 4096 and time.time() < deadline:
        time.sleep(0.05)

    reclaim_resp = client.post("/reclaim", data={"station": "BIG", "month": "2026-11"},
                               follow_redirects=False)
    assert reclaim_resp.status_code == 303
    # Tombstone is effective immediately for *new* downloads, even while the
    # old file is still streaming and therefore not yet unlinked.
    again = client.get(f"/download/generation/{gen1}", follow_redirects=False)
    assert again.status_code == 303 and "已回收" in redirect_message(again)

    thread.join(timeout=60)
    assert not thread.is_alive()
    assert received["bytes"] == expected  # complete file, no truncation
    # Once the lease ends the sweeper removes the file.
    deadline = time.time() + 15
    while path.exists() and time.time() < deadline:
        time.sleep(0.1)
    assert not path.exists()


def test_client_disconnect_releases_file_for_reclaim(make_server, volume):
    client = make_server(SLOW_ENV).client(timeout=60)
    gen1, gen2 = _make_two_generations(client, dense=True)
    path = volume / "monthly" / "BIG" / f"monthly-2026-11-gen{gen1}.parquet"

    # Read a few chunks then hang up mid-stream.
    with client.stream("GET", f"/download/generation/{gen1}") as resp:
        assert resp.status_code == 200
        seen = 0
        for chunk in resp.iter_bytes(chunk_size=4096):
            seen += len(chunk)
            if seen >= 8192:
                break
    # Give the server a moment to notice the disconnect and drop the lease.
    time.sleep(0.5)
    resp = client.post("/reclaim", data={"station": "BIG", "month": "2026-11"},
                       follow_redirects=False)
    assert resp.status_code == 303
    deadline = time.time() + 15
    while path.exists() and time.time() < deadline:
        time.sleep(0.1)
    assert not path.exists(), "disconnected download must not keep the file pinned"
    # New download of the reclaimed link is refused.
    assert client.get(f"/download/generation/{gen1}", follow_redirects=False).status_code == 303


def test_slow_download_does_not_block_upload_or_other_files(make_server, volume):
    client = make_server(SLOW_ENV).client(timeout=60)
    gen1, gen2 = _make_two_generations(client, dense=True)
    started = threading.Event()
    release = threading.Event()

    def slow_stream():
        with client.stream("GET", f"/download/generation/{gen1}") as resp:
            assert resp.status_code == 200
            it = resp.iter_bytes(chunk_size=1024)
            next(it)  # first chunk received, server still has bytes to send
            started.set()
            release.wait(timeout=20)
            for _ in it:
                pass

    thread = threading.Thread(target=slow_stream)
    thread.start()
    assert started.wait(timeout=15)

    # Uploads still work while the slow download holds gen1.
    t0 = time.time()
    r = upload(client, hours_csv("BIG", "2026-11-11"))
    assert r.status_code == 303
    wait_for(client, ("BIG", "2026-11"), timeout=20)
    assert time.time() - t0 < 15

    # Reclaim proceeds for what it can: the leased file stays pending, all
    # other reclaimable files of the station-month are deleted right away.
    resp = client.post("/reclaim", data={"station": "BIG", "month": "2026-11"},
                       follow_redirects=False)
    assert resp.status_code == 303
    data = _status(client)
    # The leased file is now either still streaming or finished; either way
    # the deletion was not lost. Give the sweeper a beat and check the
    # persistent record rather than the transient in-flight list.
    assert data["reclaim_items"], "reclaim records must exist"
    # Old CSVs were reclaimed even though one file is busy (possibly still
    # pending, possibly done after the read finished).
    deadline = time.time() + 10
    while (volume / "inbox" / "BIG-2026-11-10-v1.csv").exists() and time.time() < deadline:
        time.sleep(0.1)
    assert not (volume / "inbox" / "BIG-2026-11-10-v1.csv").exists()
    # The reclaimed old generation link is refused regardless.
    assert client.get(f"/download/generation/{gen1}", follow_redirects=False).status_code == 303

    release.set()
    thread.join(timeout=20)
    assert not thread.is_alive()
    deadline = time.time() + 15
    while _status(client)["reclaim_pending"] and time.time() < deadline:
        time.sleep(0.2)
    assert _status(client)["reclaim_pending"] == []


def test_dashboard_shows_reclaim_preview_and_history(server):
    client = server.client()
    _make_two_generations(client)
    page = client.get("/reclaim?station=BIG&month=2026-11").text
    assert "回收预览" in page
    assert "将回收" in page and "保留" in page
    # History row marks reclaimed generation instead of offering a link.
    client.post("/reclaim", data={"station": "BIG", "month": "2026-11"},
                follow_redirects=False)
    page = client.get("/").text
    assert "已回收" in page
    assert "已删除 / 待清理文件" in page


def test_restart_completes_interrupted_and_crash_deletes(server, volume):
    """Pending deletions survive a SIGKILL and finish on the next start."""
    client = server.client()
    gen1, gen2 = _make_two_generations(client)

    # Simulate the crash window: tombstone committed, unlink never attempted.
    import sqlite3
    conn = sqlite3.connect(volume / "state.db")
    conn.row_factory = sqlite3.Row
    conn.execute("UPDATE daily_versions SET reclaimed_at='x', reclaim_reason='测试' "
                 "WHERE id=1")
    conn.execute("UPDATE monthly_generations SET reclaimed_at='x', "
                 "reclaim_reason='测试' WHERE id=?", (gen1,))
    conn.execute(
        "INSERT INTO reclaim_items(station, month, kind, table_name, row_id, day, "
        "path, status, reason) VALUES ('BIG','2026-11','daily_parquet',"
        "'daily_versions',1,'2026-11-10',?, 'pending', '测试')",
        (str(volume / "daily" / "BIG" / "daily-2026-11-10-v1.parquet"),))
    conn.execute(
        "INSERT INTO reclaim_items(station, month, kind, table_name, row_id, day, "
        "path, status, reason) VALUES ('BIG','2026-11','monthly_parquet',"
        "'monthly_generations',?, NULL, ?, 'pending', '测试')",
        (gen1, str(volume / "monthly" / "BIG" / f"monthly-2026-11-gen{gen1}.parquet")))
    conn.commit()
    conn.close()

    server.stop()
    srv = Server(volume, free_port())
    srv.start()
    try:
        c2 = srv.client()
        deadline = time.time() + 15
        while time.time() < deadline:
            data = c2.get("/api/status").json()
            if not data["reclaim_pending"]:
                break
            time.sleep(0.2)
        assert data["reclaim_pending"] == []
        assert not (volume / "daily" / "BIG" / "daily-2026-11-10-v1.parquet").exists()
        assert not (volume / "monthly" / "BIG" / f"monthly-2026-11-gen{gen1}.parquet").exists()
        # Protected current files survived the restart.
        assert c2.get("/download/monthly/BIG/2026-11").status_code == 200
        assert c2.get("/download/daily/BIG/2026-11-10").status_code == 200
        # Reclaimed link reports "已回收", not "文件不存在".
        resp = c2.get(f"/download/generation/{gen1}", follow_redirects=False)
        assert resp.status_code == 303 and "已回收" in redirect_message(resp)
    finally:
        srv.stop()
        server.proc = None

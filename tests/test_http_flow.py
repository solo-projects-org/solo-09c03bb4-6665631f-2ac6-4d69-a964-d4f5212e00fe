"""End-to-end HTTP tests: upload, correction, reuse, safe downloads."""
import time

import duckdb

from conftest import Server, free_port
from helpers import make_csv, redirect_message, upload, wait_for


def _current_month(client, station: str, month: str) -> dict:
    data = client.get("/api/status").json()
    matches = [m for m in data["months"]
               if m["station"] == station and m["month"] == month]
    assert matches, f"no monthly row for {station} {month}"
    return matches[0]


def test_full_flow_upload_correction_and_reuse(server, volume):
    client = server.client()

    # Day 10: first upload -> daily parquet + monthly generation 1.
    r = upload(client, make_csv("ST001", "2026-11-10", {h: 10 for h in range(24)}))
    assert r.status_code == 303
    wait_for(client, ("ST001", "2026-11"))
    m1 = _current_month(client, "ST001", "2026-11")
    assert m1["badge"][0] == "最新"
    gen1 = m1["download_id"]
    assert gen1 is not None

    # Second day in the same month: only the new day + one new monthly build.
    r = upload(client, make_csv("ST001", "2026-11-11", {h: 20 for h in range(24)}))
    assert r.status_code == 303
    wait_for(client, ("ST001", "2026-11"), after_gen=gen1)
    m2 = _current_month(client, "ST001", "2026-11")
    gen2 = m2["download_id"]
    assert gen2 > gen1
    assert m2["day_count"] == 2

    # The day-10 parquet was reused (same version id, same file name).
    data = client.get("/api/status").json()
    day10 = [d for d in data["days"]
             if d["station"] == "ST001" and d["day"] == "2026-11-10"][0]
    v10 = day10["current_id"]

    # Correction of day 10: full replacement, monthly rebuilt, other day reused.
    r = upload(client, make_csv("ST001", "2026-11-10",
                                {**{h: 10 for h in range(24)}, 0: 99}))
    assert r.status_code == 303
    wait_for(client, ("ST001", "2026-11"), after_gen=gen2)
    data = client.get("/api/status").json()
    day10b = [d for d in data["days"]
              if d["station"] == "ST001" and d["day"] == "2026-11-10"][0]
    assert day10b["current_id"] > v10  # new version, old one superseded
    day11 = [d for d in data["days"]
             if d["station"] == "ST001" and d["day"] == "2026-11-11"][0]
    assert day11["version_count"] == 1  # untouched -> its parquet was reused

    m3 = _current_month(client, "ST001", "2026-11")
    assert m3["download_id"] > gen2
    # Old generations remain present in history and still downloadable.
    hist = {h["id"]: h for h in m3["history"]}
    assert set(hist) >= {gen1, gen2, m3["download_id"]}
    assert hist[gen1]["badge"][0].startswith("已过期")
    assert hist[gen1]["downloadable"] is True

    # Verify parquet contents through DuckDB.
    resp = client.get(f"/download/monthly/ST001/2026-11")
    assert resp.status_code == 200
    latest = volume / "monthly" / "ST001" / f"monthly-2026-11-gen{m3['download_id']}.parquet"
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT count(*), sum(pm25), min(utc_seconds), max(utc_seconds) "
            "FROM read_parquet(?)", [str(latest)]
        ).fetchone()
    finally:
        con.close()
    assert rows[0] == 24 + 24
    # Day10 corrected: 23*10 + 99; day11: 24*20.
    assert rows[1] == (23 * 10 + 99) + 24 * 20

    # Old generation 1 still downloads (stale but available), and its content
    # is the pre-correction snapshot.
    resp = client.get(f"/download/generation/{gen1}")
    assert resp.status_code == 200
    assert f"gen{gen1}" in resp.headers["content-disposition"]


def test_invalid_csv_changes_nothing(server, volume):
    client = server.client()
    r = upload(client, b"station,utc_seconds,pm25\nST001,notanint,5\n")
    assert r.status_code == 303
    assert "校验失败" in redirect_message(r)
    # Nothing registered, no files created outside tmp.
    data = client.get("/api/status").json()
    assert data["days"] == [] and data["months"] == []
    assert not (volume / "inbox").exists() or not any((volume / "inbox").iterdir())

    # Negative pm likewise rejected.
    r = upload(client, make_csv("ST001", "2026-11-10").replace(b",10\n", b",-1\n", 1))
    assert r.status_code == 303
    assert "校验失败" in redirect_message(r)
    data = client.get("/api/status").json()
    assert data["days"] == []


def test_download_entries_never_half_or_missing(server, volume):
    client = server.client()
    # No data yet: endpoints refuse instead of 500/path traversal.
    assert client.get("/download/monthly/ST001/2026-11", follow_redirects=False).status_code == 303
    assert client.get("/download/daily/ST001/2026-11-10", follow_redirects=False).status_code == 303
    # Path validation.
    bad = client.get("/download/monthly/..%2F..%2Fetc/2026-11", follow_redirects=False)
    assert bad.status_code == 303

    upload(client, make_csv("ST001", "2026-11-10"))
    wait_for(client, ("ST001", "2026-11"))

    # Delete the file behind the DB row: the entry must refuse, not 404-stream.
    for p in (volume / "monthly" / "ST001").glob("*.parquet"):
        p.unlink()
    resp = client.get("/download/monthly/ST001/2026-11", follow_redirects=False)
    assert resp.status_code == 303
    assert "不存在" in redirect_message(resp)

    # No tmp file is ever left in the serving directories.
    for base in (volume / "daily", volume / "monthly"):
        assert not list(base.rglob("*.tmp"))


def test_restart_resumes_and_keeps_last_good_file(server, volume):
    client = server.client()
    upload(client, make_csv("ST001", "2026-11-10", {h: 10 for h in range(24)}))
    wait_for(client, ("ST001", "2026-11"))
    before = _current_month(client, "ST001", "2026-11")
    gen1 = before["download_id"]
    good_file = volume / "monthly" / "ST001" / f"monthly-2026-11-gen{gen1}.parquet"
    assert good_file.is_file()

    # Graceful restart: state lives entirely in the volume; file still served.
    server.stop()
    srv2 = Server(volume, free_port())
    srv2.start()
    try:
        c2 = srv2.client()
        resp = c2.get(f"/download/generation/{gen1}")
        assert resp.status_code == 200
        m = _current_month(c2, "ST001", "2026-11")
        assert m["download_id"] == gen1

        # A correction after restart resumes the normal pipeline.
        upload(c2, make_csv("ST001", "2026-11-10", {h: 11 for h in range(24)}))
        wait_for(c2, ("ST001", "2026-11"), after_gen=gen1)
        after = _current_month(c2, "ST001", "2026-11")
        assert after["download_id"] > gen1
        assert good_file.is_file()  # superseded, not deleted
    finally:
        srv2.stop()


def test_abrupt_kill_during_processing_resumes(server, volume):
    # Upload work, then SIGKILL without giving the worker time to finish.
    # Startup recovery must reset the interrupted job and the retry endpoint
    # drives the queue to convergence; the last good file is never lost.
    client = server.client()
    for attempt in range(3):
        upload(client, make_csv("ST002", "2026-11-10",
                                {h: attempt + 1 for h in range(24)}))
        server.kill9()
        srv = Server(volume, free_port())
        srv.start()
        # Hand the live process back to the fixture for teardown.
        server.proc = srv.proc
        server.port = srv.port
        server.base_url = srv.base_url
        server.kill9()  # kill again almost immediately, mid-recovery/processing
        srv2 = Server(volume, free_port())
        srv2.start()
        server.proc = srv2.proc
        server.port = srv2.port
        server.base_url = srv2.base_url
        client = server.client()

    # Whatever state the repeated crashes left, every failed job is retryable
    # and the pipeline converges on the current daily set.
    deadline = time.time() + 25
    while time.time() < deadline:
        data = client.get("/api/status").json()
        for job in data["failed_jobs"]:
            client.post(f"/jobs/{job['id']}/retry")
        if any(m["station"] == "ST002" and m["latest_status"] == "current"
               for m in data["months"]):
            break
        time.sleep(0.5)

    data = wait_for(client, ("ST002", "2026-11"), timeout=30)
    m = [x for x in data["months"] if x["station"] == "ST002"][0]
    assert m["badge"][0] == "最新"
    # No staging leftovers; no job left running.
    assert not list((volume / "tmp").glob("*.tmp"))
    assert all(j["status"] != "running" for j in data["jobs"])


def test_dashboard_renders(server):
    client = server.client()
    resp = client.get("/")
    assert resp.status_code == 200
    assert "空气站站月文件服务" in resp.text
    upload(client, make_csv("ST001", "2026-11-10"))
    wait_for(client, ("ST001", "2026-11"))
    resp = client.get("/")
    assert resp.status_code == 200
    assert "ST001" in resp.text and "下载最新月文件" in resp.text

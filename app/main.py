"""FastAPI application: upload, retry, safe downloads and the dashboard.

Single-instance deployment only (see docker-compose.yml): one worker thread,
one SQLite writer. All mutable state is under DATA_VOLUME.
"""
from __future__ import annotations

import threading
from pathlib import Path

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import (config, db, downloads, fsutil, reclaim, status as status_mod,
               worker)
from .csv_validation import CsvError, parse_csv

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="空气站站月文件服务", version="1.1.0")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

_stop_event: threading.Event | None = None
_sweep_stop_event: threading.Event | None = None


@app.on_event("startup")
def _startup() -> None:
    global _stop_event, _sweep_stop_event
    config.ensure_dirs()
    db.init_db()
    _, _stop_event = worker.start_worker()
    # Finish deletions interrupted by a crash, then keep retrying busy/failed
    # ones in the background.
    reclaim.resume_interrupted()
    _, _sweep_stop_event = reclaim.start_sweeper()


@app.on_event("shutdown")
def _shutdown() -> None:
    if _stop_event is not None:
        _stop_event.set()
    if _sweep_stop_event is not None:
        _sweep_stop_event.set()


# --------------------------------------------------------------------------- #
# Pages
# --------------------------------------------------------------------------- #
@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, msg: str | None = None, err: str | None = None,
              reclaim_station: str | None = None, reclaim_month: str | None = None):
    data = status_mod.list_overview()
    context = {"data": data, "msg": msg, "err": err,
               "reclaim_plan": None, "reclaim_station": reclaim_station or "",
               "reclaim_month": reclaim_month or ""}
    if reclaim_station and reclaim_month:
        try:
            month = reclaim.normalize_month(reclaim_month)
            with db.get_conn() as conn:
                context["reclaim_plan"] = reclaim.build_plan(
                    conn, reclaim_station, month)
            context["reclaim_month"] = month
        except ValueError as exc:
            context["err"] = str(exc)
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context=context,
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/status")
def api_status():
    return JSONResponse(status_mod.list_overview())


# --------------------------------------------------------------------------- #
# Upload (full replacement of one station / one UTC day)
# --------------------------------------------------------------------------- #
@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    raw = await file.read()
    try:
        parsed = parse_csv(raw)
        db.validate_station(parsed.station)
        db.validate_day(parsed.day)
    except (CsvError, ValueError) as exc:
        # Validation happens before any write: the previous day file is intact.
        return RedirectResponse(f"/?err={_q('CSV 校验失败，未保存任何内容：' + str(exc))}",
                                status_code=303)

    # 1. Register the new version first so the file name carries its durable id.
    with db.get_conn() as conn:
        with db.tx(conn):
            cur = conn.execute(
                "INSERT INTO daily_versions(station, day, csv_path, status) "
                "VALUES (?, ?, '', 'building')",
                (parsed.station, parsed.day),
            )
            daily_id = int(cur.lastrowid)
        csv_final = config.inbox_csv_path(parsed.station, parsed.day, daily_id)
        conn.execute(
            "UPDATE daily_versions SET csv_path=? WHERE id=?",
            (str(csv_final), daily_id),
        )

    # 2. Persist the raw CSV atomically. Failure here must leave the old day
    #    fully untouched (nothing has been marked current yet).
    tmp_csv = fsutil.tmp_target(csv_final, f"upload{daily_id}")
    try:
        tmp_csv.write_bytes(raw)
        fsutil.atomic_publish(tmp_csv, csv_final)
    except OSError as exc:
        with db.get_conn() as conn:
            conn.execute(
                "UPDATE daily_versions SET status='failed', error=? WHERE id=?",
                (f"CSV 落盘失败: {exc}", daily_id),
            )
        fsutil.discard_tmp(tmp_csv)
        return RedirectResponse(
            f"/?err={_q('保存失败，原日档保留：' + str(exc))}", status_code=303
        )

    # 3. Enqueue the durable build job (daily parquet -> monthly rebuild).
    with db.get_conn() as conn:
        with db.tx(conn):
            worker.enqueue_daily_build(conn, daily_id)
    worker.wakeup()

    return RedirectResponse(
        f"/?msg={_q(f'已接收 {parsed.station} {parsed.day} 第 {daily_id} 版，排队转写中')}",
        status_code=303,
    )


# --------------------------------------------------------------------------- #
# Retry
# --------------------------------------------------------------------------- #
@app.post("/jobs/{job_id}/retry")
def retry_job(job_id: int):
    with db.get_conn() as conn:
        with db.tx(conn):
            job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if job is None:
                return RedirectResponse(f"/?err={_q('作业不存在')}", status_code=303)
            if job["status"] != "failed":
                state = job["status"]
                return RedirectResponse(
                    f"/?err={_q(f'作业 {job_id} 状态为 {state}，仅失败作业可重试')}",
                    status_code=303,
                )

            if job["kind"] == "daily_build":
                conn.execute(
                    "UPDATE daily_versions SET status='building', error=NULL "
                    "WHERE id=? AND status='failed'",
                    (job["ref_id"],),
                )
            else:
                gen = conn.execute(
                    "SELECT station, month FROM monthly_generations WHERE id=?",
                    (job["ref_id"],),
                ).fetchone()
                # A correction that landed after the failure already queued a
                # newer generation: retrying this one can never be current.
                newer = conn.execute(
                    "SELECT COUNT(*) AS c FROM monthly_generations "
                    "WHERE station=? AND month=? AND id>?",
                    (gen["station"], gen["month"], job["ref_id"]),
                ).fetchone()["c"]
                if newer:
                    conn.execute(
                        "UPDATE jobs SET status='obsolete', "
                        "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                        (job_id,),
                    )
                    conn.execute(
                        "UPDATE monthly_generations SET status='obsolete', "
                        "error='已存在更新的补算作业' WHERE id=? AND status='failed'",
                        (job["ref_id"],),
                    )
                    return RedirectResponse(
                        f"/?err={_q('该失败作业已有更新的补算接算，无需重试')}",
                        status_code=303,
                    )
                conn.execute(
                    "UPDATE monthly_generations SET status='building', error=NULL "
                    "WHERE id=? AND status='failed'",
                    (job["ref_id"],),
                )

            conn.execute(
                "UPDATE jobs SET status='pending', error=NULL, "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                (job_id,),
            )
    worker.wakeup()
    return RedirectResponse(f"/?msg={_q(f'作业 {job_id} 已重新排队')}", status_code=303)


# --------------------------------------------------------------------------- #
# Reclaim old files (per station-month)
# --------------------------------------------------------------------------- #
@app.get("/reclaim", response_class=HTMLResponse)
def reclaim_preview(request: Request, station: str, month: str):
    """Preview which files a reclamation would delete and why the rest stay."""
    try:
        month = reclaim.normalize_month(month)
        with db.get_conn() as conn:
            plan = reclaim.build_plan(conn, station, month)
    except ValueError as exc:
        return RedirectResponse(f"/?err={_q(str(exc))}", status_code=303)
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"data": status_mod.list_overview(), "msg": None, "err": None,
                 "reclaim_plan": plan, "reclaim_station": station,
                 "reclaim_month": month},
    )


@app.post("/reclaim")
def reclaim_run(station: str = Form(...), month: str = Form(...)):
    """Tombstone and then delete old files of one station-month.

    The tombstones commit first, so from the moment this returns old links
    answer "已回收" even if the physical unlink is still pending or fails.
    """
    try:
        month = reclaim.normalize_month(month)
        with db.get_conn() as conn:
            summary = reclaim.request_reclaim(conn, station, month)
    except ValueError as exc:
        return RedirectResponse(f"/?err={_q(str(exc))}", status_code=303)

    if summary["daily"] == 0 and summary["monthly"] == 0:
        return RedirectResponse(
            f"/?msg={_q(f'{station} {month} 没有可回收的旧文件；受保护文件保持不变')}",
            status_code=303,
        )

    # Physical deletion happens outside the request's write transaction and
    # outside any global lock: a slow download only delays its own file.
    reclaim.wakeup()
    reclaim.sweep()
    text = (f"{station} {month} 已回收 {summary['daily']} 个旧日档、"
            f"{summary['monthly']} 个旧月档；删除在后台完成，"
            "失败或中断的记录会继续重试")
    return RedirectResponse(f"/?msg={_q(text)}", status_code=303)


# --------------------------------------------------------------------------- #
# Downloads — every path is derived from a registered row and re-checked on
# disk, so no entry can ever point at a .tmp or a missing file.
# --------------------------------------------------------------------------- #
def _reclaimed_response(row) -> RedirectResponse:
    reason = row["reclaim_reason"] or "旧版本已清理"
    return RedirectResponse(
        f"/?err={_q(f'该文件已回收，不能再下载：{reason}')}",
        status_code=303,
    )


def _reclaim_lookup(table: str, row_id: int):
    """Re-read only the tombstone fields from a fresh connection.

    Used to close the race between reading the download row and opening the
    file: if a reclamation committed in that window, the link must report
    "已回收", not a generic missing-file error.
    """
    def lookup():
        with db.get_conn() as conn:
            return conn.execute(
                f"SELECT reclaimed_at, reclaim_reason FROM {table} WHERE id=?",
                (row_id,),
            ).fetchone()
    return lookup


def _safe_file_response(path: str, filename: str, row=None, lookup=None):
    # The durable tombstone wins over whatever is still on disk: a link that
    # was reclaimed never re-opens, even if unlink failed or is still pending.
    if row is not None and row["reclaimed_at"]:
        return _reclaimed_response(row)

    p = Path(path)
    # Defence in depth: registered files must live under the data volume.
    config.DATA_VOLUME.resolve()
    if not str(p.absolute()).startswith(str(config.DATA_VOLUME) + "/"):
        return RedirectResponse(f"/?err={_q('非法文件路径')}", status_code=303)

    # Lease before the existence check so a concurrent reclamation either
    # skips this file (we hold it) or wins first (we refuse).
    leased = downloads.acquire_existing(p)
    if leased is None:
        # The file may have been reclaimed between the metadata read and the
        # open: re-read the row so the user still gets the "已回收" message.
        fresh = lookup() if lookup is not None else None
        if fresh is not None and fresh["reclaimed_at"]:
            return _reclaimed_response(fresh)
        return RedirectResponse(
            f"/?err={_q('文件在数据卷上不存在，已阻止下载半份/缺失文件')}", status_code=303
        )
    lease, stat_result = leased
    if lookup is not None:
        fresh = lookup()
        if fresh is not None and fresh["reclaimed_at"]:
            # Tombstone committed after our first read: refuse, release lease
            # so the sweeper can delete the file.
            downloads.release(lease)
            return _reclaimed_response(fresh)
    return downloads.LeasedFileResponse(
        str(p), lease, filename=filename, stat_result=stat_result,
        media_type="application/vnd.apache.parquet",
    )


@app.get("/download/daily/version/{version_id}")
def download_daily_version(version_id: int):
    with db.get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM daily_versions WHERE id=? AND status='ready'",
            (version_id,),
        ).fetchone()
    if row is None or not row["parquet_path"]:
        return RedirectResponse(f"/?err={_q('该日档版本不可下载')}", status_code=303)
    return _safe_file_response(
        row["parquet_path"],
        f"daily-{row['station']}-{row['day']}-v{row['id']}.parquet",
        row, _reclaim_lookup("daily_versions", row["id"]),
    )


@app.get("/download/daily/{station}/{day}")
def download_current_daily(station: str, day: str):
    try:
        db.validate_station(station)
        db.validate_day(day)
    except ValueError as exc:
        return RedirectResponse(f"/?err={_q(str(exc))}", status_code=303)
    with db.get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM daily_versions WHERE station=? AND day=? AND is_current=1",
            (station, day),
        ).fetchone()
    if row is None or row["status"] != "ready" or not row["parquet_path"]:
        return RedirectResponse(
            f"/?err={_q(f'{station} {day} 尚无可用的当前日档（转写中或失败）')}",
            status_code=303,
        )
    return _safe_file_response(
        row["parquet_path"], f"daily-{station}-{day}-v{row['id']}.parquet", row,
        _reclaim_lookup("daily_versions", row["id"]),
    )


@app.get("/download/monthly/{station}/{month:path}")
def download_current_monthly(station: str, month: str):
    try:
        db.validate_station(station)
        db.validate_month(month)
    except ValueError as exc:
        return RedirectResponse(f"/?err={_q(str(exc))}", status_code=303)
    with db.get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM monthly_generations WHERE station=? AND month=? "
            "AND status='current'",
            (station, month),
        ).fetchone()
    if row is None or not row["parquet_path"]:
        message = (f"{station} {month} 当前没有已发布的最新月文件；"
                   "若有旧版请在历史版本中下载")
        return RedirectResponse(f"/?err={_q(message)}", status_code=303)
    return _safe_file_response(
        row["parquet_path"], f"monthly-{station}-{month}-gen{row['id']}.parquet", row,
        _reclaim_lookup("monthly_generations", row["id"]),
    )


@app.get("/download/generation/{gen_id}")
def download_generation(gen_id: int):
    """Old (superseded) generations stay downloadable until reclaimed."""
    with db.get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM monthly_generations WHERE id=? "
            "AND status IN ('current','superseded')",
            (gen_id,),
        ).fetchone()
    if row is None or not row["parquet_path"]:
        return RedirectResponse(
            f"/?err={_q('该月文件版本未发布或已作废，不能下载')}", status_code=303
        )
    return _safe_file_response(
        row["parquet_path"],
        f"monthly-{row['station']}-{row['month']}-gen{row['id']}.parquet",
        row, _reclaim_lookup("monthly_generations", row["id"]),
    )


def _q(text: str) -> str:
    from urllib.parse import quote

    return quote(text, safe="")

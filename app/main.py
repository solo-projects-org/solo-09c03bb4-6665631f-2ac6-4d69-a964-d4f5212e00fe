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

from . import config, db, fsutil, status as status_mod, worker
from .csv_validation import CsvError, parse_csv

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="空气站站月文件服务", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

_stop_event: threading.Event | None = None


@app.on_event("startup")
def _startup() -> None:
    global _stop_event
    config.ensure_dirs()
    db.init_db()
    _, _stop_event = worker.start_worker()


@app.on_event("shutdown")
def _shutdown() -> None:
    if _stop_event is not None:
        _stop_event.set()


# --------------------------------------------------------------------------- #
# Pages
# --------------------------------------------------------------------------- #
@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, msg: str | None = None, err: str | None = None):
    data = status_mod.list_overview()
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"data": data, "msg": msg, "err": err},
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
# Downloads — every path is derived from a registered row and re-checked on
# disk, so no entry can ever point at a .tmp or a missing file.
# --------------------------------------------------------------------------- #
def _safe_file_response(path: str, filename: str):
    p = Path(path)
    if not p.is_file():
        return RedirectResponse(
            f"/?err={_q('文件在数据卷上不存在，已阻止下载半份/缺失文件')}", status_code=303
        )
    # Defence in depth: registered files must live under the data volume.
    config.DATA_VOLUME.resolve()
    if not str(p.resolve()).startswith(str(config.DATA_VOLUME) + "/"):
        return RedirectResponse(f"/?err={_q('非法文件路径')}", status_code=303)
    return FileResponse(str(p), filename=filename,
                        media_type="application/vnd.apache.parquet")


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
        row["parquet_path"], f"daily-{station}-{day}-v{row['id']}.parquet"
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
        row["parquet_path"], f"monthly-{station}-{month}-gen{row['id']}.parquet"
    )


@app.get("/download/generation/{gen_id}")
def download_generation(gen_id: int):
    """Old (superseded) generations stay downloadable; unfinished ones do not."""
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
    )


def _q(text: str) -> str:
    from urllib.parse import quote

    return quote(text, safe="")

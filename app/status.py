"""Read models powering the web dashboard and the JSON status API."""
from __future__ import annotations

from pathlib import Path

from . import db, reclaim

# UI badge order matters for the colour coding in the template.
MONTHLY_BADGE = {
    "current": ("最新", "ok"),
    "building": ("补算中", "busy"),
    "failed": ("失败", "bad"),
    "superseded": ("已过期(可下载旧版)", "stale"),
    "obsolete": ("作业过期(由后续接算)", "stale"),
}
DAILY_BADGE = {
    "ready": ("最新", "ok"),
    "building": ("转写中", "busy"),
    "failed": ("失败", "bad"),
}
JOB_BADGE = {
    "pending": ("排队", "busy"),
    "running": ("运行中", "busy"),
    "done": ("完成", "ok"),
    "failed": ("失败", "bad"),
    "obsolete": ("过期作废", "stale"),
}
RECLAIM_KIND = {
    "csv": ("原始 CSV", "stale"),
    "daily_parquet": ("日 Parquet", "stale"),
    "monthly_parquet": ("月 Parquet", "stale"),
}
JOB_BADGE = {
    "pending": ("排队", "busy"),
    "running": ("运行中", "busy"),
    "done": ("完成", "ok"),
    "failed": ("失败", "bad"),
    "obsolete": ("过期作废", "stale"),
}


def _file_ready(path: str | None) -> bool:
    return bool(path and Path(path).is_file())


def _job_for(jobs: list[dict], kind: str, ref_id: int | None) -> dict | None:
    if ref_id is None:
        return None
    return next(
        (j for j in jobs if j["kind"] == kind and j["ref_id"] == ref_id
         and j["status"] in ("pending", "running", "failed")),
        None,
    )


def list_overview() -> dict:
    with db.get_conn() as conn:
        dailies = [dict(r) for r in conn.execute(
            "SELECT * FROM daily_versions ORDER BY station, day, id DESC"
        ).fetchall()]
        monthlies = [dict(r) for r in conn.execute(
            "SELECT * FROM monthly_generations ORDER BY station, month, id DESC"
        ).fetchall()]
        jobs = [dict(r) for r in conn.execute(
            "SELECT * FROM jobs ORDER BY id DESC LIMIT 50"
        ).fetchall()]

    # Group generations per station-month (rows already id-desc).
    gens_by_sm: dict[tuple[str, str], list[dict]] = {}
    for g in monthlies:
        gens_by_sm.setdefault((g["station"], g["month"]), []).append(g)

    days_by_sm: dict[tuple[str, str], list[dict]] = {}
    for d in dailies:
        days_by_sm.setdefault((d["station"], d["day"][:7]), []).append(d)

    month_rows: list[dict] = []
    for (station, month), gens in sorted(gens_by_sm.items()):
        latest = gens[0]
        current = next((g for g in gens if g["status"] == "current"), None)
        current_days = [
            d for d in days_by_sm.get((station, month), []) if d["is_current"]
        ]
        active_job = _job_for(jobs, "monthly_build", latest["id"])
        month_rows.append({
            "station": station,
            "month": month,
            "badge": MONTHLY_BADGE[latest["status"]],
            "latest_status": latest["status"],
            "latest_id": latest["id"],
            "error": latest["error"],
            "is_latest": current is not None and current["id"] == latest["id"],
            "download_id": current["id"] if current else None,
            "download_ready": _file_ready(current["parquet_path"] if current else None)
                and not current["reclaimed_at"],
            "job": active_job,
            "day_count": len(current_days),
            "row_count": current["row_count"] if current else None,
            "reclaimed_versions": sum(1 for g in gens if g["reclaimed_at"]),
            "history": [
                {
                    "id": g["id"],
                    "status": g["status"],
                    "badge": MONTHLY_BADGE[g["status"]],
                    "error": g["error"],
                    "created_at": g["created_at"],
                    "published_at": g["published_at"],
                    "reclaimed_at": g["reclaimed_at"],
                    "reclaim_reason": g["reclaim_reason"],
                    "downloadable": _file_ready(g["parquet_path"])
                        and not g["reclaimed_at"]
                        and g["status"] in ("current", "superseded"),
                }
                for g in gens
            ],
        })

    daily_rows: list[dict] = []
    versions_by_sd: dict[tuple[str, str], list[dict]] = {}
    for d in dailies:  # id-desc per station/day
        versions_by_sd.setdefault((d["station"], d["day"]), []).append(d)
    for (station, day), versions in sorted(versions_by_sd.items()):
        cur = next((v for v in versions if v["is_current"]), None)
        head = cur or versions[0]
        daily_rows.append({
            "station": station,
            "day": day,
            "version_count": len(versions),
            "current_id": cur["id"] if cur else None,
            "badge": DAILY_BADGE[head["status"]],
            "row_count": cur["row_count"] if cur else None,
            "error": head["error"],
            "download_ready": _file_ready(cur["parquet_path"] if cur else None)
                and head["status"] == "ready" and not head["reclaimed_at"],
            "job": _job_for(jobs, "daily_build", head["id"]),
        })

    totals = conn.execute(
        "SELECT COUNT(*) AS c, SUM(status='deleted') AS d, "
        "SUM(status='pending') AS p FROM reclaim_items"
    ).fetchone()
    reclaim_items = [dict(r) for r in conn.execute(
        "SELECT * FROM reclaim_items ORDER BY id DESC LIMIT 300"
    ).fetchall()]
    for item in reclaim_items:
        item["kind_badge"] = RECLAIM_KIND.get(item["kind"], (item["kind"], "stale"))

    return {
        "months": month_rows,
        "days": daily_rows,
        "jobs": jobs,
        "job_badges": JOB_BADGE,
        "failed_jobs": [j for j in jobs if j["status"] == "failed"],
        "reclaim_items": reclaim_items,
        # Totals come from the whole table; the list above is capped at 300.
        "reclaim_pending": [i for i in reclaim_items if i["status"] == "pending"],
        "reclaim_pending_count": totals["p"] or 0,
        "reclaim_deleted_count": totals["d"] or 0,
        "reclaim_items_truncated": (totals["c"] or 0) > len(reclaim_items),
    }

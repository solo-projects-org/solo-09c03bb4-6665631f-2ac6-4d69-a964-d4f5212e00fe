#!/usr/bin/env python3
"""Demo: reclaim an old version while its download is in progress.

Run against a freshly started service (docker compose up):

    python3 scripts/demo_reclaim.py [BASE_URL]

What it does, entirely through the public HTTP API:

1. uploads samples/before/ST001-2026-11-10.csv  -> generation 1
2. uploads samples/after/ST001-2026-11-10.csv   -> generation 2 (gen1 stale)
3. starts a *slow* download of generation 1 (the old link)
4. while that download is streaming, POSTs the per-station-month reclamation
5. shows that the old link now says "已回收" for new requests while the
   in-flight download still completes byte-for-byte
6. shows the protected current month file is still downloadable and that the
   dashboard lists deleted / protected / pending files with reasons

The service must expose the demo throttle so a small sample still spans
several chunks:

    DOWNLOAD_CHUNK_SIZE_BYTES=64 DOWNLOAD_CHUNK_DELAY_SECONDS=0.1 \
        docker compose up --build

(or without Docker: the same env vars before `uvicorn app.main:app`).
"""
from __future__ import annotations

import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8000"


def upload(path: Path) -> None:
    with httpx.Client(base_url=BASE, timeout=30, follow_redirects=False) as c:
        r = c.post("/upload", files={"file": (path.name, path.read_bytes(), "text/csv")})
    print(f"upload {path.name}: HTTP {r.status_code}")


def wait_for_generation(after: int | None, timeout: float = 30) -> int:
    deadline = time.time() + timeout
    with httpx.Client(base_url=BASE, timeout=10) as c:
        while time.time() < deadline:
            data = c.get("/api/status").json()
            for m in data["months"]:
                if m["station"] == "ST001" and m["month"] == "2026-11" \
                        and m["latest_status"] == "current" \
                        and (after is None or (m["download_id"] or 0) > after):
                    return m["download_id"]
            time.sleep(0.3)
    raise SystemExit("站月文件迟迟未发布，请查看页面上的失败原因")


def main() -> None:
    upload(ROOT / "samples" / "before" / "ST001-2026-11-10.csv")
    upload(ROOT / "samples" / "before" / "ST001-2026-11-11.csv")
    gen1 = wait_for_generation(None)
    upload(ROOT / "samples" / "after" / "ST001-2026-11-10.csv")
    gen2 = wait_for_generation(gen1)
    print(f"已发布 gen{gen1}（旧）与 gen{gen2}（当前）")

    received = {"bytes": 0}
    error: list[str] = []

    def slow_download() -> None:
        try:
            with httpx.Client(base_url=BASE, timeout=120) as c:
                with c.stream("GET", f"/download/generation/{gen1}") as resp:
                    if resp.status_code != 200:
                        error.append(f"旧链接下载失败: HTTP {resp.status_code}")
                        return
                    for chunk in resp.iter_bytes(chunk_size=512):
                        received["bytes"] += len(chunk)
                        time.sleep(0.05)
                        if received["bytes"] == len(chunk):
                            print("慢速下载已开始（旧 gen 链接）")
        except Exception as exc:  # noqa: BLE001 - demo script
            error.append(str(exc))

    thread = threading.Thread(target=slow_download)
    thread.start()
    time.sleep(1.0)

    with httpx.Client(base_url=BASE, timeout=30, follow_redirects=False) as c:
        r = c.post("/reclaim", data={"station": "ST001", "month": "2026-11"})
        print(f"回收请求: HTTP {r.status_code} -> "
              f"{urllib.parse.unquote(r.headers.get('location', ''))}")
        # A brand-new download of the same old link is refused immediately,
        # even though the first download is still streaming.
        again = c.get(f"/download/generation/{gen1}")
        print(f"回收后再点旧链接: HTTP {again.status_code} -> "
              f"{urllib.parse.unquote(again.headers.get('location', ''))}")
        # The current, protected month file is intact.
        cur = c.get("/download/monthly/ST001/2026-11")
        print(f"受保护的当前月文件: HTTP {cur.status_code}, {len(cur.content)} 字节")

    thread.join(timeout=120)
    if error:
        raise SystemExit("下载失败: " + "; ".join(error))
    print(f"已开始的下载完整结束: {received['bytes']} 字节，未被截断")

    time.sleep(1.0)  # let the sweeper finish deferred deletes
    with httpx.Client(base_url=BASE, timeout=10) as c:
        data = c.get("/api/status").json()
    print(f"已删除 {data['reclaim_deleted_count']} 个文件，"
          f"待清理 {data['reclaim_pending_count']} 个")
    print(f"打开 {BASE}/ 可看到已删除 / 受保护 / 待清理清单及原因。")


if __name__ == "__main__":
    main()

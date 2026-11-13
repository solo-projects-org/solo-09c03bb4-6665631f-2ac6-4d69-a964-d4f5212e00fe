"""Helpers for building valid station-day CSV payloads in tests."""
from __future__ import annotations

import datetime as dt
import io
from urllib.parse import unquote, urlparse


def day_start(day: str) -> int:
    d = dt.date.fromisoformat(day)
    return int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).timestamp())


def make_csv(station: str, day: str, values: dict[int, int] | None = None,
             rows: int = 24) -> bytes:
    """Hourly samples for one UTC day; values maps hour -> pm25."""
    values = values or {}
    start = day_start(day)
    buf = io.StringIO()
    buf.write("station,utc_seconds,pm25\n")
    for hour in range(rows):
        pm = values.get(hour, 10 + hour)
        buf.write(f"{station},{start + hour * 3600},{pm}\n")
    return buf.getvalue().encode("utf-8")


def upload(client: httpx.Client, payload: bytes, filename: str = "day.csv") -> httpx.Response:
    return client.post(
        "/upload",
        files={"file": (filename, payload, "text/csv")},
        follow_redirects=False,
    )


def wait_for(client: httpx.Client, path: str, timeout: float = 15.0,
             after_gen: int | None = None) -> dict:
    """Poll until the station-month has a *current* generation newer than
    ``after_gen`` (defaults to any current generation). Requiring progress
    avoids reading a previous generation during the rebuilding window."""
    import time

    station, month = path
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = client.get("/api/status").json()
        for m in last["months"]:
            if m["station"] != station or m["month"] != month:
                continue
            if m["latest_status"] == "current" and (
                after_gen is None or (m["download_id"] or 0) > after_gen
            ):
                return last
        time.sleep(0.2)
    raise AssertionError(f"monthly {path} never advanced: {last}")


def redirect_message(response: httpx.Response) -> str:
    """Decode the ?err= / ?msg= text carried by a 303 redirect."""
    query = urlparse(response.headers["location"]).query
    for pair in query.split("&"):
        if pair.startswith(("err=", "msg=")):
            return unquote(pair.split("=", 1)[1])
    return ""

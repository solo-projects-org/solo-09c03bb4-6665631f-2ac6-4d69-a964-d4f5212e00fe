"""Parsing and validation of the station-day CSV uploads.

The contract (single station, single UTC day):

* header exactly: ``station,utc_seconds,pm25`` (a UTF-8 BOM is tolerated)
* every row belongs to one station; ``utc_seconds`` is a non-negative integer
  inside one UTC calendar day; ``pm25`` is a non-negative integer (ug/m3)
* duplicate timestamps are rejected; the whole file is one replacement unit,
  so any malformed row rejects the whole upload and nothing is registered.
"""
from __future__ import annotations

import datetime as dt
import io
from dataclasses import dataclass

from . import config

HEADER = ("station", "utc_seconds", "pm25")
DAY_SECONDS = 24 * 60 * 60


class CsvError(ValueError):
    """Any user-facing problem with an uploaded CSV."""


@dataclass(frozen=True)
class ParsedDay:
    station: str
    day: str
    rows: list[tuple[str, int, int]]

    @property
    def row_count(self) -> int:
        return len(self.rows)


def _split_fields(line: str, line_no: int) -> list[str]:
    # Simple comma split is enough: the schema has three integer/text columns
    # and quotes are not part of a valid station id. A stray quote => error.
    fields = line.split(",")
    if len(fields) != 3:
        raise CsvError(f"第 {line_no} 行: 应有 3 列 (station,utc_seconds,pm25)，实际 {len(fields)} 列")
    return [f.strip() for f in fields]


def parse_csv(raw: bytes) -> ParsedDay:
    if len(raw) == 0:
        raise CsvError("文件为空")
    if len(raw) > config.MAX_CSV_BYTES:
        raise CsvError(f"文件超过 {config.MAX_CSV_BYTES // (1024 * 1024)} MiB 上限")

    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvError(f"不是合法 UTF-8 文本: {exc}") from exc

    reader = io.StringIO(text, newline="")
    lines = reader.readlines()
    if not lines:
        raise CsvError("文件为空")

    header = [h.strip() for h in lines[0].rstrip("\r\n").split(",")]
    if tuple(header) != HEADER:
        raise CsvError(f"表头必须是 {','.join(HEADER)}，实际为 {','.join(header)}")

    station: str | None = None
    seen_seconds: set[int] = set()
    rows: list[tuple[str, int, int]] = []
    day_start: int | None = None

    for idx, raw_line in enumerate(lines[1:], start=2):
        line = raw_line.strip()
        if not line:
            continue  # tolerate blank trailing lines
        if len(rows) >= config.MAX_CSV_ROWS:
            raise CsvError(f"行数超过 {config.MAX_CSV_ROWS} 上限")

        st, sec_text, pm_text = _split_fields(line, idx)
        if not st:
            raise CsvError(f"第 {idx} 行: 站号不能为空")
        if station is None:
            station = st
        elif st != station:
            raise CsvError(f"第 {idx} 行: 一份文件只能包含一个站号，出现了 {st!r} 与 {station!r}")

        try:
            sec = int(sec_text)
        except ValueError:
            raise CsvError(f"第 {idx} 行: utc_seconds 必须是整数，得到 {sec_text!r}")
        if sec < 0:
            raise CsvError(f"第 {idx} 行: utc_seconds 不能为负数")

        try:
            pm = int(pm_text)
        except ValueError:
            raise CsvError(f"第 {idx} 行: pm25 必须是整数 μg/m³，得到 {pm_text!r}")
        if pm < 0:
            raise CsvError(f"第 {idx} 行: pm25 不能为负数")

        if sec in seen_seconds:
            raise CsvError(f"第 {idx} 行: utc_seconds={sec} 重复")
        seen_seconds.add(sec)
        rows.append((st, sec, pm))

    if station is None or not rows:
        raise CsvError("没有任何数据行")

    # All timestamps must fall inside a single UTC calendar day.
    first_sec = min(seen_seconds)
    day_start = first_sec - (first_sec % DAY_SECONDS)
    day_end = day_start + DAY_SECONDS
    for sec in seen_seconds:
        if not day_start <= sec < day_end:
            raise CsvError(
                f"时间戳 {sec} 与首个时间戳不在同一个 UTC 日 "
                f"({dt.datetime.fromtimestamp(day_start, dt.timezone.utc).date()})"
            )

    day = dt.datetime.fromtimestamp(day_start, dt.timezone.utc).strftime("%Y-%m-%d")
    rows.sort(key=lambda r: r[1])
    return ParsedDay(station=station, day=day, rows=rows)

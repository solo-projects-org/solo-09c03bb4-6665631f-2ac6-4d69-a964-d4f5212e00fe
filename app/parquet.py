"""DuckDB-backed Parquet transcription and monthly merging.

All writes go to a ``.tmp`` file first and are fsynced + atomically renamed by
the caller, so a crash can never leave a half-written Parquet at a path that
the download layer might serve.
"""
from __future__ import annotations

import duckdb

COLUMNS = "station VARCHAR, utc_seconds BIGINT, pm25 INTEGER"


def write_daily_parquet(rows: list[tuple[str, int, int]], tmp_path) -> int:
    """Write one validated station-day to ``tmp_path`` (Parquet, sorted)."""
    con = duckdb.connect()
    try:
        con.execute(f"CREATE TABLE t({COLUMNS})")
        con.executemany("INSERT INTO t VALUES (?, ?, ?)", rows)
        con.execute(
            "COPY (SELECT station, utc_seconds, pm25 FROM t ORDER BY utc_seconds) "
            "TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(tmp_path)],
        )
        count = con.execute("SELECT count(*) FROM t").fetchone()[0]
    finally:
        con.close()
    return int(count)


def merge_monthly_parquet(daily_paths: list[str], tmp_path) -> int:
    """Merge the given daily Parquet files into one station-month file."""
    if not daily_paths:
        raise ValueError("月合并至少需要一个日档")
    con = duckdb.connect()
    try:
        # glob_list binds a Python list as a filename list (SQL IN-clause style).
        con.execute(
            "COPY ("
            " SELECT station, utc_seconds, pm25 FROM read_parquet(?)"
            " ORDER BY utc_seconds"
            ") TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [daily_paths, str(tmp_path)],
        )
        count = con.execute(
            "SELECT count(*) FROM read_parquet(?)", [daily_paths]
        ).fetchone()[0]
    finally:
        con.close()
    return int(count)


def parquet_row_count(path) -> int:
    con = duckdb.connect()
    try:
        return int(
            con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]
        )
    finally:
        con.close()


def verify_daily_parquet(path, expected_rows: int) -> None:
    """Integrity check used before a daily file is published as current."""
    con = duckdb.connect()
    try:
        info = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
        names = [c[0] for c in info]
        if names != ["station", "utc_seconds", "pm25"]:
            raise RuntimeError(f"日 Parquet 列不匹配: {names}")
        stats = con.execute(
            "SELECT count(*), min(utc_seconds), max(utc_seconds), min(pm25) "
            "FROM read_parquet(?)",
            [str(path)],
        ).fetchone()
    finally:
        con.close()
    if stats[0] != expected_rows:
        raise RuntimeError(f"日 Parquet 行数 {stats[0]} 与期望 {expected_rows} 不符")
    if stats[1] is None or stats[1] < 0 or stats[2] < stats[1]:
        raise RuntimeError("日 Parquet 时间戳范围异常")
    if stats[3] is None or stats[3] < 0:
        raise RuntimeError("日 Parquet 含非法 pm25")



"""Unit tests for CSV parsing and validation."""
import pytest

from app.csv_validation import CsvError, parse_csv

HEADER = "station,utc_seconds,pm25\n"
DAY0 = 1794268800  # 2026-11-10T00:00:00Z


def test_valid_hourly_file():
    raw = (HEADER +
           "".join(f"S1,{DAY0 + h * 3600},{h * 2}\n" for h in range(24))).encode()
    parsed = parse_csv(raw)
    assert parsed.station == "S1"
    assert parsed.day == "2026-11-10"
    assert parsed.row_count == 24
    assert parsed.rows[0][1] == DAY0  # sorted


def test_utf8_bom_tolerated():
    raw = ("﻿" + HEADER + f"S1,{DAY0},5\n").encode()
    parsed = parse_csv(raw)
    assert parsed.station == "S1"


def test_bad_header():
    with pytest.raises(CsvError, match="表头"):
        parse_csv(b"station,time,pm\nS1,1,2\n")


def test_negative_pm_rejected():
    with pytest.raises(CsvError, match="pm25 不能为负"):
        parse_csv((HEADER + f"S1,{DAY0},-1\n").encode())


def test_negative_seconds_rejected():
    with pytest.raises(CsvError, match="utc_seconds 不能为负"):
        parse_csv((HEADER + "S1,-5,1\n").encode())


def test_non_integer_pm_rejected():
    with pytest.raises(CsvError, match="pm25 必须是整数"):
        parse_csv((HEADER + f"S1,{DAY0},3.5\n").encode())


def test_duplicate_seconds_rejected():
    with pytest.raises(CsvError, match="重复"):
        parse_csv((HEADER + f"S1,{DAY0},1\nS1,{DAY0},2\n").encode())


def test_two_stations_rejected():
    with pytest.raises(CsvError, match="一个站号"):
        parse_csv((HEADER + f"S1,{DAY0},1\nS2,{DAY0 + 60},2\n").encode())


def test_cross_day_timestamps_rejected():
    with pytest.raises(CsvError, match="同一个 UTC 日"):
        parse_csv((HEADER +
                   f"S1,{DAY0},1\nS1,{DAY0 + 24 * 3600},2\n").encode())


def test_empty_and_oversized():
    with pytest.raises(CsvError):
        parse_csv(b"")
    big = (HEADER + f"S1,{DAY0},1\n") * 100000
    with pytest.raises(CsvError):
        parse_csv(big.encode())


def test_wrong_column_count():
    with pytest.raises(CsvError, match="3 列"):
        parse_csv((HEADER + f"S1,{DAY0}\n").encode())

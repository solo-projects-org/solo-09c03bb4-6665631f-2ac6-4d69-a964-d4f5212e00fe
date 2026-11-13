"""Filesystem locations and global limits.

Everything mutable lives under ``DATA_VOLUME`` so the whole state (uploaded
CSVs, daily/monthly Parquet files and the SQLite database) survives container
restarts by mounting one volume there.
"""
from __future__ import annotations

import os
from pathlib import Path

DATA_VOLUME = Path(os.environ.get("DATA_VOLUME", "/data")).resolve()

DB_PATH = DATA_VOLUME / "state.db"
INBOX_DIR = DATA_VOLUME / "inbox"        # validated raw CSV uploads, kept for audit
DAILY_DIR = DATA_VOLUME / "daily"        # <station>/daily-<day>-v<id>.parquet
MONTHLY_DIR = DATA_VOLUME / "monthly"    # <station>/monthly-<month>-gen<id>.parquet
TMP_DIR = DATA_VOLUME / "tmp"            # staging files, never reachable via download

# A single station-day has at most 86400 one-second rows; 10 MiB is ample.
MAX_CSV_BYTES = 10 * 1024 * 1024
MAX_CSV_ROWS = 100_000

# Station ids are used in filesystem paths, so keep them conservative.
STATION_RE = r"[A-Za-z0-9_.-]{1,32}"

# Optional, demo/test-only: pause after each streamed 64 KiB chunk so a slow
# download can be observed (and reclaimed) from the web UI. 0 in production.
DOWNLOAD_CHUNK_DELAY = float(os.environ.get("DOWNLOAD_CHUNK_DELAY_SECONDS", "0"))
DOWNLOAD_CHUNK_SIZE = int(os.environ.get("DOWNLOAD_CHUNK_SIZE_BYTES", "65536"))


def all_dirs() -> tuple[Path, ...]:
    # Computed dynamically so tests can point DATA_VOLUME at a temp directory.
    return (INBOX_DIR, DAILY_DIR, MONTHLY_DIR, TMP_DIR)


def ensure_dirs() -> None:
    for directory in all_dirs():
        directory.mkdir(parents=True, exist_ok=True)


def station_daily_dir(station: str) -> Path:
    return DAILY_DIR / station


def station_monthly_dir(station: str) -> Path:
    return MONTHLY_DIR / station


def daily_parquet_path(station: str, day: str, version_id: int) -> Path:
    return station_daily_dir(station) / f"daily-{day}-v{version_id}.parquet"


def monthly_parquet_path(station: str, month: str, gen_id: int) -> Path:
    return station_monthly_dir(station) / f"monthly-{month}-gen{gen_id}.parquet"


def inbox_csv_path(station: str, day: str, version_id: int) -> Path:
    return INBOX_DIR / f"{station}-{day}-v{version_id}.csv"

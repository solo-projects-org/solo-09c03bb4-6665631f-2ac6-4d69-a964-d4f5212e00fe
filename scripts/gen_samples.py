"""Generate the before/after correction sample pair for station ST001.

Run:  python3 scripts/gen_samples.py
The pair covers the same UTC day (2026-11-10). The "after" file is a full
replacement: it corrects four wrong PM2.5 readings and adds two missing
half-hour timestamps, exactly matching the upload contract.
"""
from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAY = dt.datetime(2026, 11, 10, tzinfo=dt.timezone.utc)
STATION = "ST001"


def ts(hour: int, minute: int = 0) -> int:
    return int((DAY.replace(hour=hour, minute=minute)).timestamp())


# Hourly readings; the first two and the evening peak values are wrong in the
# original file (instrument zero-drift), corrected below.
before_values = {
    0: 8, 1: 9, 2: 7, 3: 6, 4: 7, 5: 12, 6: 28, 7: 45,
    8: 52, 9: 47, 10: 38, 11: 33, 12: 30, 13: 29, 14: 31,
    15: 36, 16: 44, 17: 61, 18: 72, 19: 68, 20: 55, 21: 39,
    22: 24, 23: 15,
}
corrections = {0: 12, 1: 13, 18: 64, 19: 60}
after_values = {**before_values, **corrections}
# Two half-hour samples missing from the original upload.
after_extra = {ts(7, 30): 48, ts(12, 30): 31}


def write_csv(path: Path, values: dict[int, int]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["station", "utc_seconds", "pm25"])
        for sec, pm in sorted(values.items()):
            writer.writerow([STATION, sec, pm])


def main() -> None:
    before = {ts(h): pm for h, pm in before_values.items()}
    after = {ts(h): pm for h, pm in after_values.items()}
    after.update(after_extra)

    before_path = ROOT / "samples" / "before" / f"{STATION}-2026-11-10.csv"
    after_path = ROOT / "samples" / "after" / f"{STATION}-2026-11-10.csv"
    write_csv(before_path, before)
    write_csv(after_path, after)
    print(f"wrote {before_path} ({len(before)} rows)")
    print(f"wrote {after_path} ({len(after)} rows)")

    # A second day, so the month has two daily files and the page can show
    # that non-current days are reused and then reclaimed independently.
    day2 = dt.datetime(2026, 11, 11, tzinfo=dt.timezone.utc)
    day2_values = {
        (int((day2.replace(hour=h)).timestamp())): pm
        for h, pm in enumerate(
            [6, 7, 9, 14, 21, 30, 41, 49, 55, 52, 44, 38,
             35, 34, 37, 43, 51, 63, 70, 66, 57, 45, 33, 22])
    }
    day2_path = ROOT / "samples" / "before" / f"{STATION}-2026-11-11.csv"
    write_csv(day2_path, day2_values)
    print(f"wrote {day2_path} ({len(day2_values)} rows)")


if __name__ == "__main__":
    main()

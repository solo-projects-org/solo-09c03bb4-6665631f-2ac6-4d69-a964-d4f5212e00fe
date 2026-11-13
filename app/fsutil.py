"""Atomic file publishing helpers.

Rule of the system: a download URL may only ever point at a complete file.
Every Parquet/CSV is therefore built at a ``.tmp`` path, fsynced and then
renamed onto its final path. On the same filesystem ``os.replace`` is atomic,
so readers see either the old file or the new one, never a partial write.
"""
from __future__ import annotations

import os
from pathlib import Path

from . import config


def tmp_target(final_path: Path, tag: str) -> Path:
    return config.TMP_DIR / f"{final_path.name}.{tag}.{os.getpid()}.tmp"


def atomic_publish(tmp_path: Path, final_path: Path) -> None:
    """Flush + fsync a staging file, then atomically move it into place."""
    if not tmp_path.exists():
        raise RuntimeError(f"暂存文件不存在: {tmp_path}")
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(tmp_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp_path, final_path)
    # Best-effort directory fsync so the rename survives a host crash too.
    try:
        dfd = os.open(final_path.parent, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except OSError:
        pass


def discard_tmp(tmp_path: Path) -> None:
    try:
        tmp_path.unlink(missing_ok=True)
    except OSError:
        pass


def cleanup_tmp_dir() -> list[str]:
    """Remove staging leftovers from previous process lifetimes."""
    removed: list[str] = []
    if not config.TMP_DIR.exists():
        return removed
    for child in config.TMP_DIR.iterdir():
        if child.suffix == ".tmp":
            try:
                child.unlink()
                removed.append(str(child))
            except OSError:
                pass
    return removed

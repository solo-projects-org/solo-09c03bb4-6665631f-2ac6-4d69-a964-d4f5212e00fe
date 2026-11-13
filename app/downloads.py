"""In-process download leases.

A reclaimed file may already be streaming to a client. On POSIX an open file
descriptor survives ``unlink``, but a download that has been selected from the
database and not yet opened would break. Leases close that window:

* a download acquires a token *before* it re-checks the reclaim tombstone and
  before it opens the file;
* the reclaimer's ``unlink_if_idle`` checks the token table and performs the
  ``unlink`` under the same lock, so it can never race the open;
* a slow or disconnected client only holds its own tokens: deleting other
  files and all uploads keep working.

A lease is released exactly once, in the response coroutine's ``finally``;
that same ``finally`` also runs on client disconnect and task cancellation,
so a dead download can never pin a file.
"""
from __future__ import annotations

import itertools
import os
import threading
from pathlib import Path

import anyio
from starlette.responses import FileResponse

from . import config

_lock = threading.Lock()
_leases: dict[str, set[int]] = {}
_tokens = itertools.count(1)


class Lease:
    __slots__ = ("key", "token")

    def __init__(self, key: str, token: int) -> None:
        self.key = key
        self.token = token


def canonical(path: str | Path) -> str:
    return str(Path(path).absolute())


def acquire(path: str | Path) -> Lease:
    key = canonical(path)
    token = next(_tokens)
    with _lock:
        _leases.setdefault(key, set()).add(token)
    return Lease(key, token)


def acquire_existing(path: str | Path) -> tuple[Lease, os.stat_result] | None:
    """Lease a file and stat it atomically w.r.t. :func:`unlink_if_idle`.

    Returns ``None`` when the file has vanished, so a download can never start
    streaming a file that was reclaimed a moment earlier.
    """
    key = canonical(path)
    token = next(_tokens)
    with _lock:
        try:
            stat_result = os.stat(path)
        except OSError:
            return None
        _leases.setdefault(key, set()).add(token)
    return Lease(key, token), stat_result


def release(lease: Lease | None) -> None:
    if lease is None:
        return
    with _lock:
        tokens = _leases.get(lease.key)
        if tokens is not None:
            tokens.discard(lease.token)
            if not tokens:
                _leases.pop(lease.key, None)


def busy(path: str | Path) -> bool:
    with _lock:
        return bool(_leases.get(canonical(path)))


def active_count() -> int:
    with _lock:
        return sum(len(tokens) for tokens in _leases.values())


def unlink_if_idle(path: str | Path) -> tuple[str, str | None]:
    """Delete ``path`` unless a download holds it.

    Returns ``("deleted", None)`` (also when the file is already gone),
    ``("busy", None)`` while a lease exists, or ``("failed", message)``.
    The check and the unlink happen under the lease lock: a download either
    acquired its token first (so we skip) or acquires it after the unlink (and
    then sees the durable tombstone and refuses).
    """
    key = canonical(path)
    with _lock:
        if _leases.get(key):
            return "busy", None
        p = Path(path)
        if not p.exists():
            return "deleted", None
        try:
            p.unlink()
        except OSError as exc:
            return "failed", f"{type(exc).__name__}: {exc}"
        return "deleted", None


class LeasedFileResponse(FileResponse):
    """``FileResponse`` that holds a download lease for its whole lifetime.

    The lease is released in ``finally`` so it covers a client disconnect and
    task cancellation too: the file is never left pinned by a dead download,
    and the bytes already promised keep flowing from the open descriptor even
    if a reclamation deletes the name after the lease is gone.
    """

    def __init__(self, path: str, lease: Lease, **kwargs) -> None:
        super().__init__(path, **kwargs)
        self._lease: Lease | None = lease
        self.chunk_size = config.DOWNLOAD_CHUNK_SIZE

    async def _handle_simple(self, send, send_header_only, send_pathsend):
        # Demo/test only: a configurable inter-chunk pause makes a "slow
        # download" observable from the UI without a huge sample file.
        delay = config.DOWNLOAD_CHUNK_DELAY
        if delay <= 0 or send_header_only or send_pathsend:
            return await super()._handle_simple(send, send_header_only, send_pathsend)
        await send({"type": "http.response.start", "status": self.status_code,
                    "headers": self.raw_headers})
        async with self._open_file() as file:
            more_body = True
            while more_body:
                chunk = await file.read(self.chunk_size)
                more_body = len(chunk) == self.chunk_size
                await send({"type": "http.response.body", "body": chunk,
                            "more_body": more_body})
                if more_body:
                    await anyio.sleep(delay)

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            release(self._lease)
            self._lease = None


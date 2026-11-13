"""Shared fixtures: isolated data volume and a real uvicorn server subprocess.

Tests run against an actual HTTP server (not an in-process ASGI client) so that
restart/crash-recovery scenarios exercise the real startup path.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_ready(base_url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(base_url + "/health", timeout=1) as resp:
                if resp.status == 200:
                    return
        except OSError as exc:
            last = exc
        time.sleep(0.2)
    raise RuntimeError(f"server not ready: {last}")


class Server:
    def __init__(self, volume: Path, port: int, env_extra: dict[str, str] | None = None):
        self.volume = volume
        self.port = port
        self.base_url = f"http://127.0.0.1:{port}"
        self.proc: subprocess.Popen | None = None
        self.env_extra = env_extra or {}

    def start(self) -> None:
        env = os.environ.copy()
        env["DATA_VOLUME"] = str(self.volume)
        env["PYTHONPATH"] = str(ROOT)
        env.update(self.env_extra)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(self.port), "--log-level", "warning"],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        wait_ready(self.base_url)

    def stop(self, timeout: float = 10.0) -> str:
        assert self.proc is not None
        self.proc.terminate()
        try:
            out, _ = self.proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            out, _ = self.proc.communicate()
        self.proc = None
        return out or ""

    def kill9(self) -> str:
        """Simulate an abrupt mid-task crash (no graceful shutdown)."""
        assert self.proc is not None
        self.proc.kill()
        out, _ = self.proc.communicate()
        self.proc = None
        return out or ""

    def client(self, timeout: float = 10) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, timeout=timeout)


@pytest.fixture()
def volume(tmp_path: Path) -> Path:
    vol = tmp_path / "data"
    vol.mkdir()
    return vol


@pytest.fixture()
def server(volume: Path):
    srv = Server(volume, free_port())
    srv.start()
    yield srv
    if srv.proc is not None:
        srv.stop()


@pytest.fixture()
def make_server(volume: Path):
    """Factory for a server with extra environment (chunk size, delays…)."""
    servers: list[Server] = []

    def factory(env_extra: dict[str, str] | None = None) -> Server:
        srv = Server(volume, free_port(), env_extra)
        srv.start()
        servers.append(srv)
        return srv

    yield factory
    for srv in servers:
        if srv.proc is not None:
            srv.stop()


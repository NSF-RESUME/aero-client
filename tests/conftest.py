"""Shared pytest fixtures for the AERO client tests."""

import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest
import uvicorn

from tests.download_server import create_app

_DATA_DIR = Path(__file__).parent / "data"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class ServerHandle:
    """Handle to the running test server."""

    base_url: str
    flows: dict = field(default_factory=dict)

    def add_flow(self, flow_id: str, filename: str, name: str, id: str) -> None:
        """Register the data dict returned by ``GET /flow/{flow_id}``."""
        self.flows[flow_id] = {"filename": filename, "name": name, "id": id}


@pytest.fixture
def download_server():
    """Run the FastAPI test server in-process and yield a ServerHandle.

    Serves files from ``tests/data``; tests register flows via
    ``handle.add_flow(...)``. The server runs on a free port in a daemon thread
    and is shut down on teardown.
    """
    flows: dict = {}
    app = create_app(_DATA_DIR, flows=flows)

    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait for the server to come up.
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("test server did not start in time")
        time.sleep(0.02)

    try:
        yield ServerHandle(base_url=f"http://127.0.0.1:{port}", flows=flows)
    finally:
        server.should_exit = True
        thread.join(timeout=10)

"""Hermetic test for aero_client.jobs.download() against a local FastAPI server.

Unlike tests/jobs_test.py (which targets a real AERO server), this runs fully
offline: the ``download_server`` fixture serves a file locally and answers the
``/flow/{flow_id}`` lookup, and CONF is pointed at it. download() is then exercised
end-to-end and the retrieved file is compared against the file the server served.
"""

import hashlib
import os
from pathlib import Path

_DATA_DIR = Path(__file__).parent / "data"
_CONFIG = str(_DATA_DIR / "config.toml")
FILENAME = "Stickney Water Reclamation Plant - North.csv"

# Ensure importing aero_client.utils (which loads CONF at import time) finds a
# real config without requiring ~/.aero. The test repoints CONF at the running
# server below; this only needs to make the import succeed.
os.environ.setdefault("AERO_CONFIG_FILE", _CONFIG)

import aero_client.utils as utils  # noqa: E402
from aero_client.config import load_conf  # noqa: E402
from aero_client.jobs import download  # noqa: E402


def test_download_against_local_server(download_server, tmp_path, monkeypatch):
    download_server.add_flow("dummy-flow", FILENAME, name="post_preds", id="data-1")

    # Bypass any ambient HTTP(S) proxy for the local server (requests honors these).
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")

    # Point CONF at the running test server. download() imports CONF lazily at
    # call time, so reassigning it here is reliable (monkeypatch auto-reverts).
    conf = load_conf(_CONFIG, symlink=False)
    conf.server_url = download_server.base_url
    conf.server_address = download_server.base_url
    monkeypatch.setattr(utils, "CONF", conf)

    kwargs = {
        "aero": {
            "flow_id": "dummy-flow",
            "output_data": {
                "post_preds": {"temp_dir": str(tmp_path)},
            },
        }
    }

    _, out = download(**kwargs)

    meta = out["aero"]["output_data"]["post_preds"]
    served = (_DATA_DIR / FILENAME).read_bytes()

    assert meta["download"] is True
    assert meta["id"] == "data-1"
    assert Path(meta["file"]).read_bytes() == served
    # checksum is md5 of the raw retrieved bytes -- authoritative content check.
    assert meta["checksum"] == hashlib.md5(served).hexdigest()

"""Hermetic tests for aero_client.jobs.download() against a local FastAPI server.

Unlike tests/jobs_test.py (which targets a real AERO server), these run fully
offline: the ``download_server`` fixture serves a file locally and answers the
``/flow/{flow_id}`` lookup, and CONF is pointed at it. download() is then exercised
end-to-end and the retrieved file is compared against the file the server served.
"""

import hashlib
import os
from pathlib import Path
from urllib.parse import quote

_DATA_DIR = Path(__file__).parent / "data"
_CONFIG = str(_DATA_DIR / "config.toml")
FILENAME = "Stickney Water Reclamation Plant - North.csv"

# Ensure importing aero_client.utils (which loads CONF at import time) finds a
# real config without requiring ~/.aero. The tests repoint CONF at the running
# server below; this only needs to make the import succeed.
os.environ.setdefault("AERO_CONFIG_FILE", _CONFIG)

import aero_client.utils as utils  # noqa: E402
from aero_client.config import load_conf  # noqa: E402
from aero_client.jobs import download  # noqa: E402


def _point_conf_at_server(server, monkeypatch):
    """Bypass any ambient proxy and point CONF at the running test server.

    download() imports CONF lazily at call time, so reassigning it here is
    reliable (monkeypatch auto-reverts after the test).
    """
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")

    conf = load_conf(_CONFIG, symlink=False)
    conf.server_url = server.base_url
    conf.server_address = server.base_url
    monkeypatch.setattr(utils, "CONF", conf)

    # The local server ignores the AERO auth header, so the Globus token is
    # irrelevant -- stub load_tokens() so the test needs no real token file.
    monkeypatch.setattr(
        utils, "load_tokens", lambda: {conf.portal_client_id: {"refresh_token": "x"}}
    )


def test_download_against_local_server(download_server, tmp_path, monkeypatch):
    download_server.add_flow("dummy-flow", FILENAME, name="post_preds", id="data-1")
    _point_conf_at_server(download_server, monkeypatch)

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


def test_download_basic_auth(download_server, tmp_path, monkeypatch):
    import requests

    # Flow url points at the basic-auth endpoint (no creds in the url).
    download_server.add_flow(
        "auth-flow", FILENAME, name="post_preds", id="data-1", secure=True
    )
    _point_conf_at_server(download_server, monkeypatch)

    # Credentials come from <aero_dir>/<host>.yaml (i.e. ~/.aero/<host>.yaml).
    # The server host is 127.0.0.1, so download() reads 127.0.0.1.yaml.
    creds_dir = tmp_path / "aero"
    creds_dir.mkdir()
    monkeypatch.setattr(utils.CONF, "aero_dir", creds_dir)
    user, pwd = download_server.auth
    (creds_dir / "127.0.0.1.yaml").write_text(f"username: {user}\npassword: {pwd}\n")

    # Sanity: the secure endpoint really does require auth.
    unauth = requests.get(f"{download_server.base_url}/secure-files/{quote(FILENAME)}")
    assert unauth.status_code == 401

    kwargs = {
        "aero": {
            "flow_id": "auth-flow",
            "output_data": {
                "post_preds": {"temp_dir": str(tmp_path)},
            },
        }
    }

    _, out = download(**kwargs)

    meta = out["aero"]["output_data"]["post_preds"]
    served = (_DATA_DIR / FILENAME).read_bytes()

    assert meta["download"] is True
    assert Path(meta["file"]).read_bytes() == served
    assert meta["checksum"] == hashlib.md5(served).hexdigest()

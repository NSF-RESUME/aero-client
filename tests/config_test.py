"""Tests for profile-aware config loading in aero_client.config.load_conf.

Fully offline: builds config.toml fixtures under tmp_path and calls load_conf
directly (symlink=False), so no server, Globus creds, or ~/.aero file is needed.
"""

import pytest

from aero_client.config import load_conf

_PROFILES = """\
[default]
server = "https://prod.example.org/aero"
cache_dir = "{cache}"
client_uuid = "11111111-1111-1111-1111-111111111111"
portal_client_id = "11111111-1111-1111-1111-111111111111"

[testing]
server = "http://127.0.0.1:8000"
cache_dir = "{cache}"
client_uuid = "22222222-2222-2222-2222-222222222222"
portal_client_id = "22222222-2222-2222-2222-222222222222"
"""

_LEGACY = """\
client_uuid = "33333333-3333-3333-3333-333333333333"
portal_client_id = "33333333-3333-3333-3333-333333333333"

[aero]
  server = "https://legacy.example.org"
  cache_dir = "{cache}"
"""


def _write(tmp_path, text):
    cfg = tmp_path / "config.toml"
    cfg.write_text(text.format(cache=tmp_path))
    return str(cfg)


def test_default_profile(tmp_path):
    conf = load_conf(_write(tmp_path, _PROFILES), symlink=False)
    assert conf.server_url == "https://prod.example.org/aero"
    assert conf.client_uuid == "11111111-1111-1111-1111-111111111111"


def test_selected_profile(tmp_path):
    conf = load_conf(_write(tmp_path, _PROFILES), symlink=False, profile="testing")
    assert conf.server_url == "http://127.0.0.1:8000"
    assert conf.client_uuid == "22222222-2222-2222-2222-222222222222"


def test_unknown_profile_raises(tmp_path):
    with pytest.raises(KeyError):
        load_conf(_write(tmp_path, _PROFILES), symlink=False, profile="bogus")


def test_legacy_format_loads_as_default(tmp_path):
    conf = load_conf(_write(tmp_path, _LEGACY), symlink=False)
    assert conf.server_url == "https://legacy.example.org"
    assert conf.client_uuid == "33333333-3333-3333-3333-333333333333"

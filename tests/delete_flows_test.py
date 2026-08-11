"""`aero delete-flows` — the API helpers and the confirmation gate.

Deleting is irreversible and cancels live Globus timers, so the prompt matters
as much as the request does.
"""

import importlib
import sys

import pytest


DATA_ID = "6f76e7b5-0f1d-4b4d-9769-e8b85dca5266"


@pytest.fixture(name="api")
def api_fixture(monkeypatch):
    """Import aero_client.api with auth stubbed; it mints a token at import."""
    from aero_client import utils

    monkeypatch.setattr(utils, "_client_auth", lambda: "test-token")
    sys.modules.pop("aero_client.api", None)
    module = importlib.import_module("aero_client.api")
    yield module
    sys.modules.pop("aero_client.api", None)


class _Resp:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.content = b"error"

    def json(self):
        return self._payload


def test_get_data_flows_hits_the_right_url(api, monkeypatch):
    seen = {}

    def _get(url, headers=None, **kw):
        seen["url"] = url
        return _Resp([{"flow_id": "f1", "role": "ingestion"}])

    monkeypatch.setattr(api.requests, "get", _get)

    out = api.get_data_flows(DATA_ID)

    assert seen["url"].endswith(f"/data/{DATA_ID}/flows")
    assert out[0]["role"] == "ingestion"


def test_delete_data_flows_uses_the_delete_verb(api, monkeypatch):
    seen = {}

    def _delete(url, headers=None, **kw):
        seen["url"] = url
        return _Resp([{"flow_id": "f1", "role": "analysis", "provenance_deleted": 2}])

    monkeypatch.setattr(api.requests, "delete", _delete)

    out = api.delete_data_flows(DATA_ID)

    assert seen["url"].endswith(f"/data/{DATA_ID}/flows")
    assert out[0]["provenance_deleted"] == 2


def test_delete_raises_on_an_error_response(api, monkeypatch):
    monkeypatch.setattr(api.requests, "delete", lambda *a, **k: _Resp(None, 404))

    with pytest.raises(api.ClientError):
        api.delete_data_flows(DATA_ID)


# --------------------------------------------------------------------------
# the CLI's confirmation gate
# --------------------------------------------------------------------------


ATTACHED = [
    {
        "flow_id": "d725132c",
        "role": "ingestion",
        "policy": 4,
        "description": None,
        "timer_job_id": None,
    },
    {
        "flow_id": "f9aaedb0",
        "role": "analysis",
        "policy": 2,
        "description": "LHS results CSV summary",
        "timer_job_id": None,
    },
]


@pytest.fixture(name="cli")
def cli_fixture(api, monkeypatch):
    """Run the CLI with the api module stubbed, recording deletes."""
    from aero_client import cli as cli_module

    deletes = []
    monkeypatch.setattr(api, "get_data_flows", lambda data_id: ATTACHED)
    monkeypatch.setattr(
        api,
        "delete_data_flows",
        lambda data_id: deletes.append(data_id)
        or [{**f, "provenance_deleted": 1, "timer": None} for f in ATTACHED],
    )

    def run(argv, answer=None):
        monkeypatch.setattr(sys, "argv", ["aero", *argv])
        if answer is not None:
            monkeypatch.setattr("builtins.input", lambda _prompt="": answer)
        cli_module.main()
        return deletes

    return run


def test_a_negative_answer_deletes_nothing(cli, capsys):
    deletes = cli(["delete-flows", DATA_ID], answer="n")

    assert deletes == []
    assert "Aborted" in capsys.readouterr().out


def test_bare_enter_deletes_nothing(cli, capsys):
    """The prompt is [y/N] -- the default must be the safe one."""
    deletes = cli(["delete-flows", DATA_ID], answer="")

    assert deletes == []


def test_confirming_deletes(cli, capsys):
    deletes = cli(["delete-flows", DATA_ID], answer="y")

    assert deletes == [DATA_ID]
    out = capsys.readouterr().out
    assert "2 flow(s) deleted" in out


def test_yes_flag_skips_the_prompt(cli, monkeypatch, capsys):
    def no_prompt(_p=""):
        raise AssertionError("-y must not prompt")

    monkeypatch.setattr("builtins.input", no_prompt)
    deletes = cli(["delete-flows", DATA_ID, "-y"])

    assert deletes == [DATA_ID]


def test_the_preview_names_what_will_go(cli, capsys):
    cli(["delete-flows", DATA_ID], answer="n")

    out = capsys.readouterr().out
    assert "ingestion" in out and "analysis" in out
    assert "d725132c" in out and "f9aaedb0" in out
    assert "INGESTION_EVENT" in out and "ANY" in out
    # the consequences, not just the count
    assert "provenance" in out and "timer" in out


def test_nothing_attached_does_not_prompt(api, monkeypatch, capsys):
    from aero_client import cli as cli_module

    monkeypatch.setattr(api, "get_data_flows", lambda data_id: [])
    monkeypatch.setattr(
        api, "delete_data_flows", lambda data_id: pytest.fail("must not delete")
    )
    monkeypatch.setattr(
        "builtins.input", lambda _p="": pytest.fail("must not prompt")
    )
    monkeypatch.setattr(sys, "argv", ["aero", "delete-flows", DATA_ID])

    cli_module.main()

    assert "No flows are attached" in capsys.readouterr().out

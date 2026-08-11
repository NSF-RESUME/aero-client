"""Timer-driven ingestion sources.

A source is pulled on the notify webhook by default; policy=INGESTION with a
timer_delay pulls it on a schedule instead. Combinations that cannot work are
rejected rather than silently ignored.
"""

import importlib
import sys

import pytest


URL = "https://minio.internal:9000/traffic/report.xml.gz"
COLLECTION = {
    "collection_uuid": "94d05b66-bf20-435d-a406-2577096b6cb6",
    "collection_url": "https://g-18d480.a4b6ed.a567.data.globus.org/",
}


@pytest.fixture(name="api")
def api_fixture(monkeypatch):
    """Import aero_client.api with auth stubbed; it mints a token at import."""
    from aero_client import utils

    monkeypatch.setattr(utils, "_client_auth", lambda: "test-token")
    sys.modules.pop("aero_client.api", None)
    module = importlib.import_module("aero_client.api")
    yield module
    sys.modules.pop("aero_client.api", None)


@pytest.fixture(name="registered")
def registered_fixture(api, monkeypatch):
    """Capture what create_source passes to register_flow."""
    calls = []
    monkeypatch.setattr(api, "get_source_type", lambda n: None)
    monkeypatch.setattr(
        api, "register_flow", lambda **kw: calls.append(kw) or {"id": "flow"}
    )
    monkeypatch.setattr(api, "register_aero_function", lambda fn: "stage-uuid")
    return calls


def test_the_default_is_notify_driven(api, registered):
    from aero_client.utils import PolicyEnum

    api.create_source(name="traffic", url=URL, endpoint_uuid="e", **COLLECTION)

    assert registered[0]["policy"] is PolicyEnum.INGESTION_EVENT
    assert registered[0]["timer_delay"] is None


def test_a_timer_driven_source(api, registered):
    from aero_client.utils import PolicyEnum

    api.create_source(
        name="traffic",
        url=URL,
        endpoint_uuid="e",
        policy=PolicyEnum.INGESTION,
        timer_delay=3600,
        **COLLECTION,
    )

    assert registered[0]["policy"] is PolicyEnum.INGESTION
    assert registered[0]["timer_delay"] == 3600


def test_ingestion_without_a_timer_defers_to_the_server(api, registered):
    """Omitting the delay is allowed; the server applies its own default."""
    from aero_client.utils import PolicyEnum

    api.create_source(
        name="traffic",
        url=URL,
        endpoint_uuid="e",
        policy=PolicyEnum.INGESTION,
        **COLLECTION,
    )

    assert registered[0]["timer_delay"] is None


def test_a_timer_without_the_matching_policy_is_rejected(api, registered):
    """The flag used to be parsed and silently dropped; now it has to be honest."""
    with pytest.raises(ValueError) as exc:
        api.create_source(
            name="traffic", url=URL, endpoint_uuid="e", timer_delay=3600, **COLLECTION
        )

    assert "policy=INGESTION" in str(exc.value)
    assert registered == []


def test_a_timer_on_a_no_copy_source_is_rejected(api, registered):
    """No-copy registers no flow at all, so there is nothing to schedule."""
    from aero_client.utils import PolicyEnum

    with pytest.raises(ValueError) as exc:
        api.create_source(
            name="traffic",
            url=URL,
            no_copy=True,
            policy=PolicyEnum.INGESTION,
            timer_delay=3600,
        )

    assert "no-copy" in str(exc.value)
    assert registered == []


def test_a_policy_that_does_not_produce_a_source_is_rejected(api, registered):
    from aero_client.utils import PolicyEnum

    with pytest.raises(ValueError) as exc:
        api.create_source(
            name="traffic",
            url=URL,
            endpoint_uuid="e",
            policy=PolicyEnum.ANY,
            **COLLECTION,
        )

    assert "aero register" in str(exc.value)
    assert registered == []


def test_adding_to_an_existing_type_warns_rather_than_failing(api, monkeypatch, caplog):
    """The same YAML is run repeatedly to add urls; erroring would break that.

    But the policy and timer genuinely are not applied, so say so.
    """
    from aero_client.utils import PolicyEnum

    monkeypatch.setattr(api, "get_source_type", lambda n: {"data_id": "existing"})
    monkeypatch.setattr(api, "add_type_url", lambda n, u: {"data_id": "existing"})
    monkeypatch.setattr(
        api, "register_flow", lambda **kw: pytest.fail("must not register a flow")
    )

    with caplog.at_level("WARNING"):
        out = api.create_source(
            name="traffic",
            url=URL,
            type_name="traffic",
            endpoint_uuid="e",
            policy=PolicyEnum.INGESTION,
            timer_delay=3600,
            **COLLECTION,
        )

    assert out["data_id"] == "existing"
    assert "not applied" in caplog.text


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


@pytest.fixture(name="cli")
def cli_fixture(api, monkeypatch):
    from aero_client import cli as cli_module

    calls = []
    monkeypatch.setattr(api, "get_source_type", lambda n: None)
    monkeypatch.setattr(
        api, "create_source", lambda **kw: calls.append(kw) or {"id": "new-data-id"}
    )

    def run(argv):
        monkeypatch.setattr(sys, "argv", ["aero", *argv])
        cli_module.main()
        return calls

    return run


def _base_argv():
    return [
        "create",
        "-n",
        "traffic",
        "-u",
        URL,
        "-C",
        COLLECTION["collection_uuid"],
        "-c",
        COLLECTION["collection_url"],
        "-g",
        "endpoint-uuid",
    ]


def test_cli_passes_policy_and_timer(cli):
    from aero_client.utils import PolicyEnum

    calls = cli([*_base_argv(), "-p", "INGESTION", "-t", "3600"])

    assert calls[0]["policy"] is PolicyEnum.INGESTION
    assert calls[0]["timer_delay"] == 3600


def test_cli_defaults_to_notify_driven(cli):
    from aero_client.utils import PolicyEnum

    calls = cli(_base_argv())

    assert calls[0]["policy"] is PolicyEnum.INGESTION_EVENT
    assert calls[0]["timer_delay"] is None


def test_cli_reads_policy_and_timer_from_yaml(cli, tmp_path):
    from aero_client.utils import PolicyEnum

    cfg = tmp_path / "source.yaml"
    cfg.write_text(
        "name: traffic\n"
        f"url: {URL}\n"
        f"collection_uuid: {COLLECTION['collection_uuid']}\n"
        f"collection_url: {COLLECTION['collection_url']}\n"
        "endpoint_uuid: endpoint-uuid\n"
        "policy: INGESTION\n"
        "timer: 900\n"
    )

    calls = cli(["create", "-f", str(cfg)])

    assert calls[0]["policy"] is PolicyEnum.INGESTION
    assert calls[0]["timer_delay"] == 900


def test_cli_flags_override_the_yaml(cli, tmp_path):
    cfg = tmp_path / "source.yaml"
    cfg.write_text(
        "name: traffic\n"
        f"url: {URL}\n"
        f"collection_uuid: {COLLECTION['collection_uuid']}\n"
        f"collection_url: {COLLECTION['collection_url']}\n"
        "endpoint_uuid: endpoint-uuid\n"
        "policy: INGESTION\n"
        "timer: 900\n"
    )

    calls = cli(["create", "-f", str(cfg), "-t", "60"])

    assert calls[0]["timer_delay"] == 60


def test_cli_rejects_an_unknown_policy(cli):
    with pytest.raises(SystemExit):
        cli([*_base_argv(), "-p", "SOMETIMES"])


def test_cli_surfaces_a_rejected_combination_as_a_usage_error(api, monkeypatch):
    """A ValueError from the API should read as a CLI error, not a traceback."""
    from aero_client import cli as cli_module

    monkeypatch.setattr(api, "get_source_type", lambda n: None)
    monkeypatch.setattr(sys, "argv", ["aero", *_base_argv(), "-t", "3600"])

    with pytest.raises(SystemExit):
        cli_module.main()


# --------------------------------------------------------------------------
# kwargs for the verifier function
# --------------------------------------------------------------------------


def test_kwargs_reach_the_flow(api, registered):
    api.create_source(
        name="output",
        url=URL,
        endpoint_uuid="e",
        function_uuid="fn",
        kwargs={"bin_freq": "5min"},
        **COLLECTION,
    )

    assert registered[0]["kwargs"] == {"bin_freq": "5min"}


def test_kwargs_on_a_no_copy_source_are_rejected(api, registered):
    """No function runs, so there is nothing to pass them to."""
    with pytest.raises(ValueError) as exc:
        api.create_source(
            name="output", url=URL, no_copy=True, kwargs={"bin_freq": "5min"}
        )

    assert "bin_freq" in str(exc.value)
    assert registered == []


def test_cli_passes_kwargs(cli):
    calls = cli([*_base_argv(), "-k", "bin_freq=5min", "foo=7"])

    assert calls[0]["kwargs"] == {"bin_freq": "5min", "foo": "7"}


def test_cli_kwargs_merge_over_the_yaml(cli, tmp_path):
    cfg = tmp_path / "source.yaml"
    cfg.write_text(
        "name: output\n"
        f"url: {URL}\n"
        f"collection_uuid: {COLLECTION['collection_uuid']}\n"
        f"collection_url: {COLLECTION['collection_url']}\n"
        "endpoint_uuid: endpoint-uuid\n"
        "kwargs:\n"
        "  bin_freq: 1min\n"
        "  db_dsn: postgres://x\n"
    )

    calls = cli(["create", "-f", str(cfg), "-k", "bin_freq=5min"])

    # the flag overrides that one key; the rest of the file survives
    assert calls[0]["kwargs"] == {"bin_freq": "5min", "db_dsn": "postgres://x"}


def test_no_kwargs_is_an_empty_dict(cli):
    calls = cli(_base_argv())

    assert calls[0]["kwargs"] == {}

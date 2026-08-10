"""Client side of typed notification sources: fetch-by-url and create dispatch."""

from pathlib import Path

import pytest
import requests

from aero_client.utils import aero_format


TRIGGER = "http://127.0.0.1:9000/traffic/LinkTrafficReport.xml.gz"
SIGNED = f"{TRIGGER}?X-Amz-Signature=abc&X-Amz-Expires=3600"


class _Resp:
    def __init__(self, content=b"payload", status_code=200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} for url")


@pytest.fixture(name="fetched")
def fetched_fixture(monkeypatch):
    """Record every requests.get aero_format makes, and what headers it sent."""
    calls = []

    def _get(url, headers=None, **kwargs):
        calls.append({"url": url, "headers": headers})
        return _Resp()

    monkeypatch.setattr("requests.get", _get)
    return calls


def _kwargs(tmp_path, **input_extra):
    entry = {"id": "data-uuid", "version": 3, "tmp_dir": str(tmp_path)}
    entry.update(input_extra)
    # Only "aero" is stripped before the call; any other top-level key is passed
    # to the user function as one of its own kwargs.
    return {
        "aero": {
            "input_data": {"report": entry},
            "output_data": {},
            "flow_id": "flow1",
        },
    }


def test_signed_url_is_preferred_when_present(tmp_path, fetched):
    seen = {}

    def user_function(report):
        seen["path"] = report
        seen["content"] = Path(report).read_bytes()
        return []

    aero_format(user_function)(
        **_kwargs(tmp_path, trigger_url=TRIGGER, signed_url=SIGNED)
    )

    assert len(fetched) == 1
    assert fetched[0]["url"] == SIGNED
    # a presigned url carries its own auth; a Globus transfer token would be wrong
    assert fetched[0]["headers"] is None
    assert seen["content"] == b"payload"


def test_trigger_url_used_when_no_signature(tmp_path, fetched):
    """§7b: a run this object didn't trigger still resolves a url, just unsigned."""

    def user_function(report):
        return []

    aero_format(user_function)(**_kwargs(tmp_path, trigger_url=TRIGGER))

    assert fetched[0]["url"] == TRIGGER


def test_temp_file_keeps_the_object_name(tmp_path, fetched):
    seen = {}

    def user_function(report):
        seen["path"] = report
        return []

    aero_format(user_function)(
        **_kwargs(tmp_path, trigger_url=TRIGGER, signed_url=SIGNED)
    )

    # named from the trigger url, so no signing query params leak into the name
    assert Path(seen["path"]).name == "LinkTrafficReport.xml.gz"


def test_error_response_raises_instead_of_being_written(tmp_path, monkeypatch):
    """A 403 from an expired signature must not reach the function as data."""

    def _get(url, headers=None, **kwargs):
        return _Resp(content=b"<Error>AccessDenied</Error>", status_code=403)

    monkeypatch.setattr("requests.get", _get)

    def user_function(report):
        raise AssertionError("must not be called with an error page")

    with pytest.raises(requests.HTTPError):
        aero_format(user_function)(
            **_kwargs(tmp_path, trigger_url=TRIGGER, signed_url=SIGNED)
        )


def test_urls_passed_only_to_functions_that_ask(tmp_path, fetched):
    seen = {}

    def wants_urls(report, report_url, report_signed_url):
        seen["url"] = report_url
        seen["signed"] = report_signed_url
        return []

    aero_format(wants_urls)(
        **_kwargs(tmp_path, trigger_url=TRIGGER, signed_url=SIGNED)
    )
    assert seen == {"url": TRIGGER, "signed": SIGNED}


def test_functions_without_url_params_are_untouched(tmp_path, fetched):
    def plain(report):
        return []

    # would TypeError on an unexpected kwarg if the urls were passed unconditionally
    aero_format(plain)(**_kwargs(tmp_path, trigger_url=TRIGGER, signed_url=SIGNED))


def test_collection_fetch_still_used_without_a_trigger_url(tmp_path, fetched, monkeypatch):
    """Copy sources keep reading from the Globus collection, with their token."""
    monkeypatch.setattr(
        "aero_client.utils.get_transfer_token", lambda collection_uuid: "tok"
    )

    def user_function(report):
        return []

    aero_format(user_function)(
        **_kwargs(
            tmp_path,
            collection_uuid="c-uuid",
            collection_url="https://collection.example/",
            file_bn="stored-object-name",
        )
    )

    assert fetched[0]["url"] == "https://collection.example/stored-object-name"
    assert fetched[0]["headers"] == {"Authorization": "Bearer tok"}


def test_temp_files_are_cleaned_up(tmp_path, fetched):
    def user_function(report):
        return []

    aero_format(user_function)(
        **_kwargs(tmp_path, trigger_url=TRIGGER, signed_url=SIGNED)
    )

    assert list(tmp_path.iterdir()) == []


# --------------------------------------------------------------------------
# create_source dispatch
# --------------------------------------------------------------------------


@pytest.fixture(name="api")
def api_fixture(monkeypatch):
    """Import aero_client.api with auth stubbed.

    The module mints an access token at import time, which would otherwise drop
    into an interactive Globus login prompt.
    """
    import importlib
    import sys

    from aero_client import utils

    monkeypatch.setattr(utils, "_client_auth", lambda: "test-token")
    sys.modules.pop("aero_client.api", None)
    module = importlib.import_module("aero_client.api")
    yield module
    sys.modules.pop("aero_client.api", None)


def test_source_id_reads_every_response_shape(api):
    source_id = api.source_id

    # flow registration (copy path)
    assert source_id({"contributed_to": [{"id": "flow-data"}]}) == "flow-data"
    # POST /data/source (no-copy path)
    assert source_id({"id": "bare-data"}) == "bare-data"
    # POST /data/types/{name}/urls (existing type)
    assert source_id({"name": "traffic", "data_id": "typed-data"}) == "typed-data"
    assert source_id({}) is None


def test_create_source_adds_url_to_an_existing_type(api, monkeypatch):
    calls = {}
    monkeypatch.setattr(api, "get_source_type", lambda n: {"data_id": "existing"})
    monkeypatch.setattr(
        api, "add_type_url", lambda n, u: calls.setdefault("add", (n, u)) or {}
    )
    monkeypatch.setattr(
        api, "register_flow", lambda **kw: pytest.fail("must not register a flow")
    )

    api.create_source(name="traffic", url=TRIGGER, type_name="traffic")
    assert calls["add"] == ("traffic", TRIGGER)


def test_create_source_no_copy_skips_the_flow(api, monkeypatch):
    calls = {}
    monkeypatch.setattr(api, "get_source_type", lambda n: None)
    monkeypatch.setattr(
        api,
        "create_data_source",
        lambda **kw: calls.setdefault("data_source", kw) or {"id": "new"},
    )
    monkeypatch.setattr(
        api, "register_flow", lambda **kw: pytest.fail("must not register a flow")
    )

    api.create_source(
        name="traffic", url=TRIGGER, type_name="traffic", no_copy=True
    )

    assert calls["data_source"]["no_copy"] is True
    assert calls["data_source"]["type_name"] == "traffic"


def test_create_source_copy_path_passes_the_type_through(api, monkeypatch):
    calls = {}
    monkeypatch.setattr(api, "get_source_type", lambda n: None)
    monkeypatch.setattr(
        api, "register_flow", lambda **kw: calls.setdefault("flow", kw) or {}
    )

    api.create_source(
        name="traffic",
        url=TRIGGER,
        collection_uuid="c",
        collection_url="https://c/",
        endpoint_uuid="e",
        function_uuid="f",
        type_name="traffic",
    )

    assert calls["flow"]["output_data"]["traffic"]["type"] == "traffic"


def test_create_source_untyped_copy_path_is_unchanged(api, monkeypatch):
    calls = {}
    monkeypatch.setattr(
        api, "get_source_type", lambda n: pytest.fail("no type lookup expected")
    )
    monkeypatch.setattr(
        api, "register_flow", lambda **kw: calls.setdefault("flow", kw) or {}
    )

    api.create_source(
        name="legacy",
        url=TRIGGER,
        collection_uuid="c",
        collection_url="https://c/",
        endpoint_uuid="e",
        function_uuid="f",
    )

    assert "type" not in calls["flow"]["output_data"]["legacy"]

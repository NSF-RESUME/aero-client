"""Which revision an input is, passed to the analysis function.

`<name>_version` and `<name>_version_id` follow the same opt-in-by-parameter-name
rule as the url extras, and apply whatever the copy mode -- every input has a
version, whether or not AERO holds its bytes.
"""

import pytest


A_URL = "https://phire.egs.anl.gov:9000/temporary/wind/a.csv"
VERSION_ID = "4af0e417-0527-43c8-99e7-22cfd88ef951"


class _Resp:
    content = b"payload"
    status_code = 200

    def raise_for_status(self):
        pass


@pytest.fixture(name="fetched", autouse=True)
def fetched_fixture(monkeypatch):
    urls = []
    monkeypatch.setattr(
        "requests.get", lambda url, headers=None, **kw: urls.append(url) or _Resp()
    )
    monkeypatch.setattr(
        "aero_client.utils.get_transfer_token", lambda collection_uuid: "tok"
    )
    return urls


def _run(fn, entry, tmp_path):
    from aero_client.utils import aero_format

    entry.setdefault("tmp_dir", str(tmp_path))
    return aero_format(fn)(
        **{"aero": {"input_data": {"a_input": entry}, "output_data": {}, "flow_id": "f"}}
    )


COPY = {"id": "d", "version": 7, "version_id": VERSION_ID,
        "collection_url": "https://c/", "file_bn": "stored", "collection_uuid": "u"}
NO_COPY = {"id": "d", "version": 7, "version_id": VERSION_ID, "trigger_url": A_URL}
OPTED_OUT = {"id": "d", "version": 7, "version_id": VERSION_ID,
             "trigger_url": A_URL, "fetch": False}


@pytest.mark.parametrize(
    "entry, path",
    [(COPY, "copy"), (NO_COPY, "no-copy"), (OPTED_OUT, "fetch:false")],
    ids=["copy", "no-copy", "fetch-false"],
)
def test_the_version_reaches_the_function_on_every_path(entry, path, tmp_path):
    seen = {}

    def run(a_input=None, a_input_url=None, a_input_version=None,
            a_input_version_id=None):
        seen.update(version=a_input_version, version_id=a_input_version_id)
        return None

    _run(run, dict(entry), tmp_path)

    assert seen == {"version": 7, "version_id": VERSION_ID}, f"failed on {path}"


def test_a_function_that_asks_for_neither_is_unaffected(tmp_path):
    """The existing signature gate: extras are opt-in."""

    def run(a_input):
        return None

    _run(run, dict(COPY), tmp_path)  # would TypeError if passed unconditionally


def test_either_one_alone_works(tmp_path):
    seen = {}

    def only_number(a_input, a_input_version=None):
        seen["number"] = a_input_version
        return None

    def only_id(a_input, a_input_version_id=None):
        seen["id"] = a_input_version_id
        return None

    _run(only_number, dict(COPY), tmp_path)
    _run(only_id, dict(COPY), tmp_path)

    assert seen == {"number": 7, "id": VERSION_ID}


def test_each_input_gets_its_own_version(tmp_path):
    from aero_client.utils import aero_format

    seen = {}

    def run(a_input, b_input, a_input_version=None, b_input_version=None,
            a_input_version_id=None, b_input_version_id=None):
        seen.update(a=a_input_version, b=b_input_version,
                    a_id=a_input_version_id, b_id=b_input_version_id)
        return None

    aero_format(run)(
        **{"aero": {"input_data": {
            "a_input": {**COPY, "version": 7, "version_id": "id-a",
                        "tmp_dir": str(tmp_path)},
            "b_input": {**COPY, "version": 2, "version_id": "id-b",
                        "tmp_dir": str(tmp_path)},
        }, "output_data": {}, "flow_id": "f"}}
    )

    assert seen == {"a": 7, "b": 2, "a_id": "id-a", "b_id": "id-b"}


def test_an_unresolved_version_id_arrives_as_none(tmp_path):
    """Nothing crashes if the entry never picked one up."""
    seen = {}

    def run(a_input, a_input_version_id=None):
        seen["id"] = a_input_version_id
        return None

    entry = {k: v for k, v in COPY.items() if k != "version_id"}
    _run(run, entry, tmp_path)

    assert seen["id"] is None


def test_get_versions_records_the_version_id(monkeypatch):
    """It is in the /latest payload already; it was simply being discarded.

    get_versions imports inside its body so it can be serialized to a Globus
    Compute worker, so there is no module-level `requests` to patch -- the real
    module's `get` is what it will resolve to.
    """
    import aero_client.jobs as jobs
    from aero_client import utils

    latest = {
        "id": VERSION_ID,
        "version": 7,
        "data_file": {"file_name": "stored", "encoding": "utf-8"},
        "no_copy": False,
    }

    class _R:
        status_code = 200

        def json(self):
            return latest

    monkeypatch.setattr("requests.get", lambda *a, **k: _R())
    monkeypatch.setattr(
        utils, "load_tokens",
        lambda: {utils.CONF.portal_client_id: {"refresh_token": "t"}},
    )

    params = [{"kwargs": {"aero": {"input_data": {
        "a_input": {"id": "d", "version": None}}}}}]
    out = jobs.get_versions(*params)

    entry = out[0]["kwargs"]["aero"]["input_data"]["a_input"]
    assert entry["version"] == 7
    assert entry["version_id"] == VERSION_ID

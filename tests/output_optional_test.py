"""An analysis may declare no output_data and store its own results.

`aero_format` then has nothing to upload: the function returns None, and AERO
records only the run and the input versions it consumed.
"""

import pytest

from aero_client.utils import aero_format
from aero_client.utils import AeroOutput


@pytest.fixture(name="uploads")
def uploads_fixture(monkeypatch):
    """Record every gcs_save call instead of hitting a Globus collection."""
    calls = []

    def _save(path, collection_url, collection_uuid):
        calls.append({"path": path, "collection_url": collection_url})
        return {
            "created_at": "now",
            "checksum": "abc",
            "size": 1,
            "file_bn": "stored-name",
            "file_format": ("text/csv", None),
        }

    monkeypatch.setattr("aero_client.utils.gcs_save", _save)
    return calls


def _kwargs(output_data):
    return {"aero": {"input_data": {}, "output_data": output_data, "flow_id": "f1"}}


def test_no_outputs_declared_and_none_returned(uploads):
    """The whole point: nothing declared, nothing returned, nothing uploaded."""

    def stores_its_own_results():
        return None

    out = aero_format(stores_its_own_results)(**_kwargs({}))

    assert uploads == []
    assert out["aero"]["output_data"] == {}
    # provenance still needs these keys present for commit_analysis
    assert "input_data" in out["aero"] and "flow_id" in out["aero"]


def test_an_empty_list_counts_as_no_outputs(uploads):
    def returns_empty():
        return []

    aero_format(returns_empty)(**_kwargs({}))
    assert uploads == []


def test_declared_but_nothing_returned_is_an_error(uploads):
    def forgets_to_return():
        return None

    with pytest.raises(ValueError) as exc:
        aero_format(forgets_to_return)(
            **_kwargs({"summary": {"collection_url": "https://c/", "collection_uuid": "u"}})
        )

    assert "summary" in str(exc.value)
    assert uploads == []


def test_returned_but_nothing_declared_is_an_error(uploads):
    """Previously a KeyError deep inside the upload; now it says what's wrong."""

    def produces_an_output():
        return AeroOutput(name="summary", path="/tmp/whatever")

    with pytest.raises(ValueError) as exc:
        aero_format(produces_an_output)(**_kwargs({}))

    assert "output_data" in str(exc.value)
    assert uploads == []


def test_a_single_declared_output_still_uploads(uploads):
    def produces_an_output():
        return AeroOutput(name="summary", path="/tmp/summary.csv")

    out = aero_format(produces_an_output)(
        **_kwargs({"summary": {"collection_url": "https://c/", "collection_uuid": "u"}})
    )

    assert [u["path"] for u in uploads] == ["/tmp/summary.csv"]
    assert out["aero"]["output_data"]["summary"]["file_bn"] == "stored-name"


def test_several_declared_outputs_still_upload(uploads):
    def produces_two():
        return [
            AeroOutput(name="a", path="/tmp/a.csv"),
            AeroOutput(name="b", path="/tmp/b.csv"),
        ]

    declared = {
        "a": {"collection_url": "https://c/", "collection_uuid": "u"},
        "b": {"collection_url": "https://c/", "collection_uuid": "u"},
    }
    aero_format(produces_two)(**_kwargs(declared))

    assert sorted(u["path"] for u in uploads) == ["/tmp/a.csv", "/tmp/b.csv"]


def test_an_ingestion_output_keeps_its_download_checksum(uploads):
    """A url on the entry means download already computed the real checksum.

    gcs_save's checksum is of the staged copy, so it must not overwrite it --
    dedup for that source depends on the original.
    """

    def stage():
        return AeroOutput(name="src", path="/tmp/pulled")

    declared = {
        "src": {
            "collection_url": "https://c/",
            "collection_uuid": "u",
            "url": "http://minio/bucket/a.csv",
            "checksum": "from-download",
        }
    }
    out = aero_format(stage)(**_kwargs(declared))

    assert out["aero"]["output_data"]["src"]["checksum"] == "from-download"

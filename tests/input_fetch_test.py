"""`fetch: false` — the analysis retrieves an input itself.

Only the input whose notify triggered a run gets a signed url, so on a private
bucket every other no-copy input 403s when AERO tries to fetch it. Opting out
hands over the url and downloads nothing, leaving retrieval to code that has
credentials.
"""

from pathlib import Path

import pytest

from aero_client.utils import aero_format


A_URL = "https://phire.egs.anl.gov:9000/temporary/wind2/a.csv"
B_URL = "https://phire.egs.anl.gov:9000/temporary/wind2/b.csv"
B_SIGNED = f"{B_URL}?X-Amz-Signature=abc"


class _Resp:
    content = b"payload"
    status_code = 200

    def raise_for_status(self):
        pass


@pytest.fixture(name="fetched")
def fetched_fixture(monkeypatch):
    """Record every url aero_format actually downloads."""
    urls = []

    def _get(url, headers=None, **kwargs):
        urls.append(url)
        return _Resp()

    monkeypatch.setattr("requests.get", _get)
    return urls


def _kwargs(tmp_path, inputs):
    for entry in inputs.values():
        entry.setdefault("tmp_dir", str(tmp_path))
    return {"aero": {"input_data": inputs, "output_data": {}, "flow_id": "f1"}}


def test_an_opted_out_input_is_not_downloaded(tmp_path, fetched):
    seen = {}

    def run(a_input_url, a_input_signed_url=None):
        seen["url"] = a_input_url
        seen["signed"] = a_input_signed_url
        return None

    aero_format(run)(
        **_kwargs(tmp_path, {"a_input": {"id": "a", "trigger_url": A_URL, "fetch": False}})
    )

    assert fetched == []
    assert seen == {"url": A_URL, "signed": None}


def test_the_path_parameter_is_absent_for_an_opted_out_input(tmp_path, fetched):
    """A function still expecting a path must fail, not silently get nothing."""

    def run(a_input):
        return None

    with pytest.raises(TypeError) as exc:
        aero_format(run)(
            **_kwargs(
                tmp_path, {"a_input": {"id": "a", "trigger_url": A_URL, "fetch": False}}
            )
        )

    assert "a_input" in str(exc.value)


def test_a_signed_url_is_passed_through_when_present(tmp_path, fetched):
    seen = {}

    def run(b_input_url, b_input_signed_url=None):
        seen["signed"] = b_input_signed_url
        return None

    aero_format(run)(
        **_kwargs(
            tmp_path,
            {
                "b_input": {
                    "id": "b",
                    "trigger_url": B_URL,
                    "signed_url": B_SIGNED,
                    "fetch": False,
                }
            },
        )
    )

    assert seen["signed"] == B_SIGNED
    assert fetched == []


def test_one_opted_out_alongside_one_fetched(tmp_path, fetched):
    """The live case: the sibling is unsigned, so it opts out; the trigger does not."""
    seen = {}

    def run(a_input_url, b_input, a_input_signed_url=None):
        seen["a_url"] = a_input_url
        seen["b_path"] = b_input
        return None

    aero_format(run)(
        **_kwargs(
            tmp_path,
            {
                "a_input": {"id": "a", "trigger_url": A_URL, "fetch": False},
                "b_input": {"id": "b", "trigger_url": B_URL, "signed_url": B_SIGNED},
            },
        )
    )

    # only the fetched input hit the network, and it used its signature
    assert fetched == [B_SIGNED]
    assert seen["a_url"] == A_URL
    assert Path(seen["b_path"]).name == "b.csv"


def test_every_input_opted_out_downloads_nothing(tmp_path, fetched):
    seen = {}

    def run(a_input_url, b_input_url, a_input_signed_url=None, b_input_signed_url=None):
        seen.update(a=a_input_url, b=b_input_url, a_s=a_input_signed_url,
                    b_s=b_input_signed_url)
        return None

    aero_format(run)(
        **_kwargs(
            tmp_path,
            {
                "a_input": {"id": "a", "trigger_url": A_URL, "fetch": False},
                "b_input": {
                    "id": "b", "trigger_url": B_URL, "signed_url": B_SIGNED,
                    "fetch": False,
                },
            },
        )
    )

    assert fetched == []
    assert list(tmp_path.iterdir()) == [], "no temp directories created"
    assert seen == {"a": A_URL, "b": B_URL, "a_s": None, "b_s": B_SIGNED}


def test_the_url_is_not_signature_gated(tmp_path, fetched):
    """Unlike the opt-in extras: a mistyped parameter must error, not drop it."""

    def run(wrong_name=None):
        return None

    with pytest.raises(TypeError) as exc:
        aero_format(run)(
            **_kwargs(
                tmp_path, {"a_input": {"id": "a", "trigger_url": A_URL, "fetch": False}}
            )
        )

    assert "a_input_url" in str(exc.value)


def test_an_opted_out_input_with_no_url_raises(tmp_path, fetched):
    def run(a_input_url=None):
        return None

    with pytest.raises(ValueError) as exc:
        aero_format(run)(**_kwargs(tmp_path, {"a_input": {"id": "a", "fetch": False}}))

    assert "a_input" in str(exc.value)
    assert fetched == []


def test_a_collection_input_can_opt_out_too(tmp_path, fetched):
    """Of limited use -- reading it still needs a transfer token -- but coherent."""
    seen = {}

    def run(a_input_url):
        seen["url"] = a_input_url
        return None

    aero_format(run)(
        **_kwargs(
            tmp_path,
            {
                "a_input": {
                    "id": "a",
                    "collection_url": "https://collection.example/",
                    "file_bn": "stored-name",
                    "fetch": False,
                }
            },
        )
    )

    assert seen["url"] == "https://collection.example/stored-name"
    assert fetched == []


def test_default_behavior_is_unchanged(tmp_path, fetched):
    """No fetch key: downloaded as before, and the parameter holds a path."""
    seen = {}

    def run(a_input):
        seen["path"] = a_input
        return None

    aero_format(run)(
        **_kwargs(tmp_path, {"a_input": {"id": "a", "trigger_url": A_URL}})
    )

    assert fetched == [A_URL]
    assert Path(seen["path"]).name == "a.csv"


def test_fetch_true_is_also_fetched(tmp_path, fetched):
    """Only an explicit false opts out."""

    def run(a_input):
        return None

    aero_format(run)(
        **_kwargs(tmp_path, {"a_input": {"id": "a", "trigger_url": A_URL, "fetch": True}})
    )

    assert fetched == [A_URL]

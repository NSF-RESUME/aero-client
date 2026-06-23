"""Integration test for aero_client.jobs.download().

Runs download() directly as a plain function (outside Globus Compute) against a
real AERO server with real auth, and compares the retrieved file against a
previously-saved "golden" reference copy on disk.

This is an INTEGRATION test: it requires
  - a real AERO config (either ~/.aero/config.toml, or another config.toml
    selected via the AERO_CONFIG_FILE env var, e.g.
    `AERO_CONFIG_FILE=tests/data/config.toml pytest tests/jobs_test.py`),
  - valid Globus tokens on disk (so load_tokens() succeeds),
  - network access to the server and the data URL.

Fill in the CASES below to enable it; unfilled cases are skipped.
"""

import hashlib
import os

import pytest

from aero_client.jobs import download


# (flow_id, data_name, reference_file)
#   flow_id        : a real flow registered on the configured AERO server
#   data_name      : contributed_to[0]["name"] for that flow (the output_data key)
#   reference_file : path to a previously-downloaded golden copy of that file
CASES = [
    pytest.param("ffcb573c-2492-4b5e-8bd5-fd45006b8af7", "post_preds", "https://iwss.uillinois.edu/wastewater-treatment-plant/download/168/", 
                 "tests/data/Stickney Water Reclamation Plant - North.csv", id="wastewater_csv"),
]


@pytest.mark.parametrize("flow_id, data_name, url, reference_file", CASES)
def test_download_matches_reference(flow_id, data_name, url, reference_file, tmp_path):
    if any(v.startswith("<<") for v in (flow_id, data_name, reference_file)):
        pytest.skip("Fill in flow_id, data_name, and reference_file in CASES")

    os.environ["AERO_CONFIG_FILE"] = "tests/data/config.toml"

    # Stub kwargs/args passed to download(). CONF + tokens come from the real
    # environment; output_data must be keyed by the data record's name so that
    # download() can write the retrieved file's metadata back into it.
    kwargs = {
        "aero": {
            "flow_id": flow_id,
            "url": url,
            "output_data": {
                data_name: {"temp_dir": str(tmp_path)},
            },
        }
    }
    args = ()

    _args, out = download(*args, **kwargs)

    meta = out["aero"]["output_data"][data_name]
    downloaded = open(meta["file"], "rb").read()
    expected = open(reference_file, "rb").read()

    assert meta["download"] is True
    assert downloaded == expected
    # `checksum` is the md5 of the raw retrieved bytes (response.content). This is
    # the most robust comparison: _download_http writes the file in text mode
    # (decoding with the response encoding, binary fallback on UnicodeDecodeError),
    # so for a non-UTF-8 text source the on-disk bytes may differ from the raw
    # download even when the content is correct -- rely on this assertion there.
    assert meta["checksum"] == hashlib.md5(expected).hexdigest()

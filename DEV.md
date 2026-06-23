# Developer Guide

Notes for working on `aero-client`.

## Setup

Use a virtual environment and install the package with its dev extras:

```sh
python -m venv .venv
.venv/bin/pip install -e '.[dev]'
```

The `dev` extra pulls in `pytest`, plus `fastapi` + `uvicorn` (used by the local
test server) and `pre-commit` / `tox`.

## Running the unit tests

All tests live under [`tests/`](tests/) and run with pytest:

```sh
.venv/bin/python -m pytest tests/ -v
```

### Config is loaded at import time

Importing `aero_client.utils` (which most code imports transitively) runs
`CONF = load_conf(...)` **at import time**. By default it reads
`~/.aero/config.toml` and **raises if that file is missing** — which would break
test collection. There are two ways to provide a config:

- Set the `AERO_CONFIG_FILE` env var to a config file (loaded without touching the
  `~/.aero` symlink). This is the recommended way to run the tests:

  ```sh
  AERO_CONFIG_FILE=tests/data/config.toml .venv/bin/python -m pytest tests/ -v
  ```

- Or have a real `~/.aero/config.toml` present.

`tests/data/config.toml` and `tests/data/client_tokens.json` are **gitignored**
(they can contain credentials), so a fresh checkout won't have them. A minimal
config sufficient for the offline download tests is:

```toml
# tests/data/config.toml
client_uuid = "test"
portal_client_id = "test"

[aero]
  cache_dir = "tests/data"
  server = "http://localhost"   # overridden by the local-server tests
```

### Proxies

If your shell sets `HTTP_PROXY` / `HTTPS_PROXY`, the proxy may intercept requests
to `127.0.0.1` and break the local-server tests. The tests already export
`NO_PROXY=127.0.0.1,localhost` internally (via `monkeypatch`), so no action is
needed for `pytest`. If you run the standalone server (below) and hit it yourself,
set `NO_PROXY=127.0.0.1,localhost` for your client.

## How the tests work

### `tests/download_local_test.py` — offline `download()` tests (recommended)

These exercise `aero_client.jobs.download()` end-to-end **without any external
network or the real AERO server**. They are the most reliable tests to run.

How they work:

1. **A local stand-in server** ([`tests/download_server.py`](tests/download_server.py))
   is a small FastAPI app that mimics the two calls `download()` makes:
   - `GET /flow/{flow_id}` → returns the flow JSON
     (`{"contributed_to": [{"id", "name", "url"}]}`) that `download()` parses.
   - `GET /files/{filename}` → serves a file from `tests/data/`.
   - `GET /secure-files/{filename}` → same, but behind HTTP Basic Auth (used to
     test the basic-auth download path). For a `secure` flow, `/flow` returns a url
     with credentials embedded as `<url>:user=<u>:pwd=<p>`.
2. **A pytest fixture** (`download_server` in [`tests/conftest.py`](tests/conftest.py))
   starts that app with `uvicorn` on a free port in a background thread, yields a
   handle exposing `.base_url` and `.add_flow(...)`, and shuts it down afterwards.
3. **Each test** registers a flow on the server, points `CONF.server_url` at the
   running server (`monkeypatch.setattr(utils, "CONF", ...)`; `download()` reads
   `CONF` lazily so this takes effect), and stubs `load_tokens()` (the local server
   ignores the auth header, so no real Globus token is needed). It then calls
   `download(**kwargs)` and asserts the retrieved file matches the file the server
   served (byte-for-byte and by md5 checksum).

There are two cases: a plain HTTP download and a Basic-Auth download (which also
asserts the secure endpoint returns `401` when unauthenticated).

Run just these:

```sh
AERO_CONFIG_FILE=tests/data/config.toml .venv/bin/python -m pytest tests/download_local_test.py -v
```

### `tests/jobs_test.py` — integration test (real server)

This runs `download()` against a **real** AERO server. It is parametrized by a
`CASES` list of `(flow_id, data_name, url, reference_file)` and `pytest.skip`s
entries whose placeholders are unfilled. To use it you need:

- `AERO_CONFIG_FILE` (or `~/.aero/config.toml`) pointing at a real server,
- valid Globus tokens on disk (`tests/data/client_tokens.json` keyed by
  `portal_client_id`),
- network access,

and real values filled into `CASES`. Without that setup it fails/needs skipping —
prefer the offline tests above for routine development.

### `tests/utils_test.py` — `aero_format` decorator

Tests the provenance/IO wrapper `aero_format`. ⚠️ These currently **fail**: their
fixtures predate the current `aero_format` signature (e.g. they pass
`function_args` to the user function and omit `collection_uuid`). They need
updating to match `aero_client/utils.py` before they will pass.

## Running the test server standalone

The same server can be run by hand to download against it with a real config:

```sh
NO_PROXY=127.0.0.1,localhost .venv/bin/python tests/download_server.py \
    --file "Stickney Water Reclamation Plant - North.csv" --name post_preds --port 8000
# add --secure to serve it behind HTTP Basic Auth
```

Then point a `config.toml` at `server = "http://127.0.0.1:8000"` and run `download()`.

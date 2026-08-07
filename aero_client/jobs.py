"""AERO flow compute function definition."""


def download(*args, **kwargs) -> tuple[tuple, dict[str, dict]]:
    """Download data from user-specified repository.

    Returns:
        tuple[str, str]: Path to the data and its
            associated extension.
    """
    import hashlib
    import pathlib
    import requests
    import uuid
    import json
    import urllib.parse
    from mimetypes import guess_extension
    from pathlib import Path

    from aero_client.utils import CONF
    from aero_client.utils import load_tokens

    def _is_delta_sharing(data) -> bool:
        """Test if the data url points to a Delta Sharing profile file."""
        if data["url"].startswith("file://") and "#" in data["url"]:
            try:
                f = data["url"][len("file://"): data["url"].find("#")]
                with open(f) as fin:
                    profile = json.load(fin)
                    return "bearerToken" in profile
            except Exception:
                pass
        return False

    def _download_delta_sharing(data, fn) -> tuple[bytes, str, str]:
        """Download data via Delta Sharing and write it to ``fn`` as CSV.

        Returns:
            tuple[bytes, str, str]: The file content (bytes), extension, encoding.
        """
        import delta_sharing
        from io import StringIO

        df = delta_sharing.load_as_pandas(data["url"])
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        ext = "csv"
        encoding = "utf-8"
        content = csv_buffer.getvalue()

        with open(fn, "w+") as f:
            f.write(content)

        return content.encode("utf-8"), ext, encoding

    def _http_fetch(url, fn, auth=None) -> tuple[bytes, str | None, str | None]:
        """Fetch ``url`` over HTTP (optionally with auth) and write it to ``fn``.

        Returns:
            tuple: The file content (bytes), extension, encoding.
        """
        response = requests.get(url, auth=auth)
        content_type = response.headers["content-type"]
        encoding = "utf-8" if response.encoding is None else response.encoding
        ext = guess_extension(content_type.split(";")[0])
        content = response.content

        try:
            with open(fn, "w+") as f:
                f.write(content.decode(encoding=encoding)) # type: ignore
        except UnicodeDecodeError:
            with open(fn, "wb") as f:
                f.write(content)

        return content, ext, encoding

    def _load_basic_auth(url):
        """Return ``(username, password)`` for ``url``'s host, or ``None``.

        Credentials are read from ``<aero_dir>/<host>.yaml`` (i.e. the
        ``~/.aero`` directory), a YAML file with ``username`` and ``password``
        keys named after the full url host (e.g. ``travelmidwest.com.yaml``).
        If no such file exists the download proceeds without auth.
        """
        host = urllib.parse.urlparse(url).hostname
        if not host:
            return None
        creds_file = Path(CONF.aero_dir) / f"{host}.yaml"
        if not creds_file.is_file():
            return None
        import yaml

        with open(creds_file) as f:
            creds = yaml.safe_load(f)
        return creds["username"], creds["password"]

    def _download_http(data, fn) -> tuple[bytes, str | None, str | None]:
        """Download data over HTTP and write it to ``fn``.

        Uses HTTP Basic Auth if a per-host credentials file is present (see
        ``_load_basic_auth``); otherwise performs a plain GET.
        """
        auth = _load_basic_auth(data["url"])
        return _http_fetch(data["url"], fn, auth=auth)

    outputs = list(kwargs["aero"]["output_data"].items())

    if "temp_dir" in outputs[0][1]:
        TEMP_DIR = Path(outputs[0][1]["temp_dir"])
    else:
        TEMP_DIR = pathlib.Path.home() / "aero"
        outputs[0][1]["temp_dir"] = str(TEMP_DIR)

    tokens = load_tokens()
    auth_token = tokens[CONF.portal_client_id]["refresh_token"]

    headers = {"Authorization": f"Bearer {auth_token}"}

    # assert False, CONF.server_url
    response = requests.get(
        f'{CONF.server_url}/flow/{kwargs["aero"]["flow_id"]}',
        headers=headers,
        verify=False,
    )
    flow = response.json()

    assert response.status_code == 200, response

    data = flow["contributed_to"][
        0
    ]  # assuming only one contribution / ingesting flow for now

    # Prefer a per-run source url passed via the notify webhook (e.g. a MinIO
    # presigned URL); fall back to the source's registered url.
    override_url = kwargs["aero"].get("source_url")
    if override_url:
        data["url"] = override_url

    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    bn = str(uuid.uuid4())
    fn = Path(TEMP_DIR, bn)

    if _is_delta_sharing(data):
        content, ext, encoding = _download_delta_sharing(data, fn)
    else:
        content, ext, encoding = _download_http(data, fn)

    kwargs["aero"]["output_data"][data["name"]]["id"] = data["id"]
    kwargs["aero"]["output_data"][data["name"]]["file"] = str(fn)
    kwargs["aero"]["output_data"][data["name"]]["file_bn"] = bn
    kwargs["aero"]["output_data"][data["name"]]["file_format"] = ext
    kwargs["aero"]["output_data"][data["name"]]["checksum"] = hashlib.md5(
        content
    ).hexdigest()
    kwargs["aero"]["output_data"][data["name"]]["size"] = fn.stat().st_size
    kwargs["aero"]["output_data"][data["name"]]["download"] = True
    kwargs["aero"]["output_data"][data["name"]]["encoding"] = encoding

    # Carry the notify's per-url identity and dedup flag through to /prov/new, so
    # a typed source dedups per url on the copy path too. gcs_save's metadata
    # update doesn't touch these keys.
    source_key = kwargs["aero"].get("source_key")
    if source_key:
        kwargs["aero"]["output_data"][data["name"]]["source_key"] = source_key
    if kwargs["aero"].get("dedup") is False:
        kwargs["aero"]["output_data"][data["name"]]["dedup"] = False

    return args, kwargs


def stage(*args, **kwargs):
    """Passthrough ingestion function: stage the downloaded file unchanged.

    Returns the file pulled by ``download`` as an ``AeroOutput`` so the
    ``aero_format`` wrapper's ``gcs_save`` uploads the raw object into the Globus
    guest collection (no transform). Default ingestion function for
    raw-passthrough sources.
    """
    import os

    from aero_client.utils import AeroOutput

    # aero_format passes the downloaded local path under the source's output
    # name; a passthrough source has exactly one such file entry.
    for name, value in kwargs.items():
        if isinstance(value, str) and os.path.exists(value):
            return AeroOutput(name=name, path=value)

    raise ValueError("stage: no downloaded file found among inputs")


def database_commit(*args, **kwargs) -> dict[str, int | float | str | dict]:
    """Commit ingested metadata to database

    Returns:
        dict: Response dictionary returned by user function.
    """
    import json
    import requests
    from aero_client.utils import CONF
    from aero_client.utils import load_tokens

    tokens = load_tokens()

    auth_token = tokens[CONF.portal_client_id]["refresh_token"]
    aero_headers = {"Authorization": f"Bearer {auth_token}"}

    aero_headers["Content-type"] = "application/json"

    # add provenance
    response = requests.post(
        f"{CONF.server_url}/prov/new",
        headers=aero_headers,
        verify=False,
        data=json.dumps(kwargs["aero"]),
    )

    assert response.status_code == 200, response.json()

    return response.json()


def get_versions(*function_params) -> tuple:
    """Get the desired version of the source data.

    Returns:
        Function parameters to send to user-defined analysis function.
    """
    import requests
    from aero_client.utils import CONF
    from aero_client.utils import load_tokens

    tokens = load_tokens()

    auth_token = tokens[CONF.portal_client_id]["refresh_token"]
    aero_headers = {"Authorization": f"Bearer {auth_token}"}

    for params in function_params:
        kw = params["kwargs"]

        assert "aero" in kw.keys()

        for name, md in kw["aero"]["input_data"].items():
            if md["version"] is None:
                response = requests.get(
                    f"{CONF.server_url}/data/{md['id']}/latest",
                    headers=aero_headers,
                    verify=False,
                )

                assert response.status_code == 200, response.content
                latest = response.json()
                md["version"] = latest["version"]
                md["file_bn"] = latest["data_file"]["file_name"]
                md["encoding"] = latest["data_file"]["encoding"]

                # A no-copy source keeps no bytes in AERO, so the pull needs the
                # object's own url. The server resolves it from the version's
                # object key, which means a run this source did not trigger — a
                # multi-input analysis, say — can still locate it. Only a
                # notify-triggered run also carries a signed url.
                if latest.get("no_copy") and latest.get("trigger_url"):
                    md.setdefault("trigger_url", latest["trigger_url"])

    return function_params


def commit_analysis(*arglist) -> list:
    """Commit metadata of analysis function to database.

    Returns:
        dict: Response from database update.
    """
    import json
    import requests

    from aero_client.utils import CONF
    from aero_client.utils import load_tokens

    tokens = load_tokens()

    auth_token = tokens[CONF.portal_client_id]["refresh_token"]
    aero_headers = {"Authorization": f"Bearer {auth_token}"}
    aero_headers["Content-type"] = "application/json"

    responses = []
    for task_kwargs in arglist:
        assert "input_data" in task_kwargs["aero"]
        assert "output_data" in task_kwargs["aero"]
        assert "flow_id" in task_kwargs["aero"]

        response = requests.post(
            f"{CONF.server_url}/prov/new",
            headers=aero_headers,
            verify=False,
            data=json.dumps(task_kwargs["aero"])
        )

        assert response.status_code == 200, response.content
        responses.append(response.json())

    return responses

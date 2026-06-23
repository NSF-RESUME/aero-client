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

    def _download_http(data, fn) -> tuple[bytes, str | None, str | None]:
        """Download data over HTTP and write it to ``fn``.

        Returns:
            tuple: The file content (bytes), extension, encoding.
        """
        response = requests.get(data["url"])
        content_type = response.headers["content-type"]
        encoding = response.encoding
        ext = guess_extension(content_type.split(";")[0])
        content = response.content

        try:
            with open(fn, "w+") as f:
                f.write(content.decode(encoding=encoding)) # type: ignore
        except UnicodeDecodeError:
            with open(fn, "wb") as f:
                f.write(content)

        return content, ext, encoding

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

    return args, kwargs


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


def get_versions(*function_params) -> dict:
    """Get the desired version of the source data.

    Returns:
        dict: Function parameters to send to user-defined analysis function.
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
                md["version"] = response.json()["version"]
                md["file_bn"] = response.json()["data_file"]["file_name"]
                md["encoding"] = response.json()["data_file"]["encoding"]

    return function_params


def commit_analysis(*arglist) -> dict:
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

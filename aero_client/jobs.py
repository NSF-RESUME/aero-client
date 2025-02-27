"""AERO flow compute function definition."""


def download(*args, **kwargs) -> tuple[str, str]:
    """Download data from user-specified repository.

    Returns:
        tuple[str, str]: Path to the data and its
            associated extension.
    """
    import hashlib
    import pathlib
    import requests
    import uuid
    import time
    from mimetypes import guess_extension
    from pathlib import Path

    from aero_client.utils import CONF
    from aero_client.utils import load_tokens

    task_start: float
    task_end: float

    if "metrics" in kwargs and kwargs["metrics"] is True:
        task_start = time.time_ns()

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

    data = flow["contributed_to"][
        0
    ]  # assuming only one contribution / ingesting flow for now

    response = requests.get(data["url"])
    content_type = response.headers["content-type"]
    encoding = response.encoding
    ext = guess_extension(content_type.split(";")[0])

    bn = str(uuid.uuid4())
    fn = Path(TEMP_DIR, bn)

    TEMP_DIR.mkdir(exist_ok=True, parents=True)

    try:
        with open(fn, "w+") as f:
            f.write(response.content.decode(encoding=encoding))
    except UnicodeDecodeError:
        with open(fn, "wb") as f:
            f.write(response.content)

    kwargs["aero"]["output_data"][data["name"]]["id"] = data["id"]
    kwargs["aero"]["output_data"][data["name"]]["file"] = str(fn)
    kwargs["aero"]["output_data"][data["name"]]["file_bn"] = bn
    kwargs["aero"]["output_data"][data["name"]]["file_format"] = ext
    kwargs["aero"]["output_data"][data["name"]]["checksum"] = hashlib.md5(
        response.content
    ).hexdigest()
    kwargs["aero"]["output_data"][data["name"]]["size"] = fn.stat().st_size
    kwargs["aero"]["output_data"][data["name"]]["download"] = True
    kwargs["aero"]["output_data"][data["name"]]["encoding"] = encoding

    if "metrics" in kwargs and kwargs["metrics"] is True:
        task_end = time.time_ns()
        kwargs["download_metrics"] = {
            "task_start": task_start,
            "task_end": task_end,
            "duration": task_end - task_start,
        }

    return args, kwargs


def database_commit(*args, **kwargs) -> dict[str, int | float | str | dict]:
    """Commit ingested metadata to database

    Returns:
        dict: Response dictionary returned by user function with optional metrics appended.
    """
    import json
    import requests
    import time
    from aero_client.utils import CONF
    from aero_client.utils import load_tokens

    task_start: float
    task_end: float

    if "metrics" in kwargs and kwargs["metrics"] is True:
        task_start = time.time_ns()

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

    if "metrics" in kwargs and kwargs["metrics"] is True:
        task_end = time.time_ns()
        kwargs["download_metrics"] = {
            "task_start": task_start,
            "task_end": task_end,
            "duration": task_end - task_start,
        }

        outkwargs = response.json()
        outkwargs["database_commit"] = kwargs["download_metrics"]
    else:
        outkwargs = response.json()

    return outkwargs


def get_versions(*function_params) -> dict:
    """Get the desired version of the source data.

    Returns:
        dict: Function parameters to send to user-defined analysis function.
    """
    import requests
    import time
    from aero_client.utils import CONF
    from aero_client.utils import load_tokens

    task_start: float
    task_end: float

    if function_params["metrics"] is True:
        task_start = time.time_ns()

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
                md["version"] = response.json()["data_version"]["version"]
                md["file_bn"] = response.json()["data_version"]["data_file"][
                    "file_name"
                ]
                md["encoding"] = response.json()["data_version"]["data_file"][
                    "encoding"
                ]

    if function_params["metrics"] is True:
        task_end = time.time_ns()
        function_params["get_versions_metrics"] = {
            "task_start": task_start,
            "task_end": task_end,
            "duration": task_end - task_start,
        }

    return function_params


def commit_analysis(*arglist) -> dict:
    """Commit metadata of analysis function to database.

    Returns:
        dict: Response from database update.
    """
    import json
    import requests
    import time

    from aero_client.utils import CONF
    from aero_client.utils import load_tokens

    task_start: float
    task_end: float

    if arglist["metrics"] is True:
        task_start = time.time_ns()

    tokens = load_tokens()

    auth_token = tokens[CONF.portal_client_id]["refresh_token"]
    aero_headers = {"Authorization": f"Bearer {auth_token}"}
    aero_headers["Content-type"] = "application/json"

    responses = []
    for task_kwargs in arglist:
        assert "input_data" in task_kwargs["aero"]
        assert "output_data" in task_kwargs["aero"]
        assert "flow_id" in task_kwargs["aero"]

        # do something about provenance here
        response = requests.post(
            f"{CONF.server_url}/prov/new",
            headers=aero_headers,
            verify=False,
            data=json.dumps(task_kwargs),
        )

        assert response.status_code == 200, response.content
        responses.append(response.json())

    if arglist["metrics"] is True:
        task_end = time.time_ns()
        responses.append(
            {
                "get_versions_metrics": {
                    "task_start": task_start,
                    "task_end": task_end,
                    "duration": task_end - task_start,
                }
            }
        )

    return responses

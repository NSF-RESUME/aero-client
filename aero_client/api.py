"""AERO client API module"""

import json
import logging
import requests

from pathlib import Path
from typing import Generator
from typing import Literal
from typing import Callable, TypeAlias

from globus_compute_sdk import Client

from aero_client.error import ClientError
from aero_client.jobs import commit_analysis
from aero_client.jobs import download
from aero_client.jobs import database_commit
from aero_client.jobs import get_versions
from aero_client.jobs import stage
from aero_client.utils import _client_auth
from aero_client.utils import build_url
from aero_client.utils import register_function as register_aero_function
from aero_client.utils import CONF
from aero_client.utils import PolicyEnum

logger = logging.getLogger(__name__)

AUTH_ACCESS_TOKEN = _client_auth()
JSON: TypeAlias = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None

# tmp fix
session = requests.Session()


def register_function(func: Callable):
    """
    Register function to a Globus Compute Client.
    """
    gcc = Client()
    return gcc.register_function(func)


def list_versions(data_id: str) -> JSON:
    headers = {"Authorization": f"Bearer {AUTH_ACCESS_TOKEN}"}
    url = build_url("data", data_id, "versions")
    req = session.get(
        url=url,
        headers=headers,
        verify=False,
    )

    assert req.status_code == 200, str(req.content, encoding="utf-8")
    try:
        return req.json()
    except requests.exceptions.JSONDecodeError:
        return {
            "status_code": req.status_code,
            "message": str(req.content, encoding="utf-8"),
        }


def list_metadata(
    metadata_type: Literal["data", "prov", "flow"],
) -> Generator[JSON, JSON, JSON]:
    """Get the metadata records.

    Args:
        metadata_type (Literal["data", "prov", "flow"]): List metadata of a certain type.

    Returns:
        Generation[JSON]: a generator returning up to 15 metadata records at a time.
    """
    logger.debug("Retrieving all sources from server")
    headers = {"Authorization": f"Bearer {AUTH_ACCESS_TOKEN}"}

    url = build_url(metadata_type, trailing_slash=True)
    req = session.get(
        url=url,
        headers=headers,
        verify=False,
    )

    try:
        assert req.status_code == 200, str(req.content, encoding="utf-8")
        yield req.json()

        page = 1

        while req.status_code == 200:
            page += 1
            req = session.get(url, headers=headers, params={"page": page}, verify=False)
            yield req.json()
    except requests.exceptions.JSONDecodeError:
        return {
            "status_code": req.status_code,
            "message": str(req.content, encoding="utf-8"),
        }


def search_sources(query: str) -> list[dict[str, str | int]]:
    """Get the sources that match the query

    Args:
        query (str): a Globus Search query string

    Returns:
        list[dict[str, str | int]]: list of sources matching the query
    """

    logger.debug(f"Querying the sources with {query}")
    params = {"query": query}
    headers = {"Authorization": f"Bearer {AUTH_ACCESS_TOKEN}"}
    req = requests.get(
        build_url("data", "search"), params=params, headers=headers, verify=False
    )

    assert req.status_code == 200, str(req.content, encoding="utf-8")
    try:
        resp = req.json()
    except requests.exceptions.JSONDecodeError:
        resp = {
            "status_code": req.status_code,
            "message": str(req.content, encoding="utf-8"),
        }
    return resp


def register_flow(
    endpoint_uuid: str,
    function_uuid: str,
    input_data: dict[str | dict[str | int | None]] = {},
    output_data: dict[str | dict[str, str]] = {},
    kwargs: JSON = {},
    config: str | None = None,
    description: str | None = None,
    policy: PolicyEnum = PolicyEnum.NONE,
    timer_delay: int | None = None,
    pull_function_uuid: str | None = None,
    commit_function_uuid: str | None = None,
) -> None:
    """Register user function to run as a Globus Flow on remote server periodically.

    Args:
        endpoint_uuid (str): Globus Compute endpoint uuid
        function_uuid (str): Globus Compute registered function UUID
        input_data (dict[str | dict[str, uuid | int]],  optional): The input data,
            presented in the format {"name": {"id": <aero_id>, "version": <version no. or None>}}.
            Default is None.
        output_data (dict[str | dict[str, str]], optional): The output data that will be created,
            presented in the format {"name": {"url": <url to fetch the data>}}. Default is None.
        kwargs (JSON, optional): Keyword arguments to pass to function. Default is None
        config (str, optional): Path to config file. Default is None.
        description (str | None, optional): A description of the Flow. Default is None.
        policy (PolicyEnum, optional): Which policy to use to rerun the flow. Default is never rerun.
        timer_delay (int | None, optional): The timer delay in seconds if PolicyEnum.TIMER is applied. Default is None.
        pull_function_uuid (str | none, optional): the uuid returned when registering either `aero_client.jobs.download`
            or `aero_client.jobs.get_versions` with Globus Compute. The function will register with GC if not provided,
            but issues may arise if local python version does not match endpoint python version. default is none.
        commit_function_uuid (str | none, optional): the uuid returned when registering either `aero_client.jobs.database_commit`
            or `aero_client.jobs.commit_analysis` with Globus Compute. The function will register with GC if not provided,
            but issues may arise if local python version does not match endpoint python version. default is none.

    Raises:
        ClientError: if function was not able to be registered as a flow, this error is raised

    Returns:
        str: the timer job uuid.
    """

    tasks = []
    if len(kwargs.keys()) == 0 and config is not None:
        with open(config) as f:
            tasks = json.load(f)

        if len(tasks) > 0:
            kwargs = tasks[0]

    # hack to ensure trailing slashes in URLs
    # TODO: fix
    for k, v in output_data.items():
        if v["collection_url"][-1] != "/":
            v["collection_url"] += "/"

    if policy is not None:
        if policy in (PolicyEnum.INGESTION, PolicyEnum.INGESTION_EVENT):
            if pull_function_uuid is None:
                pull_function_uuid = register_function(download)
            if commit_function_uuid is None:
                commit_function_uuid = register_function(database_commit)
        elif policy != PolicyEnum.NONE:
            if pull_function_uuid is None:
                pull_function_uuid = register_function(get_versions)
            if commit_function_uuid is None:
                commit_function_uuid = register_function(commit_analysis)

    data = {}
    data["input_data"] = input_data
    data["output_data"] = output_data
    data["description"] = description
    data["gc_endpoint"] = endpoint_uuid
    data["function_uuid"] = function_uuid
    data["pull_function_uuid"] = pull_function_uuid
    data["commit_function_uuid"] = commit_function_uuid
    data["flow_kwargs"] = kwargs
    data["rule"] = policy
    # Only send timer when set; event-driven/analysis flows have none, and the
    # server rejects an explicit null (its default applies only when omitted).
    if timer_delay is not None:
        data["timer"] = timer_delay

    if len(tasks) > 1:
        data["tasks"] = tasks

    headers = {
        "Authorization": f"Bearer {AUTH_ACCESS_TOKEN}",
        "Content-type": "application/json",
    }
    response = requests.post(
        build_url("flow", "register"),
        headers=headers,
        data=json.dumps(data),
        verify=False,
    )
    if response.status_code == 200:
        return response.json()
    raise ClientError(response.status_code, response.content)


def create_source(
    name: str,
    url: str,
    collection_uuid: str,
    collection_url: str,
    endpoint_uuid: str,
    function_uuid: str | None = None,
    description: str | None = None,
    kwargs: JSON = {},
) -> dict:
    """Register a data source as an event-driven ingestion flow (no timer).

    The source is (re-)pulled whenever the server receives
    ``POST /data/{id}/notify`` for it — e.g. driven by an S3 object-changed
    event. Use an HTTPS-reachable ``url`` (such as an S3 object URL) so the
    ingestion ``download`` function can fetch it. The pulled file is stored in
    the given Globus guest collection, so downstream analysis flows read it from
    there as usual.

    Args:
        name (str): Name for the source (becomes the output Data record).
        url (str): HTTPS URL to fetch the source file from.
        collection_uuid (str): Globus guest collection UUID to store the file in.
        collection_url (str): Globus guest collection HTTPS domain.
        endpoint_uuid (str): Globus Compute endpoint to run the ingestion on.
        function_uuid (str | None, optional): Registered Globus Compute function
            used as the ingestion verify/modify wrapper. If omitted, a
            raw-passthrough ``stage`` function is registered so the source file
            is stored unchanged.
        description (str | None, optional): Description of the source.
        kwargs (JSON, optional): Extra keyword arguments for the function.

    Returns:
        dict: The registered flow, including the created source Data ``id`` to
        reference as ``input_data`` when registering analysis flows.
    """
    if function_uuid is None:
        # Raw passthrough: register the stager aero_format-wrapped so its output
        # (the unchanged pulled file) is uploaded to the collection by gcs_save.
        function_uuid = register_aero_function(stage)

    output_data = {
        name: {
            "url": url,
            "collection_uuid": collection_uuid,
            "collection_url": collection_url,
        }
    }
    return register_flow(
        endpoint_uuid=endpoint_uuid,
        function_uuid=function_uuid,
        output_data=output_data,
        kwargs=kwargs,
        description=description,
        policy=PolicyEnum.INGESTION_EVENT,
    )


def get_flow(flow_id: str, inputs_only: bool = True) -> dict:
    """Get metadata on the flow provided a flow ID.

    Args:
        flow_id (str): The flow UUID
        inputs_only (bool): Whether to return flow input data exclusively.
            Defaults to True.

    Returns:
        dict: Flow metadata in dictionary representation.
    """

    headers = {
        "Authorization": f"Bearer {AUTH_ACCESS_TOKEN}",
        "Content-type": "application/json",
    }

    response = requests.get(
        build_url("flow", flow_id),
        headers=headers,
        verify=False,
    )

    assert response.status_code == 200, str(response.content, encoding="utf-8")

    if inputs_only:
        return response.json()["function_args"]["kwargs"]
    else:
        return response.json()


# TODO: Fix bug where it'll request to login if tokens are not present
def globus_logout():
    """Remove the Globus Auth token file to invoke login on next API access."""
    logger.debug("Removing Globus auth tokens.")
    Path(CONF.aero_dir, CONF.token_file).unlink()

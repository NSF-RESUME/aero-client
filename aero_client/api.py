"""DSaaS client API module"""

import hashlib
import io
import json
import logging
import urllib.parse
import pandas as pd
import requests
import uuid
import urllib

from pathlib import Path
from typing import Generator
from typing import Literal
from typing import TypeAlias

from globus_compute_sdk import Client

from aero_client import AUTH_ACCESS_TOKEN
from aero_client.config import CONF
from aero_client.error import ClientError
from aero_client.utils import get_collection_metadata
from aero_client.utils import get_transfer_token
from aero_client.utils import PolicyEnum

logger = logging.getLogger(__name__)

JSON: TypeAlias = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None

# tmp fix
session = requests.Session()


def register_function(func):
    gcc = Client()
    return gcc.register_function(func)


def list_versions(data_id: str) -> JSON:
    headers = {"Authorization": f"Bearer {AUTH_ACCESS_TOKEN}"}
    url = urllib.parse.urljoin(CONF.server_url, f"data/{data_id}/versions")
    req = session.get(
        url=url,
        headers=headers,
        verify=False,
    )

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

    url = urllib.parse.urljoin(CONF.server_url, metadata_type)
    req = session.get(
        url=url,
        headers=headers,
        verify=False,
    )

    try:
        yield req.json()

        page = 1

        while req.status_code == 200:
            page += 1
            req = session.get(url, headers=headers, params={"page": page})
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
        f"{CONF.server_url}/data/search", params=params, headers=headers, verify=False
    )

    try:
        resp = req.json()
    except requests.exceptions.JSONDecodeError:
        resp = {
            "status_code": req.status_code,
            "message": str(req.content, encoding="utf-8"),
        }
    return resp


def source_versions(source_id: int) -> list[dict[str, str | int]]:
    """List versions given a source id.

    Returns:
        list[dict[str, str | int]]: A list of all source versions
    """
    logger.debug(f"Requesting all versions of source id {source_id}.")
    headers = {"Authorization": f"Bearer {AUTH_ACCESS_TOKEN}"}
    req = requests.get(
        f"{CONF.server_url}/source/{source_id}/versions", headers=headers, verify=False
    )
    resp = json.loads(req.content)
    return resp


def get_file(
    ftype: Literal["source", "output"],
    id: int,
    version: int | None = None,
    output_path: str | None = None,
) -> pd.DataFrame:
    """Gets the version for the source.

    Args:
        ftype (Literal["source", "output"]): the type of data to fetch.
        id (int): ID of the source or output data to fetch
        version (int, optional): Version of the source data to fetch.
            If none provided, fetches latest version. Defaults to None.
        output_path (str, optional): Path to save data to.

    Returns:
        pd.DataFrame: DataFrame representation of the data.

    Raises:
        ClientError: If data was unable to be transferred
    """
    params = {}
    if version is not None:
        params["version"] = version

    logger.debug("Retrieving filename of specified source.")
    headers = {"Authorization": f"Bearer {AUTH_ACCESS_TOKEN}"}
    req = requests.get(
        f"{CONF.server_url}/{ftype}/{id}/file",
        params=params,
        headers=headers,
        verify=False,
    )

    if req.status_code == 200:
        # initiate Globus transfer
        f_data = req.json()

        if ftype == "source":
            if "https://" not in f_data["source"]["collection_url"]:
                f_data["source"][
                    "collection_url"
                ] = f'https://{f_data["source"]["collection_url"]}'
            url = urllib.parse.urljoin(
                f'{f_data["source"]["collection_url"]}/',
                f_data["source_file"]["file_name"],
            )
            TRANSFER_TOKEN = get_transfer_token(f_data["source"]["collection_uuid"])
        else:
            url = urllib.parse.urljoin(
                f'{f_data["output"]["url"]}/', f_data["filename"]
            )
            TRANSFER_TOKEN = get_transfer_token(f_data["output"]["collection_uuid"])

        logger.debug("Initiating Globus Transfer of file.")
        headers = {"Authorization": f"Bearer {TRANSFER_TOKEN}"}
        resp = requests.get(url, headers=headers)
        try:
            df = pd.DataFrame(resp.json())
        except Exception:
            try:
                df = pd.read_table(io.StringIO(resp.text), sep=",")
            except Exception:
                df = resp.content

        if output_path is not None:
            logger.debug("Saving Pandas DataFrame locally.")

            if isinstance(df, bytes):
                with open(output_path, "wb+") as f:
                    f.write(df)
            else:
                df.to_json(output_path)
        return df
    else:
        raise ClientError(req.status_code, req.text)


def save_output(
    data: str | bytes,
    name: str,
    collection_url: str,
    description: str,
    sources: dict[int, int] = {},
    function_uuid: str | None = None,
    kwargs: JSON | None = None,
) -> str:
    """Save input data to GCS and record provenance information to DSaaS.

    Args:
        data (str | bytes): Currently either JSON or CSV string or bytes object will work. Eventually will accept any Python object.
        name (str): Identifier to associate the data with. Will eventually help with search.
        collection_url (str): The HTTPS URL of the GCS collection to store data to.
        description (str): Text-based description of the provenance of the data
        sources (dict[int | int]): A dictionary of DSaaS source ids and versions that contributed to the production of this data. Defaults to {}.
        function_uuid (str, optional): The UUID of the function used to process the data. Defaults to ''.
        kwargs: (JSON, optional): Input arguments to the function in JSON format. Defaults to None.

    Raises:
        ClientError: if DSaaS or GCS were not able to update properly, this error is raised

    Returns:
        str: the GCS UUID of the data should the client need to query it afterwards.
    """

    collection_domain = urllib.parse.urlparse(collection_url).netloc

    collection_md = get_collection_metadata(collection_domain)

    TRANSFER_TOKEN = get_transfer_token(collection_md["DATA"][0]["id"])
    headers = {"Authorization": f"Bearer {TRANSFER_TOKEN}"}

    filename = str(uuid.uuid4())
    url = urllib.parse.urljoin(collection_url, filename)

    # store in GCS
    resp = requests.put(url, headers=headers, data=data)

    if resp.status_code == 200:
        ## store in DB
        headers["Content-type"] = "application/json"

        if isinstance(data, bytes):
            checksum = hashlib.md5(data).hexdigest()
        else:
            checksum = hashlib.md5(data.encode("utf-8")).hexdigest()

        params = {
            "output_fn": filename,
            "collection_uuid": collection_md["DATA"][0]["id"],
            "url": collection_url,
            "function_uuid": function_uuid,
            "sources": sources,
            "name": name,
            "description": description,
            "checksum": checksum,
            "kwargs": kwargs,
        }

        headers = {
            "Authorization": f"Bearer {AUTH_ACCESS_TOKEN}",
            "Content-type": "application/json",
        }
        req = requests.post(
            f"{CONF.server_url}/prov/new",
            data=json.dumps(params),
            headers=headers,
            verify=False,
        )

        if req.status_code == 200:
            return filename
        else:
            raise ClientError(req.status_code, req.text)
    else:
        raise ClientError(resp.status_code, resp.text)


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
    # assuming that it's running on our endpoint

    data = {}
    data["input_data"] = input_data
    data["output_data"] = output_data
    data["description"] = description
    data["gc_endpoint"] = endpoint_uuid
    data["function_uuid"] = function_uuid
    data["flow_kwargs"] = kwargs
    data["rule"] = policy
    data["timer_delay"] = timer_delay

    if len(tasks) > 1:
        data["tasks"] = tasks

    # if len(tasks) > 0:
    #     for t in data["tasks"]:
    #         t["endpoint"] = endpoint_uuid
    #         t["function"] = function_uuid
    # else:
    #     data["tasks"] = {}  # {"endpoint": endpoint_uuid, "function": function_uuid}

    headers = {
        "Authorization": f"Bearer {AUTH_ACCESS_TOKEN}",
        "Content-type": "application/json",
    }
    response = requests.post(
        f"{CONF.server_url}/flow/register",
        headers=headers,
        data=json.dumps(data),
        verify=False,
    )
    if response.status_code == 200:
        return response.json()
    raise ClientError(response.status_code, response.content)


# TODO: Fix bug where it'll request to login if tokens are not present
def globus_logout():
    """Remove the Globus Auth token file to invoke login on next API access."""
    logger.debug("Removing Globus auth tokens.")
    Path(CONF.dsaas_dir, CONF.token_file).unlink()


def create_source(
    name: str,
    url: str,
    collection_url: str,
    endpoint_uuid: str,
    email: str,
    timer: int | None = None,
    description: str | None = None,
    verifier: str | None = None,
    modifier: str | None = None,
    tags: str | None = None,
) -> None:
    """Create a source and store it in the database/server.

    Args:
        name (str): Source name
        url (str): URL to fetch the source data from
        collection_url (str): Globus collection domain to store source into.
        endpoint_uuid (str): Globus Compute Endpoint to use
        email (str): Email to send timer flow updates to.
        timer (int, optional): Update timer frequency in seconds. Defaults to None.
        description (str, optional): Description of the data. Defaults to None.
        verifier (str, optional): Globus Compute function UUID for the verification function. Defaults to None.
        modifier (str, optional): Globus Compute function UUID for the modifier function. Defaults to None.
        tags
    """

    collection_domain = urllib.parse.urlparse(collection_url).netloc
    collection_md = get_collection_metadata(collection_domain)

    collection_uuid = collection_md["DATA"][0]["id"]
    data = {
        "name": name,
        "url": url,
        "collection_url": collection_url,
        "collection_uuid": collection_uuid,
        "user_endpoint": endpoint_uuid,
    }
    if timer is not None:
        data["timer"] = timer
    if description is not None:
        data["description"] = description
    if verifier is not None:
        data["verifier"] = verifier
    if modifier is not None:
        data["modifier"] = modifier
    if email is not None:
        data["email"] = email
    if tags is not None:
        data["tags"] = tags
    elif email is None:
        data["email"] = ""  # todo make email optional

    # make sure user is authorized to access GCS server in flow
    _ = get_transfer_token(collection_uuid=collection_uuid)

    headers = {"Authorization": f"Bearer {AUTH_ACCESS_TOKEN}"}
    req = requests.post(
        f"{CONF.server_url}/source", json=data, headers=headers, verify=False
    )
    res = json.loads(req.content)
    return res

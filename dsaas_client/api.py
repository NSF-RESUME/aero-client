"""DSaaS client API module"""
import hashlib
import io
import json
import logging
import pandas as pd
import requests
import uuid

from pathlib import Path
from typing import TypeAlias

from dsaas_client import AUTH_ACCESS_TOKEN
from dsaas_client import TRANSFER_ACCESS_TOKEN
from dsaas_client.config import CONF
from dsaas_client.error import ClientError
from dsaas_client.utils import get_collection_metadata
from dsaas_client.utils import serialize

logger = logging.getLogger(__name__)

JSON: TypeAlias = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None


def register_function(func):
    pickled_function = serialize(func)
    headers = {"Authorization": f"Bearer {AUTH_ACCESS_TOKEN}"}
    res = requests.get(
        f"{CONF.server_url}/function",
        params={"function": pickled_function},
        headers=headers,
        verify=False,
    )
    return res.json()


def list_sources() -> list[dict[str, str | int]]:
    """Get the dictionary of all the sources.

    Returns:
        list[dict[str, str | int ]]: a list containing all available sources data
    """
    logger.debug("Retrieving all sources from server")
    headers = {"Authorization": f"Bearer {AUTH_ACCESS_TOKEN}"}
    req = requests.get(f"{CONF.server_url}/source", headers=headers, verify=False)
    resp = json.loads(req.content)
    return resp


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
        f"{CONF.server_url}/source/search", params=params, headers=headers, verify=False
    )
    resp = json.loads(req.content)
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
    source_id: int, version: int | None = None, output_path: str | None = None
) -> pd.DataFrame:
    """Gets the version for the source.

    Args:
        source_id (int): ID of the source data to fetch
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
        f"{CONF.server_url}/source/{source_id}/file",
        params=params,
        headers=headers,
        verify=False,
    )

    if req.status_code == 200:
        # initiate Globus transfer
        sf_data = req.json()
        url = f'{sf_data["collection_url"]}/{sf_data["file_name"]}'

        logger.debug("Initiating Globus Transfer of file.")
        headers = {"Authorization": f"Bearer {TRANSFER_ACCESS_TOKEN}"}
        resp = requests.get(url, headers=headers)
        try:
            df = pd.DataFrame(resp.json())
        except Exception:
            df = pd.read_table(io.StringIO(resp.text), sep=",")

        if output_path is not None:
            logger.debug("Saving Pandas DataFrame locally.")
            df.to_json(output_path)
        return df
    else:
        raise ClientError(req.status_code, req.text)


def save_output(
    data: str,
    name: str,
    collection_domain: str,
    description: str,
    sources: dict[int, int] = {},
    function_uuid: str | None = None,
    kwargs: JSON | None = None,
) -> str:
    """Save input data to GCS and record provenance information to DSaaS.

    Args:
        data (str): Currently either JSON or CSV string will work. Eventually will accept any Python object.
        name (str): Identifier to associate the data with. Will eventually help with search.
        collection_domain (str): The domain of the GCS Guest collection to store data to. Data will be stored in its root.
        description (str): Text-based description of the provenance of the data
        sources (dict[int | int]): A dictionary of DSaaS source ids and versions that contributed to the production of this data. Defaults to {}.
        function_uuid (str, optional): The UUID of the function used to process the data. Defaults to ''.
        kwargs: (JSON, optional): Input arguments to the function in JSON format. Defaults to None.

    Raises:
        ClientError: if DSaaS or GCS were not able to update properly, this error is raised

    Returns:
        str: the GCS UUID of the data should the client need to query it afterwards.
    """
    headers = {"Authorization": f"Bearer {TRANSFER_ACCESS_TOKEN}"}

    filename = str(uuid.uuid4())
    url = f"{CONF.https_server}/output/{filename}"

    # store in GCS
    resp = requests.put(url, headers=headers, data=data)

    if resp.status_code == 200:
        ## store in DB
        headers["Content-type"] = "application/json"
        checksum = hashlib.md5(data.encode("utf-8")).hexdigest()

        collection_md = get_collection_metadata(collection_domain)
        params = {
            "output_fn": filename,
            "collection_uuid": collection_md["DATA"][0]["id"],
            "collection_url": collection_md["DATA"][0]["https_server"],
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
    sources: dict[int | int] | list[int] | None = None,
    kwargs: JSON | None = None,
    config: str | None = None,
    description: str | None = None,
) -> None:
    """Register user function to run as a Globus Flow on remote server periodically.

    Args:
        endpoint_uuid (str): Globus Compute endpoint uuid
        function_uuid (str): Globus Compute registered function UUID
        sources(dict[int | int], list[int], optional): The input sources. A dict can be provided to include source version
            as value. Otherwise a list of source ids will use the latest source version. Default is None.
        kwargs (JSON, optional): Keyword arguments to pass to function. Default is None
        config (str, optional): Path to config file. Default is None.
        description (str, optional): A description of the Flow

    Raises:
        ClientError: if function was not able to be registered as a flow, this error is raised

    Returns:
        str: the timer job uuid.
    """

    if kwargs is None and config is not None:
        with open(config) as f:
            tasks = json.load(f)

        if len(tasks) > 0:
            kwargs = tasks[0]
    # assuming that it's running on our endpoint

    data = {}
    data["sources"] = sources
    data["description"] = description
    data["endpoint"] = endpoint_uuid
    data["function"] = function_uuid

    if kwargs is not None:
        data["kwargs"] = kwargs
    if len(tasks) > 1:
        data["tasks"] = tasks

    for t in data["tasks"]:
        t["endpoint"] = endpoint_uuid
        t["function"] = function_uuid

    headers = {
        "Authorization": f"Bearer {AUTH_ACCESS_TOKEN}",
        "Content-type": "application/json",
    }
    response = requests.post(
        f"{CONF.server_url}/prov/timer/{function_uuid}",
        headers=headers,
        data=json.dumps(data),
        verify=False,
    )
    if response.status_code == 200:
        return response
    raise ClientError(response.status_code, response.text)


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
    collection_md = get_collection_metadata(collection_url)

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

    headers = {"Authorization": f"Bearer {AUTH_ACCESS_TOKEN}"}
    req = requests.post(
        f"{CONF.server_url}/source", json=data, headers=headers, verify=False
    )
    print(req.content)
    res = json.loads(req.content)
    return res

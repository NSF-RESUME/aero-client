"""DSaaS client API module"""
import json
import logging
import pandas as pd
import requests
import uuid

from pathlib import Path

from osprey.client import CONF
from osprey.client import TRANSFER_ACCESS_TOKEN

from osprey.client.error import ClientError

from osprey.server.lib.serializer import serialize

logger = logging.getLogger(__name__)


def register_function(func):
    pickled_function = serialize(func)
    res = requests.get(
        f"{CONF.server_url}/function", params={"function": pickled_function}
    )
    return res.json()


def list_sources() -> list[dict[str, str | int]]:
    """Get the dictionary of all the sources.

    Returns:
        list[dict[str, str | int ]]: a list containing all available sources data
    """
    logger.debug("Retrieving all sources from server")
    req = requests.get(f"{CONF.server_url}/source")
    resp = json.loads(req.content)
    return resp


def source_versions(source_id: int) -> list[dict[str, str | int]]:
    """List versions given a source id.

    Returns:
        list[dict[str, str | int]]: A list of all source versions
    """
    logger.debug(f"Requesting all versions of source id {source_id}.")
    req = requests.get(f"{CONF.server_url}/source/{source_id}/versions")
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
    req = requests.get(f"{CONF.server_url}/source/{source_id}/file", params=params)

    if req.status_code == 200:
        # initiate Globus transfer
        headers = {"Authorization": f"Bearer {TRANSFER_ACCESS_TOKEN}"}
        url = f'{CONF.https_server}/source/{req.json()["file_name"]}'

        logger.debug("Initiating Globus Transfer of file.")
        resp = requests.get(url, headers=headers)
        try:
            df = pd.DataFrame(resp.json())
        except Exception:
            df = pd.DataFrame(resp.text)

        if output_path is not None:
            logger.debug("Saving Pandas DataFrame locally.")
            df.to_json(output_path)
        return df
    else:
        raise ClientError(req.status_code, req.text)


def save_output(
    data: str,
    name: str,
    description: str,
    sources: dict[int, int] = {},
    function_uuid: str = "",
    args: str = "",
) -> str:
    """Save input data to GCS and record provenance information to DSaaS.

    Args:
        data (str): Currently either JSON or CSV string will work. Eventually will accept any Python object.
        sources (dict[int | int]): A dictionary of DSaaS source ids and versions that contributed to the production of this data. Defaults to {}.
        name (str): Identifier to associate the data with. Will eventually help with search.
        description (str): Text-based description of the provenance of the data
        function_uuid (str, optional): The UUID of the function used to process the data. Defaults to ''.
        args: (str, optional): Input arguments to the function in string format. Defaults to ''.

    Raises:
        ClientError: if DSaaS or GCS were not able to update properly, this error is raised

    Returns:
        str: the GCS UUID of the data should the client need to query it afterwards.
    """
    headers = {"Authorization": f"Bearer {TRANSFER_ACCESS_TOKEN}"}

    filename = str(uuid.uuid4())
    url = f"{CONF.https_server}/output/{filename}"
    resp = requests.put(url, headers=headers, data=data)

    if resp.status_code == 200:
        ## store in DB
        headers = {"Content-type": "application/json"}
        params = {
            "output_fn": filename,
            "function_uuid": function_uuid,
            "sources": sources,
            "name": name,
            "description": description,
            "args": args,
        }
        req = requests.post(
            f"{CONF.server_url}/prov/new/{function_uuid}",
            data=json.dumps(params),
            headers=headers,
        )

        if req.status_code == 200:
            return filename
        else:
            raise ClientError(req.status_code, req.text)
    else:
        raise ClientError(resp.status_code, resp.text)


def register_flow(function_uuid: str, kwargs: dict):
    # assuming that it's running on our endpoint
    pass


def globus_logout():
    """Remove the Globus Auth token file to invoke login on next API access."""
    logger.debug("Removing Globus auth tokens.")
    Path(CONF.dsaas_dir, CONF.token_file).unlink()


def create_source(
    name: str,
    url: str,
    email: str,
    timer: int | None = None,
    description: str | None = None,
    verifier: str | None = None,
    modifier: str | None = None,
) -> None:
    """Create a source and store it in the database/server.

    Args:
        name (str): Source name
        url (str): URL to fetch the source data from
        email (str): Email to send timer flow updates to.
        timer (int, optional): Update timer frequency in seconds. Defaults to None.
        description (str, optional): Description of the data. Defaults to None.
        verifier (str, optional): Globus Compute function UUID for the verification function. Defaults to None.
        modifier (str, optional): Globus Compute function UUID for the modifier function. Defaults to None.
    """
    data = {"name": name, "url": url}
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
    else:
        data["email"] = ""  # todo make email optional

    req = requests.post(f"{CONF.server_url}/source", json=data)
    res = json.loads(req.content)
    return res

"""DSaaS client util module"""

import codecs
import dill
import hashlib
import json
import logging
import mimetypes
import requests
import urllib
import uuid

from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from pathlib import Path

from globus_compute_sdk import Client as ComputeClient
from globus_sdk import AccessTokenAuthorizer
from globus_sdk import NativeAppAuthClient
from globus_sdk import RefreshTokenAuthorizer
from globus_sdk import TransferClient


from aero_client.config import CONF
from aero_client.error import ClientError


_REDIRECT_URI = "https://auth.globus.org/v2/web/auth-code"
_TOKEN_PATH = Path(CONF.dsaas_dir, CONF.token_file)

logger = logging.getLogger(__name__)


class PolicyEnum(IntEnum):
    NONE = -1
    INGESTION = 0
    TIMER = 1
    ANY = 2
    ALL = 3


@dataclass
class AeroOutput:
    name: str
    path: str


def serialize(obj) -> str:
    try:
        return codecs.encode(dill.dumps(obj), "base64").decode()
    except Exception:
        raise ClientError(400, "Cannot serialize the function")


def authenticate(client: NativeAppAuthClient, scope: str):
    """Perform Globus Authentication."""

    client.oauth2_start_flow(
        redirect_uri=_REDIRECT_URI, refresh_tokens=True, requested_scopes=scope
    )

    url = client.oauth2_get_authorize_url()
    print("Please visit the following url to authenticate:")
    print(url)

    auth_code = input("Enter the auth code:")
    auth_code = auth_code.strip()
    return client.oauth2_exchange_code_for_tokens(auth_code)


def _client_auth() -> str:
    """Authorizes the client to communicate with AERO

    Returns:
        str: Access token of the authorizer
    """
    client = NativeAppAuthClient(client_id=CONF.client_uuid)
    if _TOKEN_PATH.is_file():
        tokens = load_tokens()
        auth_token = tokens[CONF.portal_client_id]["refresh_token"]

        _ = RefreshTokenAuthorizer(refresh_token=auth_token, auth_client=client)
    else:
        scopes = [
            f"https://auth.globus.org/scopes/{CONF.portal_client_id}/action_all",
            "openid",
            "profile",
            "email",
            TransferClient.scopes.all,
        ]
        token_response = authenticate(client=client, scope=scopes)

        CONF.dsaas_dir.mkdir(exist_ok=True)
        with open(_TOKEN_PATH, "w+") as f:
            json.dump(token_response.by_resource_server, f)

        auth_token = token_response.by_resource_server[CONF.portal_client_id][
            "access_token"
        ]
        _ = AccessTokenAuthorizer(access_token=auth_token)

    return auth_token


def get_transfer_token(collection_uuid: str) -> str:
    """Get the transfer token belonging to the Globus Guest Collection.

    This function first verifies whether the token already exists. If
    it does not, it generates the tokens and updates the token file.

    Args:
        collection_uuid (str): The UUID of the Globus Guest Collection.

    Returns:
        str: The transfer token for the guest collection
    """
    tokens = load_tokens()

    client = NativeAppAuthClient(client_id=CONF.client_uuid)

    if collection_uuid in tokens:
        transfer_token = tokens[collection_uuid]["access_token"]
        ref_transfer_token = tokens[collection_uuid]["refresh_token"]
        rta = RefreshTokenAuthorizer(
            refresh_token=ref_transfer_token, auth_client=client
        )
        transfer_token = rta.get_authorization_header().split(" ")[-1]
        return transfer_token

    else:
        scopes = [
            f"https://auth.globus.org/scopes/{collection_uuid}/https",
            TransferClient.scopes.all,
        ]

        token_response = authenticate(client=client, scope=scopes)

        tokens = tokens | token_response.by_resource_server
        with open(_TOKEN_PATH, "w+") as f:
            json.dump(tokens, f)

        transfer_token = token_response.by_resource_server[collection_uuid][
            "access_token"
        ]
        _ = AccessTokenAuthorizer(access_token=transfer_token)

        return transfer_token


def load_tokens():
    logger.debug("Token file exists. Instantiating tokens from authorizer.")
    with open(_TOKEN_PATH, "r") as f:
        tokens = json.load(f)

    return tokens


def get_collection_metadata(domain: str) -> None:
    tokens = load_tokens()
    transfer_token = tokens["transfer.api.globus.org"]["refresh_token"]
    client = NativeAppAuthClient(client_id=CONF.client_uuid)
    authorizer = RefreshTokenAuthorizer(
        refresh_token=transfer_token, auth_client=client
    )
    return TransferClient(authorizer=authorizer).endpoint_search(
        domain.replace(".data.globus.org", "")
    )


def download(*args, **kwargs):
    """Download data from user-specified repository.

    Returns:
        tuple[str, str]: Path to the data and its
            associated extension.
    """
    import hashlib
    import pathlib
    import requests
    import uuid
    from mimetypes import guess_extension
    from pathlib import Path

    from aero_client.config import CONF
    from aero_client.utils import load_tokens

    if "temp_dir" in kwargs:
        TEMP_DIR = kwargs["temp_dir"]
    else:
        TEMP_DIR = pathlib.Path.home() / "aero"
        kwargs["temp_dir"] = str(TEMP_DIR)

    tokens = load_tokens()
    auth_token = tokens[CONF.portal_client_id]["refresh_token"]

    headers = {"Authorization": f"Bearer {auth_token}"}

    response = requests.get(
        f'{CONF.server_url}/source/{kwargs["source_id"]}', headers=headers, verify=False
    )
    source = response.json()

    response = requests.get(source["url"])
    content_type = response.headers["content-type"]
    ext = guess_extension(content_type.split(";")[0])

    bn = str(uuid.uuid4())
    fn = Path(TEMP_DIR, bn)

    TEMP_DIR.mkdir(exist_ok=True, parents=True)

    with open(fn, "w+") as f:
        f.write(response.content.decode("utf-8"))

    kwargs["file"] = str(fn)
    kwargs["file_bn"] = bn
    kwargs["file_format"] = ext
    kwargs["checksum"] = hashlib.md5(response.content).hexdigest()
    kwargs["size"] = fn.stat().st_size
    kwargs["download"] = True

    return args, kwargs


def register_function(fn: callable):
    """Registers function with Globus Compute by registering the function with the wrapper"""
    gcc = ComputeClient()
    func_uuid = gcc.register_function(aero_format(fn))
    return func_uuid


def gcs_save(path: str, collection_url: str, collection_uuid: str) -> dict:
    # collection_domain = urllib.parse.urlparse(collection_url).netloc

    TRANSFER_TOKEN = get_transfer_token(collection_uuid)
    headers = {"Authorization": f"Bearer {TRANSFER_TOKEN}"}

    filename = str(uuid.uuid4())
    url = urllib.parse.urljoin(collection_url, filename)

    mtype = mimetypes.guess_type(path)

    if "text" not in mtype:
        with open(path, "rb") as f:
            data = f.read()
            checksum = hashlib.md5(data).hexdigest()
    else:
        with open(path, "r") as f:
            data = f.read()
            checksum = hashlib.md5(data.encode("utf-8")).hexdigest()

    # store in GCS
    resp = requests.put(url, headers=headers, data=data)

    Path(path).unlink()  # remove tmp output

    assert resp.status_code == 200, resp.content

    return {
        "created_at": datetime.now().ctime(),
        "checksum": checksum,
        "size": len(data),
        "file_bn": filename,
        "file_format": mtype,
    }


def aero_format(fn: callable):
    """AERO decorator that wraps user analysis function to capture provenance information."""
    import requests
    import urllib

    from pathlib import Path

    def wrapper(*args, **kwargs):
        fn_in = {}

        assert "aero" in kwargs.keys()

        if "output_data" in kwargs["aero"]:
            for name, val in kwargs["aero"]["output_data"].items():
                if "file" in val:
                    fn_in[name] = val["file"]
        if "input_data" in kwargs["aero"]:
            for name, val in kwargs["aero"]["input_data"].items():
                TRANSFER_TOKEN = get_transfer_token(val["collection_uuid"])
                headers = {"Authorization": f"Bearer {TRANSFER_TOKEN}"}

                resp = requests.get(
                    urllib.parse.urljoin(
                        f"{val['collection_url']}/", f"{val['file_bn']}"
                    ),
                    headers=headers,
                )

                if "tmp_dir" not in val:
                    val["tmp_dir"] = "/tmp"

                tmp_path = Path(val["tmp_dir"]) / val["file_bn"]
                with open(tmp_path, "wb+") as f:
                    f.write(resp.content)
                fn_in[name] = str(tmp_path)

        aero_args = kwargs.pop("aero")
        fn_in.update(**kwargs)

        outputs = fn(**fn_in)

        kwargs["aero"] = aero_args

        if isinstance(outputs, list):
            for ao in outputs:
                name = ao.name
                metadata = gcs_save(
                    path=ao.path,
                    collection_url=kwargs["aero"]["output_data"][name][
                        "collection_url"
                    ],
                    collection_uuid=kwargs["aero"]["output_data"][name][
                        "collection_uuid"
                    ],
                )
                kwargs["aero"]["output_data"][name].update(**metadata)
        else:
            assert isinstance(
                outputs, AeroOutput
            ), "ERROR: function output is not an AeroOutput"
            name = outputs.name
            metadata = gcs_save(
                path=outputs.path,
                collection_url=kwargs["aero"]["output_data"][name]["collection_url"],
                collection_uuid=kwargs["aero"]["output_data"][name]["collection_uuid"],
            )

            if "url" in kwargs["aero"]["output_data"][name].keys():
                metadata.pop("checksum", None)
            kwargs["aero"]["output_data"][name].update(**metadata)

        # remove tmp data
        for k, v in fn_in.items():
            if (
                k in kwargs["aero"]["input_data"]
                and isinstance(v, str)
                and Path(v).exists()
            ):
                Path(v).unlink()

        return kwargs

    return wrapper

"""DSaaS client util module"""
import codecs
import dill
import json
import logging

from enum import IntEnum
from pathlib import Path

from globus_sdk import AccessTokenAuthorizer
from globus_sdk import NativeAppAuthClient
from globus_sdk import RefreshTokenAuthorizer
from globus_sdk import TransferClient

from dsaas_client.config import CONF
from dsaas_client.error import ClientError


_REDIRECT_URI = "https://auth.globus.org/v2/web/auth-code"
_TOKEN_PATH = Path(CONF.dsaas_dir, CONF.token_file)

logger = logging.getLogger(__name__)


class PolicyEnum(IntEnum):
    TIMER = 0
    ANY = 1
    ALL = 2
    NONE = 3


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
            "urn:globus:auth:scope:transfer.api.globus.org:all",
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
        _ = RefreshTokenAuthorizer(refresh_token=ref_transfer_token, auth_client=client)
        return transfer_token

    else:
        scopes = [
            f"https://auth.globus.org/scopes/{collection_uuid}/https",
            "urn:globus:auth:scope:transfer.api.globus.org:all",
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

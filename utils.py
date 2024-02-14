"""DSaaS client util module"""
import json
import logging

from pathlib import Path

from globus_sdk import AccessTokenAuthorizer
from globus_sdk import NativeAppAuthClient
from globus_sdk import RefreshTokenAuthorizer
from globus_sdk import TransferClient

from osprey.client.config import CONF
from osprey.server.lib.globus_auth import authenticate

logger = logging.getLogger(__name__)


def _client_auth() -> str:
    """Authorizes the client for HTTPS transfers to and from the GCS server

    Returns:
        str: Access token of the authorizer
    """
    token_path = Path(CONF.dsaas_dir, CONF.token_file)
    client = NativeAppAuthClient(client_id=CONF.client_uuid)
    if token_path.is_file():
        logger.debug("Token file exists. Instantiating tokens from authorizer.")
        with open(token_path, "r") as f:
            tokens = json.load(f)
        transfer_refresh_token = tokens[CONF.collection_uuid]["refresh_token"]
        transfer_token = tokens[CONF.collection_uuid]["access_token"]
        auth_refresh_token = tokens[CONF.portal_client_id]["refresh_token"]
        auth_token = tokens[CONF.portal_client_id]["access_token"]
        authorizer = RefreshTokenAuthorizer(
            refresh_token=transfer_refresh_token, auth_client=client
        )
        _ = RefreshTokenAuthorizer(refresh_token=auth_refresh_token, auth_client=client)
    else:
        scopes = [
            f"https://auth.globus.org/scopes/{CONF.collection_uuid}/https",
            f"https://auth.globus.org/scopes/{CONF.portal_client_id}/action_all",
            "openid",
            "profile",
            "email",
            "urn:globus:auth:scope:transfer.api.globus.org:all",
        ]
        token_response = authenticate(client=client, scope=scopes)

        CONF.dsaas_dir.mkdir(exist_ok=True)
        with open(token_path, "w+") as f:
            json.dump(token_response.by_resource_server, f)

        transfer_token = token_response.by_resource_server[CONF.collection_uuid][
            "access_token"
        ]
        auth_token = token_response.by_resource_server[CONF.portal_client_id][
            "access_token"
        ]
        authorizer = AccessTokenAuthorizer(access_token=transfer_token)
        _ = AccessTokenAuthorizer(access_token=auth_token)

    _ = TransferClient(authorizer=authorizer)  # Todo: remove?
    return transfer_token, auth_token

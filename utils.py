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
        refresh_token = tokens["refresh_token"]
        transfer_token = tokens["access_token"]
        authorizer = RefreshTokenAuthorizer(
            refresh_token=refresh_token, auth_client=client
        )
    else:
        scopes = f"https://auth.globus.org/scopes/{CONF.collection_uuid}/https"
        token_response = authenticate(client=client, scope=scopes)
        globus_transfer_data = token_response.by_resource_server[CONF.collection_uuid]

        CONF.dsaas_dir.mkdir(exist_ok=True)
        with open(token_path, "w+") as f:
            json.dump(globus_transfer_data, f)

        transfer_token = globus_transfer_data["access_token"]
        authorizer = AccessTokenAuthorizer(access_token=transfer_token)

    _ = TransferClient(authorizer=authorizer)
    return transfer_token

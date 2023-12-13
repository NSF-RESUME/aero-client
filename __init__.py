from osprey.client.config import CONF
from osprey.client.utils import _client_auth

SERVER_URL = f"http://{CONF.server_address}/osprey/api/v1.0/"


def authenticate():
    return _client_auth


TRANSFER_ACCESS_TOKEN = authenticate

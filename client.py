"""Osprey Client API."""
import requests
import pickle
import os

from proxystore.proxy import Proxy
from proxystore.store import Store
from proxystore.connectors.file import FileConnector
from proxystore.connectors.file import FileKey


server_url = "http://localhost:5001/osprey/api/v1.0/proxies"
store: Store = Store(name="osprey_data_store", connector=FileConnector(store_dir='databases'))


def all_proxies() -> dict[str, list[Proxy]]:
    """Get the dictionary of all the proxied data.

    Returns:
        a dictionary containing all available proxied data
    """
    req = requests.get(server_url, 'data')
    pickle.loads(req.content)


def get_proxy(name: str) -> Proxy:
    """Get proxy by data name.

    Args:
        name (str) : the name of the data to retrieve

    Returns:
        A proxy object of the data
    """
    url = os.path.join(server_url, 'data')
    req = requests.get(url, data=name)
    key = eval(pickle.loads(req.content))
    proxy = store.proxy_from_key(key)
    return proxy

def get_metadata(name: str) -> list[dict]:
    """Get table's metadata.

    Args:
        name (str) : the name of the data to retrieve

    Returns:
        The metadata of the table
    """
    url = os.path.join(server_url, 'metadata')
    req = requests.get(url, data=name)
    return pickle.loads(req.content)

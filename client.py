"""Osprey Client API."""
import requests
import pickle

from proxystore.proxy import Proxy

server_url = "http://10.52.0.209:5000/osprey/api/v1.0/proxies"


def all_proxies() -> dict[str, list[Proxy]]:
    """Get the dictionary of all the proxied data.

    Returns:
        a dictionary containing all available proxied data
    """
    req = requests.get(server_url)
    return pickle.loads(req.content)


def get_proxy(name: str) -> Proxy:
    """Get proxy by data name.

    Args:
        name (str) : the name of the data to retrieve

    Returns:
        A proxy object of the data
    """
    req = requests.get(server_url, data=name)
    return pickle.loads(req.content)

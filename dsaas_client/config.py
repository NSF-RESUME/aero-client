"""DSaaS client config module"""
import os
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path


# TODO: maybe move to a configuration file
@dataclass
class ClientConf:
    client_uuid: str = "c78511ef-8cf7-4802-a7e1-7d56e27b1bf8"
    endpoint_uuid: str = "6dec76ea-e7fd-492e-947e-f2a92073a275"
    collection_uuid: str = "52f7f6bc-444f-439a-ad48-a4569d10c3d1"
    portal_client_id: str = "082d6a19-da16-4552-9944-e081cdaff7bc"
    https_server: str = "https://g-c952d0.1305de.36fe.data.globus.org"
    dsaas_dir: str = Path(Path.home(), ".local/share/dsaas")
    token_file: str = "client_tokens.json"
    server_address: str = field(init=False)
    server_url: str = field(init=False)

    def __post_init__(self):
        if (test := os.getenv("DSAAS_TESTENV")) is not None and int(test) == 1:
            self.server_address = "localhost:5001"
        else:
            self.server_address = "129.114.27.115:5001"
        self.server_url = f"https://{self.server_address}/osprey/api/v1.0/"


CONF = ClientConf()

"""
DSaaS client config module.
"""

import os
import logging
import shutil
import tomllib
from dataclasses import field
from pathlib import Path
from pydantic.dataclasses import dataclass


_conf_symlink_path: Path = Path.home() / ".aero"
_conf_fn: str = "config.toml"

logger = logging.getLogger("__name__")


# TODO: maybe move to a configuration file
@dataclass
class ClientConf:  # (BaseModel):
    """
    Configuration for the AERO client.
    """

    client_uuid: str = "c78511ef-8cf7-4802-a7e1-7d56e27b1bf8"
    """
    ...
    """

    portal_client_id: str = os.getenv(
        "PORTAL_CLIENT_ID"
    )  # v"082d6a19-da16-4552-9944-e081cdaff7bc"
    aero_dir: Path = field(default=Path("~/.local/share/aero").expanduser())
    token_file: str = "client_tokens.json"  # field(default_factory=str, default="client_tokens.json", init=False)
    server_address: Path = "https://aero.emews.org:5001"
    server_url: str = f"{server_address}/osprey/api/v1.0/"

    def __post_init__(self):
        # does it ever not exist? probably not so can remove
        if not self.aero_dir.exists():
            Path.mkdir(self.aero_dir, parents=True)
        if not _conf_symlink_path.exists():
            _conf_symlink_path.symlink_to(target=self.aero_dir)


def load_conf(conf_file: str, update: bool = False) -> None:
    if update:
        _conf_symlink_path.unlink(missing_ok=True)

    with open(conf_file, "rb") as f:
        config = tomllib.load(f)

    conf_kwargs = {}

    conf_kwargs["client_uuid"] = config["client_uuid"]
    conf_kwargs["portal_client_id"] = config["portal_client_id"]

    if "server" in config["aero"]:
        conf_kwargs["server_address"] = config["aero"]["server"]
    else:
        conf_kwargs["server_address"] = "https://aero.emews.org:5001"

    conf_kwargs["server_url"] = f"{conf_kwargs['server_address']}/osprey/api/v1.0/"
    conf_kwargs["aero_dir"] = Path(config["aero"]["cache_dir"]).expanduser().absolute()

    Path.mkdir(conf_kwargs["aero_dir"], parents=True, exist_ok=True)

    try:
        if _conf_symlink_path.readlink() != conf_kwargs["aero_dir"]:
            shutil.copy(conf_file, (Path(conf_kwargs["aero_dir"]) / _conf_fn))
    except FileNotFoundError:
        shutil.copy(conf_file, (Path(conf_kwargs["aero_dir"]) / _conf_fn))

    return ClientConf(**conf_kwargs)

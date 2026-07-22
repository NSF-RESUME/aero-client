"""
AERO client config module.
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
_DEFAULT_SERVER: str = "https://aero.emews.org:5001"

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

    portal_client_id: str = os.getenv("PORTAL_CLIENT_ID")  # type: ignore
    aero_dir: Path = field(default=Path("~/.local/share/aero").expanduser())
    token_file: str = "client_tokens.json"  # field(default_factory=str, default="client_tokens.json", init=False)
    server_address: str = "https://aero.emews.org:5001"
    server_url: str = f"{server_address}/osprey/api/v1.0/"

    def __post_init__(self):
        # does it ever not exist? probably not so can remove
        if not self.aero_dir.exists():
            Path.mkdir(self.aero_dir, parents=True)
        if not _conf_symlink_path.exists():
            _conf_symlink_path.symlink_to(target=self.aero_dir)


def load_conf(
    conf_file: str,
    update: bool = False,
    symlink: bool = True,
    profile: str = "default",
) -> ClientConf:
    if update:
        _conf_symlink_path.unlink(missing_ok=True)

    with open(conf_file, "rb") as f:
        config = tomllib.load(f)

    # Select the profile section. New format: one top-level table per profile
    # (e.g. [default], [testing]) holding flat keys. Legacy format: top-level
    # keys plus an [aero] table, read as the "default" profile.
    if (
        profile in config
        and isinstance(config[profile], dict)
        and "client_uuid" in config[profile]
    ):
        section = config[profile]
        client_uuid = section["client_uuid"]
        portal_client_id = section["portal_client_id"]
        server_address = section.get("server", _DEFAULT_SERVER)
        cache_dir = section["cache_dir"]
    elif profile == "default" and "client_uuid" in config:
        # legacy no-profile format: top-level keys + [aero] table
        aero = config.get("aero", {})
        client_uuid = config["client_uuid"]
        portal_client_id = config["portal_client_id"]
        server_address = aero.get("server", _DEFAULT_SERVER)
        cache_dir = aero["cache_dir"]
    else:
        raise KeyError(f"Profile '{profile}' not found in {conf_file}")

    conf_kwargs = {}
    conf_kwargs["client_uuid"] = client_uuid
    conf_kwargs["portal_client_id"] = portal_client_id
    conf_kwargs["server_address"] = server_address
    conf_kwargs["server_url"] = f"{server_address}"  # /osprey/api/v1.0/"
    conf_kwargs["aero_dir"] = Path(cache_dir).expanduser().absolute()

    if symlink:
        Path.mkdir(conf_kwargs["aero_dir"], parents=True, exist_ok=True)

        try:
            if _conf_symlink_path.readlink() != conf_kwargs["aero_dir"]:
                shutil.copy(conf_file, (Path(conf_kwargs["aero_dir"]) / _conf_fn))
        except FileNotFoundError:
            shutil.copy(conf_file, (Path(conf_kwargs["aero_dir"]) / _conf_fn))

    return ClientConf(**conf_kwargs)

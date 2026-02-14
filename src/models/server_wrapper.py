import pathlib
import logging
import os
import functools

from scp import SCPClient
from paramiko import SSHClient
from models import Config
from typing import Callable, Any, TypeVar, cast

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def skip_if_dry(func: F) -> F:
    """
    If --dry option is provided, skip the function execution
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if Config.dry():
            # Do not sync the sync if dry
            return
        else:
            return func(*args, **kwargs)

    # Cast pour que le type retourné corresponde à celui du décorateur
    return cast(F, wrapper)


class ServerWrapper:
    """
    This class all to save logs and www content into distant server througth ssh tunnel.
    Follwing envrionements variables are mandatory to work ATP2OSM_HOST, ATP2OSM_USER, ATP2OSM_PASSWORD, ATP2OSM_ROOT_PATH
    """

    @skip_if_dry
    def __init__(self):
        self.ssh = SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.connect(
            hostname=os.getenv("ATP2OSM_HOST"),
            username=os.getenv("ATP2OSM_USER"),
            password=os.getenv("ATP2OSM_PASSWORD"),
        )
        self.scp = SCPClient(transport=self.ssh.get_transport())

    @skip_if_dry
    def clean_www(self):
        self.ssh_exec("rm -rf ~/www")  # clean www folder

    @skip_if_dry
    def sync_file(self, src: pathlib.Path, dest=pathlib.Path, recursive=False) -> None:
        if not src.exists():
            logger.error(f"The file does not exists: {src.absolute}")
            exit(1)

        self.ssh_exec(f"mkdir -p {dest if recursive else dest.parent}")
        self.scp.put(src, remote_path=dest, recursive=recursive)

    @skip_if_dry
    def ssh_exec(self, command: str):
        _, stdout, stderr = self.ssh.exec_command(command)
        logger.debug(stdout.read().decode())
        error = stderr.read().decode()
        if error:
            logger.error(f"SSH error: {error}")
            return

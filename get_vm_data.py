import csv
import distutils
import logging
import os
from typing import Any, Dict, List, Union

from proxmoxer import ProxmoxAPI
from proxmoxer.core import ProxmoxAPI, ResourceException

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s:%(funcName)s:%(lineno)s: %(message)s"
)

logger = logging.getLogger(__name__)


class Env:
    @staticmethod
    def get_or(value: str, default: Union[str, bool]) -> Union[str, bool]:
        if value in os.environ and os.environ[value] != "":
            os.environ[value]
        return default

    @staticmethod
    def get_or_truth(value: str, default: bool) -> bool:
        val = Env.get_or(value, default)
        if type(val) == bool:
            return val
        return distutils.util.strtobool(Env.get_or(val, default))

    @staticmethod
    def must_get(value: str, err: str) -> str:
        if value not in os.environ or os.environ[value] == "":
            raise ValueError(f"{err} in environment variable {value}")
        return os.environ[value]


class Config:
    url: str
    user: str
    password: str
    verify_ssl: bool
    service: str

    def __init__(self) -> None:
        self.from_env()

    def from_env(self) -> None:
        self.url = Env.must_get("PROX_URL", "Must provide URL")
        self.user = Env.must_get("PROX_USER", "Must provide user")
        self.password = Env.must_get("PROX_PASSWORD", "Must provide password")
        self.verify_ssl = Env.get_or_truth("PROX_SSL", False)
        self.service = Env.get_or("PROX_SERVICE", "PVE")


def get_proxmox(config: Config) -> ProxmoxAPI:
    proxmox = ProxmoxAPI(
        host=config.url,
        user=config.user,
        password=config.password,
        verify_ssl=config.verify_ssl,
        service=config.service,
    )
    return proxmox


class NodeList:
    list: list = []
    proxmox: ProxmoxAPI

    def __init__(self, proxmox: ProxmoxAPI) -> None:
        self.proxmox = proxmox

    def get(self) -> List[str]:
        if len(self.list) != 0:
            return self.list

        for node in self.proxmox.nodes.get():
            self.list.append(node["node"])
        return self.list


def key_value_pair(data: dict) -> str:
    acum: str = ""
    for key in data:
        if acum != "":
            acum += ","
        acum += f"{key}={data[key]}"

    return acum


class VM:
    proxmox: ProxmoxAPI
    extra_info: Dict[Any, Any]
    config: dict
    vmid: int
    node: str
    fs_info: dict

    def __init__(self, vmid: int, node: str, proxmox: ProxmoxAPI) -> None:
        if vmid is None:
            raise ValueError("Invalid Argument")

        if node is None:
            raise ValueError("Invalid Argument")

        if proxmox is None:
            raise ValueError("Invalid Argument")


        self.proxmox = proxmox
        self.node = node
        self.vmid = vmid
        self.config = {}
        self.fs_info = {}
        self.extra_info = {}

    def get(self) -> Dict[str, Union[int, str]]:
        logger.info(f"Get data for {self.vmid}")
        config = self.get_config()
        extra_info = self.get_extra_info()

        return {**config, **extra_info}

    def get_config(self) -> Dict[str, Union[int, str]]:
        if self.config:
            return self.config

        self.config = self.proxmox.nodes(self.node).qemu(self.vmid).config.get()
        return self.config

    def has_agent(self) -> bool:
        return self.get_config()["agent"] == "1"

    def get_fs_info(
        self,
    ):
        if self.has_agent() == False:
            return {}

        if self.fs_info:
            return self.fs_info

        try:
            self.fs_info = self.proxmox.get(
                f"nodes/{self.node}/qemu/{self.vmid}/agent/get-fsinfo"
            )
        except ResourceException as e:
            logger.info(
                f"Can't get fs info for vm {self.vmid} on node {self.node}. Exception {e.content}"
            )
        return self.fs_info

    def get_extra_info(self) -> Dict[str, str]:
        fsinfo = self.get_fs_info()

        extraList = {}

        if fsinfo is not None:

            if "result" not in fsinfo:
                return {}

            logger.debug(fsinfo)
            if "error" in fsinfo["result"]:
                error = fsinfo["result"]["error"]
                logger.error(
                    f"error for vmid {self.vmid} class '{error['class']}' desc '{error['desc']}'"
                )
                return {}

            sum_used_bytes = 0
            sum_total_bytes = 0
            sum_unused_bytes = 0
            for disk in fsinfo["result"]:
                logger.debug(disk)
                if disk["type"] not in ["CDFS", "UDF"]:

                    if "total-bytes" not in disk:
                        logger.debug(f"skipping {disk}")
                        continue

                    unused_bytes = int(disk["total-bytes"]) - int(disk["used-bytes"])
                    extrainfo = {
                        "unused-bytes": unused_bytes,
                        "used-bytes": int(disk["used-bytes"]),
                        "total-bytes": int(disk["total-bytes"]),
                        "precent-remaining": unused_bytes
                        / int(disk["total-bytes"])
                        * 100,
                        "filesystem": disk["type"],
                    }
                    sum_used_bytes +=  int(disk["used-bytes"])
                    sum_total_bytes += int(disk["total-bytes"])
                    sum_unused_bytes += unused_bytes
                    logger.debug({sum_used_bytes, sum_total_bytes, sum_unused_bytes})

                    extraList |= {disk["mountpoint"]: key_value_pair(extrainfo)}
                else:
                    logger.debug(f"skipping {disk['mountpoint']} type {disk['type']}")

        extraList |= {
            'sum_used_bytes': sum_used_bytes,
            'sum_total_bytes': sum_total_bytes,
            'sum_unused_bytes': sum_unused_bytes,
        }

        return extraList


class VMList:
    node: NodeList
    list: list
    proxmox: ProxmoxAPI

    def __init__(self, node: NodeList, proxmox: ProxmoxAPI) -> None:
        if len(node.get()) == 0:
            raise ValueError("Node list empty")
        self.node = node
        self.list = []
        self.proxmox = proxmox

    def get(self) -> List[Dict[str, Union[int, str]]]:
        if len(self.list) != 0:
            return self.list

        for node in self.node.get():
            for vm in self.proxmox.nodes(node).qemu.get():
                logger.debug(vm)
                vmid = int(vm["vmid"])
                status = vm["status"]
                if status != "running":
                    logger.info(f"skipping {vmid} status {status}")
                    continue

                data = VM(vmid, node, self.proxmox).get()
                self.list.append(data)

        self.normalise()

        return self.list

    def normalise(self) -> None:
        keys = QemuConfig(self).get_keys()
        # Ensure all keys exist for each vm for consistency
        for vm in self.list:
            for key in keys:
                if key not in vm:
                    vm[key] = ""

    def keys(self) -> List[str]:
        return QemuConfig(self).get_keys()


class QemuConfig:
    keys: list

    def __init__(self, vm: VMList) -> None:
        if len(vm.get()) == 0:
            raise ValueError("VM list empty")
        self.vm = vm
        self.keys = []

    def get_keys(self) -> List[str]:
        if len(self.keys) != 0:
            return self.keys

        # Initialise heading list
        self.keys = list(self.vm.get()[0])
        # Ensure all keys exist
        for vm in self.vm.get():
            for item in vm:
                if item not in self.keys:
                    self.keys.append(item)
        return self.keys


class CSV:
    @staticmethod
    def output(
        filename: str, data: List[Dict[str, Union[int, str]]], heading: list
    ) -> None:
        try:
            with open(filename, "w") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=heading)
                writer.writeheader()
                for item in data:
                    writer.writerow(item)
        except IOError as e:
            logger.exception("I/O error", e)


def main() -> None:
    config = Config()
    prox = get_proxmox(config)
    node = NodeList(prox)
    vm = VMList(node, prox)
    data = vm.get()

    CSV.output("vmlist.csv", data, vm.keys())


if __name__ == "__main__":
    main()

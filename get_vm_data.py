import csv
import logging
import os
import pp

from proxmoxer import ProxmoxAPI
from proxmoxer.core import ResourceException

logging.basicConfig(
     level=logging.INFO, format="%(levelname)s:%(funcName)s:%(lineno)s: %(message)s"
)

logger = logging.getLogger(__name__)
class Env:
    @staticmethod
    def get_or(value: str, default: str) -> str:
        if value in os.environ and os.environ[value] != "":
            return value
        return default

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

    def __init__(self):
        self.from_env()

    def from_env(self):
        self.url = Env.must_get("PROX_URL", "Must provide URL")
        self.user = Env.must_get("PROX_USER", "Must provide user")
        self.password = Env.must_get("PROX_PASSWORD", "Must provide password")
        self.verify_ssl = Env.get_or("PROX_SSL", False)
        self.service = Env.get_or("PROX_SERVICE", "PVE")


def get_proxmox(config: Config):
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
    proxmox: any

    def __init__(self, proxmox):
        self.proxmox = proxmox

    def get(self):
        if len(self.list) != 0:
            return self.list

        for node in self.proxmox.nodes.get():
            self.list.append(node["node"])
        return self.list


class VM:
    proxmox: any
    extra_info: dict
    config: dict
    vmid: str
    node: str
    fs_info: dict

    def __init__(self, vmid:str, node:str, proxmox: any):
        if ((vmid or node or proxmox) == False):
            raise ValueError("Invalid Argument")
        self.proxmox = proxmox
        self.node = node
        self.vmid = vmid
        self.config = {}
        self.fs_info = {}

    def get(self):
        logger.info(f"Get data for {self.vmid}")
        config = self.get_config()
        extra_info = self.get_extra_info()

        return {**config,**extra_info}

    def get_config(self):
        if self.config:
            return self.config

        self.config = self.proxmox.nodes(self.node).qemu(self.vmid).config.get()
        return self.config

    def has_agent(self):
        return self.get_config()['agent'] == '1'

    def get_fs_info(self):
        if self.has_agent() == False:
            return {}

        if self.fs_info:
            return self.fs_info

        try:
            self.fs_info = self.proxmox.get(f'nodes/{self.node}/qemu/{self.vmid}/agent/get-fsinfo')
        except ResourceException as e:
            logger.info(f"Can't get fs info for vm {self.vmid} on node {self.node}. Exception {e.content}")
        return self.fs_info

    def get_extra_info(self):
        fsinfo = self.get_fs_info()

        if fsinfo is not None:

            if 'result' not in fsinfo:
                return {}

            logger.debug(fsinfo)
            if 'error' in fsinfo['result']:
                error = fsinfo['result']['error']
                logger.error(f"error for vmid {self.vmid} class '{error['class']}' desc '{error['desc']}'")
                return {}

            for disk in fsinfo['result']:
                logger.debug(disk)
                if disk['type'] not in ['CDFS', 'UDF']:

                    if 'total-bytes' not in disk:
                        logger.debug(f"skipping {disk}")
                        continue

                    unused_bytes = disk['total-bytes'] -  disk['used-bytes']
                    extrainfo = {
                        'unused-bytes': unused_bytes,
                        'used-bytes': disk['used-bytes'],
                        'total-bytes': disk['total-bytes'],
                        'precent-remaining': unused_bytes / disk['total-bytes'] * 100,
                        'mountpoint': disk['mountpoint'],
                        'filesystem': disk['type']
                    }

                    return extrainfo
                else:
                    logger.debug(f"skipping {disk['mountpoint']} type {disk['type']}")
        return {}

class VMList:
    node: NodeList
    list: list
    proxmox: any

    def __init__(self, node: NodeList, proxmox: any):
        if len(node.get()) == 0:
            raise ValueError("Node list empty")
        self.node = node
        self.list = []
        self.proxmox = proxmox

    def get(self):
        if len(self.list) != 0:
            return self.list

        for node in self.node.get():
            for vm in self.proxmox.nodes(node).qemu.get():
                logger.debug(vm)
                vmid = int(vm["vmid"])
                status = vm['status']
                if status != "running":
                    logger.info(f"skipping {vmid} status {status}")
                    continue

                data = VM(vmid, node, self.proxmox).get()
                self.list.append(data)

        self.normalise()

        return self.list

    def normalise(self):
        keys = QemuConfig(self).get_keys()
        # Ensure all keys exist for each vm for consistency
        for vm in self.list:
            for key in keys:
                if key not in vm:
                    vm[key] = ""

    def keys(self):
        return QemuConfig(self).get_keys()


class QemuConfig:
    keys: list

    def __init__(self, vm: VMList):
        if len(vm.get()) == 0:
            raise ValueError("VM list empty")
        self.vm = vm
        self.keys = []

    def get_keys(self):
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
    def output(filename: str, data: dict, heading: list):
        try:
            with open(filename, "w") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=heading)
                writer.writeheader()
                for item in data:
                    writer.writerow(item)
        except IOError as e:
            logger.exception("I/O error", e)


def main():
    config = Config()
    prox = get_proxmox(config)
    node = NodeList(prox)
    vm = VMList(node, prox)
    data = vm.get()

    CSV.output("vmlist.csv", data, vm.keys())


main()

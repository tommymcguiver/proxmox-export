import csv
import logging
import os

from proxmoxer import ProxmoxAPI

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s %(levelname)s:%(name)s: %(message)s"
)


class Env:
    @staticmethod
    def get_or(value: str, default: str):
        if value in os.environ and os.environ[value] != "":
            return value
        return default

    @staticmethod
    def must_get(value: str, err: str):
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
                vmid = int(vm["vmid"])
                status = vm['status']
                if status != "running":
                    print(f"skipping {vmid} status {status}")
                    continue

                print(f"processing {vmid}")
                self.list.append(self.proxmox.nodes(node).qemu(vmid).config.get())

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
        except IOError:
            print("I/O error")


def main():
    config = Config()
    prox = get_proxmox(config)
    node = NodeList(prox)
    vm = VMList(node, prox)
    data = vm.get()

    CSV.output("vmlist.csv", data, vm.keys())


main()

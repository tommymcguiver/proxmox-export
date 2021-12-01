Purpose of this application is to query Proxmox to get virtual machine configuration data for export. The currently supported format is CSV.

Types of data returned:
- Number of cores
- Number and size of hard disks and bus types
- And much more ....

For the exact type of data see the proxmox API documentation

https://pve.proxmox.com/pve-docs/api-viewer/index.html#/nodes/{node}/qemu/{vmid}/config

# Run

```bash
source env/bin/activate

PROX_URL="192.168.1.2:8006" \
    PROX_USER="test@pve" \
    PROX_PASSWORD="super-secret-not-a-real-password" \
    python3 get_vm_data.py
```

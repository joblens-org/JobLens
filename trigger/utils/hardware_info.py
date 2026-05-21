#   Copyright 2026 - 2026 wzycc
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
from flask import Flask, jsonify
import subprocess
import json
import re
import threading
from pathlib import Path

app = Flask(__name__)

_hardware_cache = None
_cache_lock = threading.Lock()

def run_cmd(cmd, default=""):
    """安全执行shell命令"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        return result.stdout.strip() if result.returncode == 0 else default
    except:
        return default

def get_system_static():
    """获取系统/主板静态信息"""
    info = {}
    
    # 优先从 sysfs 读取 DMI 信息（无需root）
    dmi_path = Path("/sys/class/dmi/id")
    if dmi_path.exists():
        files = {
            "manufacturer": "sys_vendor",
            "product_name": "product_name",
            "product_version": "product_version", 
            "serial_number": "product_serial",
            "uuid": "product_uuid"
        }
        for key, filename in files.items():
            file_path = dmi_path / filename
            if file_path.exists():
                info[key] = file_path.read_text().strip()
    
    # 如果sysfs没有，尝试dmidecode（需要root）
    if not info.get("product_name"):
        dmi_output = run_cmd("dmidecode -t system 2>/dev/null", "")
        if dmi_output:
            for line in dmi_output.split('\n'):
                if ':' in line:
                    key, val = line.split(':', 1)
                    key = key.strip().lower().replace(' ', '_')
                    if 'manufacturer' in key:
                        info['manufacturer'] = val.strip()
                    elif 'product_name' in key:
                        info['product_name'] = val.strip()
                    elif 'serial_number' in key:
                        info['serial_number'] = val.strip()
    
    return info

def get_cpu_static():
    """获取CPU型号和规格"""
    info = {}
    
    # 使用 lscpu --json（现代Linux支持）
    lscpu_json = run_cmd("lscpu -J 2>/dev/null", "")
    if lscpu_json:
        try:
            data = json.loads(lscpu_json)
            fields = {item["field"].replace(":", "").strip(): item["data"] 
                     for item in data.get("lscpu", [])}
            
            info["model"] = fields.get("Model name", "Unknown")
            info["architecture"] = fields.get("Architecture", "Unknown")
            info["cores"] = int(fields.get("Core(s) per socket", 0)) * int(fields.get("Socket(s)", 1))
            info["threads"] = int(fields.get("CPU(s)", 0))
            info["max_frequency_mhz"] = fields.get("CPU max MHz", "Unknown")
            info["vendor"] = fields.get("Vendor ID", "Unknown")
        except:
            pass
    
    # Fallback: 解析 /proc/cpuinfo
    if not info.get("model"):
        with open("/proc/cpuinfo") as f:
            for line in f:
                if "model name" in line:
                    info["model"] = line.split(":")[1].strip()
                    break
    
    return info

def get_memory_static():
    """获取内存静态规格（使用dmidecode获取详细插槽信息，需root）"""
    info = {"total_gb": 0, "slots": []}
    
    # 获取总内存大小（从 /proc/meminfo，无需root）
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("MemTotal:"):
                kb = int(line.split()[1])
                info["total_gb"] = round(kb / (1024 * 1024), 2)
                break
    
    # 获取详细插槽信息（需要root权限）
    dmi_output = run_cmd("dmidecode -t memory 2>/dev/null", "")
    if dmi_output:
        slot = {}
        for line in dmi_output.split('\n'):
            line = line.strip()
            if line.startswith("Memory Device"):
                if slot and slot.get("size") and slot["size"] != "No Module Installed":
                    info["slots"].append(slot)
                slot = {}
            elif ':' in line:
                key, val = line.split(':', 1)
                key = key.strip().lower().replace(' ', '_')
                val = val.strip()
                
                if 'size' in key and 'mb' in val.lower():
                    # 处理 "16384 MB" 格式
                    try:
                        slot["size_gb"] = int(val.split()[0]) / 1024
                    except:
                        slot["size_gb"] = val
                elif 'type' in key and 'ddr' in val.lower():
                    slot["type"] = val
                elif 'speed' in key and 'mhz' in val.lower():
                    slot["speed_mhz"] = val.replace(" MHz", "")
                elif 'manufacturer' in key:
                    slot["manufacturer"] = val
                elif 'part_number' in key:
                    slot["part_number"] = val
    
    return info

def get_storage_static():
    """获取磁盘/存储设备型号和容量"""
    devices = []
    
    # 使用 lsblk 获取磁盘信息
    lsblk_json = run_cmd("lsblk -J -o NAME,MODEL,SIZE,TYPE,ROTA,SERIAL 2>/dev/null", "")
    if lsblk_json:
        try:
            data = json.loads(lsblk_json)
            for dev in data.get("blockdevices", []):
                if dev.get("type") == "disk":  # 只取物理磁盘，不取分区
                    size_str = dev.get("size", "0")
                    # 转换 human-readable 大小为 GB
                    size_gb = parse_size_to_gb(size_str)
                    
                    devices.append({
                        "name": dev.get("name"),
                        "model": dev.get("model") or "Unknown",
                        "size_gb": size_gb,
                        "type": "hdd" if dev.get("rota") else "ssd",
                        "serial": dev.get("serial", "Unknown")
                    })
        except:
            pass
    
    # Fallback: fdisk -l
    if not devices:
        fdisk = run_cmd("fdisk -l 2>/dev/null | grep 'Disk model'", "")
        for line in fdisk.split('\n'):
            if line:
                match = re.search(r'Disk model:\s*(.+)', line)
                if match:
                    devices.append({"model": match.group(1).strip()})
    
    return devices

def get_gpu_static():
    """获取GPU型号信息"""
    gpus = []
    
    # 1. 尝试 NVIDIA（nvidia-smi）
    nvidia = run_cmd("nvidia-smi --query-gpu=name,memory.total,pci.bus_id --format=csv,noheader 2>/dev/null", "")
    if nvidia:
        for line in nvidia.split('\n'):
            if line:
                parts = line.split(',')
                if len(parts) >= 2:
                    mem = parts[1].strip().replace(" MiB", "").replace(" MiB", "")
                    try:
                        mem_gb = int(mem) / 1024
                    except:
                        mem_gb = 0
                    gpus.append({
                        "vendor": "NVIDIA",
                        "model": parts[0].strip(),
                        "memory_gb": round(mem_gb, 2),
                        "bus_id": parts[2].strip() if len(parts) > 2 else "Unknown"
                    })
    
    # 2. 尝试 AMD ROCm（rocm-smi）
    if not gpus:
        rocm = run_cmd("rocm-smi --showproductname 2>/dev/null | grep 'GPU'", "")
        if rocm:
            for line in rocm.split('\n'):
                match = re.search(r'GPU\[\d+\]\s*:\s*([^\n]+)', line)
                if match:
                    gpus.append({"vendor": "AMD", "model": match.group(1).strip()})
    
    # 3. 通用方法：lspci 查找 VGA/3D/Display 控制器
    if not gpus:
        lspci = run_cmd("lspci | grep -iE '(vga|3d|display)'", "")
        for line in lspci.split('\n'):
            if line:
                # 格式示例：01:00.0 VGA compatible controller: NVIDIA Corporation GA102 [GeForce RTX 3090] (rev a1)
                match = re.search(r':\s*(.+?)(?:\s*\(|$)', line)
                if match:
                    model = match.group(1).strip()
                    vendor = "Unknown"
                    if "nvidia" in line.lower():
                        vendor = "NVIDIA"
                    elif "amd" in line.lower() or "ati" in line.lower():
                        vendor = "AMD"
                    elif "intel" in line.lower():
                        vendor = "Intel"
                    
                    gpus.append({
                        "vendor": vendor,
                        "model": model,
                        "raw_pci_info": line
                    })
    
    return gpus

def get_network_static():
    """获取网卡硬件信息"""
    interfaces = []
    
    # 从 /sys/class/net 获取信息
    net_path = Path("/sys/class/net")
    if net_path.exists():
        for iface in net_path.iterdir():
            if iface.name == "lo":  # 跳过回环
                continue
            
            iface_info = {"name": iface.name}
            
            # 读取 MAC 地址
            address_file = iface / "address"
            if address_file.exists():
                iface_info["mac"] = address_file.read_text().strip()
            
            # 读取速度（如果是物理网卡）
            speed_file = iface / "speed"
            if speed_file.exists():
                try:
                    speed = int(speed_file.read_text().strip())
                    iface_info["speed_mbps"] = speed if speed > 0 else "Unknown"
                except:
                    pass
            
            # 读取设备型号（通过 ethtool 或 driver）
            driver_link = iface / "device" / "driver"
            if driver_link.exists():
                driver = driver_link.resolve().name
                iface_info["driver"] = driver
            
            # 使用 ethtool 获取更详细信息（需root或权限）
            ethtool = run_cmd(f"ethtool -i {iface.name} 2>/dev/null", "")
            if ethtool:
                for line in ethtool.split('\n'):
                    if "bus-info" in line:
                        iface_info["bus_info"] = line.split(':')[1].strip()
                    elif "firmware-version" in line:
                        iface_info["firmware"] = line.split(':')[1].strip()
            
            interfaces.append(iface_info)
    
    return interfaces

def parse_size_to_gb(size_str):
    """将 lsblk 的 human-readable 大小转换为 GB 数值"""
    size_str = size_str.strip()
    if not size_str:
        return 0
    
    units = {'K': 1/1024/1024, 'M': 1/1024, 'G': 1, 'T': 1024, 'P': 1024*1024}
    match = re.match(r'^([\d.]+)\s*([KMGTPEkmgtpe]?)', size_str)
    if match:
        num, unit = match.groups()
        unit = unit.upper() if unit else 'B'
        try:
            return round(float(num) * units.get(unit, 1), 2)
        except:
            return 0
    return 0

def collect_static_hardware():
    """收集所有静态硬件信息"""
    return {
        "system": get_system_static(),
        "cpu": get_cpu_static(),
        "memory": get_memory_static(),
        "storage": get_storage_static(),
        "gpu": get_gpu_static(),
        "network": get_network_static(),
        "collected_at": subprocess.check_output(["date", "+%Y-%m-%d %H:%M:%S"]).decode().strip()
    }

def hardware_info():
    global _hardware_cache
    if not _hardware_cache:
        _hardware_cache = collect_static_hardware()
    return _hardware_cache


if __name__ == '__main__':
    print(json.dumps(hardware_info(), indent=2))
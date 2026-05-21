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
"""
集群信息工具：自动发现节点上的工作负载管理器集群信息。
"""
import re
import subprocess
import socket
from typing import List, TypedDict


class ClusterDiscoveryEntry(TypedDict):
    """集群发现条目"""
    type: str   # 调度器类型："condor"、"slurm"、"pbs" 等
    tag: str    # 集群内唯一 jobid 命名空间标识
    name: str   # 全局集群唯一标识名


def _run_cmd(cmd: list, timeout: int = 10) -> str:
    """安全执行命令，返回 stdout。"""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        return result.stdout.strip() if result.returncode == 0 else ""
    except Exception:
        return ""


def _get_host_identities() -> List[str]:
    """返回当前主机的可能标识字符串，用于匹配。"""
    identities = set()
    try:
        identities.add(socket.gethostname())
        identities.add(socket.getfqdn())
    except Exception:
        pass
    return list(identities)


def _get_condor_collector_host() -> str:
    """
    获取 HTCondor 集群的 COLLECTOR_HOST 作为集群名称。
    返回值去除端口号部分（若存在 :port 则剥离）。
    """
    stdout = _run_cmd(["condor_config_val", "COLLECTOR_HOST"])
    if not stdout:
        return ""
    # 去除端口号（如 "host.domain:9618" -> "host.domain"）
    host = stdout.rsplit(":", 1)[0].strip()
    return host


def _discover_htcondor() -> List[ClusterDiscoveryEntry]:
    """
    发现本地节点上的 HTCondor schedd。
    返回结构化列表，每条包含 type/tag/name。
    """
    entries: List[ClusterDiscoveryEntry] = []
    stdout = _run_cmd(["condor_status", "-schedd", "-af", "Name", "Machine"])
    if not stdout:
        return entries

    # 获取集群名称（全局唯一标识）
    cluster_name = _get_condor_collector_host()

    host_ids = _get_host_identities()
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        name, machine = parts[0], parts[1]
        entries.append({
            "type": "condor",
            "tag": name,
            "name": cluster_name,
        })
    return entries


def _discover_slurm() -> List[ClusterDiscoveryEntry]:
    """
    发现本地 Slurm 集群。
    返回结构化列表，tag 和 name 均使用 ClusterName。
    """
    entries: List[ClusterDiscoveryEntry] = []
    stdout = _run_cmd(["scontrol", "show", "config"])
    if not stdout:
        return entries

    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("ClusterName"):
            # 格式："ClusterName = <name>"
            parts = line.split("=")
            if len(parts) >= 2:
                cluster_name = parts[1].strip()
                if cluster_name:
                    entries.append({
                        "type": "slurm",
                        "tag": cluster_name,
                        "name": cluster_name,
                    })
            break
    return entries


def discover_cluster_discovery() -> List[ClusterDiscoveryEntry]:
    """
    自动发现当前计算节点的集群信息。

    检查 HTCondor 和 Slurm 工作负载管理器。如果节点同时接入两者
    （或同一管理器的多个实例），所有发现的条目都会返回。

    返回:
        结构化的 ClusterDiscoveryEntry 列表。
        如果没有检测到工作负载管理器或发现命令失败，返回空列表。
    """
    entries: List[ClusterDiscoveryEntry] = []

    # HTCondor：type="condor"，tag=schedd名称，name=COLLECTOR_HOST
    entries.extend(_discover_htcondor())

    # Slurm：type="slurm"，tag=name=ClusterName
    entries.extend(_discover_slurm())

    return entries


# 向后兼容：保留旧函数名，内部调用新函数
def discover_cluster_tags() -> List[str]:
    """
    [已弃用] 请使用 discover_cluster_discovery() 代替。
    返回扁平标签列表，用于向后兼容。
    """
    entries = discover_cluster_discovery()
    tags: List[str] = []
    for entry in entries:
        tags.append(entry["tag"])
    return tags


if __name__ == "__main__":
    import json
    print(json.dumps(discover_cluster_discovery(), ensure_ascii=False, indent=2))

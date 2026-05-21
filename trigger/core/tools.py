#! /usr/bin/python3
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
#! /usr/bin/python3
# FileName      : ink_tools.py
# Author        : HAN Xiao
# Email         : hanx@ihep.ac.cn
# Date          : Wed Jul 16 13:31:46 2025 CST
# Last modified : Wed Jul 16 23:08:08 2025 CST
# Description   :

import os
import json
import logging
import subprocess
import re
import shlex
from core.rpc_client import RPCClient
import yaml
from datetime import datetime, timezone
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)


# Mock 支持
import sys

# 检查是否启用 mock 模式
MOCK_MODE = os.environ.get('JOBLENS_MOCK_MODE', 'none').lower()
USE_MOCK_TOOLS = MOCK_MODE in ('tools', 'all')

# 如果需要 mock 工具，导入 mock 函数
if USE_MOCK_TOOLS:
    try:
        # 将 trigger-mock 目录添加到 Python 路径
        current_dir = os.path.dirname(os.path.abspath(__file__))
        mock_dir = os.path.join(current_dir, '..', 'trigger-mock')
        if os.path.exists(mock_dir) and mock_dir not in sys.path:
            sys.path.insert(0, mock_dir)
        
        from core.mock_tools import (
            mock_run,
            mock_systemd_status,
            mock_joblens_format_metrics,
            mock_job_opt,
            mock_add_condorjob,
            mock_restart_joblens,
            mock_get_joblens_version
        )
        print(f"已启用 tools mock 模式，MOCK_MODE={MOCK_MODE}")
    except ImportError as e:
        print(f"警告: 无法导入 mock 工具函数: {e}")
        USE_MOCK_TOOLS = False



def systemd_status(unit: str):
    if 'USE_MOCK_TOOLS' in globals() and USE_MOCK_TOOLS:
        # 使用 mock 版本
        return mock_systemd_status(unit)
    
    def _run(cmd: str) -> str:
        """Run shell cmd, raise on failure, return stripped stdout."""
        try:
            completed = subprocess.run(
                shlex.split(cmd),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            return completed.stdout.strip()
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.strip() if e.stderr else f"Command failed with exit code {e.returncode}"
            raise RuntimeError(error_msg) from e
    """Return (active_state, sub_state) for a systemd unit."""
    active = _run(f"/usr/bin/systemctl show --value --property ActiveState {unit}")
    sub    = _run(f"/usr/bin/systemctl show --value --property SubState {unit}")
    return active, sub



Config_path = os.path.realpath(__file__).split('/')[:-1]
Config_path = '/'.join(Config_path)

Config_path += '/../config/config.yaml'
real_config_path = os.environ.get('JOBLENS_CONFIG_PATH', Config_path)
bin_path = ''
if real_config_path == Config_path:
    bin_path = Config_path + '/../bin/JobLens'
else:
    bin_path = '/usr/local/bin/JobLens'
try:
    config = yaml.safe_load(open(real_config_path))
except FileNotFoundError:
    config = {}
def use_rpc_opt():
    lens_cfg = config.get('lens_config', {})
    socket_path = lens_cfg.get('rpc_socket_path', '')
    timeout = lens_cfg.get('rpc_timeout', 5)
    if not socket_path:
        return False
    r = RPCClient(socket_path, timeout)
    return 'JobRegistry/job_opt' in r.get_function_list()
def job_opt(data):
    if 'USE_MOCK_TOOLS' in globals() and USE_MOCK_TOOLS:
        # 使用 mock 版本
        return mock_job_opt(data)
    
    if use_rpc_opt():
        lens_cfg = config.get('lens_config', {})
        r = RPCClient(lens_cfg.get('rpc_socket_path', ''), lens_cfg.get('rpc_timeout', 5))
        logger.info("RPC call: JobRegistry/job_opt, opt=%s, type=%s, JobID=%s, PIDs=%s, Lens=%s",
                     data.get('opt'), data.get('type'), data.get('JobID'),
                     data.get('JobPIDs'), data.get('Lens'))
        ret = r.call('JobRegistry/job_opt', data)
        if isinstance(ret, dict):
            logger.info("RPC response: status=%s, msg=%s", ret.get('status'), ret.get('msg', ''))
        return ret
    else:
        job_adder_path = config.get('collectors_config', {}).get('job_adder_fifo', '')
        if not job_adder_path:
            raise RuntimeError("无法写入作业：未配置 job_adder_fifo 且 RPC 不可用")
        logger.info("FIFO fallback: writing to %s, opt=%s, type=%s, JobID=%s",
                     job_adder_path, data.get('opt'), data.get('type'), data.get('JobID'))
        with open(job_adder_path, 'w') as f:
            f.write(json.dumps(data))
            f.flush()
        return True
    
def run(cmd, check=True):
    if 'USE_MOCK_TOOLS' in globals() and USE_MOCK_TOOLS:
        # 使用 mock 版本
        return mock_run(cmd, check)
    
    cp = subprocess.run(cmd, shell=True, text=True,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if check and cp.returncode != 0:
        raise RuntimeError(f"命令失败: {cmd}\n{cp.stdout}")
    return cp.stdout.strip()

def find_pids_by_slot(slot: str):
    """返回 slot 对应 condor_starter 下的真实子进程 PID 列表（排除 starter 自身）。"""
    if not slot.startswith('slot'):
        raise ValueError("slot 名必须以 'slot' 开头")
    num = slot[4:]
    ps_out = run(f"/usr/bin/ps -ax -o pid,cmd --no-headers | "
                 f"/usr/bin/grep -E 'condor_starter.*[s]lot{num}'")
    if not ps_out:
        raise RuntimeError(f"未找到 {slot} 对应的 condor_starter")
    starter_pid = int(ps_out.split()[0])

    # 获取进程树中所有 PID
    tree = run(f"/usr/bin/pstree -pT {starter_pid}")
    all_pids = {int(x) for x in re.findall(r'\((\d+)\)', tree)}

    # 排除 condor_starter 自身，只返回实际作业子进程
    child_pids = sorted(all_pids - {starter_pid})

    if not child_pids:
        raise RuntimeError(f"slot {slot} 的 condor_starter 下未找到子进程")

    return child_pids


def add_condorjob(data):
    if 'USE_MOCK_TOOLS' in globals() and USE_MOCK_TOOLS:
        # 使用 mock 版本
        return mock_add_condorjob(data)
    
    """
    Add a condor job to the job registry.
    :param data: dict, job data
    """
    slot = data.get('slot')
    pids = find_pids_by_slot(slot)
    logger.info("Condor job process discovery: slot=%s, pids_count=%d", slot, len(pids))
    jobid = data.get('JobID')
    if not pids:
        raise RuntimeError(f"未找到 {slot} 对应的 condor_starter 进程")
    if not jobid:
        raise RuntimeError("JobID is required")
    
    # 构造sub_attr，优先使用请求中提供的，否则给默认值避免C++端抛异常
    sub_attr = data.get('sub_attr')
    if sub_attr is None:
        sub_attr = {'cluster_id': 0, 'proc_id': 0}
        logger.debug("Condor job sub_attr auto-filled: %s", sub_attr)
    job_data = {
        "opt": "add",
        "type": "job.condor",
        "JobID": jobid,
        "JobPIDs": pids,
        "Lens": data.get('Lens', ['proc_collector']),
        "sub_attr": sub_attr
    }
    logger.info("Condor job data constructed: JobID=%s, slot=%s, PIDs=%s, Lens=%s",
                 jobid, slot, pids[:5] if len(pids) > 5 else pids, job_data['Lens'])
    return job_opt(job_data)

def add_slurmjob(data):
    """
    Add a Slurm job to the job registry.
    :param data: dict, job data containing JobID
    """
    jobid = data.get('JobID')
    if not jobid:
        raise RuntimeError("JobID is required")
    
    # 获取 Slurm 作业对应的进程列表
    process_info_list = get_job_processes(str(jobid))
    if not process_info_list:
        raise RuntimeError(f"未找到 Slurm 作业 {jobid} 对应的进程")
    
    logger.info("Slurm job process discovery: JobID=%s, pids_count=%d", jobid, len(process_info_list))
    
    # 提取 pid 列表（process_info 格式: (node_id, pid, command)）
    pids = [int(info[1]) for info in process_info_list]
    
    # 构造sub_attr，优先使用请求中提供的，否则用JobID派生默认值避免C++端抛异常
    sub_attr = data.get('sub_attr')
    if sub_attr is None:
        sub_attr = {'job_id': jobid, 'step_id': 0}
        logger.debug("Slurm job sub_attr auto-filled: %s", sub_attr)
    
    job_data = {
        "opt": "add",
        "type": "job.slurm",
        "JobID": jobid,
        "JobPIDs": pids,
        "Lens": data.get('Lens', ['proc_collector']),
        "sub_attr": sub_attr
    }
    logger.info("Slurm job data constructed: JobID=%s, PIDs=%s, Lens=%s, sub_attr=%s",
                 jobid, pids[:5] if len(pids) > 5 else pids, job_data['Lens'], sub_attr)
    return job_opt(job_data)

def get_joblens_version():    
    """
    调用 joblens 可执行文件获取 -v 输出，
    返回解析后的 dict：
    {
      'version': '0.0.9',
      'build_id': 'a51ccf2',
      'build_time_utc': datetime(2025,12,6,12,46,47,tzinfo=timezone.utc),
      'build_time_local': datetime(2025,12,6,20,46,47,tzinfo=...),
    }
    如果解析失败抛出 ValueError。
    """
    if 'USE_MOCK_TOOLS' in globals() and USE_MOCK_TOOLS:
        # 使用 mock 版本
        return mock_get_joblens_version()
    
    # 1. 执行命令
    ver = run('JobLens -v')
    if not ver:
        ver = run(bin_path + ' -v')               # 例如返回 'v0.0.9 build-a51ccf2 (2025-12-06T12:46:47Z)'
        if not ver:
            raise ValueError("未获取到版本字符串")


    # 2. 正则解析
    m = re.search(r'v(\S+).*?build-(\S+).*?\(([\dT:Z-]+)\)', ver.strip())
    if not m:
        raise ValueError(f"版本字符串格式无法识别：{ver!r}")
    version, build_id, utc_str = m.groups()

    # 3. UTC 时间 -> datetime
    try:
        utc_dt = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception as e:
        raise ValueError("build 时间格式错误") from e

    # 4. 本地时区
    local_dt = utc_dt.astimezone()

    return {
        'version': version,
        'build_id': build_id,
        'build_time_utc': utc_dt.strftime('%Y-%m-%d %H:%M:%S %Z'),
        'build_time_local': local_dt.strftime('%Y-%m-%d %H:%M:%S %Z'),
    }


def joblens_format_metrics(jobs: dict) -> str:
    if 'USE_MOCK_TOOLS' in globals() and USE_MOCK_TOOLS:
        # 使用 mock 版本
        return mock_joblens_format_metrics(jobs)
    
    """
    把 Slurm 风格的嵌套 JSON 转成 Prometheus 文本协议。
    jobs: {"102": {"JobID": 102, "process_state": [{...}, ...]}, ...}
    返回: 多行字符串，可直接写入 /metrics 接口
    """
    lines = []

    # 统一 TYPE 只写一次，避免重复
    types_written = set()

    def write_type(metric: str, desc: str):
        if metric not in types_written:
            lines.append(f"# HELP {metric} {desc}")
            lines.append(f"# TYPE {metric} gauge")
            types_written.add(metric)

    for jobid_str, job_body in jobs.items():
        jobid = job_body["JobID"]
        for proc in job_body["process_state"]:
            # 构造标签串
            labels = (
                f'jobid="{jobid}",'
                f'pid="{proc["pid"]}",'
                f'name="{proc["name"]}"'
            )

            # 1. CPU
            write_type("job_cpu_usage_percent", "Per-process CPU usage percent")
            lines.append(f'job_cpu_usage_percent{{{labels}}} {proc["cpu_usage_percent"]}')

            # 2. 内存 RSS
            write_type("job_mem_rss_kb", "Per-process RSS memory in KB")
            lines.append(f'job_mem_rss_kb{{{labels}}} {proc["mem_rss_kb"]}')

            # 3. 内存占比
            write_type("job_mem_usage_percent", "Per-process memory usage percent")
            lines.append(f'job_mem_usage_percent{{{labels}}} {proc["mem_usage_percent"]}')

            # 4. 磁盘读总量
            write_type("job_io_read_bytes_total", "Total bytes read from disk")
            lines.append(f'job_io_read_bytes_total{{{labels}}} {proc["io_read_bytes_total"]}')

            # 5. 磁盘写总量
            write_type("job_io_write_bytes_total", "Total bytes written to disk")
            lines.append(f'job_io_write_bytes_total{{{labels}}} {proc["io_write_bytes_total"]}')

            # 6. 网络收总量
            write_type("job_net_recv_bytes_total", "Total bytes received via network")
            lines.append(f'job_net_recv_bytes_total{{{labels}}} {proc["net_recv_bytes_total"]}')

            # 7. 网络发总量
            write_type("job_net_sent_bytes_total", "Total bytes sent via network")
            lines.append(f'job_net_sent_bytes_total{{{labels}}} {proc["net_sent_bytes_total"]}')

            # 8. 线程数
            write_type("job_threads_count", "Number of threads")
            lines.append(f'job_threads_count{{{labels}}} {proc["threads_cnt"]}')

            lines.append("")  # 不同样本间空行分隔

    return "\n".join(lines)

def restart_joblens():
    try:
        active, sub = systemd_status('joblens')
    except RuntimeError as e:
        print("Get Joblens status error")
        return
    healthy = (active == "active") and (sub == "running")
    if healthy == False:
        print('JobLens 没有在运行，已返回')
        return
    try:
        print('正在重启JobLens')
        run('/usr/bin/systemctl restart joblens')
    except Exception as e:
        print('JobLens重启失败, 原因:', e)

def run_command(cmd: str) -> Optional[str]:
    """执行 shell 命令并返回输出"""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True, 
            timeout=30
        )
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception as e:
        print(f"命令执行错误: {e}")
        return None


def get_job_nodes(job_id: str) -> List[str]:
    """
    功能1: 给定 JobID 返回对应的 NodeID 列表
    
    实现逻辑:
    1. 优先使用 squeue (适用于运行中的作业)
    2. 回退到 scontrol (适用于已完成/运行中的作业)
    3. 解析 NodeList 格式 (处理 node[01-05],node07 这种压缩格式)
    """
    # 方法1: 使用 squeue 查询运行中作业的节点 (最轻量)
    cmd = f"squeue -j {job_id} -h -o %N"
    output = run_command(cmd)
    
    if output:
        return _expand_nodelist(output)
    
    # 方法2: 使用 scontrol 查询 (适用于已完成作业)
    cmd = f"scontrol show job {job_id}"
    output = run_command(cmd)
    
    if output:
        # 从多行输出中提取 NodeList=xxx
        match = re.search(r'NodeList=(\S+)', output)
        if match:
            return _expand_nodelist(match.group(1))
    
    return []


def _expand_nodelist(nodelist_str: str) -> List[str]:
    """
    展开 Slurm 的节点列表格式
    例如: "node[01-03],node05" -> ["node01", "node02", "node03", "node05"]
    """
    if not nodelist_str or nodelist_str == "(null)":
        return []
    
    nodes = []
    # 匹配模式: prefix[start-end] 或 prefix[start,end,start-end]
    pattern = r'([^,\[]+)\[([^\]]+)\]'
    
    def expand_range(prefix, range_str):
        """展开单个范围字符串"""
        result = []
        for part in range_str.split(','):
            if '-' in part:
                start, end = part.split('-')
                width = len(start)  # 保持前导零
                for i in range(int(start), int(end) + 1):
                    result.append(f"{prefix}{str(i).zfill(width)}")
            else:
                result.append(f"{prefix}{part}")
        return result
    
    # 处理压缩格式
    pos = 0
    while pos < len(nodelist_str):
        match = re.search(pattern, nodelist_str[pos:])
        if not match:
            # 普通节点名，直接分割
            remaining = nodelist_str[pos:].split(',')
            nodes.extend([n.strip() for n in remaining if n.strip()])
            break
        
        # 添加匹配前的普通节点
        if match.start() > 0:
            prefix_nodes = nodelist_str[pos:pos+match.start()].split(',')
            nodes.extend([n.strip() for n in prefix_nodes if n.strip()])
        
        # 展开范围
        prefix, ranges = match.groups()
        nodes.extend(expand_range(prefix, ranges))
        pos += match.end()
    
    return list(set(nodes))  # 去重


def get_job_processes(job_id: str) -> List[Tuple[str, str, str]]:
    """
    功能2: 给定 JobID 返回该作业下的进程信息
    
    返回: [(node_id, pid, command), ...]
    
    实现逻辑 (按优先级):
    1. sstat: 适用于运行中的作业，直接查询 PID (需要权限)
    2. scontrol listpids: Slurm 内置的 PID 查询 (部分版本支持)
    """
    pids = []
    
    # 方法1: 使用 sstat 查询 (仅运行中作业，且需要适当权限)
    cmd = f"sstat -j {job_id}.0 --format=PID -h -n"
    output = run_command(cmd)
    if output:
        for line in output.split('\n'):
            pid = line.strip()
            if pid and pid.isdigit():
                pids.append(("sstat-query", pid, "unknown"))
        if pids:
            return pids
    
    # 方法2: 使用 scontrol listpids (如果 Slurm 配置支持)
    cmd = f"scontrol listpids {job_id}"
    output = run_command(cmd)
    if output and "PID" in output:
        for line in output.split('\n')[1:]:  # 跳过表头
            cols = line.split()
            if len(cols) >= 2 and cols[0].isdigit():
                pid, step_id = cols[0], cols[1]
                pids.append(("scontrol-query", pid, f"step:{step_id}"))
        if pids:
            return pids
    
    # 方法3: 到计算节点上查找 (通用方法)
    nodes = get_job_nodes(job_id)
    if not nodes:
        return []
    
    for node in nodes:
        # 查找 slurmstepd 进程 (它是所有作业进程的父进程/管理进程)
        # 然后通过 /proc/<pid>/status 的 PPid 找到其子进程
        cmd = f"ps -eo pid,ppid,comm,args | grep slurmstepd"
        output = run_command(cmd)
        
        if not output:
            continue
            
        # 解析 slurmstepd 行，找到对应 job_id 的进程
        slurm_pids = []
        for line in output.split('\n'):
            if job_id in line and 'grep' not in line:
                parts = line.split()
                if len(parts) >= 4:
                    pid, ppid, comm = parts[0], parts[1], parts[2]
                    # slurmstepd 命令行通常包含 job_id 和 step_id
                    slurm_pids.append(pid)
                    pids.append((node, pid, "slurmstepd"))
        
        # 查找这些 slurmstepd 的子进程 (实际的作业进程)
        for slurm_pid in slurm_pids:
            # 查找 PPID 匹配 slurmstepd 的进程
            cmd = f"ps --ppid {slurm_pid} -o pid,comm"
            child_output = run_command(cmd)
            if child_output:
                for line in child_output.split('\n')[1:]:  # 跳过标题
                    parts = line.split()
                    if parts:
                        child_pid = parts[0]
                        cmd_name = parts[1] if len(parts) > 1 else "unknown"
                        pids.append((node, child_pid, cmd_name))
    
    return pids


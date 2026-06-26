#!/usr/bin/env python3
"""
JobLens 集成测试 — 预设驱动的编排器 (纯 Python 实现)

用法:
    ./run_preset.py <name>             精确匹配预设
    ./run_preset.py --list             列出所有预设
    ./run_preset.py --match <glob>     glob 匹配预设
    ./run_preset.py validate <file>    YAML schema 校验

环境变量:
    DRY_RUN=1    — 仅生成 env/Vagrantfile, 不执行 vagrant 命令
    KEEP_VMS=1   — 测试后保留 VM (跳过 vagrant destroy)
"""

import argparse
import glob as py_glob
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import requests
import yaml

# ── 路径常量 ──────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
PRESETS_DIR = SCRIPT_DIR / "presets"
RUNTIME_DIR = SCRIPT_DIR / ".runtime"
VAGRANT_TEMPLATE = SCRIPT_DIR / "Vagrantfile.template"
VAGRANTFILE = SCRIPT_DIR / "Vagrantfile"


# ══════════════════════════════════════════════════════════════════════════
# CLI 参数解析
# ══════════════════════════════════════════════════════════════════════════

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="JobLens 集成测试预设编排器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    ./run_preset.py alm9-default              # 运行 alm9-default 预设
    ./run_preset.py --list                    # 列出所有可用预设
    ./run_preset.py --match "alm9-*"          # glob 匹配预设
    ./run_preset.py validate presets/alm9-default.yaml  # schema 校验
    ./run_preset.py alm9-default --skip-vagrant-up --skip-vagrant-destroy
        """,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("name", nargs="?", help="精确 preset 名称 (不含 .yaml)")
    group.add_argument("--list", "-l", action="store_true", help="列出所有可用预设")
    group.add_argument("--match", "-m", metavar="PATTERN", help="glob 匹配预设 (如 alm9-*)")

    parser.add_argument("--skip-vagrant-up", action="store_true",
                        help="跳过 vagrant up (VM 已由外部启动)")
    parser.add_argument("--skip-vagrant-destroy", action="store_true",
                        help="跳过 vagrant destroy (由外部清理 VM)")
    parser.add_argument("--keep-vms", action="store_true",
                        help="等价于 --skip-vagrant-destroy")
    parser.add_argument("extra", nargs="*", help=argparse.SUPPRESS)
    return parser


# ══════════════════════════════════════════════════════════════════════════
# 工具函数
# ══════════════════════════════════════════════════════════════════════════

def fatal(msg: str) -> None:
    """打印 FATAL 错误并退出。"""
    print(f"FATAL: {msg}", file=sys.stderr)
    sys.exit(1)


def run_vagrant(cmd: str, capture: bool = False, stdin_data: bytes | None = None) -> subprocess.CompletedProcess:
    """在 tests/integration/ 目录下运行 vagrant 命令。"""
    proc = subprocess.run(
        ["vagrant"] + cmd.split(),
        cwd=SCRIPT_DIR,
        capture_output=capture,
        input=stdin_data,
        text=(stdin_data is None),  # 仅当无二进制输入时用 text 模式
    )
    if not capture and stdin_data is not None:
        # 重新运行以正确传递二进制 stdin
        proc = subprocess.run(
            ["vagrant"] + cmd.split(),
            cwd=SCRIPT_DIR,
            input=stdin_data,
            capture_output=capture,
        )
    return proc


def vagrant_ssh(node: str, command: str, check: bool = True) -> subprocess.CompletedProcess:
    """在指定节点上通过 vagrant ssh 执行命令, 失败时打印完整输出。"""
    proc = subprocess.run(
        ["vagrant", "ssh", node, "-c", command],
        cwd=SCRIPT_DIR,
        capture_output=True,
        text=True,
    )
    if check and proc.returncode != 0:
        print(f"\n--- vagrant ssh {node} 失败 (exit {proc.returncode}) ---")
        if proc.stdout.strip():
            print(f"[stdout]\n{proc.stdout}")
        if proc.stderr.strip():
            # 过滤掉 Vagrant 自身的无关警告 (如 libvirt_ip_command)
            stderr_filtered = "\n".join(
                line for line in proc.stderr.splitlines()
                if "libvirt_ip_command" not in line
            )
            if stderr_filtered.strip():
                print(f"[stderr]\n{stderr_filtered}")
        print("---")
    if check:
        proc.check_returncode()
    return proc


def vagrant_cat(node: str, path: str) -> bytes:
    """从 VM 节点读取文件内容, 失败时打印错误。"""
    proc = subprocess.run(
        ["vagrant", "ssh", node, "-c", f"sudo cat {path}"],
        cwd=SCRIPT_DIR,
        capture_output=True,
        text=False,
    )
    if proc.returncode != 0:
        print(f"\n--- vagrant cat {node}:{path} 失败 (exit {proc.returncode}) ---")
        if proc.stderr:
            stderr_str = proc.stderr.decode(errors="replace")
            print(f"[stderr]\n{stderr_str}")
        print("---")
        proc.check_returncode()
    return proc.stdout


def vagrant_write(node: str, path: str, data: bytes) -> None:
    """通过 sudo tee 将数据写入 VM 节点文件, 失败时打印错误。"""
    proc = subprocess.run(
        ["vagrant", "ssh", node, "-c", f"sudo tee {path} > /dev/null"],
        cwd=SCRIPT_DIR,
        input=data,
        capture_output=True,
    )
    if proc.returncode != 0:
        print(f"\n--- vagrant write {node}:{path} 失败 (exit {proc.returncode}) ---")
        if proc.stderr:
            stderr_str = proc.stderr.decode(errors="replace")
            print(f"[stderr]\n{stderr_str}")
        print("---")
        proc.check_returncode()


# ══════════════════════════════════════════════════════════════════════════
# 预设发现与校验
# ══════════════════════════════════════════════════════════════════════════

VALID_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]*$")


def validate_name(name: str) -> bool:
    """校验 preset 名称合法性: 小写字母开头, 仅含 [a-z0-9_-]."""
    if re.search(r"[/\s]", name) or ".." in name:
        print(f"❌ 错误: 无效的 preset 名称 '{name}' — 包含禁止字符 (/、..、空格)", file=sys.stderr)
        return False
    if not VALID_NAME_RE.match(name):
        print(f"❌ 错误: 无效的 preset 名称 '{name}'"
              f" — 必须以小写字母开头, 仅含小写字母/数字/连字符/下划线", file=sys.stderr)
        return False
    return True


def find_presets(name: str | None = None, pattern: str | None = None) -> list[Path]:
    """
    发现预设文件, 支持三种模式:
    - name: 精确匹配 presets/<name>.yaml
    - pattern: glob 匹配 presets/<pattern>.yaml
    - 无参数: 列出所有 presets/*.yaml
    """
    if not PRESETS_DIR.is_dir():
        print(f"❌ 错误: presets 目录不存在: {PRESETS_DIR}", file=sys.stderr)
        return []

    if name:
        if not validate_name(name):
            return []
        preset_file = PRESETS_DIR / f"{name}.yaml"
        if preset_file.is_file():
            return [preset_file]
        print(f"❌ 错误: 未找到预设 '{name}' (查找路径: {preset_file})", file=sys.stderr)
        # 列出可用预设
        available = sorted(p.stem for p in PRESETS_DIR.glob("*.yaml"))
        if available:
            print("", file=sys.stderr)
            print("可用预设:", file=sys.stderr)
            for a in available:
                print(f"  - {a}", file=sys.stderr)
        return []

    if pattern:
        if re.search(r"[/\s]", pattern) or ".." in pattern:
            print(f"❌ 错误: 无效的 match 模式 '{pattern}' — 包含禁止字符 (/、..、空格)", file=sys.stderr)
            return []
        matches = sorted(PRESETS_DIR.glob(f"{pattern}.yaml"))
        if not matches:
            print(f"❌ 错误: glob 模式 '{pattern}' 没有匹配到任何预设", file=sys.stderr)
            print("提示: 使用 --list 查看所有可用预设", file=sys.stderr)
        return matches

    # 无参数 → 列出所有
    return sorted(PRESETS_DIR.glob("*.yaml"))


def list_presets():
    """列出所有可用预设文件路径。"""
    presets = find_presets()
    if not presets:
        print("⚠ 没有找到任何预设文件", file=sys.stderr)
        sys.exit(1)
    for p in presets:
        print(p)


def validate_preset(yaml_path: Path) -> bool:
    """
    YAML schema 校验 — 检查预设文件是否满足所有必填字段和格式要求。
    返回 True 表示通过校验。
    """
    if not yaml_path.is_file():
        print(f"ERROR: file not found: {yaml_path}", file=sys.stderr)
        return False

    try:
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        print(f"ERROR: failed to parse YAML file: {yaml_path}", file=sys.stderr)
        print(str(e), file=sys.stderr)
        return False

    if not isinstance(data, dict):
        print("ERROR: YAML 根节点必须是 mapping", file=sys.stderr)
        return False

    errors: list[str] = []

    # ── 1. name ──
    if "name" not in data:
        errors.append("name: missing required field")
    elif not isinstance(data["name"], str) or not data["name"].strip():
        errors.append("name: must be a non-empty string")
    elif not VALID_NAME_RE.match(data["name"]):
        errors.append(f"name: invalid name '{data['name']}' (allowed: ^[a-z][a-z0-9_-]*$)")

    # ── 2. topology ──
    if "topology" not in data:
        errors.append("topology: missing required field")
    elif not isinstance(data["topology"], dict):
        errors.append(f"topology: expected map, got {type(data['topology']).__name__}")
    else:
        topo = data["topology"]
        for required_key in ("controller", "worker"):
            if required_key not in topo:
                errors.append(f"topology: missing required key '{required_key}'")
            elif not isinstance(topo[required_key], dict):
                errors.append(f"topology.{required_key}: expected map, got {type(topo[required_key]).__name__}")
        for node_key in topo:
            if not re.match(r"^[a-z0-9_-]+$", node_key):
                errors.append(f"topology.{node_key}: invalid node key (allowed: [a-z0-9_-])")

    # ── 3. network.subnet ──
    if "network" not in data:
        errors.append("network: missing required field")
    elif not isinstance(data["network"], dict):
        errors.append(f"network: expected map, got {type(data['network']).__name__}")
    elif "subnet" not in data["network"]:
        errors.append("network.subnet: missing required field")
    else:
        subnet = data["network"]["subnet"]
        if not isinstance(subnet, str):
            errors.append(f"network.subnet: expected string, got {type(subnet).__name__}")
        else:
            cidr_pat = r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})/(\d{1,2})$"
            m = re.match(cidr_pat, subnet)
            if not m:
                errors.append(f"network.subnet: invalid CIDR format '{subnet}'")
            else:
                octets = [int(m.group(i)) for i in range(1, 5)]
                mask = int(m.group(5))
                for i, o in enumerate(octets, 1):
                    if o < 0 or o > 255:
                        errors.append(f"network.subnet: octet {i} out of range (0-255): {o}")
                        break
                else:
                    if mask < 0 or mask > 32:
                        errors.append(f"network.subnet: prefix length out of range (0-32): {mask}")

    # ── 4. schedulers ──
    if "schedulers" not in data:
        errors.append("schedulers: missing required field")
    elif not isinstance(data["schedulers"], dict):
        errors.append(f"schedulers: expected map, got {type(data['schedulers']).__name__}")
    else:
        sched = data["schedulers"]
        for sname in ("htcondor", "slurm"):
            if sname not in sched:
                errors.append(f"schedulers.{sname}: missing required field")
            elif not isinstance(sched[sname], dict):
                errors.append(f"schedulers.{sname}: expected map, got {type(sched[sname]).__name__}")
            elif "enabled" not in sched[sname]:
                errors.append(f"schedulers.{sname}.enabled: missing required field")
            elif not isinstance(sched[sname]["enabled"], bool):
                errors.append(
                    f"schedulers.{sname}.enabled: expected boolean, got {type(sched[sname]['enabled']).__name__}"
                    f" '{sched[sname]['enabled']}'"
                )

    # ── 5. joblens ──
    if "joblens" not in data:
        errors.append("joblens: missing required field")
    elif not isinstance(data["joblens"], dict):
        errors.append(f"joblens: expected map, got {type(data['joblens']).__name__}")
    else:
        jl = data["joblens"]
        for cfg_key in ("core_config", "trigger_config"):
            if cfg_key not in jl:
                errors.append(f"joblens.{cfg_key}: missing required field")
            elif not isinstance(jl[cfg_key], str) or not jl[cfg_key].strip():
                errors.append(f"joblens.{cfg_key}: must be a non-empty string file path")

    # ── 6. tests.pytest_files ──
    if "tests" not in data:
        errors.append("tests: missing required field")
    elif not isinstance(data["tests"], dict):
        errors.append(f"tests: expected map, got {type(data['tests']).__name__}")
    elif "pytest_files" not in data["tests"]:
        errors.append("tests.pytest_files: missing required field")
    else:
        pf = data["tests"]["pytest_files"]
        if not isinstance(pf, list):
            errors.append(f"tests.pytest_files: expected list, got {type(pf).__name__}")
        elif len(pf) == 0:
            errors.append("tests.pytest_files: must be non-empty list")
        else:
            for i, item in enumerate(pf):
                if not isinstance(item, str):
                    errors.append(f"tests.pytest_files[{i}]: expected string, got {type(item).__name__}")

    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        return False
    return True


# ══════════════════════════════════════════════════════════════════════════
# 环境准备 — 从 YAML 提取参数, 生成 preset_env.json
# ══════════════════════════════════════════════════════════════════════════

def extract_preset_env(preset_data: dict) -> dict:
    """从预设 YAML 解析后的数据中提取环境变量和运行时配置。"""
    preset_name = preset_data["name"]
    topo = preset_data["topology"]
    sched = preset_data["schedulers"]
    jl = preset_data["joblens"]
    tests = preset_data["tests"]

    # 构建节点列表 (按 key 排序以保证一致性)
    nodes = []
    node_names = []
    for node_key in sorted(topo.keys()):
        node = topo[node_key]
        nodes.append({
            "host": node.get("hostname", node_key),
            "ip": node["ip"],
            "cpus": node.get("cpus", 1),
        })
        node_names.append(node_key)

    # common.sh 用的 nodes-json (host + ip)
    nodes_json_common = json.dumps([{"host": n["host"], "ip": n["ip"]} for n in nodes])
    # slurm/controller.sh 用的 nodes-json (host + ip + cpus)
    # ⚠ 仅包含 worker/compute 节点 — controller 节点只运行 slurmctld，
    #    不应被列为 Slurm compute node，否则 sinfo 会显示 "unk*" 状态
    slurm_nodes = [n for n in nodes if n["host"] != "controller"]
    nodes_json_slurm = json.dumps(slurm_nodes)

    ctrl = topo["controller"]
    wrk = topo["worker"]
    ctrl_ip = ctrl["ip"]
    wrk_ip = wrk["ip"]
    trigger_url = f"http://{wrk_ip}:7592"

    # 构建环境变量导出表
    env_exports = {
        "PRESET_NAME": preset_name,
        "JOBLENS_NODES": ",".join(node_names),
        "CONTROLLER_IP": ctrl_ip,
        "WORKER_IP": wrk_ip,
        "TRIGGER_URL": trigger_url,
        "NODES_JSON_COMMON": nodes_json_common,
        "NODES_JSON_SLURM": nodes_json_slurm,
        "HTCONDOR_ENABLED": str(sched.get("htcondor", {}).get("enabled", False)).lower(),
        "CONDOR_REPO_URL": sched.get("htcondor", {}).get("repo_rpm_url", ""),
        "SLURM_ENABLED": str(sched.get("slurm", {}).get("enabled", False)).lower(),
        "UNIFIED_RPM_PATTERN": jl.get("rpm_path", ""),
        "CORE_CONFIG_PATH": str(SCRIPT_DIR / jl["core_config"]),
        "TRIGGER_CONFIG_PATH": str(SCRIPT_DIR / jl["trigger_config"]),
        "PYTEST_FILES": " ".join(tests.get("pytest_files", [])),
        "PYTEST_ARGS": tests.get("pytest_args", ""),
        "RUNTIME_DIR": str(RUNTIME_DIR),
    }

    # 添加每个节点的 Vagrantfile 环境变量
    for n in nodes:
        name_up = n["host"].upper()
        nd = topo[n["host"]]
        env_exports[f"{name_up}_BOX"] = nd["box"]
        env_exports[f"{name_up}_CPUS"] = str(nd["cpus"])
        env_exports[f"{name_up}_MEMORY"] = str(nd["memory"])
        env_exports[f"{name_up}_DISK"] = str(nd["disk"])
        env_exports[f"{name_up}_IP"] = n["ip"]

    return {
        "env_exports": env_exports,
        "preset_env_json": {
            "preset_name": preset_name,
            "controller": {"host": "controller", "ip": ctrl_ip},
            "worker": {"host": "worker", "ip": wrk_ip},
            "nodes": nodes,
            "trigger_url": trigger_url,
            "schedulers": sched,
        },
    }


def prepare_environment(preset_file: Path) -> dict:
    """Phase 2: 解析 YAML, 生成 preset_env.json, 导出环境变量。"""
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

    with open(preset_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    extracted = extract_preset_env(data)

    # 写入 preset_env.json
    env_file = RUNTIME_DIR / "preset_env.json"
    with open(env_file, "w", encoding="utf-8") as f:
        json.dump(extracted["preset_env_json"], f, indent=2)

    # 设置环境变量到当前进程
    for key, val in extracted["env_exports"].items():
        os.environ[key] = val

    print(f"✓ preset_env.json → {env_file}")
    print(f"✓ 节点: {os.environ['JOBLENS_NODES']}")
    print(f"✓ 控制器 IP: {os.environ['CONTROLLER_IP']}")
    print(f"✓ Worker IP: {os.environ['WORKER_IP']}")
    print(f"✓ Trigger URL: {os.environ['TRIGGER_URL']}")

    return extracted


def generate_vagrantfile():
    """Phase 3: 从模板生成 Vagrantfile。"""
    if not VAGRANT_TEMPLATE.is_file():
        fatal(f"Vagrantfile 模板不存在: {VAGRANT_TEMPLATE}")

    # 模板使用 Ruby ENV.fetch 从环境变量读取, 直接复制即可
    subprocess.run(["cp", str(VAGRANT_TEMPLATE), str(VAGRANTFILE)], check=True)
    print(f"✓ Vagrantfile: {VAGRANTFILE}")


# ══════════════════════════════════════════════════════════════════════════
# Demo Job 预检
# ══════════════════════════════════════════════════════════════════════════

def poll_job_discovery(worker_ip: str, subtype: str, timeout: int = 30, interval: int = 2) -> str | None:
    """
    轮询 JobLens API 直到发现指定 subtype 的作业。
    返回 job_id 或 None (超时)。
    """
    api_url = f"http://{worker_ip}:7592/joblens/jobs"
    last_error: str | None = None
    last_response: str | None = None
    attempt = 0
    deadline = time.time() + timeout
    while time.time() < deadline:
        attempt += 1
        try:
            resp = requests.get(api_url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                jobs = data.get("jobs", [])
                job_count = data.get("job_count", len(jobs))
                # 每 3 次尝试输出一次状态
                if attempt % 3 == 1:
                    subtypes_found = [j.get("subtype", "?") for j in jobs]
                    print(f"    [{subtype} 轮询 #{attempt}] job_count={job_count}, subtypes={subtypes_found}")
                for job in jobs:
                    if subtype in job.get("subtype", "").lower():
                        return job.get("job_id", "?")
                last_response = f"HTTP {resp.status_code}, job_count={job_count}, jobs={len(jobs)}"
            else:
                last_response = f"HTTP {resp.status_code}: {resp.text[:200]}"
                print(f"    [{subtype} 轮询 #{attempt}] 非 200 响应: {last_response}")
        except requests.ConnectionError as e:
            last_error = f"ConnectionError: {e}"
            if attempt == 1:
                print(f"    [{subtype} 轮询 #{attempt}] 无法连接 {api_url}: {e}")
        except requests.Timeout as e:
            last_error = f"Timeout: {e}"
            if attempt <= 2:
                print(f"    [{subtype} 轮询 #{attempt}] 请求超时 {api_url}: {e}")
        except requests.RequestException as e:
            last_error = f"RequestException: {e}"
            if attempt <= 2:
                print(f"    [{subtype} 轮询 #{attempt}] 请求异常: {e}")
        except json.JSONDecodeError as e:
            last_error = f"JSONDecodeError: {e}"
            print(f"    [{subtype} 轮询 #{attempt}] JSON 解析失败: {e}")
        time.sleep(interval)

    # 超时后输出诊断汇总
    print(f"    [{subtype}] 轮询超时 ({timeout}s, {attempt} 次尝试)")
    if last_error:
        print(f"    [{subtype}] 最后一次异常: {last_error}")
    if last_response:
        print(f"    [{subtype}] 最后一次可解析响应: {last_response}")
    return None


def _diagnose_scheduler(htcondor_enabled: bool, slurm_enabled: bool) -> None:
    """Job 未发现时的调度器诊断 — 输出 condor_q / condor_status / sinfo / scontrol / 各服务日志。"""
    print("  === 调度器诊断 ===", flush=True)

    # Fast-fail: 快速检查 VM 是否仍在运行, 避免后续 vagrant ssh 命令逐个超时
    try:
        status_proc = subprocess.run(
            ["vagrant", "status"], cwd=SCRIPT_DIR,
            capture_output=True, text=True, timeout=10,
        )
        if "running" not in status_proc.stdout:
            print("  ℹ VM 不在运行状态, 跳过调度器诊断", flush=True)
            print(f"  vagrant status:\n{status_proc.stdout.strip()[:500]}", flush=True)
            return
    except (subprocess.TimeoutExpired, subprocess.SubprocessError, OSError) as e:
        print(f"  ℹ 无法获取 VM 状态 ({e}), 跳过调度器诊断", flush=True)
        return

    if htcondor_enabled:
        print("  --- HTCondor 守护进程状态 (controller) ---", flush=True)
        try:
            proc = subprocess.run(
                ["vagrant", "ssh", "controller", "-c",
                 "echo '=== systemctl is-active condor ==='; systemctl is-active condor 2>&1 || true; "
                 "echo '=== condor_q -all ==='; condor_q -all 2>&1 || true; "
                 "echo '=== condor_q -better ==='; condor_q -better-analyze 2>&1 || true"],
                cwd=SCRIPT_DIR, capture_output=True, text=True, timeout=20,
            )
            if proc.stdout.strip():
                print(proc.stdout, flush=True)
            else:
                print("  (空输出)", flush=True)
        except Exception as e:
            print(f"  condor 状态查询失败: {e}", flush=True)

        print("  --- HTCondor 节点状态 (controller) ---", flush=True)
        try:
            proc = subprocess.run(
                ["vagrant", "ssh", "controller", "-c",
                 "condor_status -any 2>&1 || true"],
                cwd=SCRIPT_DIR, capture_output=True, text=True, timeout=15,
            )
            if proc.stdout.strip():
                print(proc.stdout, flush=True)
            else:
                print("  (空输出)", flush=True)
        except Exception as e:
            print(f"  condor_status 失败: {e}", flush=True)

        print("  --- HTCondor 日志 (controller, 最后 10 行) ---", flush=True)
        try:
            proc = subprocess.run(
                ["vagrant", "ssh", "controller", "-c",
                 "sudo journalctl -u condor --no-pager -n 10 2>&1 || true"],
                cwd=SCRIPT_DIR, capture_output=True, text=True, timeout=15,
            )
            if proc.stdout.strip():
                print(proc.stdout, flush=True)
            else:
                print("  (空输出)", flush=True)
        except Exception as e:
            print(f"  condor journalctl 失败: {e}", flush=True)

    if slurm_enabled:
        print("  --- Slurm 分区/节点状态 (controller) ---", flush=True)
        try:
            proc = subprocess.run(
                ["vagrant", "ssh", "controller", "-c",
                 "echo '=== sinfo ==='; sinfo 2>&1 || true; "
                 "echo '=== scontrol show node ==='; scontrol show node 2>&1 | head -50 || true"],
                cwd=SCRIPT_DIR, capture_output=True, text=True, timeout=15,
            )
            if proc.stdout.strip():
                print(proc.stdout, flush=True)
            else:
                print("  (sinfo/scontrol 空输出)", flush=True)
        except Exception as e:
            print(f"  sinfo/scontrol 失败: {e}", flush=True)

        print("  --- Slurm 作业队列 (controller) ---", flush=True)
        try:
            proc = subprocess.run(
                ["vagrant", "ssh", "controller", "-c",
                 "squeue 2>&1 || true"],
                cwd=SCRIPT_DIR, capture_output=True, text=True, timeout=15,
            )
            if proc.stdout.strip():
                print(proc.stdout, flush=True)
            else:
                print("  (squeue 空输出 — 无等待或运行中的作业)", flush=True)
        except Exception as e:
            print(f"  squeue 失败: {e}", flush=True)

        print("  --- slurmctld 日志 (controller, 最后 20 行) ---", flush=True)
        try:
            proc = subprocess.run(
                ["vagrant", "ssh", "controller", "-c",
                 "sudo journalctl -u slurmctld --no-pager -n 20 2>&1 || true"],
                cwd=SCRIPT_DIR, capture_output=True, text=True, timeout=15,
            )
            if proc.stdout.strip():
                print(proc.stdout, flush=True)
            else:
                print("  (空输出)", flush=True)
        except Exception as e:
            print(f"  slurmctld journalctl 失败: {e}", flush=True)

        print("  --- slurmd 日志 (worker, 最后 20 行) ---", flush=True)
        try:
            proc = subprocess.run(
                ["vagrant", "ssh", "worker", "-c",
                 "sudo journalctl -u slurmd --no-pager -n 20 2>&1 || true"],
                cwd=SCRIPT_DIR, capture_output=True, text=True, timeout=15,
            )
            if proc.stdout.strip():
                print(proc.stdout, flush=True)
            else:
                print("  (空输出)", flush=True)
        except Exception as e:
            print(f"  slurmd journalctl 失败: {e}", flush=True)

    print("  --- eBPF 程序列表 (worker) ---", flush=True)
    try:
        proc = subprocess.run(
            ["vagrant", "ssh", "worker", "-c",
             "sudo bpftool prog list 2>&1 | grep -i joblens || echo '(未找到 joblens eBPF 程序)'"],
            cwd=SCRIPT_DIR, capture_output=True, text=True, timeout=15,
        )
        if proc.stdout.strip():
            print(proc.stdout, flush=True)
        else:
            print("  (空输出)", flush=True)
    except Exception as e:
        print(f"  bpftool 查询失败: {e}", flush=True)

    print("  --- JobLens 服务状态 (worker) ---", flush=True)
    try:
        proc = subprocess.run(
            ["vagrant", "ssh", "worker", "-c",
             "echo '=== systemctl ==='; systemctl is-active joblens joblens-trigger 2>&1 || true; "
             "echo '=== journalctl joblens (最后 30 行) ==='; "
             "sudo journalctl -u joblens --no-pager -n 30 2>&1 || true; "
             "echo '=== journalctl joblens-trigger (最后 15 行) ==='; "
             "sudo journalctl -u joblens-trigger --no-pager -n 15 2>&1 || true"],
            cwd=SCRIPT_DIR, capture_output=True, text=True, timeout=15,
        )
        if proc.stdout.strip():
            print(proc.stdout, flush=True)
        else:
            print("  (空输出)", flush=True)
    except Exception as e:
        print(f"  JobLens 状态查询失败: {e}", flush=True)


def _vagrant_capture(node: str, command: str, timeout: int = 15) -> tuple[str, str]:
    """在 VM 节点上执行命令并返回 (stdout, stderr), 失败返回空字符串。"""
    try:
        proc = subprocess.run(
            ["vagrant", "ssh", node, "-c", command],
            cwd=SCRIPT_DIR, capture_output=True, text=True, timeout=timeout,
        )
        return proc.stdout, proc.stderr
    except Exception:
        return "", ""


def run_demo_preflight() -> bool:
    """
    Demo job 预检 — 向所有启用的调度器提交 demo job, 通过 JobLens API 轮询验证。
    返回 True 表示全部通过, False 表示至少一个失败。
    """
    total_tests = 0
    passed = 0
    htcondor_enabled = os.environ.get("HTCONDOR_ENABLED", "false") == "true"
    slurm_enabled = os.environ.get("SLURM_ENABLED", "false") == "true"
    worker_ip = os.environ["WORKER_IP"]

    # ── 预检前诊断：API 可达性 + 初始作业计数 ──
    api_url = f"http://{worker_ip}:7592/joblens/jobs"
    print(f"  → 预检 API 可达性: {api_url}", flush=True)
    try:
        resp = requests.get(api_url, timeout=10)
        data = resp.json()
        print(f"    API 响应: HTTP {resp.status_code}, job_count={data.get('job_count', '?')}, "
              f"jobs={len(data.get('jobs', []))}", flush=True)
    except requests.ConnectionError:
        print(f"    ✗ 无法连接 {api_url} — 宿主机可能无法路由到 VM 私有网络", flush=True)
        print(f"    提示: 检查宿主机是否可达 {worker_ip} (private_network {worker_ip}/24)", flush=True)
    except requests.Timeout:
        print(f"    ✗ API 请求超时: {api_url}", flush=True)
    except Exception as e:
        print(f"    ✗ API 请求异常: {e}", flush=True)

    # ── HTCondor demo ──────────────────────────────────────────────────
    if htcondor_enabled:
        total_tests += 1
        t_start = time.time()
        print(f"  → [{time.strftime('%H:%M:%S')}] 提交 HTCondor demo job...", flush=True)

        # 提交前 HTCondor 状态快照
        stdout, _ = _vagrant_capture("controller", "condor_status -schedd 2>&1 || true")
        if stdout.strip():
            print(f"    [提交前] condor_status -schedd:\n{stdout}", flush=True)

        try:
            result = subprocess.run(
                ["vagrant", "ssh", "controller", "-c",
                 "condor_submit /vagrant/test_jobs/helloworld.condor"],
                cwd=SCRIPT_DIR, check=True, capture_output=True, text=True,
            )
            # 输出 condor_submit 结果
            for line in result.stdout.splitlines():
                if line.strip():
                    print(f"    {line.strip()}", flush=True)

            # 立即检查队列
            try:
                qresult = subprocess.run(
                    ["vagrant", "ssh", "controller", "-c",
                     "echo '=== condor_q ==='; condor_q 2>&1 || true; "
                     "echo '=== condor_q -better ==='; condor_q -better-analyze 2>&1 || true"],
                    cwd=SCRIPT_DIR, capture_output=True, text=True, timeout=10,
                )
                if qresult.stdout.strip():
                    print(f"    [提交后] HTCondor 队列:\n{qresult.stdout}", flush=True)
                else:
                    print("    [提交后] condor_q 输出为空 (作业可能已完成或提交失败)", flush=True)
            except Exception:
                pass

            print(f"    → 轮询 JobLens API 等待 HTCondor 作业发现 (最多 30s)...", flush=True)
            job_id = poll_job_discovery(worker_ip, "condor", timeout=30)
            elapsed = time.time() - t_start
            if job_id:
                print(f"  PASSED: htcondor demo job discovered ({job_id}) [{elapsed:.1f}s]", flush=True)
                passed += 1
            else:
                print(f"  FAILED: htcondor demo job not discovered (timeout 30s, elapsed {elapsed:.1f}s)",
                      flush=True)
                # 失败时额外诊断: 检查 HTCondor 守护进程状态
                stdout, _ = _vagrant_capture("controller",
                    "echo '=== systemctl condor ==='; systemctl is-active condor 2>&1 || true; "
                    "echo '=== condor_status -any ==='; condor_status -any 2>&1 || true")
                if stdout.strip():
                    print(f"    [诊断] HTCondor 状态:\n{stdout}", flush=True)
        except subprocess.CalledProcessError as e:
            elapsed = time.time() - t_start
            print(f"  FAILED: htcondor demo job submit failed [{elapsed:.1f}s]", flush=True)
            print(f"    stdout: {e.stdout if e.stdout else '(空)'}", flush=True)
            print(f"    stderr: {e.stderr if e.stderr else '(空)'}", flush=True)

    # ── Slurm demo ─────────────────────────────────────────────────────
    if slurm_enabled:
        total_tests += 1
        t_start = time.time()
        print(f"  → [{time.strftime('%H:%M:%S')}] 提交 Slurm demo job...", flush=True)

        # 提交前 Slurm 状态快照
        stdout, _ = _vagrant_capture("controller",
            "echo '=== sinfo ==='; sinfo 2>&1 || true; "
            "echo '=== scontrol show node ==='; scontrol show node 2>&1 | head -30 || true")
        if stdout.strip():
            print(f"    [提交前] Slurm 状态:\n{stdout}", flush=True)

        # 检查关键服务状态
        stdout, _ = _vagrant_capture("worker",
            "echo '=== slurmd status ==='; systemctl is-active slurmd 2>&1 || true; "
            "echo '=== munge status ==='; systemctl is-active munge 2>&1 || true")
        if stdout.strip():
            print(f"    [提交前] Worker 服务状态:\n{stdout}", flush=True)

        try:
            result = subprocess.run(
                ["vagrant", "ssh", "controller", "-c",
                 "sbatch /vagrant/test_jobs/helloworld.sbatch"],
                cwd=SCRIPT_DIR, check=True, capture_output=True, text=True,
            )
            sbatch_job_id = ""
            for line in result.stdout.splitlines():
                if line.strip():
                    print(f"    {line.strip()}", flush=True)
                if "Submitted batch job" in line:
                    sbatch_job_id = line.strip().split()[-1]

            # 立即检查队列和作业状态
            if sbatch_job_id:
                try:
                    qresult = subprocess.run(
                        ["vagrant", "ssh", "controller", "-c",
                         f"echo '=== squeue ==='; squeue 2>&1 || true; "
                         f"echo '=== scontrol show job {sbatch_job_id} ==='; "
                         f"scontrol show job {sbatch_job_id} 2>&1 || true"],
                        cwd=SCRIPT_DIR, capture_output=True, text=True, timeout=10,
                    )
                    if qresult.stdout.strip():
                        print(f"    [提交后] Job {sbatch_job_id} 状态:\n{qresult.stdout}", flush=True)
                    else:
                        print(f"    [提交后] Job {sbatch_job_id}: scontrol 无输出 (作业可能已完成)", flush=True)
                except Exception:
                    pass
            else:
                # sbatch 成功但没有解析到 JobID — 输出完整 sinfo/squeue
                try:
                    qresult = subprocess.run(
                        ["vagrant", "ssh", "controller", "-c",
                         "echo '=== sinfo ==='; sinfo 2>&1 || true; "
                         "echo '=== squeue ==='; squeue 2>&1 || true"],
                        cwd=SCRIPT_DIR, capture_output=True, text=True, timeout=10,
                    )
                    if qresult.stdout.strip():
                        print(f"    [提交后] Slurm 状态 (无法解析 JobID):\n{qresult.stdout}", flush=True)
                except Exception:
                    pass

            print(f"    → 轮询 JobLens API 等待 Slurm 作业发现 (最多 30s)...", flush=True)
            job_id = poll_job_discovery(worker_ip, "slurm", timeout=30)
            elapsed = time.time() - t_start
            if job_id:
                print(f"  PASSED: slurm demo job discovered ({job_id}) [{elapsed:.1f}s]", flush=True)
                passed += 1
            else:
                print(f"  FAILED: slurm demo job not discovered (timeout 30s, elapsed {elapsed:.1f}s)",
                      flush=True)
                # 失败时额外诊断: 获取 slurmctld/slurmd 日志
                stdout, _ = _vagrant_capture("controller",
                    "echo '=== slurmctld 日志 (最后 15 行) ==='; "
                    "sudo journalctl -u slurmctld --no-pager -n 15 2>&1 || true")
                if stdout.strip():
                    print(f"    [诊断] slurmctld 日志:\n{stdout}", flush=True)
                stdout, _ = _vagrant_capture("worker",
                    "echo '=== slurmd 日志 (最后 15 行) ==='; "
                    "sudo journalctl -u slurmd --no-pager -n 15 2>&1 || true")
                if stdout.strip():
                    print(f"    [诊断] slurmd 日志:\n{stdout}", flush=True)
        except subprocess.CalledProcessError as e:
            elapsed = time.time() - t_start
            print(f"  FAILED: slurm demo job submit failed [{elapsed:.1f}s]", flush=True)
            print(f"    stdout: {e.stdout if e.stdout else '(空)'}", flush=True)
            print(f"    stderr: {e.stderr if e.stderr else '(空)'}", flush=True)
            # sbatch 失败时也输出 Slurm 状态
            stdout, _ = _vagrant_capture("controller",
                "echo '=== sinfo ==='; sinfo 2>&1 || true; "
                "echo '=== slurmctld status ==='; systemctl status slurmctld --no-pager -l 2>&1 || true")
            if stdout.strip():
                print(f"    [诊断] Slurm 控制器状态:\n{stdout}", flush=True)

    # ── 汇总 ──────────────────────────────────────────────────────────
    if total_tests == 0:
        print("  ℹ 无启用的调度器, 跳过 demo 预检", flush=True)
        return True
    if passed == total_tests:
        print("  PASSED: all demo jobs discovered", flush=True)
        return True
    else:
        print(f"  SUMMARY: {passed}/{total_tests} demo jobs passed", flush=True)
        # 任一失败则输出完整诊断 (fast-fail: 诊断失败不影响主流程)
        try:
            _diagnose_scheduler(htcondor_enabled, slurm_enabled)
        except Exception as e:
            print(f"  ⚠ 调度器诊断异常 (非致命, 已跳过): {e}", flush=True)
        return False


# ══════════════════════════════════════════════════════════════════════════
# RPM 文件解析
# ══════════════════════════════════════════════════════════════════════════

def resolve_rpm_glob(pattern: str) -> Path:
    """解析 RPM glob pattern → 第一个匹配的绝对路径。"""
    matches = sorted(SCRIPT_DIR.glob(pattern))
    if not matches:
        fatal(f"未找到 RPM 匹配: {SCRIPT_DIR}/{pattern}")
    return matches[0].resolve()


# ══════════════════════════════════════════════════════════════════════════
# 主编排函数
# ══════════════════════════════════════════════════════════════════════════

def run_preset(preset_name: str, skip_vagrant_up: bool = False,
               skip_vagrant_destroy: bool = False):
    """
    完整编排工作流: 加载预设 → 环境准备 → Vagrantfile → VM 启动 →
    provisioning → demo 预检 → 测试 → 清理。
    """
    dry_run = os.environ.get("DRY_RUN", "0") == "1"
    keep_vms = os.environ.get("KEEP_VMS", "0") == "1" or skip_vagrant_destroy
    phase = 1
    phase_start = time.time()

    def _tick(label: str) -> float:
        """打印阶段耗时并返回当前时间戳。"""
        elapsed = time.time() - phase_start
        print(f"  [{label} 耗时 {elapsed:.1f}s]")
        return time.time()

    # ═══ Phase 1: 预设加载 & 校验 ═══════════════════════════════════
    print(f"=== Phase {phase}/9: 预设加载 ===")
    phase += 1

    presets = find_presets(name=preset_name)
    if not presets:
        fatal(f"预设加载失败 — 未找到预设 '{preset_name}'")
    preset_file = presets[0]

    if not validate_preset(preset_file):
        fatal(f"预设 schema 校验失败 — {preset_file}")
    print(f"✓ 预设: {preset_file}")
    _tick("Phase 1 预设加载")

    # ═══ Phase 2: 环境准备 ══════════════════════════════════════════
    print(f"=== Phase {phase}/9: 环境准备 ===")
    phase += 1
    prepare_environment(preset_file)
    _tick("Phase 2 环境准备")

    # ═══ Phase 3: Vagrantfile 生成 ═════════════════════════════════
    print(f"=== Phase {phase}/9: Vagrantfile 生成 ===")
    phase += 1
    generate_vagrantfile()
    _tick("Phase 3 Vagrantfile 生成")

    # ── DRY-RUN 提前退出 ──
    if dry_run:
        print("\n=== DRY-RUN 模式: 跳过所有 VM 操作 ===")
        print("已生成文件:")
        print(f"  - {RUNTIME_DIR / 'preset_env.json'}")
        print(f"  - {VAGRANTFILE}")
        print("\n导出的环境变量:")
        for key in sorted(os.environ):
            if key.startswith(("JOBLENS_", "CONTROLLER_", "WORKER_", "NODES_",
                               "HTCONDOR", "SLURM", "CONDOR", "TRIGGER",
                               "CORE_", "PYTEST", "PRESET_", "RUNTIME")):
                print(f"  {key}={os.environ[key]}")
        return

    # ═══ Phase 4: VM 启动 ══════════════════════════════════════════
    print(f"=== Phase {phase}/9: VM 启动 ===")
    phase += 1
    if skip_vagrant_up:
        print("--skip-vagrant-up: VM 已由外部启动, 跳过 vagrant up")
    else:
        # ── 诊断: 输出环境信息便于排查 provider 问题 ──
        print("--- 环境诊断 ---")
        subprocess.run(["vagrant", "--version"], cwd=SCRIPT_DIR, check=False)
        subprocess.run(["vagrant", "plugin", "list"], cwd=SCRIPT_DIR, check=False)
        subprocess.run("vagrant plugin list 2>&1 | grep -q libvirt && "
                       "echo '✓ vagrant-libvirt 插件已安装' || "
                       "echo '✗ vagrant-libvirt 插件未安装!'",
                       cwd=SCRIPT_DIR, shell=True, check=False)
        subprocess.run("systemctl is-active libvirtd 2>/dev/null && "
                       "echo '✓ libvirtd 运行中' || "
                       "echo '✗ libvirtd 未运行'",
                       cwd=SCRIPT_DIR, shell=True, check=False)
        subprocess.run("ls -la /var/run/libvirt/libvirt-sock 2>/dev/null && "
                       "echo '✓ libvirt socket 存在' || "
                       "echo '✗ libvirt socket 不存在'",
                       cwd=SCRIPT_DIR, shell=True, check=False)
        subprocess.run(["vagrant", "status"], cwd=SCRIPT_DIR, check=False)
        print("---")

        try:
            subprocess.run(
                ["vagrant", "up", "--provider=libvirt"],
                cwd=SCRIPT_DIR, check=True,
            )
        except subprocess.CalledProcessError:
            fatal("vagrant up 失败")
        print("✓ VM 启动完成")
        _tick("Phase 4 VM 启动")

    # ═══ Phase 5: Provisioning 编排 ═══════════════════════════════
    print(f"=== Phase {phase}/9: Provisioning 编排 ===")
    phase += 1

    nodes_json_common = os.environ["NODES_JSON_COMMON"]
    nodes_json_slurm = os.environ["NODES_JSON_SLURM"]
    controller_ip = os.environ["CONTROLLER_IP"]
    worker_ip = os.environ["WORKER_IP"]
    condor_repo = os.environ["CONDOR_REPO_URL"]
    htcondor_enabled = os.environ["HTCONDOR_ENABLED"] == "true"
    slurm_enabled = os.environ["SLURM_ENABLED"] == "true"

    # ── 5a: 通用初始化 (所有节点) ──
    print("--- 5a: common.sh (所有节点) ---")
    for node in os.environ["JOBLENS_NODES"].split(","):
        role = "controller" if node == "controller" else "worker"
        print(f"  → {node} (role={role})")
        try:
            vagrant_ssh(node, f"sudo bash /vagrant/provisioning/common.sh"
                             f" --hostname={node} --role={role}"
                             f" --nodes-json='{nodes_json_common}'")
        except subprocess.CalledProcessError:
            fatal(f"common.sh 失败 — 节点 {node}")
    print("✓ common.sh 完成")
    _tick("Phase 5a common 初始化")

    # ── 5b: HTCondor (若启用) ──
    if htcondor_enabled:
        print("--- 5b: HTCondor 部署 ---")
        try:
            print("  → controller (condor controller)")
            vagrant_ssh("controller",
                        f"sudo bash /vagrant/provisioning/condor/controller.sh"
                        f" --repo-url={condor_repo} --hostname=controller"
                        f" --controller-ip={controller_ip}")
            print("  → worker (condor worker)")
            vagrant_ssh("worker",
                        f"sudo bash /vagrant/provisioning/condor/worker.sh"
                        f" --repo-url={condor_repo} --hostname=worker"
                        f" --controller-host=controller --controller-ip={controller_ip}"
                        f" --worker-ip={worker_ip}")
        except subprocess.CalledProcessError:
            fatal("condor 部署失败")
        print("✓ HTCondor 部署完成")
    else:
        print("--- 5b: HTCondor — 跳过 (未启用) ---")
    _tick("Phase 5b HTCondor")

    # ── 5c: Slurm (若启用) ──
    if slurm_enabled:
        print("--- 5c: Slurm 部署 ---")
        try:
            print("  → controller (slurm controller)")
            vagrant_ssh("controller",
                        f"sudo bash /vagrant/provisioning/slurm/controller.sh"
                        f" --rpm-dir=/vagrant/rpms --hostname=controller"
                        f" --controller-ip={controller_ip}"
                        f" --nodes-json='{nodes_json_slurm}'")

            # Slurm 文件传输 (munge.key + slurm.conf)
            print("  → 传输 munge.key & slurm.conf")
            slurm_runtime = RUNTIME_DIR / "slurm"
            slurm_runtime.mkdir(parents=True, exist_ok=True)

            munge_key = vagrant_cat("controller", "/etc/munge/munge.key")
            (slurm_runtime / "munge.key").write_bytes(munge_key)

            slurm_conf = vagrant_cat("controller", "/etc/slurm/slurm.conf")
            (slurm_runtime / "slurm.conf").write_bytes(slurm_conf)

            # 在 worker 上创建运行时目录, 然后注入文件
            vagrant_ssh("worker", "sudo mkdir -p /var/tmp/slurm_runtime")
            vagrant_write("worker", "/var/tmp/slurm_runtime/munge.key", munge_key)
            vagrant_write("worker", "/var/tmp/slurm_runtime/slurm.conf", slurm_conf)

            print("  → worker (slurm worker)")
            vagrant_ssh("worker",
                        f"sudo bash /vagrant/provisioning/slurm/worker.sh"
                        f" --rpm-dir=/vagrant/rpms --hostname=worker"
                        f" --controller-host=controller"
                        f" --runtime-dir=/var/tmp/slurm_runtime")
        except subprocess.CalledProcessError:
            fatal("slurm 部署失败")
        print("✓ Slurm 部署完成")
    else:
        print("--- 5c: Slurm — 跳过 (未启用) ---")
    _tick("Phase 5c Slurm")

    # ── 5d: RPM 文件挂载 ──
    print("--- 5d: RPM 文件挂载 ---")
    rpms_dir = SCRIPT_DIR / "rpms"
    rpms_dir.mkdir(exist_ok=True)

    unified_rpm = resolve_rpm_glob(os.environ["UNIFIED_RPM_PATTERN"])

    if unified_rpm.parent.resolve() != rpms_dir.resolve():
        subprocess.run(["cp", str(unified_rpm), str(rpms_dir / unified_rpm.name)], check=True)
    else:
        print("  统一 RPM 已在 rpms/ 目录, 跳过复制")
    print(f"✓ RPM 文件就绪: rpms/{unified_rpm.name}")

    # ── 5e: JobLens 配置注入 ──
    print("--- 5e: JobLens 配置注入 ---")
    core_cfg = Path(os.environ["CORE_CONFIG_PATH"])
    trigger_cfg = Path(os.environ["TRIGGER_CONFIG_PATH"])
    if not core_cfg.is_file():
        fatal(f"Core 配置文件不存在 — {core_cfg}")
    if not trigger_cfg.is_file():
        fatal(f"Trigger 配置文件不存在 — {trigger_cfg}")
    subprocess.run(["cp", str(core_cfg), str(RUNTIME_DIR / "joblens_core.yaml")], check=True)
    subprocess.run(["cp", str(trigger_cfg), str(RUNTIME_DIR / "joblens_trigger.yaml")], check=True)
    print("✓ 配置文件就绪: .runtime/joblens_core.yaml, .runtime/joblens_trigger.yaml")

    # Phase 5e 在 vagrant up (Phase 4) 之后才创建配置文件,
    # vagrant-libvirt 默认使用 rsync synced_folder, 仅在 vagrant up 时同步一次。
    # 因此必须手动 rsync 将新文件推送到 VM, 否则 Phase 5f deploy.sh 会报文件不存在。
    try:
        subprocess.run(["vagrant", "rsync"], cwd=SCRIPT_DIR, check=True)
    except subprocess.CalledProcessError:
        fatal("vagrant rsync 失败 — 配置文件无法同步到 VM")
    print("  ✓ 配置文件已同步到 VM")

    # ── 5f: JobLens 部署 ──
    print("--- 5f: JobLens 部署 ---")
    try:
        vagrant_ssh("worker",
                    f"sudo bash /vagrant/provisioning/joblens/deploy.sh"
                    f" --rpm-path=/vagrant/rpms/{unified_rpm.name}"
                    f" --core-config=/vagrant/.runtime/joblens_core.yaml"
                    f" --trigger-config=/vagrant/.runtime/joblens_trigger.yaml")
    except subprocess.CalledProcessError:
        fatal("joblens/deploy.sh 失败")
    print("✓ JobLens 部署完成")
    _tick("Phase 5 Provisioning 编排")

    # ═══ Phase 6: Demo job 预检 ═══════════════════════════════════
    print(f"=== Phase {phase}/9: Demo job 预检 ===")
    phase += 1
    # Fast-fail: demo 预检异常不阻塞后续流程 (Phase 7 跳过测试, Phase 8 仍需清理 VM)
    try:
        demo_failed = not run_demo_preflight()
    except Exception as e:
        print(f"⚠ demo 预检异常 (非致命, 继续后续阶段): {e}", file=sys.stderr)
        demo_failed = True
    if not demo_failed:
        print("✓ demo 预检完成")
    else:
        print("⚠ demo 预检失败 (非致命), 继续后续阶段", file=sys.stderr)
    _tick("Phase 6 Demo 预检")

    # ═══ Phase 7: 运行测试 ════════════════════════════════════════
    print(f"=== Phase {phase}/9: 运行测试 ===")
    phase += 1
    test_failed = False

    if demo_failed:
        print("⚠ demo 预检失败，跳过 pytest", file=sys.stderr)
    elif not os.environ.get("PYTEST_FILES"):
        print("⚠ 警告: 预设中未定义 pytest_files, 跳过测试", file=sys.stderr)
    else:
        pytest_files = os.environ["PYTEST_FILES"]
        pytest_args = os.environ["PYTEST_ARGS"]
        # CI 环境自动添加 --junitxml
        junit_part = ""
        if os.environ.get("CI") or os.environ.get("GITHUB_ACTIONS"):
            junit_part = " --junitxml=test-results.xml"
        cmd = (f"pytest --skip-vagrant-up --skip-vagrant-destroy"
               f"{junit_part} {pytest_args} {pytest_files}")
        print(f"  {cmd}")
        try:
            subprocess.run(cmd.split(), cwd=SCRIPT_DIR, check=True)
        except subprocess.CalledProcessError:
            test_failed = True
            print("FATAL: pytest 测试失败 — 将在清理后退出", file=sys.stderr)
        else:
            print("✓ 测试全部通过")

    _tick("Phase 7 运行测试")

    # ═══ Phase 8: 清理 ════════════════════════════════════════════
    print(f"=== Phase {phase}/9: 清理 ===")
    phase += 1
    if keep_vms:
        print("KEEP_VMS=1 — 跳过 VM 销毁")
    else:
        try:
            subprocess.run(["vagrant", "destroy", "-f"], cwd=SCRIPT_DIR, check=True)
        except subprocess.CalledProcessError:
            print("⚠ 警告: vagrant destroy 失败 (可能 VM 已被手动销毁)", file=sys.stderr)
        else:
            print("✓ VM 已销毁")

    if test_failed:
        fatal("测试阶段失败, 请检查上述错误")

    print(f"\n{'═' * 40}")
    print(f"  run_preset '{preset_name}' 完成 ✓")
    print(f"{'═' * 40}")
    _tick("总耗时")


# ══════════════════════════════════════════════════════════════════════════
# 入口
# ══════════════════════════════════════════════════════════════════════════

def main():
    parser = build_parser()

    # 手动处理 validate 子命令 (argparse 的 subparser 对此场景 overkill)
    if len(sys.argv) >= 2 and sys.argv[1] == "validate":
        if len(sys.argv) < 3:
            print("ERROR: usage: run_preset.py validate <yaml_file>", file=sys.stderr)
            sys.exit(1)
        yaml_path = Path(sys.argv[2])
        valid = validate_preset(yaml_path)
        sys.exit(0 if valid else 1)

    args = parser.parse_args()

    if args.list:
        list_presets()
    elif args.match:
        matches = find_presets(pattern=args.match)
        if not matches:
            sys.exit(1)
        for p in matches:
            print(p)
    elif args.name:
        run_preset(args.name,
                   skip_vagrant_up=args.skip_vagrant_up,
                   skip_vagrant_destroy=args.skip_vagrant_destroy or args.keep_vms)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env bash
# JobLens 集成测试 — 预设文件发现、名称校验与 schema 验证
# 用法:
#   source run_preset.sh               # 加载函数 (其他脚本引用)
#   bash run_preset.sh <name>          # 精确匹配
#   bash run_preset.sh --match <glob>  # glob 匹配
#   bash run_preset.sh --list          # 列出所有预设
#   bash run_preset.sh validate <file> # YAML schema 校验
set -euo pipefail

# 脚本所在目录 (查找 presets/ 目录)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRESETS_DIR="${SCRIPT_DIR}/presets"

# ─────────────────────────────────────────────────────────────────────
# validate_name — 校验 preset 名称合法性
# 规则:
#   - 必须以小写字母开头
#   - 仅含小写字母、数字、连字符(-)、下划线(_)
#   - 禁止路径穿越字符: /、..、空格
# 参数:
#   $1 — 待校验的 preset 名称
# 返回值: 0 = 合法, 1 = 非法
# ─────────────────────────────────────────────────────────────────────
validate_name() {
    local name="$1"

    # 拒绝路径穿越 / 空白字符
    if [[ "$name" =~ [/[:space:]] ]] || [[ "$name" == *".."* ]]; then
        echo "❌ 错误: 无效的 preset 名称 '${name}' — 包含禁止字符 (/、..、空格)" >&2
        return 1
    fi

    # 格式校验: ^[a-z][a-z0-9_-]*$
    if [[ ! "$name" =~ ^[a-z][a-z0-9_-]*$ ]]; then
        echo "❌ 错误: 无效的 preset 名称 '${name}' — 必须以小写字母开头, 仅含小写字母/数字/连字符/下划线" >&2
        return 1
    fi

    return 0
}

# ─────────────────────────────────────────────────────────────────────
# find_presets — 发现预设文件, 支持三种模式
# 模式:
#   find_presets <name>             精确匹配 presets/<name>.yaml
#   find_presets --match <glob>     glob 匹配 presets/<glob>.yaml
#   find_presets --list             列出 presets/ 下所有 .yaml 文件
# 注意: 仅搜索一级目录, 不递归
# 返回值: 0 = 找到, 1 = 未找到或参数错误
# ─────────────────────────────────────────────────────────────────────
find_presets() {
    # ── --list 模式 ──
    if [[ "${1:-}" == "--list" ]]; then
        if [[ ! -d "$PRESETS_DIR" ]]; then
            echo "❌ 错误: presets 目录不存在: ${PRESETS_DIR}" >&2
            return 1
        fi

        local files=()
        for f in "${PRESETS_DIR}"/*.yaml; do
            if [[ -f "$f" ]]; then
                files+=("$f")
            fi
        done

        if [[ ${#files[@]} -eq 0 ]]; then
            echo "⚠ 没有找到任何预设文件" >&2
            return 1
        fi

        printf '%s\n' "${files[@]}"
        return 0
    fi

    # ── --match 模式 ──
    if [[ "${1:-}" == "--match" ]]; then
        local pattern="${2:-}"
        if [[ -z "$pattern" ]]; then
            echo "❌ 错误: --match 需要参数, 例如: run_preset.sh --match 'alm9-*'" >&2
            return 1
        fi

        # 拒绝路径穿越
        if [[ "$pattern" =~ [/[:space:]] ]] || [[ "$pattern" == *".."* ]]; then
            echo "❌ 错误: 无效的 match 模式 '${pattern}' — 包含禁止字符 (/、..、空格)" >&2
            return 1
        fi

        local matches=()
        for f in "${PRESETS_DIR}"/${pattern}.yaml; do
            if [[ -f "$f" ]]; then
                matches+=("$f")
            fi
        done

        if [[ ${#matches[@]} -eq 0 ]]; then
            echo "❌ 错误: glob 模式 '${pattern}' 没有匹配到任何预设" >&2
            echo "提示: 使用 --list 查看所有可用预设" >&2
            return 1
        fi

        printf '%s\n' "${matches[@]}"
        return 0
    fi

    # ── 精确名称模式 (默认) ──
    local name="${1:-}"
    if [[ -z "$name" ]]; then
        echo "❌ 错误: 缺少参数。用法:" >&2
        echo "  run_preset.sh <preset-name>          精确匹配" >&2
        echo "  run_preset.sh --match <glob-pattern>  glob 匹配" >&2
        echo "  run_preset.sh --list                  列出所有预设" >&2
        echo "  run_preset.sh validate <yaml_file>    YAML schema 校验" >&2
        return 1
    fi

    # 校验名称合法性
    validate_name "$name" || return 1

    local preset_file="${PRESETS_DIR}/${name}.yaml"
    if [[ -f "$preset_file" ]]; then
        echo "$preset_file"
        return 0
    fi

    # 未找到 — 列出可用预设作为提示
    echo "❌ 错误: 未找到预设 '${name}' (查找路径: ${preset_file})" >&2
    if [[ -d "$PRESETS_DIR" ]]; then
        local found=0
        for f in "${PRESETS_DIR}"/*.yaml; do
            if [[ -f "$f" ]]; then
                if [[ $found -eq 0 ]]; then
                    echo "" >&2
                    echo "可用预设:" >&2
                fi
                echo "  - $(basename "$f" .yaml)" >&2
                found=1
            fi
        done
    fi
    return 1
}

# ─────────────────────────────────────────────────────────────────────
# 内部: 简易 YAML 解析器 (python3 stdlib, 无外部库)
# ─────────────────────────────────────────────────────────────────────

# _yaml_to_dict <yaml_file>
# 读取 YAML 文件，输出 JSON 格式的 dict 到 stdout。
# 支持: 注释 (#), 缩进嵌套 (2空格), key: value, - list 项, 空行。
_yaml_to_dict() {
    local yaml_file="$1"
    python3 -c '
import sys, json, re

def parse_yaml(filepath):
    """简易 YAML → dict 解析器。仅处理本 schema 所需的 YAML 子集。"""
    with open(filepath, "r") as f:
        lines = f.readlines()

    root = {}
    stack = [(-1, root, None)]  # (indent, container, key), -1 防止根节点被弹出

    # 当前正在构建的列表引用
    current_list = None
    current_list_indent = -1

    for lineno, raw in enumerate(lines, 1):
        line = raw.rstrip("\n")
        # 跳过空行和纯注释行
        if not line.strip() or line.strip().startswith("#"):
            continue

        # 计算缩进 (仅计算前导空格)
        indent = len(line) - len(line.lstrip(" "))
        content = line.strip()

        # 跳过行内注释
        if "#" in content:
            # 简单处理: 找到第一个不在引号内的 #
            in_quote = False
            for i, ch in enumerate(content):
                if ch in ("\x27", "\x22"):
                    in_quote = not in_quote
                elif ch == "#" and not in_quote:
                    content = content[:i].strip()
                    break

        if not content:
            continue

        # 处理列表项: "- value" 或 "- key: value"
        if content.startswith("- "):
            list_value = content[2:].strip()
            # 弹出到列表所在的缩进级别
            while stack and stack[-1][0] >= indent:
                stack.pop()

            if stack:
                parent_container, parent_key = stack[-1][1], stack[-1][2]
            else:
                parent_container, parent_key = root, None

            # 新建或获取当前列表
            if parent_key is not None and isinstance(parent_container, dict):
                # 检测空 dict 占位符 (key: 被误解析为 dict, 实际是 list)
                if isinstance(parent_container, dict) and not parent_container and len(stack) >= 2:
                    gp_dict = stack[-2][1]
                    if isinstance(gp_dict, dict):
                        gp_dict[parent_key] = []
                        target_list = gp_dict[parent_key]
                        stack[-1] = (stack[-1][0], target_list, parent_key)
                    else:
                        target_list = parent_container
                elif parent_key not in parent_container:
                    parent_container[parent_key] = []
                    target_list = parent_container[parent_key]
                else:
                    target_list = parent_container[parent_key]
            else:
                target_list = parent_container

            # 列表项值
            if ":" in list_value:
                # 列表项是 dict: "- key: value"
                k, v = list_value.split(":", 1)
                k, v = k.strip(), v.strip()
                item_dict = {}
                item_dict[k] = _parse_scalar(v)
                target_list.append(item_dict)
            else:
                target_list.append(_parse_scalar(list_value))
            continue

        # 处理 key: value
        if ":" in content:
            key, _, raw_value = content.partition(":")
            key = key.strip()
            raw_value = raw_value.strip()

            # 弹出栈中缩进 >= 当前行的项
            while stack and stack[-1][0] >= indent:
                stack.pop()

            if stack:
                parent, _ = stack[-1][1], stack[-1][2]
            else:
                parent, _ = root, None

            if not raw_value:
                # 值是嵌套结构 (indented block)，创建子 dict
                new_map = {}
                if isinstance(parent, dict):
                    parent[key] = new_map
                elif isinstance(parent, list):
                    # 列表中的 dict 项
                    parent.append({key: new_map})
                else:
                    parent = new_map
                stack.append((indent, new_map, key))
            else:
                # 标量值
                val = _parse_scalar(raw_value)
                if isinstance(parent, dict):
                    parent[key] = val
                elif isinstance(parent, list):
                    parent.append({key: val})
                elif isinstance(parent, str):
                    # 顶层键值
                    root[key] = val
                # 不在栈上 push，因为这是叶子节点
            continue

        # 无法解析的行
        print(f"ERROR: line {lineno}: cannot parse: {content}", file=sys.stderr)
        sys.exit(1)

    return root


def _parse_scalar(raw):
    """解析 YAML 标量值: bool, int, float, null, string."""
    raw = raw.strip()
    # 去掉引号
    if (raw.startswith("\x27") and raw.endswith("\x27")) or \
       (raw.startswith("\x22") and raw.endswith("\x22")):
        return raw[1:-1]
    # YAML booleans
    if raw.lower() in ("true", "yes", "on"):
        return True
    if raw.lower() in ("false", "no", "off"):
        return False
    if raw.lower() in ("null", "~", ""):
        return None
    # 整数
    try:
        return int(raw)
    except ValueError:
        pass
    # 浮点数
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


# 入口
result = parse_yaml(sys.argv[1])
json.dump(result, sys.stdout, indent=2)
' "$yaml_file"
}

# ─────────────────────────────────────────────────────────────────────
# validate_preset <yaml_file> — YAML schema 校验
# ─────────────────────────────────────────────────────────────────────

validate_preset() {
    local yaml_file="${1:-}"
    local errors=0

    # 检查参数
    if [[ -z "$yaml_file" ]]; then
        echo "ERROR: usage: validate_preset <yaml_file>" >&2
        return 1
    fi

    if [[ ! -f "$yaml_file" ]]; then
        echo "ERROR: file not found: $yaml_file" >&2
        return 1
    fi

    # 解析 YAML → JSON
    local parsed_json
    if ! parsed_json=$(_yaml_to_dict "$yaml_file" 2>/dev/null); then
        echo "ERROR: failed to parse YAML file: $yaml_file" >&2
        _yaml_to_dict "$yaml_file"  # 重新执行以输出具体错误
        return 1
    fi

    # 将 JSON 写入临时文件，用 python3 heredoc 读取校验
    local json_tmp
    json_tmp=$(mktemp /tmp/joblens-validate-XXXXXX.json)
    echo "$parsed_json" > "$json_tmp"
    python3 - "$json_tmp" << 'PYEOF'
import sys, json, re

data = json.load(open(sys.argv[1]))
errors = []

# ── 1. name ──
if "name" not in data:
    errors.append("name: missing required field")
elif not isinstance(data["name"], str) or not data["name"].strip():
    errors.append("name: must be a non-empty string")
elif not re.match(r"^[a-z][a-z0-9_-]*$", data["name"]):
    errors.append("name: invalid name '%s' (allowed: ^[a-z][a-z0-9_-]*$)" % data["name"])

# ── 2. topology ──
if "topology" not in data:
    errors.append("topology: missing required field")
elif not isinstance(data["topology"], dict):
    errors.append("topology: expected map, got %s" % type(data["topology"]).__name__)
else:
    topo = data["topology"]
    if "controller" not in topo:
        errors.append("topology: missing required key 'controller'")
    elif not isinstance(topo["controller"], dict):
        errors.append("topology.controller: expected map, got %s" % type(topo["controller"]).__name__)
    if "worker" not in topo:
        errors.append("topology: missing required key 'worker'")
    elif not isinstance(topo["worker"], dict):
        errors.append("topology.worker: expected map, got %s" % type(topo["worker"]).__name__)
    for node_key in topo:
        if not re.match(r"^[a-z0-9_-]+$", node_key):
            errors.append("topology.%s: invalid node key (allowed: [a-z0-9_-])" % node_key)

# ── 3. network.subnet ──
if "network" not in data:
    errors.append("network: missing required field")
elif not isinstance(data["network"], dict):
    errors.append("network: expected map, got %s" % type(data["network"]).__name__)
elif "subnet" not in data["network"]:
    errors.append("network.subnet: missing required field")
else:
    subnet = data["network"]["subnet"]
    if not isinstance(subnet, str):
        errors.append("network.subnet: expected string, got %s" % type(subnet).__name__)
    else:
        cidr_pat = r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})/(\d{1,2})$"
        m = re.match(cidr_pat, subnet)
        if not m:
            errors.append("network.subnet: invalid CIDR format '%s'" % subnet)
        else:
            octets = [int(m.group(i)) for i in range(1, 5)]
            mask = int(m.group(5))
            ok = True
            for i, o in enumerate(octets, 1):
                if o < 0 or o > 255:
                    errors.append("network.subnet: octet %d out of range (0-255): %d" % (i, o))
                    ok = False
                    break
            if ok and (mask < 0 or mask > 32):
                errors.append("network.subnet: prefix length out of range (0-32): %d" % mask)

# ── 4. schedulers ──
if "schedulers" not in data:
    errors.append("schedulers: missing required field")
elif not isinstance(data["schedulers"], dict):
    errors.append("schedulers: expected map, got %s" % type(data["schedulers"]).__name__)
else:
    sched = data["schedulers"]
    for sname in ("htcondor", "slurm"):
        if sname not in sched:
            errors.append("schedulers.%s: missing required field" % sname)
        elif not isinstance(sched[sname], dict):
            errors.append("schedulers.%s: expected map, got %s" % (sname, type(sched[sname]).__name__))
        elif "enabled" not in sched[sname]:
            errors.append("schedulers.%s.enabled: missing required field" % sname)
        elif not isinstance(sched[sname]["enabled"], bool):
            errors.append("schedulers.%s.enabled: expected boolean, got %s '%s'" % (
                sname, type(sched[sname]["enabled"]).__name__, sched[sname]["enabled"]))

# ── 5. joblens ──
if "joblens" not in data:
    errors.append("joblens: missing required field")
elif not isinstance(data["joblens"], dict):
    errors.append("joblens: expected map, got %s" % type(data["joblens"]).__name__)
else:
    jl = data["joblens"]
    for cfg_key in ("core_config", "trigger_config"):
        if cfg_key not in jl:
            errors.append("joblens.%s: missing required field" % cfg_key)
        elif not isinstance(jl[cfg_key], str) or not jl[cfg_key].strip():
            errors.append("joblens.%s: must be a non-empty string file path" % cfg_key)

# ── 6. tests.pytest_files ──
if "tests" not in data:
    errors.append("tests: missing required field")
elif not isinstance(data["tests"], dict):
    errors.append("tests: expected map, got %s" % type(data["tests"]).__name__)
elif "pytest_files" not in data["tests"]:
    errors.append("tests.pytest_files: missing required field")
else:
    pf = data["tests"]["pytest_files"]
    if not isinstance(pf, list):
        errors.append("tests.pytest_files: expected list, got %s" % type(pf).__name__)
    elif len(pf) == 0:
        errors.append("tests.pytest_files: must be non-empty list")
    else:
        for i, item in enumerate(pf):
            if not isinstance(item, str):
                errors.append("tests.pytest_files[%d]: expected string, got %s" % (i, type(item).__name__))

if errors:
    for e in errors:
        print("ERROR: %s" % e, file=sys.stderr)
    sys.exit(1)
else:
    sys.exit(0)
PYEOF
    local ret=$?
    rm -f "$json_tmp"
    if [[ $ret -ne 0 ]]; then
        return 1
    fi
    return 0
}

# ─────────────────────────────────────────────────────────────────────
# run_demo_preflight — Demo job 预检
# 向所有启用的调度器提交 demo job，通过 JobLens API 轮询验证自动发现
# 超时保护: 每个调度器 30 秒，每 2 秒轮询一次
# 返回值: 0 = 全部通过, 1 = 至少一个失败（逐个尝试所有启用的调度器）
# 依赖变量 (来自 run_preset):
#   HTCONDOR_ENABLED, SLURM_ENABLED, WORKER_IP
# ─────────────────────────────────────────────────────────────────────
run_demo_preflight() {
    local total_tests=0
    local passed=0
    local timeout=30
    local interval=2
    local elapsed resp found_id

    # ── HTCondor demo ──────────────────────────────────────────────
    if [[ "${HTCONDOR_ENABLED:-false}" == "true" ]]; then
        total_tests=$((total_tests + 1))
        echo "  → 提交 HTCondor demo job..."
        if vagrant ssh controller -c "condor_submit /vagrant/test_jobs/helloworld.condor" >/dev/null 2>&1; then
            elapsed=0
            while [[ $elapsed -lt $timeout ]]; do
                resp=$(curl -sf --max-time 5 "http://${WORKER_IP}:7592/joblens/jobs" 2>/dev/null || true)
                if [[ -n "$resp" ]]; then
                    if found_id=$(echo "$resp" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for job in data.get('jobs', []):
    if 'condor' in job.get('subtype', '').lower():
        print(job.get('job_id', '?'))
        sys.exit(0)
sys.exit(1)
" 2>/dev/null); then
                        echo "PASSED: htcondor demo job discovered (${found_id})"
                        passed=$((passed + 1))
                        break
                    fi
                fi
                sleep "$interval"
                elapsed=$((elapsed + interval))
            done
            if [[ $elapsed -ge $timeout ]]; then
                echo "FAILED: htcondor demo job not discovered (timeout 30s)"
            fi
        else
            echo "FAILED: htcondor demo job submit failed"
        fi
    fi

    # ── Slurm demo ─────────────────────────────────────────────────
    if [[ "${SLURM_ENABLED:-false}" == "true" ]]; then
        total_tests=$((total_tests + 1))
        echo "  → 提交 Slurm demo job..."
        if vagrant ssh controller -c "sbatch /vagrant/test_jobs/helloworld.sbatch" >/dev/null 2>&1; then
            elapsed=0
            while [[ $elapsed -lt $timeout ]]; do
                resp=$(curl -sf --max-time 5 "http://${WORKER_IP}:7592/joblens/jobs" 2>/dev/null || true)
                if [[ -n "$resp" ]]; then
                    if found_id=$(echo "$resp" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for job in data.get('jobs', []):
    if 'slurm' in job.get('subtype', '').lower():
        print(job.get('job_id', '?'))
        sys.exit(0)
sys.exit(1)
" 2>/dev/null); then
                        echo "PASSED: slurm demo job discovered (${found_id})"
                        passed=$((passed + 1))
                        break
                    fi
                fi
                sleep "$interval"
                elapsed=$((elapsed + interval))
            done
            if [[ $elapsed -ge $timeout ]]; then
                echo "FAILED: slurm demo job not discovered (timeout 30s)"
            fi
        else
            echo "FAILED: slurm demo job submit failed"
        fi
    fi

    # ── 汇总 ──────────────────────────────────────────────────────
    if [[ "$total_tests" -eq 0 ]]; then
        echo "  ℹ 无启用的调度器, 跳过 demo 预检"
        return 0
    fi
    if [[ "$passed" -eq "$total_tests" ]]; then
        echo "PASSED: all demo jobs discovered"
        return 0
    else
        echo "SUMMARY: ${passed}/${total_tests} demo jobs passed"
        return 1
    fi
}

# ─────────────────────────────────────────────────────────────────────
# run_preset — 主编排函数
# 完整工作流: 加载预设 → 环境准备 → Vagrantfile → VM 启动 →
#             provisioning → demo 预检 → 测试 → 清理
# 参数:
#   $1 — preset 名称 (不含路径和后缀, 如 alm9-default)
# 环境变量:
#   DRY_RUN=1 — 仅生成 env/Vagrantfile, 不执行 vagrant 命令
#   KEEP_VMS=1 — 测试后保留 VM (跳过 vagrant destroy)
# ─────────────────────────────────────────────────────────────────────
run_preset() {
    local preset_name="$1"
    local runtime_dir="${SCRIPT_DIR}/.runtime"
    local phase=1
    local demo_failed=0

    # ═══════════════════════════════════════════════════════════════
    # Phase 1: 预设加载 & 校验
    # ═══════════════════════════════════════════════════════════════
    echo "=== Phase ${phase}/9: 预设加载 ==="
    phase=$((phase + 1))

    local preset_file
    preset_file=$(find_presets "$preset_name") || {
        echo "FATAL: 预设加载失败 — 未找到预设 '${preset_name}'" >&2
        exit 1
    }

    validate_preset "$preset_file" || {
        echo "FATAL: 预设 schema 校验失败 — ${preset_file}" >&2
        exit 1
    }

    echo "✓ 预设: ${preset_file}"

    # ═══════════════════════════════════════════════════════════════
    # Phase 2: 解析 YAML → 提取环境变量 & 生成 preset_env.json
    # ═══════════════════════════════════════════════════════════════
    echo "=== Phase ${phase}/9: 环境准备 ==="
    phase=$((phase + 1))

    mkdir -p "$runtime_dir"

    # 步骤 2a: 解析 YAML 为 JSON, 保存到临时文件
    local json_tmp
    json_tmp=$(mktemp /tmp/joblens-preset-XXXXXX.json)
    if ! _yaml_to_dict "$preset_file" > "$json_tmp" 2>/dev/null; then
        rm -f "$json_tmp"
        echo "FATAL: YAML 解析失败 — ${preset_file}" >&2
        _yaml_to_dict "$preset_file"  # 重新执行以输出具体错误
        exit 1
    fi

    # 步骤 2b: Python 提取器 — 读取 JSON, 输出 bash export 语句 + 生成 preset_env.json
    local env_exports
    if ! env_exports=$(python3 - "$SCRIPT_DIR" "$json_tmp" << 'PYEXTRACT'
import sys, json, os, glob as py_glob

script_dir = sys.argv[1]
json_file = sys.argv[2]

with open(json_file) as f:
    data = json.load(f)

preset_name = data["name"]
topo = data["topology"]
sched = data["schedulers"]
jl = data["joblens"]
tests = data["tests"]

# 构建节点列表 (按 key 排序以保证一致性)
nodes = []
node_names = []
for node_key in sorted(topo.keys()):
    node = topo[node_key]
    nodes.append({
        "host": node.get("hostname", node_key),
        "ip": node["ip"],
        "cpus": node.get("cpus", 1)
    })
    node_names.append(node_key)

# 为 common.sh 构建 nodes-json (host + ip)
nodes_json_common = json.dumps([{"host": n["host"], "ip": n["ip"]} for n in nodes])

# 为 slurm/controller.sh 构建 nodes-json (host + ip + cpus)
nodes_json_slurm = json.dumps(
    [{"host": n["host"], "ip": n["ip"], "cpus": n["cpus"]} for n in nodes]
)

# 提取 controller / worker 信息
ctrl = topo["controller"]
wrk = topo["worker"]
ctrl_ip = ctrl["ip"]
wrk_ip = wrk["ip"]

# Trigger URL (worker 节点运行 JobLens + Trigger)
trigger_url = f"http://{wrk_ip}:7592"

# ═══ 生成 .runtime/preset_env.json ═══
runtime_dir = os.path.join(script_dir, ".runtime")
os.makedirs(runtime_dir, exist_ok=True)
env_file = os.path.join(runtime_dir, "preset_env.json")
preset_env = {
    "preset_name": preset_name,
    "controller": {"host": "controller", "ip": ctrl_ip},
    "worker": {"host": "worker", "ip": wrk_ip},
    "nodes": nodes,
    "trigger_url": trigger_url
}
with open(env_file, "w") as f:
    json.dump(preset_env, f, indent=2)

# ═══ RPM 路径 pattern (原始值, 在 Phase 5d 中由 bash glob 解析) ═══
# 相对于 SCRIPT_DIR 解析为绝对 pattern, 但延迟解析到部署阶段
rpm_pattern = jl.get("rpm_path", "")
trigger_rpm_pattern = jl.get("trigger_rpm_path", "")

# ═══ 配置文件路径 (相对于 SCRIPT_DIR) ═══
core_config_path = os.path.join(script_dir, jl["core_config"])
trigger_config_path = os.path.join(script_dir, jl["trigger_config"])

# ═══ 调度器配置 ═══
htcondor_enabled = "true" if sched.get("htcondor", {}).get("enabled", False) \
                   else "false"
condor_repo_url = sched.get("htcondor", {}).get("repo_rpm_url", "")
slurm_enabled = "true" if sched.get("slurm", {}).get("enabled", False) \
                else "false"

# ═══ 测试配置 ═══
pytest_files = " ".join(tests.get("pytest_files", []))
pytest_args = tests.get("pytest_args", "")

# ═══ 输出 bash export 语句 (由调用方 eval 执行) ═══
print(f'export PRESET_NAME="{preset_name}"')
print(f'export JOBLENS_NODES="{",".join(node_names)}"')
for n in nodes:
    name_up = n["host"].upper()
    nd = topo[n["host"]]
    print(f'export {name_up}_BOX="{nd["box"]}"')
    print(f'export {name_up}_CPUS="{nd["cpus"]}"')
    print(f'export {name_up}_MEMORY="{nd["memory"]}"')
    print(f'export {name_up}_DISK="{nd["disk"]}"')
    print(f'export {name_up}_IP="{n["ip"]}"')
print(f'export CONTROLLER_IP="{ctrl_ip}"')
print(f'export WORKER_IP="{wrk_ip}"')
print(f'export TRIGGER_URL="{trigger_url}"')
print(f"export NODES_JSON_COMMON='{nodes_json_common}'")
print(f"export NODES_JSON_SLURM='{nodes_json_slurm}'")
print(f'export HTCONDOR_ENABLED="{htcondor_enabled}"')
print(f'export CONDOR_REPO_URL="{condor_repo_url}"')
print(f'export SLURM_ENABLED="{slurm_enabled}"')
print(f'export CORE_RPM_PATTERN="{rpm_pattern}"')
print(f'export TRIGGER_RPM_PATTERN="{trigger_rpm_pattern}"')
print(f'export CORE_CONFIG_PATH="{core_config_path}"')
print(f'export TRIGGER_CONFIG_PATH="{trigger_config_path}"')
print(f'export PYTEST_FILES="{pytest_files}"')
print(f'export PYTEST_ARGS="{pytest_args}"')
print(f'export RUNTIME_DIR="{runtime_dir}"')
print(f'export PRESET_ENV_FILE="{env_file}"')
PYEXTRACT
    ); then
        rm -f "$json_tmp"
        echo "FATAL: 环境变量提取失败" >&2
        exit 1
    fi
    rm -f "$json_tmp"

    # 注入环境变量到当前 shell
    eval "$env_exports"

    echo "✓ preset_env.json → ${PRESET_ENV_FILE}"
    echo "✓ 节点: ${JOBLENS_NODES}"
    echo "✓ 控制器 IP: ${CONTROLLER_IP}"
    echo "✓ Worker IP: ${WORKER_IP}"
    echo "✓ Trigger URL: ${TRIGGER_URL}"

    # ═══════════════════════════════════════════════════════════════
    # Phase 3: Vagrantfile 生成
    # ═══════════════════════════════════════════════════════════════
    echo "=== Phase ${phase}/9: Vagrantfile 生成 ==="
    phase=$((phase + 1))

    local vagrant_template="${SCRIPT_DIR}/Vagrantfile.template"
    local vagrantfile="${SCRIPT_DIR}/Vagrantfile"

    if [[ ! -f "$vagrant_template" ]]; then
        echo "FATAL: Vagrantfile 模板不存在: ${vagrant_template}" >&2
        exit 1
    fi

    # 模板使用 Ruby ENV.fetch 从环境变量读取, 直接复制即可
    cp "$vagrant_template" "$vagrantfile"
    echo "✓ Vagrantfile: ${vagrantfile}"

    # ── DRY-RUN 提前退出 ──
    if [[ "${DRY_RUN:-0}" == "1" ]]; then
        echo ""
        echo "=== DRY-RUN 模式: 跳过所有 VM 操作 ==="
        echo "已生成文件:"
        echo "  - ${PRESET_ENV_FILE}"
        echo "  - ${vagrantfile}"
        echo ""
        echo "导出的环境变量:"
        env | grep -E '^(JOBLENS_|CONTROLLER_|WORKER_|NODES_|HTCONDOR|SLURM|CONDOR|TRIGGER|CORE_|PYTEST|PRESET_|RUNTIME)' | sort || true
        return 0
    fi

    # ═══════════════════════════════════════════════════════════════
    # Phase 4: VM 启动
    # ═══════════════════════════════════════════════════════════════
    echo "=== Phase ${phase}/9: VM 启动 ==="
    phase=$((phase + 1))

    cd "$SCRIPT_DIR"
    vagrant up --provider=libvirt || {
        echo "FATAL: vagrant up 失败" >&2
        exit 1
    }
    echo "✓ VM 启动完成"

    # ═══════════════════════════════════════════════════════════════
    # Phase 5: Provisioning 编排
    # ═══════════════════════════════════════════════════════════════
    echo "=== Phase ${phase}/9: Provisioning 编排 ==="
    phase=$((phase + 1))

    # ── 5a: 通用初始化 (所有节点) ──
    echo "--- 5a: common.sh (所有节点) ---"
    IFS=',' read -ra NODE_LIST <<< "$JOBLENS_NODES"
    for node in "${NODE_LIST[@]}"; do
        role="worker"
        [[ "$node" == "controller" ]] && role="controller"
        echo "  → ${node} (role=${role})"
        vagrant ssh "$node" -c "sudo bash /vagrant/provisioning/common.sh --hostname=${node} --role=${role} --nodes-json='${NODES_JSON_COMMON}'" || {
            echo "FATAL: common.sh 失败 — 节点 ${node}" >&2
            exit 1
        }
    done
    echo "✓ common.sh 完成"

    # ── 5b: HTCondor (若启用) ──
    if [[ "$HTCONDOR_ENABLED" == "true" ]]; then
        echo "--- 5b: HTCondor 部署 ---"
        echo "  → controller (condor controller)"
        vagrant ssh controller -c "sudo bash /vagrant/provisioning/condor/controller.sh --repo-url=${CONDOR_REPO_URL} --hostname=controller --controller-ip=${CONTROLLER_IP}" || {
            echo "FATAL: condor/controller.sh 失败" >&2
            exit 1
        }
        echo "  → worker (condor worker)"
        vagrant ssh worker -c "sudo bash /vagrant/provisioning/condor/worker.sh --repo-url=${CONDOR_REPO_URL} --hostname=worker --controller-host=controller --controller-ip=${CONTROLLER_IP} --worker-ip=${WORKER_IP}" || {
            echo "FATAL: condor/worker.sh 失败" >&2
            exit 1
        }
        echo "✓ HTCondor 部署完成"
    else
        echo "--- 5b: HTCondor — 跳过 (未启用) ---"
    fi

    # ── 5c: Slurm (若启用) ──
    if [[ "$SLURM_ENABLED" == "true" ]]; then
        echo "--- 5c: Slurm 部署 ---"
        echo "  → controller (slurm controller)"
        vagrant ssh controller -c "sudo bash /vagrant/provisioning/slurm/controller.sh --rpm-dir=/vagrant/rpms --hostname=controller --controller-ip=${CONTROLLER_IP} --nodes-json='${NODES_JSON_SLURM}'" || {
            echo "FATAL: slurm/controller.sh 失败" >&2
            exit 1
        }

        # Slurm 文件传输 (munge.key + slurm.conf) — 固化逻辑
        echo "  → 传输 munge.key & slurm.conf"
        local slurm_runtime="${runtime_dir}/slurm"
        mkdir -p "$slurm_runtime"
        vagrant ssh controller -c "sudo cat /etc/munge/munge.key" > "${slurm_runtime}/munge.key" || {
            echo "FATAL: 无法从 controller 提取 munge.key" >&2
            exit 1
        }
        vagrant ssh controller -c "sudo cat /etc/slurm/slurm.conf" > "${slurm_runtime}/slurm.conf" || {
            echo "FATAL: 无法从 controller 提取 slurm.conf" >&2
            exit 1
        }
        vagrant ssh worker -c "sudo mkdir -p /var/tmp/slurm_runtime" || {
            echo "FATAL: 无法在 worker 创建 /var/tmp/slurm_runtime" >&2
            exit 1
        }
        vagrant ssh worker -c "sudo tee /var/tmp/slurm_runtime/munge.key" < "${slurm_runtime}/munge.key" > /dev/null || {
            echo "FATAL: 无法传输 munge.key 到 worker" >&2
            exit 1
        }
        vagrant ssh worker -c "sudo tee /var/tmp/slurm_runtime/slurm.conf" < "${slurm_runtime}/slurm.conf" > /dev/null || {
            echo "FATAL: 无法传输 slurm.conf 到 worker" >&2
            exit 1
        }

        echo "  → worker (slurm worker)"
        vagrant ssh worker -c "sudo bash /vagrant/provisioning/slurm/worker.sh --rpm-dir=/vagrant/rpms --hostname=worker --controller-host=controller --runtime-dir=/var/tmp/slurm_runtime" || {
            echo "FATAL: slurm/worker.sh 失败" >&2
            exit 1
        }
        echo "✓ Slurm 部署完成"
    else
        echo "--- 5c: Slurm — 跳过 (未启用) ---"
    fi

    # ── 5d: RPM 文件挂载 ──
    echo "--- 5d: RPM 文件挂载 ---"
    mkdir -p "${SCRIPT_DIR}/rpms"

    # 解析 RPM glob pattern → 实际文件
    local core_rpm_path trigger_rpm_path
    core_rpm_path=$( (cd "${SCRIPT_DIR}" && ls ${CORE_RPM_PATTERN} 2>/dev/null | head -1) ) || true
    trigger_rpm_path=$( (cd "${SCRIPT_DIR}" && ls ${TRIGGER_RPM_PATTERN} 2>/dev/null | head -1) ) || true

    if [[ -z "$core_rpm_path" ]]; then
        echo "FATAL: 未找到 JobLens Core RPM — 模式: ${SCRIPT_DIR}/${CORE_RPM_PATTERN}" >&2
        exit 1
    fi
    if [[ -z "$trigger_rpm_path" ]]; then
        echo "FATAL: 未找到 JobLens Trigger RPM — 模式: ${SCRIPT_DIR}/${TRIGGER_RPM_PATTERN}" >&2
        exit 1
    fi

    # 使路径为绝对路径
    core_rpm_path="${SCRIPT_DIR}/${core_rpm_path}"
    trigger_rpm_path="${SCRIPT_DIR}/${trigger_rpm_path}"

    cp "${core_rpm_path}" "${SCRIPT_DIR}/rpms/" || {
        echo "FATAL: 无法复制 Core RPM — ${core_rpm_path}" >&2
        exit 1
    }
    cp "${trigger_rpm_path}" "${SCRIPT_DIR}/rpms/" || {
        echo "FATAL: 无法复制 Trigger RPM — ${trigger_rpm_path}" >&2
        exit 1
    }
    local core_rpm_filename trigger_rpm_filename
    core_rpm_filename=$(basename "$core_rpm_path")
    trigger_rpm_filename=$(basename "$trigger_rpm_path")
    echo "✓ RPM 文件就绪: rpms/${core_rpm_filename}, rpms/${trigger_rpm_filename}"

    # ── 5e: JobLens 配置注入 ──
    echo "--- 5e: JobLens 配置注入 ---"
    if [[ ! -f "$CORE_CONFIG_PATH" ]]; then
        echo "FATAL: Core 配置文件不存在 — ${CORE_CONFIG_PATH}" >&2
        exit 1
    fi
    if [[ ! -f "$TRIGGER_CONFIG_PATH" ]]; then
        echo "FATAL: Trigger 配置文件不存在 — ${TRIGGER_CONFIG_PATH}" >&2
        exit 1
    fi
    cp "${CORE_CONFIG_PATH}" "${runtime_dir}/joblens_core.yaml"
    cp "${TRIGGER_CONFIG_PATH}" "${runtime_dir}/joblens_trigger.yaml"
    echo "✓ 配置文件就绪: .runtime/joblens_core.yaml, .runtime/joblens_trigger.yaml"

    # ── 5f: JobLens 部署 ──
    echo "--- 5f: JobLens 部署 ---"
    vagrant ssh worker -c "sudo bash /vagrant/provisioning/joblens/deploy.sh --rpm-path=/vagrant/rpms/${core_rpm_filename} --trigger-rpm-path=/vagrant/rpms/${trigger_rpm_filename} --core-config=/vagrant/.runtime/joblens_core.yaml --trigger-config=/vagrant/.runtime/joblens_trigger.yaml" || {
        echo "FATAL: joblens/deploy.sh 失败" >&2
        exit 1
    }
    echo "✓ JobLens 部署完成"

    # ═══════════════════════════════════════════════════════════════
    # Phase 6: Demo job 预检
    # ═══════════════════════════════════════════════════════════════
    echo "=== Phase ${phase}/9: Demo job 预检 ==="
    phase=$((phase + 1))

    run_demo_preflight && demo_failed=0 || demo_failed=1
    if [[ "$demo_failed" -eq 0 ]]; then
        echo "✓ demo 预检完成"
    else
        echo "⚠ demo 预检失败 (非致命), 继续后续阶段" >&2
    fi

    # ═══════════════════════════════════════════════════════════════
    # Phase 7: 运行测试
    # ═══════════════════════════════════════════════════════════════
    echo "=== Phase ${phase}/9: 运行测试 ==="
    phase=$((phase + 1))

    local test_failed=0
    if [[ "${demo_failed:-0}" -eq 1 ]]; then
        echo "⚠ demo 预检失败，跳过 pytest" >&2
    elif [[ -z "$PYTEST_FILES" ]]; then
        echo "⚠ 警告: 预设中未定义 pytest_files, 跳过测试" >&2
    else
        echo "  pytest ${PYTEST_FILES} ${PYTEST_ARGS} --skip-vagrant-up --skip-vagrant-destroy"
        cd "$SCRIPT_DIR"
        # shellcheck disable=SC2086
        pytest --skip-vagrant-up --skip-vagrant-destroy ${PYTEST_ARGS} ${PYTEST_FILES} || {
            test_failed=1
            echo "FATAL: pytest 测试失败 — 将在清理后退出" >&2
        }
        if [[ "$test_failed" -eq 0 ]]; then
            echo "✓ 测试全部通过"
        fi
    fi

    # ═══════════════════════════════════════════════════════════════
    # Phase 8: 清理
    # ═══════════════════════════════════════════════════════════════
    echo "=== Phase ${phase}/9: 清理 ==="
    phase=$((phase + 1))

    if [[ "${KEEP_VMS:-0}" == "1" ]]; then
        echo "KEEP_VMS=1 — 跳过 VM 销毁"
    else
        cd "$SCRIPT_DIR"
        vagrant destroy -f || {
            echo "⚠ 警告: vagrant destroy 失败 (可能 VM 已被手动销毁)" >&2
        }
        echo "✓ VM 已销毁"
    fi

    # 若测试失败, 在清理后退出非 0
    if [[ "$test_failed" -ne 0 ]]; then
        echo "FATAL: 测试阶段失败, 请检查上述错误" >&2
        exit 1
    fi

    echo ""
    echo "══════════════════════════════════════════"
    echo "  run_preset '${preset_name}' 完成 ✓"
    echo "══════════════════════════════════════════"
}

# ─────────────────────────────────────────────────────────────────────
# 直接执行脚本时的命令路由
# ─────────────────────────────────────────────────────────────────────
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    case "${1:-}" in
        --list)
            find_presets --list
            ;;
        --match)
            shift
            find_presets --match "$@"
            ;;
        validate)
            shift
            validate_preset "$@"
            ;;
        *)
            run_preset "$@"
            ;;
    esac
fi

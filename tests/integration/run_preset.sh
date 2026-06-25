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
            find_presets "$@"
            ;;
    esac
fi

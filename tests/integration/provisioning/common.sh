#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# JobLens 集成测试 — 通用 VM 初始化脚本 (AlmaLinux 9)
# 用法: sudo bash common.sh --hostname=<name> --role=<controller|worker> --nodes-json='[...]'
# ============================================================================

# ---- 默认值 ----
HOSTNAME=""
ROLE=""
NODES_JSON=""
OS_TYPE="almalinux9"
DRY_RUN=false

# ---- 打印帮助信息 ----
print_help() {
    cat << 'HELP'
JobLens 集成测试 — 通用 VM 初始化脚本

用法: sudo bash common.sh [选项]

必填参数:
  --hostname <name>        节点主机名
  --role <role>            节点角色: controller | worker
  --nodes-json <json>      所有节点 JSON 数组，格式:
                           '[{"host":"controller","ip":"192.168.56.10"},{"host":"worker","ip":"192.168.56.20"}]'

可选参数:
  --os-type <type>         OS 类型 (默认: almalinux9)
  --dry-run                仅打印将要执行的操作，不实际修改系统
  --help                   显示此帮助信息

功能:
  1. 设置主机名 (hostnamectl)
  2. 从 --nodes-json 动态生成 /etc/hosts
  3. 禁用 SELinux
  4. 停止并禁用 firewalld
  5. 安装 EPEL 仓库并启用 CRB
  6. 安装基础软件包 (vim, curl, wget, git, python3, bpftool)
  7. 设置时区为 Asia/Shanghai
  8. 验证 BTF 支持 (eBPF 依赖，FATAL 失败)
  9. 验证 eBPF 特性探测 (非致命)
  10. 生成 root ED25519 SSH 密钥对 (VM 间 SCP 通信)

示例:
  # controller 节点
  sudo bash common.sh \
    --hostname=controller \
    --role=controller \
    --nodes-json='[{"host":"controller","ip":"192.168.56.10"},{"host":"worker","ip":"192.168.56.20"}]'

  # worker 节点
  sudo bash common.sh \
    --hostname=worker \
    --role=worker \
    --nodes-json='[{"host":"controller","ip":"192.168.56.10"},{"host":"worker","ip":"192.168.56.20"}]'

  # 预览模式 (不实际执行)
  sudo bash common.sh --hostname=controller --role=controller \
    --nodes-json='[{"host":"controller","ip":"192.168.56.10"}]' --dry-run
HELP
}

# ---- 参数解析 ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --hostname=*)   HOSTNAME="${1#*=}"; shift ;;
        --hostname)     HOSTNAME="$2"; shift 2 ;;
        --role=*)       ROLE="${1#*=}"; shift ;;
        --role)         ROLE="$2"; shift 2 ;;
        --nodes-json=*) NODES_JSON="${1#*=}"; shift ;;
        --nodes-json)   NODES_JSON="$2"; shift 2 ;;
        --os-type=*)    OS_TYPE="${1#*=}"; shift ;;
        --os-type)      OS_TYPE="$2"; shift 2 ;;
        --dry-run)      DRY_RUN=true; shift ;;
        --help)         print_help; exit 0 ;;
        -h)             print_help; exit 0 ;;
        *)              echo "未知参数: $1" >&2; print_help; exit 1 ;;
    esac
done

# ---- 无参数调用: 打印 help 并退出 ----
if [[ -z "$HOSTNAME" && -z "$ROLE" && -z "$NODES_JSON" ]]; then
    print_help
    exit 1
fi

# ---- 参数校验 ----
if [[ -z "$HOSTNAME" ]]; then
    echo "FATAL: 缺少必填参数 --hostname" >&2
    print_help
    exit 1
fi
if [[ -z "$ROLE" ]]; then
    echo "FATAL: 缺少必填参数 --role" >&2
    print_help
    exit 1
fi
if [[ -z "$NODES_JSON" ]]; then
    echo "FATAL: 缺少必填参数 --nodes-json" >&2
    print_help
    exit 1
fi
if [[ "$ROLE" != "controller" && "$ROLE" != "worker" ]]; then
    echo "FATAL: --role 必须是 controller 或 worker，当前值: ${ROLE}" >&2
    exit 1
fi

# ---- 检查 root 权限 (dry-run 模式跳过) ----
if [[ "$DRY_RUN" == false && "$EUID" -ne 0 ]]; then
    echo "FATAL: 此脚本需要 root 权限运行 (使用 sudo)"
    exit 1
fi

echo "============================================"
echo "  JobLens 通用 VM 初始化: ${HOSTNAME} (角色: ${ROLE})"
if [[ "$DRY_RUN" == true ]]; then
    echo "  [DRY-RUN 模式] 仅预览，不执行实际操作"
fi
echo "============================================"

# ---- 1. 设置主机名 ----
echo "==> 设置主机名为 ${HOSTNAME}"
if [[ "$DRY_RUN" == true ]]; then
    echo "   [DRY-RUN] hostnamectl set-hostname ${HOSTNAME}"
else
    hostnamectl set-hostname "${HOSTNAME}"
fi

# ---- 2. 从 --nodes-json 动态生成 /etc/hosts (幂等) ----
echo "==> 从 --nodes-json 生成 /etc/hosts"
if ! command -v python3 &>/dev/null; then
    echo "FATAL: 需要 python3 来解析 --nodes-json，请先安装 python3"
    exit 1
fi

# 用 python3 解析 JSON 并输出 "ip host" 行
HOSTS_ENTRIES=$(python3 -c "
import json, sys
try:
    nodes = json.loads(sys.argv[1])
    for n in nodes:
        print(f'{n[\"ip\"]} {n[\"host\"]}')
except Exception as e:
    print(f'FATAL: 解析 --nodes-json 失败: {e}', file=sys.stderr)
    sys.exit(1)
" "${NODES_JSON}") || exit 1

echo "   解析到的节点:"
echo "${HOSTS_ENTRIES}" | while read -r entry; do
    echo "     ${entry}"
done

if [[ "$DRY_RUN" == true ]]; then
    echo "   [DRY-RUN] 将写入以下条目到 /etc/hosts:"
    echo "${HOSTS_ENTRIES}" | while read -r entry; do
        echo "     ${entry}"
    done
else
    while IFS= read -r entry; do
        if ! grep -qF "${entry}" /etc/hosts 2>/dev/null; then
            echo "${entry}" >> /etc/hosts
            echo "   添加: ${entry}"
        else
            echo "   已存在，跳过: ${entry}"
        fi
    done <<< "${HOSTS_ENTRIES}"
fi

# ---- 3. 禁用 SELinux ----
echo "==> 禁用 SELinux"
if [[ "$DRY_RUN" == true ]]; then
    echo "   [DRY-RUN] setenforce 0"
    echo "   [DRY-RUN] sed -i 's/^SELINUX=.*/SELINUX=disabled/' /etc/selinux/config"
else
    setenforce 0 2>/dev/null || echo "   WARNING: setenforce 失败 (可能已禁用)"
    if [ -f /etc/selinux/config ]; then
        sed -i 's/^SELINUX=.*/SELINUX=disabled/' /etc/selinux/config
        echo "   /etc/selinux/config 已更新为 SELINUX=disabled"
    else
        echo "   WARNING: /etc/selinux/config 不存在，跳过"
    fi
fi

# ---- 4. 停止并禁用 firewalld ----
echo "==> 停止并禁用 firewalld"
if [[ "$DRY_RUN" == true ]]; then
    echo "   [DRY-RUN] systemctl stop firewalld"
    echo "   [DRY-RUN] systemctl disable firewalld"
else
    systemctl stop firewalld 2>/dev/null || echo "   INFO: firewalld 未运行"
    systemctl disable firewalld 2>/dev/null || echo "   INFO: firewalld 未启用"
fi

# ---- 5. 安装 EPEL 仓库 ----
echo "==> 安装 EPEL 仓库"
if [[ "$DRY_RUN" == true ]]; then
    echo "   [DRY-RUN] dnf install -y epel-release"
    echo "   [DRY-RUN] dnf config-manager --set-enabled crb"
else
    if ! rpm -q epel-release &>/dev/null; then
        dnf install -y epel-release
        echo "   EPEL 安装完成"
    else
        echo "   EPEL 已安装，跳过"
    fi

    # 启用 CRB (CodeReady Builder) — EPEL 包的间接依赖需要
    echo "==> 启用 CRB 仓库"
    dnf config-manager --set-enabled crb 2>/dev/null || \
      dnf config-manager --set-enabled powertools 2>/dev/null || \
      echo "   WARNING: 无法启用 CRB/powertools (可能已启用或不存在)"
fi

# ---- 6. 安装基础软件包 ----
echo "==> 安装基础软件包"
BASE_PACKAGES=(
    vim
    curl
    wget
    git
    python3
    python3-pip
    bpftool
)
if [[ "$DRY_RUN" == true ]]; then
    echo "   [DRY-RUN] dnf install -y ${BASE_PACKAGES[*]}"
else
    dnf install -y "${BASE_PACKAGES[@]}"
fi

# ---- 7. 设置时区 ----
echo "==> 设置时区为 Asia/Shanghai"
if [[ "$DRY_RUN" == true ]]; then
    echo "   [DRY-RUN] timedatectl set-timezone Asia/Shanghai"
else
    timedatectl set-timezone Asia/Shanghai
    echo "   当前时区: $(timedatectl show --property=Timezone --value)"
fi

# ---- 8. 验证 BTF 支持 (FATAL) ----
echo "==> 验证 BTF 支持"
if [[ "$DRY_RUN" == true ]]; then
    echo "   [DRY-RUN] 跳过 BTF 检查 (dry-run 模式)"
else
    if ls /sys/kernel/btf/vmlinux &>/dev/null; then
        echo "   PASS: /sys/kernel/btf/vmlinux 存在"
    else
        echo "FATAL: BTF 不可用 — /sys/kernel/btf/vmlinux 不存在"
        echo "  请确保内核编译时启用了 CONFIG_DEBUG_INFO_BTF=y"
        exit 1
    fi
fi

# ---- 9. 验证 eBPF 特性探测 (NON-FATAL) ----
echo "==> 验证 eBPF 特性探测"
if [[ "$DRY_RUN" == true ]]; then
    echo "   [DRY-RUN] 跳过 eBPF 特性探测 (dry-run 模式)"
else
    set +e
    BPF_OUTPUT=$(bpftool feature probe 2>&1)
    BPF_EXIT=$?
    set -e

    if [ "${BPF_EXIT}" -eq 0 ] && echo "${BPF_OUTPUT}" | grep -qE "bpf|tracepoint|ringbuf"; then
        echo "   PASS: eBPF 特性探测正常"
        echo "${BPF_OUTPUT}" | grep -E "bpf|tracepoint|ringbuf" || true
    else
        echo "   WARNING: eBPF 特性探测未找到预期关键字 (非致命)"
        if [ "${BPF_EXIT}" -ne 0 ]; then
            echo "   bpftool 退出码: ${BPF_EXIT}"
        fi
        echo "   输出摘要:"
        echo "${BPF_OUTPUT}" | head -20 || true
    fi
fi

# ---- 10. 配置 root SSH (VM 之间 SCP 通信) ----
echo "==> 配置 root SSH (生成 root 专用密钥对)"
if [[ "$DRY_RUN" == true ]]; then
    echo "   [DRY-RUN] mkdir -p /root/.ssh"
    echo "   [DRY-RUN] ssh-keygen -t ed25519 -N \"\" -f /root/.ssh/id_ed25519 -C \"root@${HOSTNAME}\""
    echo "   [DRY-RUN] cat /root/.ssh/id_ed25519.pub >> /root/.ssh/authorized_keys"
    echo "   [DRY-RUN] cat /home/vagrant/.ssh/authorized_keys >> /root/.ssh/authorized_keys"
    echo "   [DRY-RUN] chmod 700 /root/.ssh"
    echo "   [DRY-RUN] chmod 600 /root/.ssh/id_ed25519"
    echo "   [DRY-RUN] chmod 644 /root/.ssh/id_ed25519.pub"
    echo "   [DRY-RUN] chmod 600 /root/.ssh/authorized_keys"
else
    mkdir -p /root/.ssh

    # 生成 root 专用 ED25519 密钥对 (幂等: 已存在则跳过)
    if [ ! -f /root/.ssh/id_ed25519 ]; then
      ssh-keygen -t ed25519 -N "" -f /root/.ssh/id_ed25519 -C "root@${HOSTNAME}"
      echo "   root ED25519 密钥对已生成"
    else
      echo "   root 密钥对已存在，跳过生成"
    fi

    # 将自己的公钥加入 authorized_keys (允许自己登录自己，也允许其他 VM 的 root 登录)
    cat /root/.ssh/id_ed25519.pub >> /root/.ssh/authorized_keys 2>/dev/null || true

    # 也追加 vagrant 的公钥（允许 vagrant 用户通过其密钥登录 root）
    if [ -f /home/vagrant/.ssh/authorized_keys ]; then
      cat /home/vagrant/.ssh/authorized_keys >> /root/.ssh/authorized_keys 2>/dev/null || true
    fi

    chmod 700 /root/.ssh
    chmod 600 /root/.ssh/id_ed25519      2>/dev/null || true
    chmod 644 /root/.ssh/id_ed25519.pub  2>/dev/null || true
    chmod 600 /root/.ssh/authorized_keys 2>/dev/null || true
    echo "   root SSH 已配置 (VM 间 SCP 可用)"
fi

# ---- 完成 ----
echo "============================================"
echo "  通用初始化完成: ${HOSTNAME} (角色: ${ROLE})"
if [[ "$DRY_RUN" == true ]]; then
    echo "  [DRY-RUN] 以上为预览，未执行任何实际操作"
fi
echo "============================================"

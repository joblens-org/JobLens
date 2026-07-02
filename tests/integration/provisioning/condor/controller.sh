#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# HTCondor 控制器节点部署脚本 (参数化版本)
# 部署组件: master, collector, negotiator, schedd (不含 startd)
# 安全配置: 完全禁用认证/完整性/加密 (仅测试环境)
# =============================================================================

# ---------------------------------------------------------------------------
# 帮助信息
# ---------------------------------------------------------------------------
usage() {
    cat << 'USAGE'
用法: controller.sh --repo-url=<URL> --hostname=<NAME> --controller-ip=<IP> [--dry-run] [--help]

参数说明:
  --repo-url <url>         HTCondor repo RPM 完整 URL (必填)
  --hostname <name>        本节点主机名, 同时作为 CONDOR_HOST (必填)
  --controller-ip <ip>     控制器 IP, 用于推导 NETWORK_INTERFACE 子网 (必填)
  --dry-run                仅打印将要生成的配置和执行命令, 不实际修改系统
  --help                   显示本帮助信息

示例:
  bash controller.sh \\
      --repo-url=https://htcss-downloads.chtc.wisc.edu/repo/25.x/htcondor-release-current.el9.noarch.rpm \\
      --hostname=controller \\
      --controller-ip=192.168.56.10 \\
      --dry-run
USAGE
}

# ---------------------------------------------------------------------------
# 工具函数: 从完整 IP 推导子网通配符
# 例如: 192.168.56.10 → 192.168.56.*
# ---------------------------------------------------------------------------
derive_subnet() {
    local ip="$1"
    echo "${ip%.*}.*"
}

# =============================================================================
# 参数解析
# =============================================================================
REPO_URL=""
HOSTNAME=""
CONTROLLER_IP=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo-url=*)        REPO_URL="${1#*=}" ;;
        --repo-url)          REPO_URL="$2"; shift ;;
        --hostname=*)        HOSTNAME="${1#*=}" ;;
        --hostname)          HOSTNAME="$2"; shift ;;
        --controller-ip=*)   CONTROLLER_IP="${1#*=}" ;;
        --controller-ip)     CONTROLLER_IP="$2"; shift ;;
        --dry-run)           DRY_RUN=true ;;
        --help)              usage; exit 0 ;;
        *)                   echo "错误: 未知参数 '$1'" >&2; usage >&2; exit 1 ;;
    esac
    shift
done

# =============================================================================
# 参数校验
# =============================================================================
missing=()
[[ -z "$REPO_URL"      ]] && missing+=("--repo-url")
[[ -z "$HOSTNAME"      ]] && missing+=("--hostname")
[[ -z "$CONTROLLER_IP" ]] && missing+=("--controller-ip")

if [[ ${#missing[@]} -gt 0 ]]; then
    echo "错误: 缺少必填参数: ${missing[*]}" >&2
    usage >&2
    exit 1
fi

NETWORK_PATTERN="$(derive_subnet "$CONTROLLER_IP")"

# =============================================================================
# dry-run 模式: 打印配置后退出, 不做任何系统修改
# =============================================================================
if $DRY_RUN; then
    echo "==> [DRY-RUN] 将使用以下参数:"
    echo "    REPO_URL        = ${REPO_URL}"
    echo "    HOSTNAME        = ${HOSTNAME}"
    echo "    CONTROLLER_IP   = ${CONTROLLER_IP}"
    echo "    NETWORK_PATTERN = ${NETWORK_PATTERN}"
    echo ""
    echo "==> [DRY-RUN] 将写入 /etc/condor/config.d/99-test-cluster.conf:"
    cat << DRYEOF
# 测试集群 — 控制器节点 (最简配置)
DAEMON_LIST = MASTER, COLLECTOR, NEGOTIATOR, SCHEDD
CONDOR_HOST = ${HOSTNAME}

# 测试环境: 允许所有来源
ALLOW_WRITE = *
ALLOW_READ  = *
ALLOW_ADVERTISE_STARTD = *
ALLOW_ADVERTISE_SCHEDD = *
ALLOW_ADVERTISE_MASTER = *

# 关闭所有安全 (SEC_DEFAULT 不够, 需要逐级显式设置)
SEC_DEFAULT_AUTHENTICATION = NEVER
SEC_READ_AUTHENTICATION = NEVER
SEC_WRITE_AUTHENTICATION = NEVER
SEC_ADVERTISE_STARTD_AUTHENTICATION = NEVER
SEC_ADVERTISE_SCHEDD_AUTHENTICATION = NEVER
SEC_ADVERTISE_MASTER_AUTHENTICATION = NEVER
SEC_DEFAULT_ENCRYPTION = NEVER
SEC_DEFAULT_INTEGRITY = NEVER

# 禁用 shared_port (测试池不需要)
USE_SHARED_PORT = False
DRYEOF
    echo ""
    echo "==> [DRY-RUN] 将写入 /etc/condor/config.d/50-network.conf:"
    cat << DRYEOF
# 使用 IP 模式而非接口名
# HTCondor 会自动选择匹配 ${NETWORK_PATTERN} 的接口来通信和广播
NETWORK_INTERFACE = ${NETWORK_PATTERN}
DRYEOF
    echo ""
    echo "==> [DRY-RUN] 将执行: hostnamectl set-hostname ${HOSTNAME}"
    echo "==> [DRY-RUN] 将执行: dnf install -y ${REPO_URL}"
    echo "==> [DRY-RUN] 将执行: dnf install -y condor"
    echo "==> [DRY-RUN] 将执行: systemctl enable --now condor"
    echo "==> [DRY-RUN] 然后将等待 HTCondor 守护进程上线 (最长 60 秒) 并输出诊断信息"
    exit 0
fi

# =============================================================================
# 实际部署: controller 节点
# =============================================================================

echo "==> 设置主机名: ${HOSTNAME}..."
hostnamectl set-hostname "${HOSTNAME}"

echo "==> 添加 HTCondor 仓库 (${REPO_URL})..."
dnf install -y dnf-plugins-core
dnf config-manager --set-enabled crb 2>/dev/null || true
dnf install -y "${REPO_URL}"

echo "==> 安装 HTCondor 软件包..."
dnf install -y condor

echo "==> 确保配置目录存在..."
mkdir -p /etc/condor/config.d

echo "==> 写入 HTCondor 集群配置 (99-test-cluster.conf)..."
cat > /etc/condor/config.d/99-test-cluster.conf << EOF
# 测试集群 — 控制器节点 (最简配置)
DAEMON_LIST = MASTER, COLLECTOR, NEGOTIATOR, SCHEDD
CONDOR_HOST = ${HOSTNAME}

# 测试环境: 允许所有来源
ALLOW_WRITE = *
ALLOW_READ  = *
ALLOW_ADVERTISE_STARTD = *
ALLOW_ADVERTISE_SCHEDD = *
ALLOW_ADVERTISE_MASTER = *

# 关闭所有安全 (SEC_DEFAULT 不够, 需要逐级显式设置)
SEC_DEFAULT_AUTHENTICATION = NEVER
SEC_READ_AUTHENTICATION = NEVER
SEC_WRITE_AUTHENTICATION = NEVER
SEC_ADVERTISE_STARTD_AUTHENTICATION = NEVER
SEC_ADVERTISE_SCHEDD_AUTHENTICATION = NEVER
SEC_ADVERTISE_MASTER_AUTHENTICATION = NEVER
SEC_DEFAULT_ENCRYPTION = NEVER
SEC_DEFAULT_INTEGRITY = NEVER

# 禁用 shared_port (测试池不需要)
USE_SHARED_PORT = False
EOF

echo "==> 写入 HTCondor 网络配置 (50-network.conf)..."
cat > /etc/condor/config.d/50-network.conf << EOF
# 使用 IP 模式而非接口名 (eth1 在 AlmaLinux 9 可能是 ens4/ens5)
# HTCondor 会自动选择匹配 ${NETWORK_PATTERN} 的接口来通信和广播
NETWORK_INTERFACE = ${NETWORK_PATTERN}
EOF

echo "==> 启用并启动 HTCondor 服务..."
systemctl enable --now condor

echo "==> 诊断信息 ==="
echo "网络接口:"
ip addr show | grep -E 'inet |eth|ens' || true
echo "HTCondor 使用的接口:"
condor_config_val NETWORK_INTERFACE 2>/dev/null || true
echo "CONDOR_HOST:"
condor_config_val CONDOR_HOST 2>/dev/null || true
echo "DAEMON_LIST:"
condor_config_val DAEMON_LIST 2>/dev/null || true
echo "监听端口:"
ss -tlnp | grep condor || true
echo "==> 诊断结束 =="

echo "==> 等待 HTCondor 守护进程上线 (最长 60 秒)..."
for i in $(seq 1 12); do
    sleep 5
    if condor_status -schedd &>/dev/null && condor_status -negotiator &>/dev/null; then
        echo "==> HTCondor 控制器守护进程已全部在线"
        exit 0
    fi
    echo "    轮询 ${i}/12: 等待 schedd 和 negotiator 上线..."
done

echo "ERROR: HTCondor 控制器在 60 秒内未就绪"
condor_status -any 2>&1 || true
exit 1

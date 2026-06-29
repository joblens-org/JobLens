#!/usr/bin/env bash
# HTCondor worker provisioning — 在 Worker 节点部署 master + startd (参数化版本)
set -euo pipefail

# ---------------------------------------------------------------------------
# 帮助信息
# ---------------------------------------------------------------------------
usage() {
    cat << 'USAGE'
用法: worker.sh --repo-url=<URL> --hostname=<NAME> --controller-host=<HOST> --controller-ip=<IP> --worker-ip=<IP> [--dry-run] [--help]

参数说明:
  --repo-url <url>           HTCondor repo RPM 完整 URL (必填)
  --hostname <name>          本节点主机名 (必填)
  --controller-host <host>   控制器主机名, 用于 CONDOR_HOST 和连通性检查 (必填)
  --controller-ip <ip>       控制器 IP, 用于连通性检查 (必填)
  --worker-ip <ip>           Worker IP, 用于推导 NETWORK_INTERFACE 子网 (必填)
  --dry-run                  仅打印将要生成的配置和执行命令, 不实际修改系统
  --help                     显示本帮助信息

示例:
  bash worker.sh \\
      --repo-url=https://htcss-downloads.chtc.wisc.edu/repo/25.x/htcondor-release-current.el9.noarch.rpm \\
      --hostname=worker \\
      --controller-host=controller \\
      --controller-ip=192.168.56.10 \\
      --worker-ip=192.168.56.20 \\
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
CONTROLLER_HOST=""
CONTROLLER_IP=""
WORKER_IP=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo-url=*)         REPO_URL="${1#*=}" ;;
        --repo-url)           REPO_URL="$2"; shift ;;
        --hostname=*)         HOSTNAME="${1#*=}" ;;
        --hostname)           HOSTNAME="$2"; shift ;;
        --controller-host=*)  CONTROLLER_HOST="${1#*=}" ;;
        --controller-host)    CONTROLLER_HOST="$2"; shift ;;
        --controller-ip=*)    CONTROLLER_IP="${1#*=}" ;;
        --controller-ip)      CONTROLLER_IP="$2"; shift ;;
        --worker-ip=*)        WORKER_IP="${1#*=}" ;;
        --worker-ip)          WORKER_IP="$2"; shift ;;
        --dry-run)            DRY_RUN=true ;;
        --help)               usage; exit 0 ;;
        *)                    echo "错误: 未知参数 '$1'" >&2; usage >&2; exit 1 ;;
    esac
    shift
done

# =============================================================================
# 参数校验
# =============================================================================
missing=()
[[ -z "$REPO_URL"        ]] && missing+=("--repo-url")
[[ -z "$HOSTNAME"        ]] && missing+=("--hostname")
[[ -z "$CONTROLLER_HOST" ]] && missing+=("--controller-host")
[[ -z "$CONTROLLER_IP"   ]] && missing+=("--controller-ip")
[[ -z "$WORKER_IP"       ]] && missing+=("--worker-ip")

if [[ ${#missing[@]} -gt 0 ]]; then
    echo "错误: 缺少必填参数: ${missing[*]}" >&2
    usage >&2
    exit 1
fi

NETWORK_PATTERN="$(derive_subnet "$WORKER_IP")"

# =============================================================================
# dry-run 模式: 打印配置后退出, 不做任何系统修改
# =============================================================================
if $DRY_RUN; then
    echo "==> [DRY-RUN] 将使用以下参数:"
    echo "    REPO_URL         = ${REPO_URL}"
    echo "    HOSTNAME         = ${HOSTNAME}"
    echo "    CONTROLLER_HOST  = ${CONTROLLER_HOST}"
    echo "    CONTROLLER_IP    = ${CONTROLLER_IP}"
    echo "    WORKER_IP        = ${WORKER_IP}"
    echo "    NETWORK_PATTERN  = ${NETWORK_PATTERN}"
    echo ""
    echo "==> [DRY-RUN] 将写入 /etc/condor/config.d/99-test-cluster.conf:"
    cat << DRYEOF
# 测试集群 — Worker 节点 (最简配置)
DAEMON_LIST = MASTER, STARTD
CONDOR_HOST = ${CONTROLLER_HOST}

# 测试环境: 允许所有来源
ALLOW_WRITE = *
ALLOW_READ  = *
ALLOW_ADVERTISE_STARTD = *
ALLOW_ADVERTISE_MASTER = *

# 关闭所有安全 (逐级显式)
SEC_DEFAULT_AUTHENTICATION = NEVER
SEC_READ_AUTHENTICATION = NEVER
SEC_WRITE_AUTHENTICATION = NEVER
SEC_ADVERTISE_STARTD_AUTHENTICATION = NEVER
SEC_ADVERTISE_MASTER_AUTHENTICATION = NEVER
SEC_DEFAULT_ENCRYPTION = NEVER
SEC_DEFAULT_INTEGRITY = NEVER

# 接受所有作业
START = TRUE

# 禁用 shared_port
USE_SHARED_PORT = False
DRYEOF
    echo ""
    echo "==> [DRY-RUN] 将写入 /etc/condor/config.d/50-network.conf:"
    cat << DRYEOF
# 使用 IP 模式匹配 private_network 接口
# (eth1 在不同系统上可能叫 ens4/ens5)
NETWORK_INTERFACE = ${NETWORK_PATTERN}
DRYEOF
    echo ""
    echo "==> [DRY-RUN] 将执行: hostnamectl set-hostname ${HOSTNAME}"
    echo "==> [DRY-RUN] 将执行: dnf install -y ${REPO_URL}"
    echo "==> [DRY-RUN] 将执行: dnf install -y condor"
    echo "==> [DRY-RUN] 将执行: systemctl enable --now condor"
    echo "==> [DRY-RUN] 然后将执行连通性检查 (${CONTROLLER_HOST}/${CONTROLLER_IP}) + 诊断 + 等待 startd 就绪"
    exit 0
fi

# =============================================================================
# 实际部署: worker 节点
# =============================================================================

echo "[worker] 设置主机名: ${HOSTNAME}..."
hostnamectl set-hostname "${HOSTNAME}"

echo "[worker] 添加 HTCondor 仓库 (${REPO_URL})..."
dnf install -y dnf-plugins-core
dnf config-manager --set-enabled crb 2>/dev/null || true
dnf install -y "${REPO_URL}"

echo "[worker] 安装 HTCondor 软件包..."
dnf install -y condor

echo "[worker] 创建 99-test-cluster.conf (worker 角色)..."
cat > /etc/condor/config.d/99-test-cluster.conf << EOF
# 测试集群 — Worker 节点 (最简配置)
DAEMON_LIST = MASTER, STARTD
CONDOR_HOST = ${CONTROLLER_HOST}

# 测试环境: 允许所有来源
ALLOW_WRITE = *
ALLOW_READ  = *
ALLOW_ADVERTISE_STARTD = *
ALLOW_ADVERTISE_MASTER = *

# 关闭所有安全 (逐级显式)
SEC_DEFAULT_AUTHENTICATION = NEVER
SEC_READ_AUTHENTICATION = NEVER
SEC_WRITE_AUTHENTICATION = NEVER
SEC_ADVERTISE_STARTD_AUTHENTICATION = NEVER
SEC_ADVERTISE_MASTER_AUTHENTICATION = NEVER
SEC_DEFAULT_ENCRYPTION = NEVER
SEC_DEFAULT_INTEGRITY = NEVER

# 接受所有作业
START = TRUE

# 禁用 shared_port
USE_SHARED_PORT = False
EOF

echo "[worker] 创建 50-network.conf (匹配 ${NETWORK_PATTERN} 接口)..."
cat > /etc/condor/config.d/50-network.conf << EOF
# 使用 IP 模式匹配 private_network 接口
# (eth1 在不同系统上可能叫 ens4/ens5)
NETWORK_INTERFACE = ${NETWORK_PATTERN}
EOF

echo "[worker] 启用并启动 condor 服务..."
systemctl enable --now condor

echo "[worker] === 诊断信息 ==="
echo "[worker] 1) 网络接口与IP:"
ip addr show | grep -E 'inet |eth|ens' || true
echo "[worker] 2) DNS解析 ${CONTROLLER_HOST}:"
getent hosts "${CONTROLLER_HOST}" || echo "  FAIL: ${CONTROLLER_HOST} 无法解析"
echo "[worker] 3) Ping ${CONTROLLER_HOST} 连通性:"
ping -c 2 -W 3 "${CONTROLLER_HOST}" 2>&1 || echo "  FAIL: ping 不通 ${CONTROLLER_HOST}"
echo "[worker] 4) TCP端口 9618 连通性 (${CONTROLLER_HOST}):"
timeout 5 bash -c "echo >/dev/tcp/${CONTROLLER_HOST}/9618" 2>&1 \
  && echo "  OK: 9618 可达" || echo "  FAIL: 9618 不可达"
echo "[worker] 5) HTCondor 配置:"
condor_config_val NETWORK_INTERFACE 2>/dev/null || true
condor_config_val CONDOR_HOST 2>/dev/null || true
echo "[worker] 6) HTCondor 日志 (全部, 最近 50 行):"
journalctl -u condor --no-pager -n 50 2>/dev/null || true
echo "[worker] 6b) MasterLog (前 30 行):"
cat /var/log/condor/MasterLog 2>/dev/null | head -30 || true
echo "[worker] 6c) StartLog (前 30 行):"
cat /var/log/condor/StartLog 2>/dev/null | head -30 || true
echo "[worker] 6d) CollectorLog 存在?:"
ls -la /var/log/condor/ 2>/dev/null || true
echo "[worker] 7) 尝试直接连接 collector (${CONTROLLER_HOST}):"
condor_status -collector -direct "${CONTROLLER_HOST}" 2>&1 || true
echo "[worker] 7b) condor 进程列表:"
ps aux | grep -i condor | grep -v grep || echo "  WARNING: 无 condor 进程"
echo "[worker] 7c) DAEMON_LIST 配置:"
condor_config_val DAEMON_LIST 2>/dev/null || true
echo "[worker] === 诊断结束 ==="

echo "[worker] 等待 startd 就绪（最长 90s，需要连接远程 collector）..."
for i in $(seq 1 18); do
    if condor_status -startd 2>/dev/null | tail -n +4 | grep -q .; then
        echo "[worker] startd 就绪，当前可用 slot 数:"
        condor_status -startd
        exit 0
    fi
    echo "[worker] 等待中... (${i}/18)"
    sleep 5
done

echo "[worker] 错误: startd 在 90s 内未就绪" >&2
condor_status -startd 2>&1 || true
exit 1

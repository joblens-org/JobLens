#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Slurm 控制器部署 — EPEL 9 安装 (Slurm 22.05.9)
# 参考: https://packages.fedoraproject.org/pkgs/slurm/slurm/epel-9.html
# =============================================================================

usage() {
    cat << 'USAGE'
用法: controller.sh --rpm-dir <path> --hostname <name> --controller-ip <ip> --nodes-json <json> [选项]

Slurm 控制器部署脚本 — 安装 slurmctld + munge, 动态生成 slurm.conf, 启动服务。

必填参数:
  --rpm-dir <path>      Slurm 预构建 RPM 目录
  --hostname <name>     本节点主机名 (用作 ClusterName 和 SlurmctldHost)
  --controller-ip <ip>  控制器 IP 地址 (写入 SlurmctldAddr)
  --nodes-json <json>   所有计算节点 JSON 数组
                        格式: [{"host":"name","ip":"addr","cpus":N},...]

可选参数:
  --help, -h            显示此帮助信息
  --dry-run             仅生成并打印 slurm.conf, 不安装任何包或启动服务

示例:
  controller.sh --rpm-dir=/tmp/slurm-rpms --hostname=controller \\
    --controller-ip=192.168.56.10 \\
    --nodes-json='[{"host":"worker","ip":"192.168.56.20","cpus":2}]'
USAGE
}

# ---- 参数解析 ----
RPM_DIR=""
HOSTNAME=""
CONTROLLER_IP=""
NODES_JSON=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --rpm-dir=*)
            RPM_DIR="${1#*=}"
            shift
            ;;
        --rpm-dir)
            RPM_DIR="$2"
            shift 2
            ;;
        --hostname=*)
            HOSTNAME="${1#*=}"
            shift
            ;;
        --hostname)
            HOSTNAME="$2"
            shift 2
            ;;
        --controller-ip=*)
            CONTROLLER_IP="${1#*=}"
            shift
            ;;
        --controller-ip)
            CONTROLLER_IP="$2"
            shift 2
            ;;
        --nodes-json=*)
            NODES_JSON="${1#*=}"
            shift
            ;;
        --nodes-json)
            NODES_JSON="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "错误: 未知参数: $1" >&2
            usage
            exit 1
            ;;
    esac
done

# ---- 参数校验 ----
fatal() {
    echo "FATAL: $*" >&2
    exit 1
}

if [[ -z "${RPM_DIR}" ]]; then
    fatal "缺少 --rpm-dir 参数"
fi
if [[ ! -d "${RPM_DIR}" ]]; then
    fatal "RPM 目录不存在: ${RPM_DIR}"
fi
if [[ -z "${HOSTNAME}" ]]; then
    fatal "缺少 --hostname 参数"
fi
if [[ -z "${CONTROLLER_IP}" ]]; then
    fatal "缺少 --controller-ip 参数"
fi
if [[ -z "${NODES_JSON}" ]]; then
    fatal "缺少 --nodes-json 参数"
fi

# ---- 校验并解析 nodes-json ----
if ! echo "${NODES_JSON}" | python3 -c "import json, sys; json.load(sys.stdin)" 2>/dev/null; then
    fatal "--nodes-json 不是有效的 JSON: ${NODES_JSON}"
fi

# 检查每个节点是否有必需的 host 和 ip 字段
if ! echo "${NODES_JSON}" | python3 -c "
import json, sys
nodes = json.load(sys.stdin)
for n in nodes:
    if 'host' not in n or 'ip' not in n:
        print(f'节点缺少 host 或 ip 字段: {n}', file=sys.stderr)
        sys.exit(1)
" 2>/dev/null; then
    fatal "--nodes-json 中某些节点缺少 host 或 ip 字段"
fi

# ---- 从 nodes-json 生成 NodeName 行和节点列表 ----
NODENAME_LINES=$(echo "${NODES_JSON}" | python3 -c "
import json, sys
nodes = json.load(sys.stdin)
for n in nodes:
    host = n['host']
    ip = n['ip']
    cpus = n.get('cpus', 1)
    print(f'NodeName={host} NodeAddr={ip} CPUs={cpus} State=UNKNOWN')
")

NODE_LIST=$(echo "${NODES_JSON}" | python3 -c "
import json, sys
nodes = json.load(sys.stdin)
print(','.join(n['host'] for n in nodes))
")

# ---- slurm.conf 内容生成 ----
generate_slurm_conf() {
    cat << SLURM_EOF
ClusterName=${HOSTNAME}
SlurmctldHost=${HOSTNAME}
SlurmctldAddr=${CONTROLLER_IP}
SlurmUser=slurm
AuthType=auth/munge
CryptoType=crypto/munge
ReturnToService=1
SelectType=select/linear
SlurmctldPidFile=/var/run/slurmctld.pid
SlurmdPidFile=/var/run/slurmd.pid
SlurmdSpoolDir=/var/spool/slurmd
StateSaveLocation=/var/spool/slurmctld
SlurmctldLogFile=/var/log/slurm/slurmctld.log
SlurmdLogFile=/var/log/slurm/slurmd.log
${NODENAME_LINES}
PartitionName=debug Nodes=${NODE_LIST} Default=YES MaxTime=INFINITE State=UP
SLURM_EOF
}

# ---- dry-run 模式: 仅输出配置并退出 ----
if [[ "${DRY_RUN}" == "true" ]]; then
    echo "[dry-run] 参数汇总:"
    echo "  RPM 目录:    ${RPM_DIR}"
    echo "  主机名:      ${HOSTNAME}"
    echo "  IP:          ${CONTROLLER_IP}"
    echo "  计算节点数:  $(echo "${NODES_JSON}" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")"
    echo ""
    echo "[dry-run] 生成的 slurm.conf:"
    echo "============================================"
    generate_slurm_conf
    echo "============================================"
    exit 0
fi

# ---- 正式安装 ----
echo "[controller] 安装 Slurm 控制器 (EPEL 9)..."
echo "  主机名: ${HOSTNAME}"
echo "  IP: ${CONTROLLER_IP}"
echo "  RPM 目录: ${RPM_DIR}"
echo "  计算节点: ${NODE_LIST}"

# ---- 1. 确保 CRB + EPEL 可用 ----
dnf install -y dnf-plugins-core epel-release
dnf config-manager --set-enabled crb 2>/dev/null || true

# ---- 2. 安装 Slurm 控制器 + munge ----
dnf install -y slurm-slurmctld slurm-perlapi munge

# EPEL 包可能不会自动创建 slurm 用户，手动确保存在
if ! id slurm &>/dev/null; then
  useradd -r -s /bin/false -d /var/spool/slurm slurm
fi

# ---- 3. 生成 munge 认证密钥 ----
echo "[controller] 生成 munge key..."
mkdir -p /etc/munge
dd if=/dev/urandom bs=1 count=1024 > /etc/munge/munge.key
chmod 400 /etc/munge/munge.key
chown munge:munge /etc/munge/munge.key
systemctl enable --now munge

# ---- 4. 创建 spool 和日志目录 ----
mkdir -p /var/spool/slurmctld /var/spool/slurmd /var/log/slurm
chown slurm:slurm /var/spool/slurmctld /var/log/slurm
chmod 755 /var/spool/slurmctld

# ---- 5. 生成 slurm.conf ----
echo "[controller] 生成 /etc/slurm/slurm.conf..."
mkdir -p /etc/slurm
generate_slurm_conf > /etc/slurm/slurm.conf
echo "[controller] slurm.conf 已写入 /etc/slurm/slurm.conf"

# ---- 6. 启动 slurmctld ----
systemctl enable --now slurmctld

# ---- 7. 等待就绪 ----
echo "[controller] 等待 slurmctld 就绪..."
for i in $(seq 1 6); do
  sleep 5
  if sinfo 2>/dev/null | grep -q debug; then
    echo "[controller] slurmctld 就绪"
    sinfo
    exit 0
  fi
  echo "[controller] 第 ${i}/6 次检查..."
done

echo "[controller] slurmctld 未在 30s 内就绪 (worker 暂未注册, 正常)" >&2
sinfo 2>&1 || true

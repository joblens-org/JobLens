#!/usr/bin/env bash
# Slurm worker provisioning — EPEL 9 安装 slurmd + munge
# 参考: https://packages.fedoraproject.org/pkgs/slurm/slurm/epel-9.html
set -euo pipefail

# =============================================================================
# Slurm Worker 部署脚本
# 依赖 controller 先完成部署并将 munge.key / slurm.conf 注入到运行时目录
# =============================================================================

usage() {
    cat << 'USAGE'
用法: worker.sh --rpm-dir <path> --hostname <name> --controller-host <host> [选项]

Slurm Worker 部署脚本 — 安装 slurmd + munge, 从运行时目录读取密钥和配置, 验证注册。

必填参数:
  --rpm-dir <path>          Slurm 预构建 RPM 目录
  --hostname <name>         本节点主机名 (用于 sinfo 节点名称匹配)
  --controller-host <host>  slurmctld 控制器主机名 (用于连通性检查和 SSH)

可选参数:
  --runtime-dir <path>      运行时文件注入目录 (默认: /var/tmp/slurm_runtime)
  --help, -h                显示此帮助信息

示例:
  worker.sh --rpm-dir=/tmp/slurm-rpms --hostname=worker \\
    --controller-host=controller --runtime-dir=/var/tmp/slurm_runtime
USAGE
}

# ---- 参数解析 ----
RPM_DIR=""
HOSTNAME=""
CONTROLLER_HOST=""
RUNTIME_DIR="/var/tmp/slurm_runtime"

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
        --controller-host=*)
            CONTROLLER_HOST="${1#*=}"
            shift
            ;;
        --controller-host)
            CONTROLLER_HOST="$2"
            shift 2
            ;;
        --runtime-dir=*)
            RUNTIME_DIR="${1#*=}"
            shift
            ;;
        --runtime-dir)
            RUNTIME_DIR="$2"
            shift 2
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
if [[ -z "${CONTROLLER_HOST}" ]]; then
    fatal "缺少 --controller-host 参数"
fi

echo "============================================"
echo "  Slurm Worker 配置: ${HOSTNAME}"
echo "  slurmctld 主机:   ${CONTROLLER_HOST}"
echo "  运行时目录:       ${RUNTIME_DIR}"
echo "============================================"

# ---- 1. 安装 Slurm worker + munge ----
echo "==> 安装 slurm-slurmd + munge (EPEL 9)..."
dnf install -y dnf-plugins-core epel-release
dnf config-manager --set-enabled crb 2>/dev/null || true
dnf install -y slurm-slurmd munge

if ! id slurm &>/dev/null; then
  useradd -r -s /bin/false -d /var/spool/slurm slurm
fi

# ---- 2. 从运行时注入目录读取 controller 生成的 munge key ----
echo "==> 从 ${RUNTIME_DIR}/ 读取 munge key..."
if [ ! -f "${RUNTIME_DIR}/munge.key" ]; then
    echo "FATAL: 未找到 munge key — 请确保 controller 的 slurm 部署先于 worker 执行" >&2
    echo "  预期路径: ${RUNTIME_DIR}/munge.key" >&2
    echo "  提示: CI 环境通过 vagrant ssh 在 host 端提取后注入; 手动环境请参考 Makefile provision" >&2
    exit 1
fi
cp "${RUNTIME_DIR}/munge.key" /etc/munge/munge.key
chmod 400 /etc/munge/munge.key
chown munge:munge /etc/munge/munge.key
echo "   munge key 已安装并设置权限"

# ---- 3. 启动 munge ----
echo "==> 启动 munge..."
systemctl enable --now munge

# ---- 4. 验证本机 munge 认证 ----
echo "==> 验证 munge 认证..."
if munge -n | unmunge >/dev/null; then
    echo "   PASS: munge 认证正常"
else
    echo "FATAL: munge 认证失败" >&2
    exit 1
fi

# ---- 5. 从运行时注入目录读取 slurm.conf ----
echo "==> 从 ${RUNTIME_DIR}/ 读取 slurm.conf..."
if [ ! -f "${RUNTIME_DIR}/slurm.conf" ]; then
    echo "FATAL: 未找到 slurm.conf — 请确保 controller 的 slurm 部署先于 worker 执行" >&2
    echo "  预期路径: ${RUNTIME_DIR}/slurm.conf" >&2
    echo "  提示: CI 环境通过 vagrant ssh 在 host 端提取后注入; 手动环境请参考 Makefile provision" >&2
    exit 1
fi
cp "${RUNTIME_DIR}/slurm.conf" /etc/slurm/slurm.conf
echo "   slurm.conf 已安装"

# ---- 6. 创建 slurmd spool 和日志目录 ----
echo "==> 创建 slurmd 目录..."
mkdir -p /var/spool/slurmd /var/log/slurm
chown slurm:slurm /var/spool/slurmd /var/log/slurm
chmod 755 /var/spool/slurmd

# ---- 7. 启动 slurmd ----
echo "==> 启动 slurmd..."
systemctl enable --now slurmd

# ---- 8. 诊断: 检查 slurmd 状态和 controller 连通性 ----
echo "==> 诊断: slurmd + controller 连通性..."
echo "   slurmd 状态:"
systemctl status slurmd --no-pager -l 2>/dev/null || true
echo ""
echo "   slurmd 日志 (最近 20 行):"
journalctl -u slurmd --no-pager -n 20 2>/dev/null || true
echo ""
echo "   slurmctld 端口连通性 (${CONTROLLER_HOST}:6817):"
timeout 5 bash -c "echo >/dev/tcp/${CONTROLLER_HOST}/6817" 2>&1 \
  && echo "   PASS: slurmctld 可达" \
  || echo "   FAIL: slurmctld 不可达"
echo ""
echo "   当前 sinfo (controller 视角):"
sinfo 2>&1 || true

# ---- 9. 等待节点注册并进入 idle ----
echo "==> 等待 ${HOSTNAME} 注册到 slurmctld 并进入 idle (最长 60s)..."
NODE_READY=false
for i in $(seq 1 12); do
    NODE_STATE=$(sinfo -h -o "%t" -n "${HOSTNAME}" 2>/dev/null || true)
    if [ -z "${NODE_STATE}" ]; then
        echo "   等待中... (${i}/12) — 节点尚未出现在 sinfo 中"
    else
        echo "   等待中... (${i}/12) — 节点状态: ${NODE_STATE}"
        if echo "${NODE_STATE}" | grep -qE 'idle|mix|alloc'; then
            echo "   PASS: ${HOSTNAME} 状态为 ${NODE_STATE} (第 ${i} 次检查)"
            NODE_READY=true
            break
        fi
        # 如果节点存在但状态不对，尝试激活
        if echo "${NODE_STATE}" | grep -qE 'down|drain'; then
            echo "   尝试激活节点 (scontrol update)..."
            scontrol update NodeName="${HOSTNAME}" State=RESUME 2>&1 || true
        fi
    fi
    sleep 5
done

# ---- 10. 最终验证 ----
echo "==> 最终节点状态:"
sinfo -n "${HOSTNAME}" 2>&1 || true
echo ""
echo "   slurmd 最终状态:"
systemctl status slurmd --no-pager -l 2>/dev/null || true

if [ "${NODE_READY}" = "true" ]; then
    echo "============================================"
    echo "  Slurm Worker 配置完成: idle ✓"
    echo "============================================"
else
    echo "FATAL: ${HOSTNAME} 未在 60s 内注册到 slurmctld 或进入就绪状态" >&2
    echo "  完整 sinfo 输出:"
    sinfo 2>&1 || true
    echo ""
    echo "  slurmctld 日志 (${CONTROLLER_HOST}):"
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "${CONTROLLER_HOST}" \
      "sudo journalctl -u slurmctld --no-pager -n 30" 2>/dev/null || \
      echo "   (无法获取 controller 日志 — SSH 不通)"
    exit 1
fi

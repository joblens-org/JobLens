#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Slurm 控制器部署 — EPEL 9 安装 (Slurm 22.05.9)
# 参考: https://packages.fedoraproject.org/pkgs/slurm/slurm/epel-9.html
# =============================================================================

echo "[controller] 安装 Slurm 控制器 (EPEL 9)..."

# ---- 1. 确保 CRB + EPEL 可用 ----
dnf install -y dnf-plugins-core epel-release
dnf config-manager --set-enabled crb 2>/dev/null || true

# ---- 2. 安装 Slurm 控制器 + munge ----
# slurm-slurmctld: 控制器守护进程
# slurm-perlapi:   Perl API (sinfo 等命令需要)
# slurm 和 slurm-libs 自动作为依赖安装
dnf install -y slurm-slurmctld slurm-perlapi munge

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

# ---- 5. 生成最小 slurm.conf ----
cat > /etc/slurm/slurm.conf << 'SLURM_EOF'
ClusterName=test-cluster
SlurmctldHost=controller
SlurmUser=slurm
AuthType=auth/munge
CryptoType=crypto/munge
ProctrackType=proctrack/linuxproc
ReturnToService=1
TaskPlugin=task/none
NodeName=worker CPUs=2 State=UNKNOWN
PartitionName=debug Nodes=worker Default=YES MaxTime=INFINITE State=UP
SLURM_EOF

# ---- 6. 启动 slurmctld ----
systemctl enable --now slurmctld

# ---- 7. 复制配置到共享目录供 worker 使用 ----
mkdir -p /vagrant/.runtime/slurm
cp /etc/munge/munge.key /vagrant/.runtime/slurm/munge.key
cp /etc/slurm/slurm.conf /vagrant/.runtime/slurm/slurm.conf
chmod 600 /vagrant/.runtime/slurm/munge.key
chmod 644 /vagrant/.runtime/slurm/slurm.conf

# ---- 8. 等待就绪 ----
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

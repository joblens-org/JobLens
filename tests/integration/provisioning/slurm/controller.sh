#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Slurm 控制器部署脚本 (T6)
# 目标: VM1 (controller, 192.168.56.10)
# 部署: slurmctld + munge，生成最小 slurm.conf，等待 worker 节点注册
# =============================================================================

echo "[controller] 开始安装 Slurm 控制器组件..."

# ---- 1. 安装软件包 ----
# Slurm 官方文档: https://slurm.schedmd.com/quickstart_admin.html
# 官方推荐从源码构建 RPM; EPEL 中的 slurm 包为非官方版本,
# 但对集成测试足够 (无需 GPU/PMIx/数据库等高级功能)
dnf install -y slurm-slurmctld slurm-perlapi munge 2>/dev/null || {
  dnf install -y dnf-plugins-core
  dnf config-manager --set-enabled crb 2>/dev/null || true
  dnf install -y slurm-slurmctld slurm-perlapi munge
}

# ---- 2. 生成 munge 认证密钥 ----
mkdir -p /etc/munge
dd if=/dev/urandom bs=1 count=1024 > /etc/munge/munge.key
chmod 400 /etc/munge/munge.key
chown munge:munge /etc/munge/munge.key

# ---- 3. 启动 munge (slurmctld 依赖 munge 认证) ----
systemctl enable --now munge

# ---- 4. 创建 spool 目录并设置权限 ----
mkdir -p /var/spool/slurmctld /var/spool/slurmd
chown slurm:slurm /var/spool/slurmctld

# ---- 5. 生成最小 slurm.conf ----
cat > /etc/slurm/slurm.conf << 'SLURM_CONF_EOF'
# Slurm 测试集群配置 — 最小化，仅测试所需字段
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
SLURM_CONF_EOF

# ---- 6. 将 Slurm 共享配置放入 Vagrant 默认共享目录，供 worker 节点读取 ----
mkdir -p /vagrant/.runtime/slurm
cp /etc/munge/munge.key /vagrant/.runtime/slurm/munge.key
cp /etc/slurm/slurm.conf /vagrant/.runtime/slurm/slurm.conf
chmod 600 /vagrant/.runtime/slurm/munge.key
chmod 644 /vagrant/.runtime/slurm/slurm.conf

# ---- 7. 启动 slurmctld ----
systemctl enable --now slurmctld

# ---- 8. 等待 worker 节点注册 (最多轮询 6 次 × 5s = 30s) ----
echo "[controller] 等待 worker 节点注册到 debug 分区..."
for i in $(seq 1 6); do
    sleep 5
    if sinfo 2>/dev/null | grep -q debug; then
        echo "[controller] worker 节点已注册到 debug 分区。"
        sinfo
        exit 0
    fi
    echo "[controller] 第 ${i} 次检查未就绪，继续等待..."
done

echo "[controller] 超时: worker 节点未在 30s 内注册。" >&2
exit 1

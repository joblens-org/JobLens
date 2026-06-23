#!/usr/bin/env bash
# Slurm worker provisioning — EPEL 9 安装 slurmd + munge
# 参考: https://packages.fedoraproject.org/pkgs/slurm/slurm/epel-9.html
set -euo pipefail

echo "============================================"
echo "  Slurm Worker 配置: worker (192.168.56.20)"
echo "============================================"

# ---- 1. 安装 Slurm worker + munge ----
echo "==> 安装 slurm-slurmd + munge (EPEL 9)..."
dnf install -y dnf-plugins-core epel-release
dnf config-manager --set-enabled crb 2>/dev/null || true
dnf install -y slurm-slurmd munge

# ---- 2. 从 Vagrant 共享目录复制 controller 生成的 munge key ----
echo "==> 从共享目录复制 munge key..."
if [ ! -f /vagrant/.runtime/slurm/munge.key ]; then
    echo "FATAL: 未找到 munge key — 请先在 controller 执行 slurm/controller.sh" >&2
    exit 1
fi
cp /vagrant/.runtime/slurm/munge.key /etc/munge/munge.key
chmod 400 /etc/munge/munge.key
chown munge:munge /etc/munge/munge.key
echo "   munge key 已复制并设置权限"

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

# ---- 5. 从共享目录复制 slurm.conf ----
echo "==> 从共享目录复制 slurm.conf..."
if [ ! -f /vagrant/.runtime/slurm/slurm.conf ]; then
    echo "FATAL: 未找到 slurm.conf — 请先在 controller 执行 slurm/controller.sh" >&2
    exit 1
fi
cp /vagrant/.runtime/slurm/slurm.conf /etc/slurm/slurm.conf
echo "   slurm.conf 已复制"

# ---- 6. 创建 slurmd spool 和日志目录 ----
echo "==> 创建 slurmd 目录..."
mkdir -p /var/spool/slurmd /var/log/slurm
chown slurm:slurm /var/spool/slurmd /var/log/slurm
chmod 755 /var/spool/slurmd

# ---- 7. 启动 slurmd ----
echo "==> 启动 slurmd..."
systemctl enable --now slurmd

# ---- 8. 激活 worker 节点 ----
echo "==> 激活 worker 节点..."
scontrol update NodeName=worker State=RESUME
echo "   节点激活命令已发送"

# ---- 9. 等待 worker 进入 idle (最长 30s) ----
echo "==> 等待 worker 进入 idle 状态..."
for i in $(seq 1 6); do
    if sinfo -h -o "%t" -n worker 2>/dev/null | grep -q idle; then
        echo "   PASS: worker 状态为 idle (第 ${i} 次检查)"
        break
    fi
    echo "   等待中... (${i}/6)"
    sleep 5
done

# ---- 10. 最终验证 ----
echo "==> 最终节点状态:"
sinfo -n worker || true

if sinfo -h -o "%t" -n worker 2>/dev/null | grep -q idle; then
    echo "============================================"
    echo "  Slurm Worker 配置完成: idle ✓"
    echo "============================================"
else
    echo "FATAL: worker 未在 30s 内进入 idle" >&2
    sinfo 2>&1 || true
    exit 1
fi

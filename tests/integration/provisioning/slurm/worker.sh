#!/usr/bin/env bash
# Slurm worker provisioning — deploys slurmd + munge on worker node (VM2)
# 前提: T3 (common.sh) 已运行，T6 (controller.sh) 已完成且 munge key + slurm.conf 已就绪
set -euo pipefail

echo "============================================"
echo "  Slurm Worker 配置: worker (192.168.56.20)"
echo "============================================"

# ---- 1. 安装 Slurm worker 和 Munge ----
echo "==> 安装 slurm-slurmd + munge..."
dnf install -y slurm-slurmd slurm-perlapi munge 2>/dev/null || \
  dnf --enablerepo=crb install -y slurm-slurmd slurm-perlapi munge

# ---- 2. 从 Vagrant 共享目录复制 controller 生成的 munge key ----
echo "==> 从共享目录复制 munge key..."
if [ ! -f /vagrant/.runtime/slurm/munge.key ]; then
    echo "FATAL: 未找到 /vagrant/.runtime/slurm/munge.key，请先在 controller 执行 slurm/controller.sh" >&2
    exit 1
fi
cp /vagrant/.runtime/slurm/munge.key /etc/munge/munge.key
chmod 400 /etc/munge/munge.key
chown munge:munge /etc/munge/munge.key
echo "   munge key 已复制并设置权限"

# ---- 3. 启用并启动 Munge ----
echo "==> 启用并启动 munge..."
systemctl enable --now munge

# ---- 4. 验证本机 Munge 认证 (阻塞步骤) ----
echo "==> 验证本机 munge 认证..."
if munge -n | unmunge >/dev/null; then
    echo "   PASS: munge 认证正常"
else
    echo "FATAL: munge 认证失败 — 检查 munge key 是否一致且服务正在运行" >&2
    exit 1
fi

# ---- 5. 从 Vagrant 共享目录复制 slurm.conf ----
echo "==> 从共享目录复制 slurm.conf..."
if [ ! -f /vagrant/.runtime/slurm/slurm.conf ]; then
    echo "FATAL: 未找到 /vagrant/.runtime/slurm/slurm.conf，请先在 controller 执行 slurm/controller.sh" >&2
    exit 1
fi
cp /vagrant/.runtime/slurm/slurm.conf /etc/slurm/slurm.conf
echo "   slurm.conf 已复制"

# ---- 6. 创建 slurmd spool 目录 ----
echo "==> 创建 slurmd spool 目录..."
mkdir -p /var/spool/slurmd
chown slurm:slurm /var/spool/slurmd
echo "   /var/spool/slurmd 已创建"

# ---- 7. 启用并启动 slurmd ----
echo "==> 启用并启动 slurmd..."
systemctl enable --now slurmd

# ---- 8. 通过 slurm.conf 中的 controller 激活 worker 节点 ----
echo "==> 激活 worker 节点..."
scontrol update NodeName=worker State=RESUME
echo "   节点激活命令已发送"

# ---- 9. 等待 worker 节点进入 idle 状态 (最长 30s) ----
echo "==> 等待 worker 节点进入 idle 状态..."
for i in $(seq 1 6); do
    if sinfo -h -o "%t" -n worker 2>/dev/null | grep -q idle; then
        echo "   PASS: worker 节点状态为 idle (第 ${i} 次检查)"
        break
    fi
    echo "   等待中... (${i}/6)"
    sleep 5
done

# ---- 10. 最终验证 ----
echo "==> 最终节点状态:"
sinfo -n worker || true

# 再次确认状态为 idle（非 polling 场景下作为最终检查）
if sinfo -h -o "%t" -n worker 2>/dev/null | grep -q idle; then
    echo "============================================"
    echo "  Slurm Worker 配置完成: idle ✓"
    echo "============================================"
else
    echo "FATAL: worker 节点未在 30s 内进入 idle 状态" >&2
    sinfo 2>&1 || true
    exit 1
fi

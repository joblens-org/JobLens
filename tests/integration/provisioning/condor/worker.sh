#!/usr/bin/env bash
# HTCondor worker provisioning — deploys master + startd on worker node (VM2)
set -euo pipefail

echo "[worker] 添加 HTCondor 官方仓库 (EL9)..."
dnf install -y dnf-plugins-core
dnf config-manager --set-enabled crb 2>/dev/null || true
dnf install -y \
  https://htcss-downloads.chtc.wisc.edu/repo/25.x/htcondor-release-current.el9.noarch.rpm

echo "[worker] 安装 HTCondor 软件包..."
dnf install -y condor

echo "[worker] 创建 99-test-cluster.conf (worker 角色)..."
cat > /etc/condor/config.d/99-test-cluster.conf <<'EOF'
# 测试集群 — Worker 节点 (最简配置)
DAEMON_LIST = MASTER, STARTD
CONDOR_HOST = controller

# 测试环境: 允许所有来源
ALLOW_WRITE = *
ALLOW_READ  = *

# 关闭安全
SEC_DEFAULT_AUTHENTICATION = NEVER
SEC_DEFAULT_ENCRYPTION = NEVER
SEC_DEFAULT_INTEGRITY = NEVER

# 接受所有作业
START = TRUE
EOF

echo "[worker] 创建 50-network.conf (绑定 eth1)..."
cat > /etc/condor/config.d/50-network.conf <<'EOF'
# 网络配置 — 测试环境绑定 eth1
NETWORK_INTERFACE = eth1
BIND_ALL_INTERFACES = True
EOF

echo "[worker] 启用并启动 condor 服务..."
systemctl enable --now condor

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

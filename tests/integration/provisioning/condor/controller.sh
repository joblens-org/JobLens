#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# HTCondor 控制器节点部署脚本
# 部署组件: master, collector, negotiator, schedd (不含 startd)
# 安全配置: 完全禁用认证/完整性/加密 (仅测试环境)
# =============================================================================

echo "==> 添加 HTCondor 官方仓库..."
curl -fsSL https://htcondor.org/repo/current/htcondor-release-current.el9.noarch.rpm \
  -o /tmp/htcondor-release.rpm
rpm -i /tmp/htcondor-release.rpm

echo "==> 安装 HTCondor 软件包..."
dnf install -y condor

echo "==> 确保配置目录存在..."
mkdir -p /etc/condor/config.d

echo "==> 写入 HTCondor 集群配置 (99-test-cluster.conf)..."
cat > /etc/condor/config.d/99-test-cluster.conf << 'EOF'
# 测试集群配置 — 仅控制器守护进程列表
DAEMON_LIST = MASTER, COLLECTOR, NEGOTIATOR, SCHEDD

# 控制器主机名 (由 T3 common.sh 配置 /etc/hosts 解析)
CONDOR_HOST = controller

# 访问控制: 允许测试域名通配符 + 私有网络 IP 范围
ALLOW_WRITE = *.test.local, 192.168.56.*
ALLOW_READ  = *.test.local, 192.168.56.*

# 安全配置: 测试环境完全禁用安全机制
SEC_DEFAULT_AUTHENTICATION = NEVER
SEC_DEFAULT_INTEGRITY      = NEVER
SEC_DEFAULT_ENCRYPTION     = NEVER
EOF

echo "==> 写入 HTCondor 网络配置 (50-network.conf)..."
cat > /etc/condor/config.d/50-network.conf << 'EOF'
# 网络接口配置 — eth1 为 Vagrant private_network 适配器
NETWORK_INTERFACE    = eth1
BIND_ALL_INTERFACES  = True
EOF

echo "==> 启用并启动 HTCondor 服务..."
systemctl enable --now condor

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

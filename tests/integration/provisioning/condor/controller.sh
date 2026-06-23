#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# HTCondor 控制器节点部署脚本
# 部署组件: master, collector, negotiator, schedd (不含 startd)
# 安全配置: 完全禁用认证/完整性/加密 (仅测试环境)
# =============================================================================

echo "==> 添加 HTCondor 官方仓库 (EL9)..."
# 官方文档: https://htcondor.readthedocs.io/en/lts/getting-htcondor/from-our-repositories.html
# EL9 需要 EPEL + CRB 仓库
dnf install -y dnf-plugins-core
dnf config-manager --set-enabled crb 2>/dev/null || true
dnf install -y \
  https://htcss-downloads.chtc.wisc.edu/repo/25.x/htcondor-release-current.el9.noarch.rpm

echo "==> 安装 HTCondor 软件包..."
dnf install -y condor

echo "==> 确保配置目录存在..."
mkdir -p /etc/condor/config.d

echo "==> 写入 HTCondor 集群配置 (99-test-cluster.conf)..."
cat > /etc/condor/config.d/99-test-cluster.conf << 'EOF'
# 测试集群 — 控制器节点 (最简配置)
# 参考: https://htcondor.readthedocs.io/en/lts/getting-htcondor/admin-quick-start.html
DAEMON_LIST = MASTER, COLLECTOR, NEGOTIATOR, SCHEDD
CONDOR_HOST = controller

# 测试环境: 允许所有来源
ALLOW_WRITE = *
ALLOW_READ  = *

# 关闭安全 (仅测试)
SEC_DEFAULT_AUTHENTICATION = NEVER
SEC_DEFAULT_ENCRYPTION = NEVER
SEC_DEFAULT_INTEGRITY = NEVER

# 禁用 shared_port (25.x 默认启用, 测试池不需要)
USE_SHARED_PORT = False
EOF

echo "==> 写入 HTCondor 网络配置 (50-network.conf)..."
cat > /etc/condor/config.d/50-network.conf << 'EOF'
# 使用 IP 模式而非接口名 (eth1 在 AlmaLinux 9 可能是 ens4/ens5)
# HTCondor 会自动选择匹配 192.168.56.* 的接口来通信和广播
NETWORK_INTERFACE = 192.168.56.*
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

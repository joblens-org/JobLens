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

echo "[worker] 创建 50-network.conf (匹配 192.168.56.* 接口)..."
cat > /etc/condor/config.d/50-network.conf <<'EOF'
# 使用 IP 模式匹配 private_network 接口
# (eth1 在不同系统上可能叫 ens4/ens5)
NETWORK_INTERFACE = 192.168.56.*
EOF

echo "[worker] 启用并启动 condor 服务..."
systemctl enable --now condor

echo "[worker] === 诊断信息 ==="
echo "[worker] 1) 网络接口与IP:"
ip addr show | grep -E 'inet |eth|ens' || true
echo "[worker] 2) DNS解析 controller:"
getent hosts controller || echo "  FAIL: controller 无法解析"
echo "[worker] 3) Ping controller 连通性:"
ping -c 2 -W 3 controller 2>&1 || echo "  FAIL: ping 不通 controller"
echo "[worker] 4) TCP端口 9618 连通性:"
timeout 5 bash -c "echo >/dev/tcp/controller/9618" 2>&1 \
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
echo "[worker] 7) 尝试直接连接 collector:"
condor_status -collector -direct controller 2>&1 || true
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

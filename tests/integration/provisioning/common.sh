#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# JobLens 集成测试 — 通用 VM 初始化脚本 (AlmaLinux 9)
# 用法: sudo bash common.sh <hostname>
#   VM1 (controller): sudo bash common.sh controller
#   VM2 (worker):     sudo bash common.sh worker
# ============================================================================

# ---- 参数校验 ----
HOSTNAME="${1:-}"
if [ -z "$HOSTNAME" ]; then
    echo "FATAL: 未提供 hostname 参数"
    echo "用法: sudo bash common.sh <controller|worker>"
    exit 1
fi

echo "============================================"
echo "  JobLens 通用 VM 初始化: ${HOSTNAME}"
echo "============================================"

# ---- 检查 root 权限 ----
if [ "$EUID" -ne 0 ]; then
    echo "FATAL: 此脚本需要 root 权限运行 (使用 sudo)"
    exit 1
fi

# ---- 1. 设置主机名 ----
echo "==> 设置主机名为 ${HOSTNAME}"
hostnamectl set-hostname "${HOSTNAME}"

# ---- 2. 配置 /etc/hosts (幂等) ----
echo "==> 配置 /etc/hosts"
HOSTS_ENTRIES=(
    "192.168.56.10 controller"
    "192.168.56.20 worker"
)
for entry in "${HOSTS_ENTRIES[@]}"; do
    if ! grep -qF "${entry}" /etc/hosts 2>/dev/null; then
        echo "${entry}" >> /etc/hosts
        echo "   添加: ${entry}"
    else
        echo "   已存在，跳过: ${entry}"
    fi
done

# ---- 3. 禁用 SELinux ----
echo "==> 禁用 SELinux"
setenforce 0 2>/dev/null || echo "   WARNING: setenforce 失败 (可能已禁用)"
if [ -f /etc/selinux/config ]; then
    sed -i 's/^SELINUX=.*/SELINUX=disabled/' /etc/selinux/config
    echo "   /etc/selinux/config 已更新为 SELINUX=disabled"
else
    echo "   WARNING: /etc/selinux/config 不存在，跳过"
fi

# ---- 4. 停止并禁用 firewalld ----
echo "==> 停止并禁用 firewalld"
systemctl stop firewalld 2>/dev/null || echo "   INFO: firewalld 未运行"
systemctl disable firewalld 2>/dev/null || echo "   INFO: firewalld 未启用"

# ---- 5. 安装 EPEL 仓库 ----
echo "==> 安装 EPEL 仓库"
if ! rpm -q epel-release &>/dev/null; then
    dnf install -y epel-release
    echo "   EPEL 安装完成"
else
    echo "   EPEL 已安装，跳过"
fi

# 启用 CRB (CodeReady Builder) — EPEL 包的间接依赖需要
echo "==> 启用 CRB 仓库"
dnf config-manager --set-enabled crb 2>/dev/null || \
  dnf config-manager --set-enabled powertools 2>/dev/null || \
  echo "   WARNING: 无法启用 CRB/powertools (可能已启用或不存在)" 

# ---- 6. 安装基础软件包 ----
echo "==> 安装基础软件包"
dnf install -y \
    vim \
    curl \
    wget \
    git \
    python3 \
    python3-pip \
    bpftool

# ---- 7. 设置时区 ----
echo "==> 设置时区为 Asia/Shanghai"
timedatectl set-timezone Asia/Shanghai
echo "   当前时区: $(timedatectl show --property=Timezone --value)"

# ---- 8. 验证 BTF 支持 (FATAL) ----
echo "==> 验证 BTF 支持"
if ls /sys/kernel/btf/vmlinux &>/dev/null; then
    echo "   PASS: /sys/kernel/btf/vmlinux 存在"
else
    echo "FATAL: BTF 不可用 — /sys/kernel/btf/vmlinux 不存在"
    echo "  请确保内核编译时启用了 CONFIG_DEBUG_INFO_BTF=y"
    exit 1
fi

# ---- 9. 验证 eBPF 特性探测 (NON-FATAL) ----
echo "==> 验证 eBPF 特性探测"
set +e
BPF_OUTPUT=$(bpftool feature probe 2>&1)
BPF_EXIT=$?
set -e

if [ "${BPF_EXIT}" -eq 0 ] && echo "${BPF_OUTPUT}" | grep -qE "bpf|tracepoint|ringbuf"; then
    echo "   PASS: eBPF 特性探测正常"
    echo "${BPF_OUTPUT}" | grep -E "bpf|tracepoint|ringbuf" || true
else
    echo "   WARNING: eBPF 特性探测未找到预期关键字 (非致命)"
    if [ "${BPF_EXIT}" -ne 0 ]; then
        echo "   bpftool 退出码: ${BPF_EXIT}"
    fi
    echo "   输出摘要:"
    echo "${BPF_OUTPUT}" | head -20 || true
fi

# ---- 10. 配置 root SSH (VM 之间 SCP 通信) ----
echo "==> 配置 root SSH (生成 root 专用密钥对)"
mkdir -p /root/.ssh

# 生成 root 专用 ED25519 密钥对 (幂等: 已存在则跳过)
if [ ! -f /root/.ssh/id_ed25519 ]; then
  ssh-keygen -t ed25519 -N "" -f /root/.ssh/id_ed25519 -C "root@${HOSTNAME}"
  echo "   root ED25519 密钥对已生成"
else
  echo "   root 密钥对已存在，跳过生成"
fi

# 将自己的公钥加入 authorized_keys (允许自己登录自己，也允许其他 VM 的 root 登录)
cat /root/.ssh/id_ed25519.pub >> /root/.ssh/authorized_keys 2>/dev/null || true

# 也追加 vagrant 的公钥（允许 vagrant 用户通过其密钥登录 root）
if [ -f /home/vagrant/.ssh/authorized_keys ]; then
  cat /home/vagrant/.ssh/authorized_keys >> /root/.ssh/authorized_keys 2>/dev/null || true
fi

chmod 700 /root/.ssh
chmod 600 /root/.ssh/id_ed25519      2>/dev/null || true
chmod 644 /root/.ssh/id_ed25519.pub  2>/dev/null || true
chmod 600 /root/.ssh/authorized_keys 2>/dev/null || true
echo "   root SSH 已配置 (VM 间 SCP 可用)"

# ---- 完成 ----
echo "============================================"
echo "  通用初始化完成: ${HOSTNAME}"
echo "============================================"

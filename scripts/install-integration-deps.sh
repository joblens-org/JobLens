#!/usr/bin/env bash
# JobLens 集成测试 — 宿主依赖安装脚本
# 在 CI (ubuntu-latest) 或本地 Ubuntu/Debian 宿主机上安装:
#   - KVM 权限配置
#   - libvirt + QEMU 虚拟化栈
#   - Vagrant + vagrant-libvirt 插件
#   - RPM 构建工具
#
# 用法:
#   sudo bash scripts/install-integration-deps.sh
#
# 注意:
#   - 仅支持 Ubuntu/Debian 系 (CI 运行环境)
#   - 需要 root 权限
#   - 幂等 — 可多次运行
set -euo pipefail

echo "=============================================="
echo "  JobLens 集成测试 — 安装宿主依赖"
echo "=============================================="

# ── 检查 root ──────────────────────────────────────────────────────────
if [ "$EUID" -ne 0 ]; then
    echo "FATAL: 需要 root 权限 (使用 sudo)" >&2
    exit 1
fi

# ── 检查 OS ────────────────────────────────────────────────────────────
if [ ! -f /etc/os-release ]; then
    echo "FATAL: 无法检测操作系统 (仅支持 Ubuntu/Debian)" >&2
    exit 1
fi
source /etc/os-release
case "$ID" in
    ubuntu|debian) ;;
    *)
        echo "FATAL: 不支持的操作系统: $ID (仅支持 Ubuntu/Debian)" >&2
        exit 1
        ;;
esac

echo ""
echo "=== Step 0: 启用 KVM 设备权限 ==="

# ── KVM 权限 (CI 环境必须, 本地跳过也可) ─────────────────────────
if [ -e /dev/kvm ]; then
    echo "/dev/kvm 已存在: $(ls -la /dev/kvm | awk '{print $1, $3, $4}')"
else
    echo "WARNING: /dev/kvm 不存在 — 可能是容器环境, 跳过 KVM 权限配置"
fi

# 写入 udev 规则 (幂等: 覆盖写入)
cat > /etc/udev/rules.d/99-kvm4all.rules << 'UDEVEOF'
KERNEL=="kvm", GROUP="kvm", MODE="0666", OPTIONS+="static_node=kvm"
UDEVEOF

if command -v udevadm >/dev/null 2>&1; then
    udevadm control --reload-rules 2>/dev/null || true
    udevadm trigger --name-match=kvm 2>/dev/null || true
fi

echo "PASS: KVM 权限配置完成"

# ────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Step 1: 安装 libvirt + KVM 虚拟化栈 ==="

apt-get update -qq

# 检查是否已安装 (避免重复安装)
MISSING_PKGS=""
for pkg in qemu-kvm libvirt-daemon-system libvirt-clients \
           libvirt-dev virtinst bridge-utils cpu-checker \
           qemu-utils dnsmasq-base; do
    if ! dpkg -s "$pkg" >/dev/null 2>&1; then
        MISSING_PKGS="$MISSING_PKGS $pkg"
    fi
done

if [ -n "$MISSING_PKGS" ]; then
    echo "安装: $MISSING_PKGS"
    # shellcheck disable=SC2086
    apt-get install -y --no-install-recommends $MISSING_PKGS
else
    echo "所有 libvirt/KVM 包已安装"
fi

# ────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Step 2: 配置 libvirt socket 权限 ==="

# 启动 libvirtd (幂等, 兼容非 systemd 环境如 GitHub Actions)
echo "正在启动 libvirtd..."
if systemctl start libvirtd 2>/dev/null; then
    echo "  libvirtd started via systemctl"
elif service libvirtd start 2>/dev/null; then
    echo "  libvirtd started via service"
else
    echo "  systemctl/service 均不可用, 尝试直接启动 libvirtd daemon"
    libvirtd -d -l 2>/dev/null || true
fi
sleep 2

# 直接 chmod socket — CI 临时环境, 不需要持久化
SOCKET_FOUND=false
for sock in /var/run/libvirt/libvirt-sock*; do
    if [ -e "$sock" ]; then
        chmod 777 "$sock"
        ls -la "$sock"
        SOCKET_FOUND=true
    fi
done

# 如果上面没有 socket, 尝试重启后再次 chmod
if [ "$SOCKET_FOUND" != "true" ]; then
    echo "未找到 libvirt socket, 重启 libvirtd..."
    systemctl restart libvirtd 2>/dev/null || libvirtd -d -l 2>/dev/null || true
    sleep 3
    for sock in /var/run/libvirt/libvirt-sock*; do
        if [ -e "$sock" ]; then
            chmod 777 "$sock"
            ls -la "$sock"
            SOCKET_FOUND=true
        fi
    done
fi

if [ "$SOCKET_FOUND" = "true" ]; then
    echo "PASS: libvirt socket 权限已配置"
else
    echo "WARNING: 仍未找到 libvirt socket (可能系统使用不同路径)"
fi

# 验证 libvirt 连接
echo ""
echo "=== 验证 libvirt 连接 ==="
set +e
if command -v virsh >/dev/null 2>&1; then
    virsh -c qemu:///system list 2>&1 || true
fi
set -e

# ────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Step 3: 安装 Vagrant + vagrant-libvirt 插件 ==="

# 安装 Vagrant (幂等: 检查是否已安装)
if command -v vagrant >/dev/null 2>&1; then
    echo "Vagrant 已安装: $(vagrant --version)"
else
    echo "添加 HashiCorp APT 仓库..."
    curl -fsSL https://apt.releases.hashicorp.com/gpg \
        | gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
    echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
        | tee /etc/apt/sources.list.d/hashicorp.list
    apt-get update -qq
    apt-get install -y vagrant
    vagrant --version
fi

# 安装 vagrant-libvirt
echo ""
echo "=== 安装 vagrant-libvirt 插件 ==="
apt-get install -y --no-install-recommends ruby-dev pkg-config

# Vagrant 插件必须用普通用户安装 (sudo 下插件装到 /root/.vagrant.d/, CI 用户不可见)
# 检测是否通过 sudo 调用, 若是则切回原用户执行 vagrant 命令
VAGRANT_CMD="vagrant"
if [ -n "${SUDO_USER:-}" ] && [ "${SUDO_USER}" != "root" ]; then
    VAGRANT_CMD="sudo -u ${SUDO_USER} vagrant"
    echo "  检测到 sudo 环境, vagrant 命令以 ${SUDO_USER} 用户执行"
fi

if $VAGRANT_CMD plugin list 2>/dev/null | grep -q 'vagrant-libvirt'; then
    echo "vagrant-libvirt 插件已安装, 跳过"
else
    echo "正在安装 vagrant-libvirt..."
    $VAGRANT_CMD plugin install vagrant-libvirt
fi
echo "已安装插件:"
$VAGRANT_CMD plugin list 2>/dev/null
# 强制验证插件安装成功
if ! $VAGRANT_CMD plugin list 2>/dev/null | grep -q 'vagrant-libvirt'; then
    echo "FATAL: vagrant-libvirt 插件安装失败!" >&2
    exit 1
fi

# ────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Step 4: 安装 RPM 构建工具 ==="

if ! dpkg -s rpm >/dev/null 2>&1; then
    apt-get install -y --no-install-recommends rpm
fi
echo "RPM 工具版本: $(rpm --version 2>/dev/null || echo '未安装')"

echo ""
echo "=============================================="
echo "  宿主依赖安装完成 ✓"
echo "=============================================="

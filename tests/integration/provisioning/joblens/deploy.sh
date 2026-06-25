#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# JobLens 部署脚本 — VM2 (worker, 192.168.56.20)
# 用法: sudo bash deploy.sh --rpm-path=<path> --trigger-rpm-path=<path> \
#         --core-config=<path> --trigger-config=<path> [选项]
#
# 功能:
#   1. 从参数指定路径安装预编译的 JobLens Core + Trigger RPM
#   2. 从外部配置文件复制到 /etc/JobLens/ (不再内联生成)
#   3. 创建运行时目录并启动 systemd 服务
#   4. 健康检查 + 作业计数验证
# ============================================================================

# ---- 打印帮助信息 ----
usage() {
    cat << 'USAGE'
JobLens 部署脚本 — 从外部配置文件部署到 worker VM

用法: sudo bash deploy.sh --rpm-path=<path> --trigger-rpm-path=<path> \
        --core-config=<path> --trigger-config=<path> [选项]

必填参数:
  --rpm-path <path>            JobLens Core RPM 文件路径
  --trigger-rpm-path <path>    Trigger RPM 文件路径
  --core-config <path>         JobLens Core 配置文件路径 (VM 内路径)
  --trigger-config <path>      Trigger 配置文件路径 (VM 内路径)

可选参数:
  --config-dest <path>         配置文件安装目标目录 (默认: /etc/JobLens)
  --output-log <path>          FileWriter JSONL 输出路径
                               (默认: /var/log/joblens/output.log,
                                与外部配置文件不同时自动覆盖)
  --help, -h                   显示此帮助信息

功能:
  1. 从参数指定路径安装 JobLens Core + Trigger RPM
  2. 从外部配置文件复制到目标目录 (不再内联生成 YAML)
  3. 创建运行时目录 (/var/JobLens, /var/log/joblens)
  4. 启动 systemd 服务 (先 joblens, 再 joblens-trigger)
  5. 健康检查重试 (最多 3 次, 间隔 5s)
  6. 作业计数 API 验证

示例:
  sudo bash deploy.sh \
    --rpm-path=/vagrant/rpms/joblens-0.3.0.rpm \
    --trigger-rpm-path=/vagrant/rpms/joblens-trigger-0.3.0.rpm \
    --core-config=/vagrant/.runtime/joblens_core.yaml \
    --trigger-config=/vagrant/.runtime/joblens_trigger.yaml

USAGE
}

# ---- 参数解析 ----
RPM_PATH=""
TRIGGER_RPM_PATH=""
CORE_CONFIG=""
TRIGGER_CONFIG=""
CONFIG_DEST="/etc/JobLens"
OUTPUT_LOG="/var/log/joblens/output.log"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --rpm-path=*)
            RPM_PATH="${1#*=}"
            shift
            ;;
        --rpm-path)
            RPM_PATH="$2"
            shift 2
            ;;
        --trigger-rpm-path=*)
            TRIGGER_RPM_PATH="${1#*=}"
            shift
            ;;
        --trigger-rpm-path)
            TRIGGER_RPM_PATH="$2"
            shift 2
            ;;
        --core-config=*)
            CORE_CONFIG="${1#*=}"
            shift
            ;;
        --core-config)
            CORE_CONFIG="$2"
            shift 2
            ;;
        --trigger-config=*)
            TRIGGER_CONFIG="${1#*=}"
            shift
            ;;
        --trigger-config)
            TRIGGER_CONFIG="$2"
            shift 2
            ;;
        --config-dest=*)
            CONFIG_DEST="${1#*=}"
            shift
            ;;
        --config-dest)
            CONFIG_DEST="$2"
            shift 2
            ;;
        --output-log=*)
            OUTPUT_LOG="${1#*=}"
            shift
            ;;
        --output-log)
            OUTPUT_LOG="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "错误: 未知参数: $1" >&2
            usage
            exit 1
            ;;
    esac
done

# ---- 工具函数 ----
fatal() {
    echo "FATAL: $*" >&2
    exit 1
}

# ---- 参数校验 ----
[[ -z "${RPM_PATH}" ]]          && fatal "缺少 --rpm-path 参数 (JobLens Core RPM 文件路径)"
[[ -z "${TRIGGER_RPM_PATH}" ]]  && fatal "缺少 --trigger-rpm-path 参数 (Trigger RPM 文件路径)"
[[ -z "${CORE_CONFIG}" ]]       && fatal "缺少 --core-config 参数 (Core 配置文件路径)"
[[ -z "${TRIGGER_CONFIG}" ]]    && fatal "缺少 --trigger-config 参数 (Trigger 配置文件路径)"

[[ ! -f "${RPM_PATH}" ]]         && fatal "Core RPM 文件不存在: ${RPM_PATH}"
[[ ! -f "${TRIGGER_RPM_PATH}" ]] && fatal "Trigger RPM 文件不存在: ${TRIGGER_RPM_PATH}"
[[ ! -f "${CORE_CONFIG}" ]]      && fatal "Core 配置文件不存在: ${CORE_CONFIG}"
[[ ! -f "${TRIGGER_CONFIG}" ]]   && fatal "Trigger 配置文件不存在: ${TRIGGER_CONFIG}"

# ---- 检查 root 权限 ----
if [ "$EUID" -ne 0 ]; then
    fatal "此脚本需要 root 权限运行 (使用 sudo)"
fi

echo "============================================"
echo "  JobLens 部署: VM2 (worker)"
echo "============================================"

# ============================================================================
# STEP 1: 安装 RPM 包
# ============================================================================
echo "==> STEP 1: 安装 RPM 包"

echo "  Core RPM:    ${RPM_PATH}"
echo "  Trigger RPM: ${TRIGGER_RPM_PATH}"

# 安装 RPM (用 dnf 自动解析依赖, 先装 core, 再装 trigger)
echo "  安装 Core RPM (dnf 自动解析依赖)..."
dnf install -y "${RPM_PATH}"

echo "  安装 Trigger RPM..."
dnf install -y "${TRIGGER_RPM_PATH}"

echo "  PASS: RPM 安装完成"

# ============================================================================
# STEP 2: 创建运行时目录
# ============================================================================
echo "==> STEP 2: 创建运行时目录"

mkdir -p /var/JobLens
mkdir -p /var/log/joblens

echo "  PASS: 目录已创建"

# ============================================================================
# STEP 3: 从外部文件安装配置文件
# ============================================================================
echo "==> STEP 3: 安装配置文件 (从外部文件复制)"

# Core 配置
mkdir -p "${CONFIG_DEST}/"
cp "${CORE_CONFIG}" "${CONFIG_DEST}/config.yaml"
echo "  Core 配置已安装:  ${CONFIG_DEST}/config.yaml <- ${CORE_CONFIG}"

# 如果 --output-log 与外部配置文件中的默认值不同, 覆盖 file_writer_config.path
if [[ "${OUTPUT_LOG}" != "/var/log/joblens/output.log" ]]; then
    sed -i "s|^  path: .*|  path: ${OUTPUT_LOG}|" "${CONFIG_DEST}/config.yaml"
    echo "  INFO: 输出日志路径已覆盖 -> ${OUTPUT_LOG}"
fi

# Trigger 配置
mkdir -p "${CONFIG_DEST}/trigger/"
cp "${TRIGGER_CONFIG}" "${CONFIG_DEST}/trigger/config.yaml"
echo "  Trigger 配置已安装: ${CONFIG_DEST}/trigger/config.yaml <- ${TRIGGER_CONFIG}"

echo "  PASS: 配置文件安装完成"

# ============================================================================
# STEP 4: 启动 systemd 服务 (先 Agent, 再 Trigger)
# ============================================================================
echo "==> STEP 4: 启动 systemd 服务"

# 先启动 JobLens Agent (trigger 依赖其 RPC socket)
echo "  启动 joblens (Agent)..."
systemctl enable --now joblens
sleep 2
# 立即检查 Agent 是否存活 (crash 通常发生在前 2s)
if ! systemctl is-active --quiet joblens; then
    echo "FATAL: joblens Agent 启动后立即退出"
    echo "  === systemctl status ==="
    systemctl status joblens --no-pager -l 2>/dev/null || true
    echo "  === journalctl (最后 50 行) ==="
    journalctl -u joblens --no-pager -n 50 2>/dev/null || true
    exit 1
fi

echo "  启动 joblens-trigger (Trigger)..."
systemctl enable --now joblens-trigger

echo "  PASS: 服务已启动"

# ============================================================================
# STEP 5: 等待并验证服务健康状态
# ============================================================================
echo "==> STEP 5: 验证服务健康状态"

# 等待服务就绪
echo "  等待 10s 让服务就绪..."
sleep 10

# 健康检查 (最多重试 3 次，间隔 5s)
echo "  健康检查: http://localhost:7592/joblens/healthy"
MAX_RETRIES=3
RETRY_DELAY=5
HEALTH_OK=false

for attempt in $(seq 1 ${MAX_RETRIES}); do
    if curl -sf --max-time 10 http://localhost:7592/joblens/healthy 2>/dev/null | grep -Eq '"healthy":[[:space:]]*true'; then
        echo "  PASS: 健康检查通过 (尝试 ${attempt}/${MAX_RETRIES})"
        HEALTH_OK=true
        break
    fi

    if [ "${attempt}" -lt "${MAX_RETRIES}" ]; then
        echo "  重试健康检查 (${attempt}/${MAX_RETRIES}) — ${RETRY_DELAY}s 后重试..."
        sleep "${RETRY_DELAY}"
    fi
done

if [ "${HEALTH_OK}" != "true" ]; then
    echo "FATAL: 健康检查失败 (已重试 ${MAX_RETRIES} 次)"
    echo ""
    echo "  === systemctl status joblens ==="
    systemctl status joblens --no-pager -l 2>/dev/null || true
    echo ""
    echo "  === journalctl -u joblens (最后 50 行) ==="
    journalctl -u joblens --no-pager -n 50 2>/dev/null || true
    echo ""
    echo "  === systemctl status joblens-trigger ==="
    systemctl status joblens-trigger --no-pager -l 2>/dev/null || true
    echo ""
    echo "  === journalctl -u joblens-trigger (最后 30 行) ==="
    journalctl -u joblens-trigger --no-pager -n 30 2>/dev/null || true
    exit 1
fi

# ============================================================================
# STEP 6: 验证 API 响应
# ============================================================================
echo "==> STEP 6: 验证 API 响应"

echo "  作业计数: http://localhost:7592/joblens/jobs/count"
COUNT_RESP=$(curl -sf --max-time 10 http://localhost:7592/joblens/jobs/count 2>/dev/null || true)
if echo "${COUNT_RESP}" | grep -Eq '"job_count":'; then
    echo "  PASS: API 正常响应 — ${COUNT_RESP}"
else
    echo "FATAL: API 响应异常"
    echo "  响应内容: ${COUNT_RESP:-无响应}"
    exit 1
fi

# ============================================================================
# 完成
# ============================================================================
echo "============================================"
echo "  JobLens 部署完成: VM2 (worker)"
echo "  - Agent RPC socket: /var/JobLens/rpc.sock"
echo "  - LevelDB:          /var/JobLens/job.db"
echo "  - 输出日志:          ${OUTPUT_LOG}"
echo "  - 健康检查端口:      7592"
echo "============================================"

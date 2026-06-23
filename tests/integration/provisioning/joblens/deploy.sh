#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# JobLens 部署脚本 — VM2 (worker, 192.168.56.20)
# 用法: sudo bash deploy.sh
# 前提: VM2 已完成通用初始化 (common.sh worker)
#       RPM 包已放置于 /vagrant/rpms/ 目录
#
# 功能:
#   1. 从 /vagrant/rpms/ 安装预编译的 JobLens Core + Trigger RPM
#   2. 写入 /etc/JobLens/config.yaml (FileWriter 输出)
#   3. 创建运行时目录并启动 systemd 服务
#   4. 健康检查 + 作业计数验证
# ============================================================================

echo "============================================"
echo "  JobLens 部署: VM2 (worker)"
echo "============================================"

# ---- 检查 root 权限 ----
if [ "$EUID" -ne 0 ]; then
    echo "FATAL: 此脚本需要 root 权限运行 (使用 sudo)"
    exit 1
fi

# ============================================================================
# STEP 1: 检查并安装 RPM 包
# ============================================================================
echo "==> STEP 1: 检查 RPM 包"

RPM_DIR="/vagrant/rpms"

if [ ! -d "${RPM_DIR}" ]; then
    echo "FATAL: RPM 目录不存在: ${RPM_DIR}"
    echo "  请通过 CI 构建 RPM 包并放置到 ${RPM_DIR}/"
    exit 1
fi

# 查找 core RPM (排除 trigger RPM)
CORE_RPM=$(find "${RPM_DIR}" -maxdepth 1 -name 'joblens-[0-9]*.rpm' ! -name '*-trigger-*' -print -quit 2>/dev/null || true)
if [ -z "${CORE_RPM}" ]; then
    echo "FATAL: 未找到 JobLens Core RPM 文件 (joblens-*.rpm)"
    echo "  路径: ${RPM_DIR}/joblens-*.rpm"
    echo "  请通过 CI 构建 RPM 包后再运行此脚本。"
    exit 1
fi
echo "  找到 Core RPM: $(basename "${CORE_RPM}")"

# 查找 trigger RPM
TRIGGER_RPM=$(find "${RPM_DIR}" -maxdepth 1 -name 'joblens-trigger-*.rpm' -print -quit 2>/dev/null || true)
if [ -z "${TRIGGER_RPM}" ]; then
    echo "FATAL: 未找到 JobLens Trigger RPM 文件 (joblens-trigger-*.rpm)"
    echo "  路径: ${RPM_DIR}/joblens-trigger-*.rpm"
    echo "  请通过 CI 构建 RPM 包后再运行此脚本。"
    exit 1
fi
echo "  找到 Trigger RPM: $(basename "${TRIGGER_RPM}")"

# 安装 RPM (先装 core, 再装 trigger)
echo "  安装 Core RPM..."
rpm -ivh "${CORE_RPM}"

echo "  安装 Trigger RPM..."
rpm -ivh "${TRIGGER_RPM}"

echo "  PASS: RPM 安装完成"

# ============================================================================
# STEP 2: 创建运行时目录
# ============================================================================
echo "==> STEP 2: 创建运行时目录"

mkdir -p /var/JobLens
mkdir -p /var/log/joblens

echo "  PASS: 目录已创建"

# ============================================================================
# STEP 3: 写入配置文件
# ============================================================================
echo "==> STEP 3: 写入 /etc/JobLens/config.yaml"

cat > /etc/JobLens/config.yaml << 'YAMLEOF'
# JobLens 集成测试配置 — VM2 (worker)
# 使用 FileWriter 输出 JSONL 到 /var/log/joblens/output.log

# ============================================================
# Core Configuration
# ============================================================
lens_config:
  rpc_socket_path: /var/JobLens/rpc.sock
  lock_path: /var/JobLens/JobLens.lock
  max_collector_threads: 2
  log_level: debug

# ============================================================
# Job Registry Configuration
# ============================================================
job_registry_config:
  job_db_path: /var/JobLens/job.db
  auto_add_condorjob: true
  auto_add_slurmjob: true

# ============================================================
# Collector Configuration — CPUMemCollector only
# ============================================================
collectors_config:
  enable_collector_perf: true
  default_freq: 1
  collectors:
    - name: cpumem_collector
      type: CPUMemCollector
      config: cpumem_collector_config

cpumem_collector_config:
  freq: 1
  use_writers:
    - file_writer

# ============================================================
# Writer Configuration — FileWriter only
# ============================================================
writers_config:
  enable_writer_perf: true
  buffer_capacity: 256
  writers:
    - name: file_writer
      type: FileWriter
      config: file_writer_config

file_writer_config:
  path: /var/log/joblens/output.log
YAMLEOF

echo "  PASS: 配置文件已写入"

# ============================================================================
# STEP 4: 启动 systemd 服务 (先 Agent, 再 Trigger)
# ============================================================================
echo "==> STEP 4: 启动 systemd 服务"

# 先启动 JobLens Agent (trigger 依赖其 RPC socket)
echo "  启动 joblens (Agent)..."
systemctl enable --now joblens

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
    echo "  调试信息:"
    echo "  systemctl status joblens:"
    systemctl status joblens --no-pager -l 2>/dev/null || true
    echo "  systemctl status joblens-trigger:"
    systemctl status joblens-trigger --no-pager -l 2>/dev/null || true
    exit 1
fi

# ============================================================================
# STEP 6: 验证作业计数
# ============================================================================
echo "==> STEP 6: 验证作业计数"

echo "  作业计数: http://localhost:7592/joblens/jobs/count"
if curl -sf --max-time 10 http://localhost:7592/joblens/jobs/count 2>/dev/null | grep -Eq '"job_count":[[:space:]]*0'; then
    echo "  PASS: 作业计数验证通过 (job_count=0)"
else
    echo "FATAL: 作业计数验证失败"
    echo "  响应内容:"
    curl -sf --max-time 10 http://localhost:7592/joblens/jobs/count 2>/dev/null || echo "  (无响应)"
    exit 1
fi

# ============================================================================
# 完成
# ============================================================================
echo "============================================"
echo "  JobLens 部署完成: VM2 (worker)"
echo "  - Agent RPC socket: /var/JobLens/rpc.sock"
echo "  - LevelDB:          /var/JobLens/job.db"
echo "  - 输出日志:          /var/log/joblens/output.log"
echo "  - 健康检查端口:      7592"
echo "============================================"

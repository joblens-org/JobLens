"""FileWriter JSONL 字段完整性验证 — 验证 FileWriter 输出的 JSONL 格式和字段正确性.

测试覆盖:
  - JSONL 文件存在且非空 (>= 2 行)
  - 每行可解析为合法 JSON
  - 每条记录包含所有必需的顶级字段
  - @timestamp 格式符合 UTC+8 (亚洲/上海时区)
  - data 字段包含 cpu_usage_percent / mem_usage_percent (float)
  - job_info 包含 jobtype (str) / pids (list)
  - hostname 非空字符串
  - collector_name == "cpumem_collector"

全部测试通过 worker fixture 读取 /var/log/joblens/output.log,
使用 utils.parse_jsonl() 和 validate_json_schema() 进行验证.
"""

import re

from utils import (
    JOBLENS_OUTPUT_LOG,
    parse_jsonl,
    validate_json_schema,
)


# ── JSONL 文件基础检查 ─────────────────────────────────────────────────

def test_jsonl_file_exists_and_nonempty(worker) -> None:
    """验证 JSONL 输出文件存在且包含 >= 2 行非空记录.

    检查文件存在于 worker VM 上, 然后读取内容确认至少 2 条记录已写入.
    """
    # 确认文件存在
    result = worker.run(f"test -f {JOBLENS_OUTPUT_LOG}", hide=True)
    assert result.ok, f"JSONL 文件不存在: {JOBLENS_OUTPUT_LOG}"

    # 读取并统计非空行数
    text = worker.read_text(JOBLENS_OUTPUT_LOG)
    lines = [ln for ln in text.splitlines() if ln.strip()]
    assert len(lines) >= 2, (
        f"期望 JSONL 至少 2 行非空记录，实际 {len(lines)} 行"
    )


def test_jsonl_parseable(worker) -> None:
    """验证 JSONL 每一行非空行都可以解析为合法 JSON.

    使用 parse_jsonl() 跳过空行, 解析失败会抛出带行号的 ValueError.
    """
    text = worker.read_text(JOBLENS_OUTPUT_LOG)
    records = parse_jsonl(text)
    assert len(records) >= 2, (
        f"期望至少 2 条有效 JSON 记录，实际解析 {len(records)} 条"
    )


# ── 顶级字段验证 ───────────────────────────────────────────────────────

def test_required_top_fields(worker) -> None:
    """验证每条记录包含所有必需的顶级字段.

    必需字段: @timestamp, hostname, collector_name, job_info, data.
    使用 validate_json_schema() 检查字段存在性.
    """
    text = worker.read_text(JOBLENS_OUTPUT_LOG)
    records = parse_jsonl(text)

    required = ["@timestamp", "hostname", "collector_name", "job_info", "data"]
    for i, record in enumerate(records):
        missing = validate_json_schema(record, required)
        assert missing == [], (
            f"记录[{i}] 缺失顶级字段: {missing}"
        )


# ── 时间戳格式验证 ─────────────────────────────────────────────────────

def test_timestamp_format(worker) -> None:
    """验证 @timestamp 字段格式: YYYY-MM-DDTHH:MM:SS+0800 (UTC+8).

    FileWriter 硬编码了 Asia/Shanghai 时区 (UTC+8),
    时间戳必须包含 +0800 后缀而非 Z 或 UTC 偏移.
    """
    text = worker.read_text(JOBLENS_OUTPUT_LOG)
    records = parse_jsonl(text)

    # UTC+8 格式: 2024-12-31T23:59:59+0800
    ts_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+0800$")

    for i, record in enumerate(records):
        ts = record.get("@timestamp", "")
        assert ts_pattern.fullmatch(ts), (
            f"记录[{i}] @timestamp='{ts}' 不匹配格式 YYYY-MM-DDTHH:MM:SS+0800"
        )


# ── data 字段验证 ──────────────────────────────────────────────────────

def test_cpu_data_fields(worker) -> None:
    """验证 data 字段包含 cpu_usage_percent 和 mem_usage_percent (float 类型).

    仅验证字段存在和类型, 不验证具体数值.
    当前仅启用 CPUMemCollector, 因此不检查 IO/Network/GPU 字段.
    """
    text = worker.read_text(JOBLENS_OUTPUT_LOG)
    records = parse_jsonl(text)

    for i, record in enumerate(records):
        data = record.get("data", {})
        assert isinstance(data, dict), (
            f"记录[{i}] data 不是 dict 类型, 实际 {type(data)}"
        )

        cpu = data.get("cpu_usage_percent")
        assert isinstance(cpu, float), (
            f"记录[{i}] cpu_usage_percent 不是 float, 实际 {type(cpu).__name__}={cpu!r}"
        )

        mem = data.get("mem_usage_percent")
        assert isinstance(mem, float), (
            f"记录[{i}] mem_usage_percent 不是 float, 实际 {type(mem).__name__}={mem!r}"
        )


# ── job_info 字段验证 ──────────────────────────────────────────────────

def test_job_info_fields(worker) -> None:
    """验证 job_info 包含 jobtype (str) 和 pids (list) 且类型正确.

    jobtype 标识作业调度器类型 (如 "condor", "slurm", "local"),
    pids 是 eBPF 追踪到的进程 PID 列表.
    """
    text = worker.read_text(JOBLENS_OUTPUT_LOG)
    records = parse_jsonl(text)

    for i, record in enumerate(records):
        job_info = record.get("job_info", {})
        assert isinstance(job_info, dict), (
            f"记录[{i}] job_info 不是 dict, 实际 {type(job_info)}"
        )

        jt = job_info.get("jobtype")
        assert isinstance(jt, str), (
            f"记录[{i}] jobtype 不是 str, 实际 {type(jt).__name__}={jt!r}"
        )
        assert len(jt) > 0, (
            f"记录[{i}] jobtype 为空字符串"
        )

        pids = job_info.get("pids")
        assert isinstance(pids, list), (
            f"记录[{i}] pids 不是 list, 实际 {type(pids).__name__}={pids!r}"
        )


# ── hostname 验证 ──────────────────────────────────────────────────────

def test_hostname_nonempty(worker) -> None:
    """验证 hostname 字段为非空字符串.

    hostname 是采集数据时所在节点的主机名, 用于区分多节点部署的日志来源.
    """
    text = worker.read_text(JOBLENS_OUTPUT_LOG)
    records = parse_jsonl(text)

    for i, record in enumerate(records):
        hostname = record.get("hostname", "")
        assert isinstance(hostname, str) and len(hostname) > 0, (
            f"记录[{i}] hostname 为空或非字符串, 实际 {hostname!r}"
        )


# ── collector_name 验证 ────────────────────────────────────────────────

def test_collector_name_is_cpumem(worker) -> None:
    """验证 collector_name 字段为 "cpumem_collector".

    当前部署仅启用 CPUMemCollector, 因此每条记录的采集器名必须一致.
    """
    text = worker.read_text(JOBLENS_OUTPUT_LOG)
    records = parse_jsonl(text)

    for i, record in enumerate(records):
        name = record.get("collector_name")
        assert name == "cpumem_collector", (
            f"记录[{i}] collector_name='{name}'，期望 'cpumem_collector'"
        )

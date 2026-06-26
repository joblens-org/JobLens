"""JobLens 采集器性能基线集成测试。

测试覆盖:
  - 采集器性能端点 (/joblens/collectors/perf) — 信息级检查，不做严格延迟阈值
  - RPC 延迟 (/joblens/rpc/health rpc_latency_ms) — <100ms 阈值，超限 skip
  - 内存使用 (systemctl show MemoryCurrent) — <256MB 阈值，超限 skip
  - CPU 使用 (ps %cpu) — <5% 单核阈值，超限 skip
  - eBPF 程序已加载 (bpftool) — ≥1 条是关键检查，未加载 FAIL

非关键阈值超限使用 pytest.skip() 而非 pytest.fail()，
eBPF 程序未加载是致命检查（影响自动发现功能，使用 assert/fail）。
"""

import pytest

from utils import JobLensAPI, RemoteClient


# ── 采集器性能端点 ─────────────────────────────────────────────────────

def test_collection_latency(
    joblens_api: JobLensAPI,
) -> None:
    """验证 /joblens/collectors/perf 端点返回 200 且响应格式有效。

    信息级检查：不做严格延迟阈值判断。仅验证端点可用且响应为 dict 类型。
    无论是否有活跃采集器，端点均应返回有效 JSON 响应。
    """
    resp = joblens_api.collector_perf()
    assert resp.status_code == 200, (
        f"期望 HTTP 200，实际 {resp.status_code}, body={resp.text[:300]}"
    )
    data = resp.json()
    assert isinstance(data, dict), (
        f"期望 collectors/perf 响应为 dict 类型，"
        f"实际 {type(data).__name__}"
    )


# ── RPC 延迟 ───────────────────────────────────────────────────────────

def test_rpc_latency(
    joblens_api: JobLensAPI,
) -> None:
    """验证 /joblens/rpc/health 的 rpc_latency_ms < 100ms。

    使用固定阈值 100ms（Metis-review 确认的阈值）。
    超限使用 pytest.skip() 跳过 — 性能波动在测试 VM 中属于非关键问题。
    """
    RPC_LATENCY_THRESHOLD_MS = 100.0

    resp = joblens_api.rpc_health()
    assert resp.status_code == 200, (
        f"期望 HTTP 200，实际 {resp.status_code}, body={resp.text[:200]}"
    )
    data = resp.json()
    latency = data.get("rpc_latency_ms")

    assert latency is not None, (
        f"期望响应包含 rpc_latency_ms 字段，实际 keys={list(data.keys())}"
    )

    if latency > RPC_LATENCY_THRESHOLD_MS:
        pytest.skip(
            f"RPC 延迟 {latency}ms > {RPC_LATENCY_THRESHOLD_MS}ms 阈值"
            f"（非关键指标，在 VM 环境中跳过）"
        )


# ── 内存使用 ───────────────────────────────────────────────────────────

def test_memory_under_limit(
    worker: RemoteClient,
) -> None:
    """验证 JobLens 进程内存 < 256MB (268435456 字节)。

    通过 systemctl show joblens -p MemoryCurrent 获取 cgroup 内存值。
    超限使用 pytest.skip() 跳过 — 内存峰值在测试 VM 中属于非关键问题。
    """
    MEMORY_LIMIT_BYTES = 256 * 1024 * 1024  # 256 MB

    result = worker.sudo(
        "systemctl show joblens -p MemoryCurrent", hide=True, warn=True,
    )
    line = result.stdout.strip()

    if "=" not in line:
        pytest.skip(f"无法解析 MemoryCurrent 输出: '{line}'")

    _, value_str = line.split("=", 1)

    # MemoryCurrent 可能为 "[not set]" 或整数
    try:
        memory_bytes = int(value_str)
    except ValueError:
        pytest.skip(f"MemoryCurrent 值非整数（可能未设置 cgroup 记账）: '{value_str}'")

    if memory_bytes > MEMORY_LIMIT_BYTES:
        memory_mb = memory_bytes / (1024 * 1024)
        limit_mb = MEMORY_LIMIT_BYTES / (1024 * 1024)
        pytest.skip(
            f"内存使用 {memory_mb:.1f}MB > {limit_mb:.0f}MB 阈值"
            f"（非关键指标，在 VM 环境中跳过）"
        )


# ── CPU 使用 ───────────────────────────────────────────────────────────

def test_cpu_usage_reasonable(
    worker: RemoteClient,
) -> None:
    """验证 JobLens 进程 CPU 使用率 < 5%。

    通过 ps -p $(pgrep -x JobLens) -o %cpu= 获取单核 CPU 占比。
    超限使用 pytest.skip() 跳过 — CPU 峰值在测试 VM 中属于非关键问题。
    """
    CPU_THRESHOLD_PERCENT = 5.0

    result = worker.sudo(
        "ps -p $(pgrep -x JobLens) -o %cpu=", hide=True, warn=True,
    )
    cpu_str = result.stdout.strip()

    if not cpu_str:
        pytest.skip(
            "无法获取 JobLens 进程 CPU 使用率"
            "（进程可能未运行或 pgrep 未匹配到 JobLens 进程名）"
        )

    try:
        cpu_percent = float(cpu_str)
    except ValueError:
        pytest.skip(f"CPU 使用率值无法解析为浮点数: '{cpu_str}'")

    if cpu_percent > CPU_THRESHOLD_PERCENT:
        pytest.skip(
            f"CPU 使用率 {cpu_percent}% > {CPU_THRESHOLD_PERCENT}% 阈值"
            f"（非关键指标，在 VM 环境中跳过）"
        )


# ── eBPF 程序已加载 ───────────────────────────────────────────────────

def test_ebpf_programs_loaded(
    worker: RemoteClient,
) -> None:
    """验证至少加载了 1 条 JobLens eBPF 程序。

    关键检查：eBPF 程序是 JobLens 自动发现作业（HTCondor/Slurm）的核心依赖。
    如果未加载则标记为测试 FAIL — 这是一种致命的服务降级状态。

    使用 bpftool prog show 列出所有已加载程序，通过 grep -c joblens 计数。
    bpftool 不可用或计数为 0 均为 FAIL。
    """
    result = worker.sudo(
        "bpftool prog show 2>/dev/null | grep -c joblens",
        hide=True, warn=True,
    )
    count_str = result.stdout.strip()

    try:
        prog_count = int(count_str)
    except ValueError:
        pytest.fail(
            f"bpftool 输出无法解析为整数: '{count_str}'"
        )

    assert prog_count >= 1, (
        f"期望至少 1 条 JobLens eBPF 程序已加载，实际加载 {prog_count} 条。"
        f"eBPF 程序是 JobLens 自动发现作业（HTCondor/Slurm）的关键组件，"
        f"未加载属于致命的服务降级。"
    )

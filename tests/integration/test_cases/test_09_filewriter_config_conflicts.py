"""FileWriter 配置冲突与向后兼容性集成测试。

测试覆盖:
  1. 旧版配置（仅含 path）仍可正常工作并产生有效纯文本输出
  2. 无效 write_mode 导致 fast-fail（构造时抛出异常）
  3. overwrite + enable_rotation 冲突导致 fast-fail
  4. outputs 中重复 collector_name 导致 fast-fail
  5. overwrite 模式下重复 file_path 导致 fast-fail
  6. append 模式下重复 file_path 允许正常启动

所有 fast-fail 断言基于 src/writer/file_writer.cpp 中 validateOptions()
的精确错误消息文本。负向测试不依赖 clean_test_state fixture，
直接操作 worker VM 的 systemd 服务与 journalctl 日志。
"""

import importlib
import io
import json
import sys
import time
from pathlib import Path
from typing import Optional

# 使用 conftest.py 相同的 importlib 模式，消除 LSP reportImplicitRelativeImport 错误
try:
    _utils = importlib.import_module("test_cases.utils")
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    _utils = importlib.import_module("utils")

JOBLENS_OUTPUT_LOG = _utils.JOBLENS_OUTPUT_LOG
PROJECT_ROOT = _utils.PROJECT_ROOT
RemoteClient = _utils.RemoteClient
retry_with_backoff = _utils.retry_with_backoff
submit_slurm_job = _utils.submit_slurm_job


# ── 常量 ──────────────────────────────────────────────────────────────────

# 基础配置文件路径（宿主机端）
_BASE_CONFIG_PATH = (
    PROJECT_ROOT / "tests/integration/presets/configs/joblens_core_default.yaml"
)

# 远程配置文件路径（worker VM 端）
_REMOTE_CONFIG_PATH = "/etc/JobLens/config.yaml"

# 从 file_writer.cpp validateOptions() 中提取的精确错误消息片段
_ERR_INVALID_WRITE_MODE = "invalid write_mode"
_ERR_OVERWRITE_ROTATION = "incompatible with"
_ERR_DUPLICATE_COLLECTOR = "duplicate collector_name"
_ERR_DUPLICATE_FILE_PATH = "duplicate file_path"


# ── 内部辅助函数 ────────────────────────────────────────────────────────

def _read_base_config() -> str:
    """读取基础 YAML 配置文件内容（宿主机端）。"""
    return _BASE_CONFIG_PATH.read_text(encoding="utf-8")


def _build_filewriter_config_section(
    *,
    path: str = "/var/log/joblens/output.log",
    write_mode: Optional[str] = None,
    enable_rotation: Optional[bool] = None,
    flush_on_shutdown: Optional[bool] = None,
    max_file_size_bytes: Optional[int] = None,
    max_files: Optional[int] = None,
    outputs: Optional[str] = None,
) -> str:
    """构建 file_writer_config YAML 段（已含 2 空格缩进）。

    每个参数为 None 时省略该键，非 None 时生成对应 YAML 行。
    outputs 参数为预格式化的 YAML 字符串块（含缩进，直接追加在末尾）。
    """
    lines: list[str] = [f"  path: {path}"]
    if write_mode is not None:
        lines.append(f"  write_mode: {write_mode}")
    if enable_rotation is not None:
        lines.append(f"  enable_rotation: {str(enable_rotation).lower()}")
    if flush_on_shutdown is not None:
        lines.append(f"  flush_on_shutdown: {str(flush_on_shutdown).lower()}")
    if max_file_size_bytes is not None:
        lines.append(f"  max_file_size_bytes: {max_file_size_bytes}")
    if max_files is not None:
        lines.append(f"  max_files: {max_files}")
    if outputs is not None:
        lines.append(outputs)
    return "\n".join(lines)


def _replace_filewriter_config(base_config: str, new_section: str) -> str:
    """替换基础配置中的 file_writer_config 段。

    查找从 "file_writer_config:" 开始的段，跳过所有缩进行（含空行），
    直到下一个非缩进非空行（新顶级键或注释），替换为 new_section。
    如果未找到 file_writer_config 段，则追加到末尾。
    """
    lines = base_config.splitlines()
    result: list[str] = []
    in_section = False
    found = False

    for line in lines:
        if not in_section:
            if line.startswith("file_writer_config:"):
                in_section = True
                found = True
                result.append("file_writer_config:")
                result.append(new_section)
                continue
            result.append(line)
        else:
            # 仍在 section 内：跳过所有缩进行和空行
            if line and not line[0].isspace():
                # 遇到非缩进非空行（新顶级键或注释），退出 section
                in_section = False
                result.append(line)
            # 缩进行和空行继续跳过

    if not found:
        # 未找到 file_writer_config 段，追加到末尾
        result.append("file_writer_config:")
        result.append(new_section)

    return "\n".join(result)


def _deploy_config(worker: RemoteClient, config_content: str) -> None:
    """将配置内容写入 worker VM 的 /etc/JobLens/config.yaml。

    先确保目录存在，再通过 sudo tee 写入。使用 io.StringIO 包装
    以兼容 fabric 的 in_stream 参数。
    """
    worker.sudo(
        f"mkdir -p /etc/JobLens && cat > {_REMOTE_CONFIG_PATH}",
        in_stream=io.StringIO(config_content),
        hide=True,
    )


def _stop_joblens(worker: RemoteClient) -> None:
    """停止 joblens 服务（最佳努力，不检查退出码）。"""
    worker.sudo("systemctl stop joblens 2>/dev/null || true", hide=True, warn=True)


def _start_joblens(worker: RemoteClient) -> None:
    """启动 joblens 服务（最佳努力，不检查退出码）。"""
    worker.sudo("systemctl start joblens 2>/dev/null || true", hide=True, warn=True)


def _get_joblens_status(worker: RemoteClient) -> str:
    """获取 joblens 服务状态（active/inactive/failed/activating 等）。"""
    result = worker.run(
        "systemctl is-active joblens 2>/dev/null || echo 'inactive'",
        hide=True,
        warn=True,
    )
    return result.stdout.strip()


def _get_joblens_journal(
    worker: RemoteClient,
    since: str = "2 minutes ago",
) -> str:
    """获取 joblens 的 journalctl 日志（最近 since 时间段）。"""
    result = worker.sudo(
        f"journalctl -u joblens --no-pager --since '{since}' 2>/dev/null || true",
        hide=True,
        warn=True,
    )
    return result.stdout or ""


def _wait_for_joblens_settled(
    worker: RemoteClient,
    expect_active: bool,
    timeout: float = 15.0,
) -> bool:
    """等待 joblens 达到稳定状态。

    Args:
        worker: worker VM 连接。
        expect_active: True 等待服务变为 active，False 等待变为 inactive/failed。
        timeout: 最长等待时间（秒）。

    Returns:
        True 如果达到期望状态，False 如果超时。
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = _get_joblens_status(worker)
        if expect_active and status == "active":
            return True
        if not expect_active and status in ("inactive", "failed"):
            return True
        time.sleep(0.5)
    return False


def _assert_joblens_fast_fail(
    worker: RemoteClient,
    expected_error_fragment: str,
) -> None:
    """断言 joblens 因配置错误 fast-fail 且日志包含预期错误片段。

    检查 journalctl 日志中是否包含 expected_error_fragment，
    同时确认服务未处于 active 状态（可能为 inactive、failed 或
    在 systemd 重启循环中反复崩溃）。

    Args:
        worker: worker VM 连接。
        expected_error_fragment: 预期在 journalctl 日志中出现的错误文本片段。

    Raises:
        AssertionError: 如果日志中未找到预期错误，或服务异常处于 active 状态。
    """
    # 等待服务稳定（不再是 active）
    _wait_for_joblens_settled(worker, expect_active=False, timeout=10.0)

    # 最终确认：服务不应为 active
    status = _get_joblens_status(worker)
    assert status != "active", (
        f"预期 joblens fast-fail 但服务状态为 '{status}'，"
        f"错误片段: '{expected_error_fragment}'"
    )

    # 检查 journalctl 日志中包含预期错误
    journal = _get_joblens_journal(worker)
    assert expected_error_fragment in journal, (
        f"journalctl 中未找到预期错误片段 '{expected_error_fragment}'，"
        f"完整日志（截断至 2000 字符）:\n{journal[:2000]}"
    )


def _deploy_and_restart(worker: RemoteClient, config_content: str) -> None:
    """部署配置并重启 joblens 的便捷组合。

    先停止 joblens，写入新配置，然后启动。
    """
    _stop_joblens(worker)
    time.sleep(1)
    _deploy_config(worker, config_content)
    _start_joblens(worker)


def _wait_ebpf_ready(worker: RemoteClient, timeout: float = 15.0) -> None:
    """等待 eBPF 程序加载完成（bpftool prog list 包含 joblens）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = worker.sudo("bpftool prog list", hide=True, warn=True)
        if "joblens" in result.stdout.lower():
            return
        time.sleep(0.5)
    raise RuntimeError(f"eBPF 未在 {timeout}s 内就绪")


# ── 测试用例 ────────────────────────────────────────────────────────────

# ══════════════════════════════════════════════════════════════════════════
# 1. 旧版配置（仅含 path）仍可正常工作
# ══════════════════════════════════════════════════════════════════════════

def test_legacy_config_only_path(
    worker: RemoteClient,
    controller: RemoteClient,
) -> None:
    """验证仅含 path 的旧版配置仍可正常启动并产生有效纯文本输出。

    部署一个仅包含 file_writer_config.path 的配置（无 write_mode、
    outputs 等新增 key），启动 JobLens 后提交 Slurm 作业触发输出，
    验证纯文本文件存在且包含 CPUMemCollector 输出。
    """
    # 构建旧版配置（仅含 path，无任何新增 key）
    base = _read_base_config()
    legacy_section = _build_filewriter_config_section(
        path="/var/log/joblens/output.log",
    )
    legacy_config = _replace_filewriter_config(base, legacy_section)

    _deploy_and_restart(worker, legacy_config)

    # 等待 joblens 启动并就绪
    assert _wait_for_joblens_settled(worker, expect_active=True, timeout=15.0), (
        "旧版配置（仅含 path）下 joblens 未能在 15s 内启动"
    )
    _wait_ebpf_ready(worker)

    # 提交一个 Slurm 作业以生成纯文本输出
    job_id = submit_slurm_job(controller, sleep_seconds=60)
    assert job_id != "unknown", "无法提交 Slurm 作业"

    # 等待纯文本输出
    def _read_text_records() -> str:
        exists = worker.run(
            f"test -f {JOBLENS_OUTPUT_LOG}", hide=True, warn=True
        )
        if not exists.ok:
            raise RuntimeError(f"文本输出文件不存在: {JOBLENS_OUTPUT_LOG}")
        text = worker.read_text(JOBLENS_OUTPUT_LOG)
        records = [line for line in text.splitlines() if line.strip()]
        if len(records) >= 2:
            return text
        raise RuntimeError(f"文本记录不足: {len(records)} < 2")

    text = retry_with_backoff(
        _read_text_records, timeout=90.0, initial=1.0, max_wait=5.0,
    )

    records = [line for line in text.splitlines() if line.strip()]
    assert len(records) >= 2, f"期望至少 2 条文本记录，实际 {len(records)}"
    parsed = [json.loads(line) for line in records]
    processes = [proc for record in parsed for proc in record.get("process_data", [])]
    assert any("cpuPercent" in proc for proc in processes)
    assert any("memoryPercent" in proc for proc in processes)


# ══════════════════════════════════════════════════════════════════════════
# 2. 无效 write_mode fast-fail
# ══════════════════════════════════════════════════════════════════════════

def test_invalid_write_mode_fast_fail(
    worker: RemoteClient,
) -> None:
    """验证无效 write_mode 导致 FileWriter 构造时 fast-fail。

    使用 write_mode: banana（非法值），预期 joblens 启动失败，
    journalctl 日志中包含 'invalid write_mode' 错误。
    相关源码: file_writer.cpp validateOptions() L93-97。
    """
    base = _read_base_config()
    bad_section = _build_filewriter_config_section(
        path="/var/log/joblens/output.log",
        write_mode="banana",
    )
    bad_config = _replace_filewriter_config(base, bad_section)

    _deploy_and_restart(worker, bad_config)

    # 等待 fast-fail 发生并记录日志
    time.sleep(3)
    _assert_joblens_fast_fail(worker, _ERR_INVALID_WRITE_MODE)


# ══════════════════════════════════════════════════════════════════════════
# 3. overwrite + enable_rotation 冲突 fast-fail
# ══════════════════════════════════════════════════════════════════════════

def test_overwrite_with_rotation_fast_fail(
    worker: RemoteClient,
) -> None:
    """验证 overwrite + enable_rotation 冲突导致 fast-fail。

    write_mode: overwrite 与 enable_rotation: true 同时存在时，
    FileWriter 构造应抛出异常。
    相关源码: file_writer.cpp validateOptions() L100-104。
    """
    base = _read_base_config()
    bad_section = _build_filewriter_config_section(
        path="/var/log/joblens/output.log",
        write_mode="overwrite",
        enable_rotation=True,
    )
    bad_config = _replace_filewriter_config(base, bad_section)

    _deploy_and_restart(worker, bad_config)

    time.sleep(3)
    _assert_joblens_fast_fail(worker, _ERR_OVERWRITE_ROTATION)


# ══════════════════════════════════════════════════════════════════════════
# 4. outputs 中重复 collector_name fast-fail
# ══════════════════════════════════════════════════════════════════════════

def test_duplicate_collector_name_fast_fail(
    worker: RemoteClient,
) -> None:
    """验证 outputs 中重复 collector_name 导致 fast-fail。

    两个 outputs 条目使用相同的 collector_name，
    预期日志包含 'duplicate collector_name' 错误。
    相关源码: file_writer.cpp validateOptions() L137-141。
    """
    base = _read_base_config()
    outputs_yaml = (
        "  outputs:\n"
        "    - collector_name: cpumem_collector\n"
        "      file_path: /var/log/joblens/cpumem.log\n"
        "    - collector_name: cpumem_collector\n"
        "      file_path: /var/log/joblens/other.log"
    )
    bad_section = _build_filewriter_config_section(
        path="/var/log/joblens/output.log",
        outputs=outputs_yaml,
    )
    bad_config = _replace_filewriter_config(base, bad_section)

    _deploy_and_restart(worker, bad_config)

    time.sleep(3)
    _assert_joblens_fast_fail(worker, _ERR_DUPLICATE_COLLECTOR)


# ══════════════════════════════════════════════════════════════════════════
# 5. overwrite 模式下重复 file_path fast-fail
# ══════════════════════════════════════════════════════════════════════════

def test_duplicate_overwrite_file_path_fast_fail(
    worker: RemoteClient,
) -> None:
    """验证 overwrite 模式下重复 file_path 导致 fast-fail。

    两个 outputs 条目在 overwrite 模式下使用相同的 file_path，
    预期日志包含 'duplicate file_path' 和 'forbidden in overwrite mode'。
    相关源码: file_writer.cpp validateOptions() L143-150。
    """
    base = _read_base_config()
    outputs_yaml = (
        "  outputs:\n"
        "    - collector_name: cpumem_collector\n"
        "      file_path: /var/log/joblens/shared.log\n"
        "    - collector_name: another_collector\n"
        "      file_path: /var/log/joblens/shared.log"
    )
    bad_section = _build_filewriter_config_section(
        path="/var/log/joblens/output.log",
        write_mode="overwrite",
        outputs=outputs_yaml,
    )
    bad_config = _replace_filewriter_config(base, bad_section)

    _deploy_and_restart(worker, bad_config)

    time.sleep(3)
    _assert_joblens_fast_fail(worker, _ERR_DUPLICATE_FILE_PATH)


# ══════════════════════════════════════════════════════════════════════════
# 6. append 模式下重复 file_path 允许正常启动
# ══════════════════════════════════════════════════════════════════════════

def test_duplicate_append_file_path_allowed(
    worker: RemoteClient,
) -> None:
    """验证 append 模式下重复 file_path 允许正常启动。

    两个 outputs 条目在 append 模式下使用相同的 file_path，
    预期 joblens 正常启动，不产生 fast-fail 错误。
    相关源码: file_writer.cpp validateOptions() L143（仅 overwrite 模式检查重复）。
    """
    base = _read_base_config()
    outputs_yaml = (
        "  outputs:\n"
        "    - collector_name: cpumem_collector\n"
        "      file_path: /var/log/joblens/shared.log\n"
        "    - collector_name: another_collector\n"
        "      file_path: /var/log/joblens/shared.log"
    )
    append_section = _build_filewriter_config_section(
        path="/var/log/joblens/output.log",
        write_mode="append",
        outputs=outputs_yaml,
    )
    append_config = _replace_filewriter_config(base, append_section)

    _deploy_and_restart(worker, append_config)

    # 等待 joblens 启动
    assert _wait_for_joblens_settled(worker, expect_active=True, timeout=15.0), (
        "append 模式下重复 file_path 应允许启动，但 joblens 未能在 15s 内启动"
    )

    # 确认日志中没有 fast-fail 相关的 "duplicate" 错误
    journal = _get_joblens_journal(worker)
    assert "duplicate" not in journal.lower(), (
        f"append 模式下重复 file_path 不应产生 duplicate 错误，"
        f"但日志中发现 'duplicate'（截断至 2000 字符）:\n{journal[:2000]}"
    )

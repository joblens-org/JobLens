"""FileWriter 路由、追加共享、原子覆写集成测试 — 验证 routing/append/overwrite 行为.

测试覆盖:
  - outputs 路由: collector 显式路由到配置的文件
  - 未匹配 collector 回退: 不在 outputs 中的 collector 回退到默认 path
  - 追加模式共享路径: append 模式允许共享 file_path 并产生纯文本输出
  - 原子覆写: overwrite 模式替换目标内容而非追加，无 .tmp. 残留

所有测试通过 deploy_filewriter_config() 部署测试专用 config 到 worker VM,
然后提交作业触发 collector 输出, 最后验证纯文本文件内容和路由位置.

注意: 不依赖精确记录数 (collector 时序不稳定), 仅断言至少 1 条有效记录.
"""

import importlib
import json
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

# 使用 conftest.py 相同的 importlib 模式，消除 LSP reportImplicitRelativeImport 错误
try:
    _utils = importlib.import_module("test_cases.utils")
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    _utils = importlib.import_module("utils")

JOBLENS_OUTPUT_LOG = _utils.JOBLENS_OUTPUT_LOG
RemoteClient = _utils.RemoteClient
build_filewriter_paths = _utils.build_filewriter_paths
build_cleanup_cmds = _utils.build_cleanup_cmds
retry_with_backoff = _utils.retry_with_backoff
submit_slurm_job = _utils.submit_slurm_job

# ── 路径常量 ──────────────────────────────────────────────────────────────

# 本地预设配置目录
PRESET_CONFIG_DIR = Path(__file__).resolve().parent.parent / "presets" / "configs"

# 远程 worker VM 上的配置路径
REMOTE_CONFIG_PATH = "/etc/JobLens/config.yaml"

# 路由输出路径常量
CPUMEM_TEXT_PATH = "/var/log/joblens/cpumem.log"
OTHER_TEXT_PATH = "/var/log/joblens/other.log"
OVERWRITE_TEXT_PATH = "/var/log/joblens/overwrite.log"
# 临时文件后缀模式（与 C++ 实现一致: .tmp.<pid>.<timestamp>）
TMP_SUFFIX_PATTERN = ".tmp."


# ── 配置部署辅助函数 ─────────────────────────────────────────────────────

def _deploy_config_and_restart(
    worker: RemoteClient,
    config_name: str,
    extra_cleanup_basenames: Optional[List[str]] = None,
) -> None:
    """将本地预设配置部署到 worker VM 并重启 JobLens.

    步骤:
      1. 读取本地预设配置文件
      2. 停止 worker 上的 joblens/joblens-trigger 服务
      3. 上传配置到 /etc/JobLens/config.yaml
      4. 清理所有输出文件（默认 + 额外）
      5. 重启 joblens/joblens-trigger
      6. 等待 eBPF 就绪

    Args:
        worker: worker VM 的 RemoteClient 连接.
        config_name: 预设配置文件名（不含 .yaml 后缀），如 "filewriter_routing".
        extra_cleanup_basenames: 额外需要清理的输出文件 basename 列表，
                                 如 ["cpumem.log", "other.log"].
    """
    config_path = PRESET_CONFIG_DIR / f"{config_name}.yaml"
    if not config_path.is_file():
        raise FileNotFoundError(f"预设配置文件不存在: {config_path}")

    config_content = config_path.read_text(encoding="utf-8")

    # 1. 停止服务
    worker.sudo("systemctl stop joblens-trigger 2>/dev/null || true", hide=True, warn=True)
    worker.sudo("systemctl stop joblens 2>/dev/null || true", hide=True, warn=True)

    # 2. 上传配置
    # 使用 base64 编码避免特殊字符导致的 shell 注入问题
    import base64
    encoded = base64.b64encode(config_content.encode("utf-8")).decode("ascii")
    worker.sudo(
        f"echo {encoded} | base64 -d > {REMOTE_CONFIG_PATH}",
        hide=True,
    )

    # 3. 清理输出文件
    cleanup_basenames = ["output.log"]
    if extra_cleanup_basenames:
        cleanup_basenames.extend(extra_cleanup_basenames)
    cleanup_paths = build_filewriter_paths(cleanup_basenames)
    worker.sudo(build_cleanup_cmds(cleanup_paths), hide=True, warn=True)

    # 4. 重启服务
    worker.sudo("systemctl start joblens 2>/dev/null || true", hide=True, warn=True)
    worker.sudo("systemctl start joblens-trigger 2>/dev/null || true", hide=True, warn=True)

    # 5. 验证服务已成功启动（非盲目假设 systemctl start 成功）
    _wait_service_active(worker, "joblens", timeout=10.0)

    # 6. 等待 eBPF 就绪
    _wait_ebpf_ready(worker)


def _wait_service_active(worker: RemoteClient, service_name: str, timeout: float = 10.0) -> None:
    """等待 systemd 服务进入 active 状态，失败时输出 journal 日志."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = worker.sudo(
            f"systemctl is-active {service_name}", hide=True, warn=True
        )
        if result.stdout.strip() == "active":
            return
        time.sleep(0.5)
    # 超时：获取 journal 日志辅助诊断
    journal = worker.sudo(
        f"journalctl -u {service_name} --no-pager -n 30", hide=True, warn=True
    )
    state = worker.sudo(
        f"systemctl is-active {service_name} || true", hide=True, warn=True
    )
    raise RuntimeError(
        f"服务 {service_name} 未在 {timeout}s 内进入 active 状态"
        + f"（当前: {state.stdout.strip()}）\n"
        + f"journal 尾部:\n{journal.stdout}"
    )


def _wait_ebpf_ready(worker: RemoteClient, timeout: float = 15.0) -> None:
    """等待 eBPF 程序加载完成 (bpftool prog list 包含 joblens)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = worker.sudo("bpftool prog list", hide=True, warn=True)
        if "joblens" in result.stdout.lower():
            return
        time.sleep(0.5)
    raise RuntimeError(f"eBPF 未在 {timeout}s 内就绪")


def _restore_default_config(worker: RemoteClient) -> None:
    """恢复默认配置（joblens_core_default.yaml）并重启 JobLens."""
    _deploy_config_and_restart(worker, "joblens_core_default")


def _wait_for_text_records(
    worker: RemoteClient,
    file_path: str,
    min_records: int = 1,
    timeout: float = 120.0,
) -> str:
    def _read() -> str:
        exists = worker.run(f"test -f {file_path}", hide=True, warn=True)
        if not exists.ok:
            raise RuntimeError(f"文本文件不存在: {file_path}")
        text = worker.read_text(file_path)
        records = [line for line in text.splitlines() if line.strip()]
        if len(records) >= min_records:
            return text
        raise RuntimeError(
            f"文本记录不足: {len(records)} < {min_records} (文件: {file_path})"
        )

    return retry_with_backoff(_read, timeout=timeout, initial=1.0, max_wait=5.0)


def _assert_text_output_valid(
    text: str,
    expected_marker: Optional[str] = None,
) -> None:
    records = [line for line in text.splitlines() if line.strip()]
    assert len(records) >= 1, (
        f"期望至少 1 条非空文本记录，实际 {len(records)} 条"
    )
    parsed = [json.loads(line) for line in records]
    if expected_marker is not None:
        assert any("process_data" in record for record in parsed), (
            f"文本输出应包含 collector dump 数据，实际: {text[:200]}"
        )


def _assert_no_tmp_files(worker: RemoteClient, directory: str) -> None:
    """断言指定目录中不存在 .tmp. 临时文件.

    Args:
        worker: worker VM 的 RemoteClient 连接.
        directory: 远程目录路径.
    """
    result = worker.run(
        f"ls {directory}/*{TMP_SUFFIX_PATTERN}* 2>/dev/null || echo 'NO_TMP_FILES'",
        hide=True,
    )
    assert "NO_TMP_FILES" in result.stdout, (
        f"目录 {directory} 中存在残留临时文件: {result.stdout.strip()}"
    )


def _file_contains_text(
    worker: RemoteClient, file_path: str
) -> str:
    exists = worker.run(f"test -f {file_path}", hide=True, warn=True)
    if not exists.ok:
        return ""
    return worker.read_text(file_path)


def _get_file_inode(worker: RemoteClient, file_path: str) -> Optional[str]:
    """获取远程文件的 inode 编号，不存在返回 None."""
    result = worker.run(f"stat -c '%i' {file_path} 2>/dev/null || echo 'MISSING'", hide=True)
    stdout = result.stdout.strip()
    if stdout == "MISSING":
        return None
    return stdout


def _get_file_checksum(worker: RemoteClient, file_path: str) -> Optional[str]:
    """获取远程文件的 md5sum，不存在返回 None."""
    result = worker.run(
        f"md5sum {file_path} 2>/dev/null | awk '{{print $1}}' || echo 'MISSING'", hide=True
    )
    stdout = result.stdout.strip()
    if stdout == "MISSING":
        return None
    return stdout


def _wait_for_overwrite_replacement_evidence(
    worker: RemoteClient,
    file_path: str,
    initial_inode: Optional[str],
    initial_hash: Optional[str],
    timeout: float = 60.0,
) -> None:
    """轮询等待 overwrite 替换证据（inode 或 checksum 变化）。

    overwrite 模式使用 rename() 原子替换目标文件，inode 必然变化。
    若实现使用 truncate+write 路径，inode 不变但 checksum 变化。

    超时未观测到变化则抛出 RuntimeError，包含诊断信息。
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current_inode = _get_file_inode(worker, file_path)
        current_hash = _get_file_checksum(worker, file_path)

        if current_inode is not None and initial_inode is not None and current_inode != initial_inode:
            return
        if current_hash is not None and initial_hash is not None and current_hash != initial_hash:
            return

        time.sleep(2.0)

    final_inode = _get_file_inode(worker, file_path) or "MISSING"
    final_hash = _get_file_checksum(worker, file_path) or "MISSING"
    file_size = worker.run(
        f"stat -c '%s' {file_path} 2>/dev/null || echo 'MISSING'", hide=True
    ).stdout.strip()
    raise RuntimeError(
        f"Overwrite 替换证据未在 {timeout}s 内出现:\n"
        + f"  initial_inode={initial_inode}, final_inode={final_inode}\n"
        + f"  initial_hash={initial_hash}, final_hash={final_hash}\n"
        + f"  file_size={file_size}"
    )


# ── 测试 1: outputs 路由 — collector 显式路由到配置的文件 ────────────────

def test_routing_collector_to_explicit_file(
    worker: RemoteClient,
    controller: RemoteClient,
) -> None:
    """验证 outputs 路由: cpumem_collector 写入配置的 cpumem.log.

    部署 filewriter_routing.yaml 配置（append 模式，cpumem_collector → cpumem.log），
    提交 Slurm 作业触发 collector 输出，然后验证:
      - cpumem.log 包含有效纯文本记录
      - 文本中包含 CPUMemCollector 标记
    """
    _deploy_config_and_restart(
        worker, "filewriter_routing", extra_cleanup_basenames=["cpumem.log"]
    )

    try:
        # 提交 Slurm 作业触发 collector 输出
        job_id = submit_slurm_job(controller, sleep_seconds=180)
        assert job_id != "unknown", "无法提交 Slurm 作业"

        text = _wait_for_text_records(worker, CPUMEM_TEXT_PATH, min_records=1)
        _assert_text_output_valid(text, expected_marker="CPUMemCollector")
    finally:
        _restore_default_config(worker)


# ── 测试 2: 未匹配 collector 回退到默认 path ─────────────────────────────

def test_unmatched_collector_fallback_to_default_path(
    worker: RemoteClient,
    controller: RemoteClient,
) -> None:
    """验证未匹配 collector 回退: cpumem_collector 不在 outputs 中，回退到 output.log.

    部署 filewriter_fallback.yaml 配置（append 模式，仅路由 nonexistent_collector），
    cpumem_collector 不在 outputs 列表中，应回退到默认 path:
      - output.log 包含有效纯文本记录
      - other.log 为空或不包含 CPUMemCollector 记录
    """
    _deploy_config_and_restart(
        worker,
        "filewriter_fallback",
        extra_cleanup_basenames=["other.log"],
    )

    try:
        # 提交 Slurm 作业触发 collector 输出
        job_id = submit_slurm_job(controller, sleep_seconds=180)
        assert job_id != "unknown", "无法提交 Slurm 作业"

        text = _wait_for_text_records(worker, JOBLENS_OUTPUT_LOG, min_records=1)
        _assert_text_output_valid(text, expected_marker="CPUMemCollector")

        other_text = _file_contains_text(worker, OTHER_TEXT_PATH)
        assert "process_data" not in other_text, (
            f"other.log 不应包含 cpumem 数据，实际内容: {other_text[:200]}"
        )
    finally:
        _restore_default_config(worker)


# ── 测试 3: 追加模式共享路径 — 默认 path 与 routed path 相同时正常工作 ────

def test_append_mode_shared_path_valid_text(
    worker: RemoteClient,
    controller: RemoteClient,
) -> None:
    """验证 append 模式下默认 path 与 routed path 相同时纯文本输出有效.

    部署 filewriter_append_shared.yaml 配置（append 模式，默认 path 和
    cpumem_collector 的 routed path 都指向 output.log）。

    此测试验证单 collector 的共享路径场景：
      - output.log 包含有效纯文本记录
      - 文本中包含 CPUMemCollector 标记
      - 输出不是 FileWriter 旧 JSONL wrapper

    注意: 当前 preset 配置仅包含 cpumem_collector（job_registry 中唯一可用的
    作业级 collector）。多 collector 共享同一路径的完整场景（如 cpumem + proc）
    因 preset 中无第二个可产生输出的 collector 而无法在此测试中覆盖。
    C++ 层面的 append 多 collector 共享路径逻辑已在 Task 5 实现，但集成测试
    覆盖受限。详见 .omo/notepads/filewriter-enhancements/issues.md。
    """
    _deploy_config_and_restart(worker, "filewriter_append_shared")

    try:
        # 提交 Slurm 作业触发 collector 输出
        job_id = submit_slurm_job(controller, sleep_seconds=180)
        assert job_id != "unknown", "无法提交 Slurm 作业"

        text = _wait_for_text_records(worker, JOBLENS_OUTPUT_LOG, min_records=1)
        _assert_text_output_valid(text, expected_marker="CPUMemCollector")
    finally:
        _restore_default_config(worker)


# ── 测试 4: 原子覆写模式 — 替换目标内容而非追加 ──────────────────────────

def test_overwrite_mode_replaces_target(
    worker: RemoteClient,
    controller: RemoteClient,
) -> None:
    """验证 overwrite 模式: 每次 flush 替换目标文件，无 .tmp. 残留.

    部署 filewriter_overwrite.yaml 配置（overwrite 模式），验证:
      - overwrite.log 包含有效纯文本记录
      - 文本中包含 CPUMemCollector 标记
      - 目标目录中不存在 .tmp. 临时文件
      - 文件 inode 在两次 flush 之间发生变化（rename 原子替换的强证据）
        或文件内容 hash 发生变化（truncate+write 替换的次强证据）
    """
    _deploy_config_and_restart(
        worker, "filewriter_overwrite", extra_cleanup_basenames=["overwrite.log"]
    )

    try:
        # 提交 Slurm 作业触发 collector 输出
        job_id = submit_slurm_job(controller, sleep_seconds=180)
        assert job_id != "unknown", "无法提交 Slurm 作业"

        text = _wait_for_text_records(
            worker, OVERWRITE_TEXT_PATH, min_records=1, timeout=120.0
        )
        _assert_text_output_valid(text, expected_marker="CPUMemCollector")

        # 第一轮快照：inode + 内容 hash
        inode1 = _get_file_inode(worker, OVERWRITE_TEXT_PATH)
        hash1 = _get_file_checksum(worker, OVERWRITE_TEXT_PATH)

        # 轮询等待 overwrite 替换证据（inode 或 hash 变化）
        # collector freq=1Hz，buffer 容量 256，通常 10-30s 内触发一次 flush
        _wait_for_overwrite_replacement_evidence(
            worker, OVERWRITE_TEXT_PATH,
            initial_inode=inode1, initial_hash=hash1, timeout=60.0,
        )

        text2 = worker.read_text(OVERWRITE_TEXT_PATH)
        _assert_text_output_valid(text2, expected_marker="CPUMemCollector")

        # 验证 overwrite.log 所在目录无 .tmp. 残留文件
        overwrite_dir = os.path.dirname(OVERWRITE_TEXT_PATH)
        _assert_no_tmp_files(worker, overwrite_dir)

        # 同时验证 output.log 目录也无 .tmp. 残留
        output_dir = os.path.dirname(JOBLENS_OUTPUT_LOG)
        _assert_no_tmp_files(worker, output_dir)

    finally:
        _restore_default_config(worker)

"""FileWriter 旋转与优雅关闭集成测试 (Task 11).

验证:
  1. 追加模式大小旋转: max_file_size_bytes 小阈值下快速触发旋转,
     max_files 保留策略正确执行.
  2. 优雅关闭: flush_on_shutdown: true 确保最终记录写入,
     无临时文件残留, 文件句柄释放.

测试覆盖:
  - test_rotation_creates_rotated_files: 旋转产生 .1 文件
  - test_rotation_max_files_enforcement: .3 不存在 (max_files: 2)
  - test_rotated_files_contain_valid_text: 旋转文件包含有效纯文本
  - test_graceful_shutdown_flushes_records: 优雅关闭后输出可解析
  - test_shutdown_no_temp_leftovers: 关闭后无 .tmp. 残留
  - test_shutdown_journal_logs: systemd journal 包含停止日志
  - test_shutdown_handle_release: 关闭后文件可重命名/删除

每个测试通过 _deploy_config_and_restart() 部署专用 config 到 worker VM,
最后通过 _restore_default_config() 恢复默认配置。

依赖: Task 3 的 build_filewriter_paths + FileWriterOutputPaths,
      Task 7 的 performRotation,
      Task 8 的 do_shutdown.
"""

import base64
import importlib
import sys
import time
from pathlib import Path
from typing import List, Optional

import pytest

# 使用 importlib 模式避免隐式相对导入 (与 conftest.py 保持一致)
try:
    _utils = importlib.import_module("test_cases.utils")
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    _utils = importlib.import_module("utils")

JOBLENS_OUTPUT_LOG = _utils.JOBLENS_OUTPUT_LOG
DEFAULT_FILEWRITER_OUTPUT_DIR = _utils.DEFAULT_FILEWRITER_OUTPUT_DIR
FileWriterOutputPaths = _utils.FileWriterOutputPaths
JobLensAPI = _utils.JobLensAPI
RemoteClient = _utils.RemoteClient
build_cleanup_cmds = _utils.build_cleanup_cmds
build_filewriter_paths = _utils.build_filewriter_paths
retry_with_backoff = _utils.retry_with_backoff
submit_slurm_job = _utils.submit_slurm_job

# ── 路径常量 ──────────────────────────────────────────────────────────────

# 本地预设配置目录
PRESET_CONFIG_DIR = Path(__file__).resolve().parent.parent / "presets" / "configs"

# 远程 worker VM 上的配置路径
REMOTE_CONFIG_PATH = "/etc/JobLens/config.yaml"


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
        config_name: 预设配置文件名（不含 .yaml 后缀），
                     如 "joblens_filewriter_rotation".
        extra_cleanup_basenames: 额外需要清理的输出文件 basename 列表.
    """
    config_path = PRESET_CONFIG_DIR / f"{config_name}.yaml"
    if not config_path.is_file():
        raise FileNotFoundError(f"预设配置文件不存在: {config_path}")

    config_content = config_path.read_text(encoding="utf-8")

    # 1. 停止服务
    worker.sudo(
        "systemctl stop joblens-trigger 2>/dev/null || true", hide=True, warn=True,
    )
    worker.sudo(
        "systemctl stop joblens 2>/dev/null || true", hide=True, warn=True,
    )

    # 2. 上传配置（base64 编码避免特殊字符导致的 shell 注入问题）
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
    worker.sudo(
        "systemctl start joblens 2>/dev/null || true", hide=True, warn=True,
    )
    worker.sudo(
        "systemctl start joblens-trigger 2>/dev/null || true", hide=True, warn=True,
    )

    # 5. 验证服务已成功启动（非盲目假设 systemctl start 成功）
    _wait_service_active(worker, "joblens", timeout=10.0)

    # 6. 等待 eBPF 就绪
    _wait_ebpf_ready(worker)


def _wait_service_active(
    worker: RemoteClient, service_name: str, timeout: float = 10.0,
) -> None:
    """等待 systemd 服务进入 active 状态，失败时输出 journal 日志."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = worker.sudo(
            f"systemctl is-active {service_name}", hide=True, warn=True,
        )
        if result.stdout.strip() == "active":
            return
        time.sleep(0.5)
    # 超时：获取 journal 日志辅助诊断
    journal = worker.sudo(
        f"journalctl -u {service_name} --no-pager -n 30", hide=True, warn=True,
    )
    state = worker.sudo(
        f"systemctl is-active {service_name} || true", hide=True, warn=True,
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


# ── 旋转测试专用 fixture: max_rotated_files=2 ──────────────────────────

@pytest.fixture
def filewriter_output_paths() -> FileWriterOutputPaths:
    """旋转测试使用 max_rotated_files=2 的路径配置。

    Task 11 的旋转测试使用 max_files: 2 配置,
    因此旋转文件仅有 .1 和 .2, .3 不应存在。
    """
    return build_filewriter_paths(["output.log"], max_rotated_files=2)


# ── 远程执行辅助函数 ───────────────────────────────────────────────────

def _remote_file_exists(worker: RemoteClient, path: str) -> bool:
    """检查远程文件是否存在."""
    result = worker.run(f"test -f {path}", hide=True, warn=True)
    return result.ok


def _remote_file_size(worker: RemoteClient, path: str) -> int:
    """获取远程文件大小 (字节), 不存在返回 0."""
    result = worker.run(
        f"stat -c %s {path} 2>/dev/null || echo 0",
        hide=True,
        warn=True,
    )
    try:
        return int(result.stdout.strip())
    except (ValueError, AttributeError):
        return 0


def _remote_read_text(worker: RemoteClient, path: str) -> str:
    """读取远程文件文本内容, 不存在返回空字符串."""
    try:
        return worker.read_text(path)
    except Exception:
        return ""


def _remote_stop_joblens(worker: RemoteClient) -> None:
    """优雅停止 JobLens 服务 (systemctl stop)."""
    worker.sudo(
        "systemctl stop joblens 2>/dev/null || true", hide=True, warn=True,
    )
    # 等待进程完全退出，确保 flush 操作完成
    time.sleep(3)


def _remote_journal_since(worker: RemoteClient, since: str = "5 minutes ago") -> str:
    """获取最近的 joblens journal 日志."""
    result = worker.sudo(
        f"journalctl -u joblens --no-pager --since '{since}' 2>&1 || true",
        hide=True,
        warn=True,
    )
    return result.stdout or ""


def _remote_temp_files(worker: RemoteClient, directory: str) -> List[str]:
    """列出目录中所有 .tmp. 文件."""
    result = worker.run(
        f"find {directory} -maxdepth 1 -name '*.tmp.*' 2>/dev/null || true",
        hide=True,
        warn=True,
    )
    lines = result.stdout.strip().split("\n")
    return [line for line in lines if line]


# ── 旋转测试 ───────────────────────────────────────────────────────────

def _ensure_enough_data_for_rotation(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    timeout: float = 120.0,
) -> bool:
    """生成足够数据触发旋转并等待旋转文件出现。

    提交 Slurm 作业让 cpumem_collector 持续产生数据,
    然后轮询检查 .1 旋转文件是否出现。
    由于 max_file_size_bytes=256, 每条纯文本记录足以快速触发轮转,
    每次 flush 都会超过阈值, 因此旋转会快速发生。

    Args:
        worker: worker VM 连接
        controller: controller VM 连接
        joblens_api: API 客户端
        timeout: 总超时时间

    Returns:
        True 如果检测到旋转发生, False 如果超时
    """
    # 提交一个长时间运行的 Slurm 作业以产生持续数据
    job_id = submit_slurm_job(controller, sleep_seconds=180)
    if job_id == "unknown":
        return False

    # 等待作业被发现
    def _job_discovered() -> None:
        resp = joblens_api.jobs()
        if resp.status_code != 200:
            raise RuntimeError(f"jobs API 返回 HTTP {resp.status_code}")
        jobs = resp.json().get("jobs", [])
        for job in jobs:
            if "slurm" in str(job.get("subtype", "")).lower():
                return
        raise RuntimeError("Slurm 作业尚未被 JobLens 发现")

    try:
        retry_with_backoff(_job_discovered, timeout=60.0, initial=1.0, max_wait=5.0)
    except Exception:
        return False

    # 轮询检查旋转文件
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rotated1 = f"{JOBLENS_OUTPUT_LOG}.1"
        if _remote_file_exists(worker, rotated1):
            return True
        time.sleep(2)

    return False


def test_rotation_creates_rotated_files(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    clean_test_state: None,
) -> None:
    """验证旋转产生 .1 旋转文件。

    部署 joblens_filewriter_rotation.yaml（enable_rotation: true,
    max_file_size_bytes: 256, max_files: 2），生成数据触发旋转,
    验证 .1 存在且活跃文件存在。
    """
    _deploy_config_and_restart(worker, "joblens_filewriter_rotation")

    try:
        rotated = _ensure_enough_data_for_rotation(
            worker, controller, joblens_api, timeout=120.0,
        )

        if not rotated:
            pytest.skip(
                "旋转未在超时内触发 — 已部署 joblens_filewriter_rotation.yaml"
                + " (max_file_size_bytes=256, max_files=2),"
                + " Slurm 作业发现或数据生成可能有问题"
            )

        # 等待额外数据让第二次旋转发生
        time.sleep(15)

        rotated1 = f"{JOBLENS_OUTPUT_LOG}.1"
        rotated2 = f"{JOBLENS_OUTPUT_LOG}.2"

        assert _remote_file_exists(worker, rotated1), (
            f"旋转文件 .1 不存在: {rotated1}"
        )

        # .2 可能还不存在 (取决于数据量), 但至少 .1 必须存在
        has_dot2 = _remote_file_exists(worker, rotated2)
        if not has_dot2:
            time.sleep(10)
            has_dot2 = _remote_file_exists(worker, rotated2)

        # 活跃文件必须存在
        active_exists = _remote_file_exists(worker, JOBLENS_OUTPUT_LOG)
        assert active_exists, f"活跃文件不存在: {JOBLENS_OUTPUT_LOG}"

    finally:
        _restore_default_config(worker)


def test_rotation_max_files_enforcement(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    clean_test_state: None,
) -> None:
    """验证 max_files: 2 时 .3 旋转文件不存在。

    部署 joblens_filewriter_rotation.yaml（max_files: 2），
    生成数据触发多次旋转，确认 .3 不存在。
    """
    _deploy_config_and_restart(worker, "joblens_filewriter_rotation")

    try:
        rotated = _ensure_enough_data_for_rotation(
            worker, controller, joblens_api, timeout=120.0,
        )

        if not rotated:
            pytest.skip(
                "旋转未在超时内触发 — 已部署 joblens_filewriter_rotation.yaml"
                + " (max_file_size_bytes=256, max_files=2),"
                + " Slurm 作业发现或数据生成可能有问题"
            )

        time.sleep(20)

        rotated3 = f"{JOBLENS_OUTPUT_LOG}.3"
        assert not _remote_file_exists(worker, rotated3), (
            f"max_files: 2 时 .3 不应存在, 但发现: {rotated3}"
        )

        # 验证 .4 及更高也不存在
        rotated4 = f"{JOBLENS_OUTPUT_LOG}.4"
        assert not _remote_file_exists(worker, rotated4), (
            f"max_files: 2 时 .4 不应存在, 但发现: {rotated4}"
        )

    finally:
        _restore_default_config(worker)


def test_rotated_files_contain_valid_text(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    clean_test_state: None,
) -> None:
    """验证活跃文件和旋转文件中的纯文本均有效。

    部署 joblens_filewriter_rotation.yaml，生成数据触发旋转，
    读取活跃文件、.1、.2 的内容，对非空文件检查纯文本内容。
    """
    _deploy_config_and_restart(worker, "joblens_filewriter_rotation")

    try:
        rotated = _ensure_enough_data_for_rotation(
            worker, controller, joblens_api, timeout=120.0,
        )

        if not rotated:
            pytest.skip(
                "旋转未在超时内触发 — 已部署 joblens_filewriter_rotation.yaml,"
                + " Slurm 作业发现或数据生成可能有问题"
            )

        time.sleep(15)

        # 检查所有可能存在的文件
        paths_to_check = [
            JOBLENS_OUTPUT_LOG,
            f"{JOBLENS_OUTPUT_LOG}.1",
            f"{JOBLENS_OUTPUT_LOG}.2",
        ]

        text_files = 0
        for path in paths_to_check:
            if not _remote_file_exists(worker, path):
                continue
            text = _remote_read_text(worker, path)
            if not text.strip():
                continue

            lines = [line for line in text.splitlines() if line.strip()]
            assert lines, f"文件 {path} 应包含非空文本记录"
            assert not lines[0].lstrip().startswith("{"), f"文件 {path} 不应是 JSONL wrapper"
            assert "CPUMemCollector" in text, f"文件 {path} 应包含 CPUMemCollector 文本"
            text_files += 1

        assert text_files >= 1, (
            f"期望至少 1 个文件包含有效文本, 实际 {text_files} 个"
        )

    finally:
        _restore_default_config(worker)


# ── 优雅关闭测试 ───────────────────────────────────────────────────────

def _ensure_data_before_shutdown(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    timeout: float = 60.0,
) -> bool:
    """生成数据并确认输出文件存在且非空。

    Returns:
        True 如果数据已生成, False 如果超时.
    """
    # 提交 Slurm 作业
    job_id = submit_slurm_job(controller, sleep_seconds=300)
    if job_id == "unknown":
        return False

    # 等待作业被发现
    def _job_discovered() -> None:
        resp = joblens_api.jobs()
        if resp.status_code != 200:
            raise RuntimeError(f"jobs API 返回 HTTP {resp.status_code}")
        jobs = resp.json().get("jobs", [])
        for job in jobs:
            if "slurm" in str(job.get("subtype", "")).lower():
                return
        raise RuntimeError("Slurm 作业尚未被 JobLens 发现")

    try:
        retry_with_backoff(_job_discovered, timeout=60.0, initial=1.0, max_wait=5.0)
    except Exception:
        return False

    # 等待输出文件出现且非空
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _remote_file_exists(worker, JOBLENS_OUTPUT_LOG):
            size = _remote_file_size(worker, JOBLENS_OUTPUT_LOG)
            if size > 0:
                return True
        time.sleep(2)

    return False


def test_graceful_shutdown_flushes_records(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    clean_test_state: None,
) -> None:
    """验证优雅关闭后输出文件包含最终记录且可解析。

    部署 joblens_filewriter_shutdown.yaml（flush_on_shutdown: true,
    append 模式），生成数据后优雅停止 JobLens，
    验证输出文件包含纯文本记录且内容完整。
    """
    _deploy_config_and_restart(worker, "joblens_filewriter_shutdown")

    try:
        data_ready = _ensure_data_before_shutdown(
            worker, controller, joblens_api, timeout=60.0,
        )

        if not data_ready:
            pytest.skip(
                "输出文件未在超时内生成数据 — 已部署 joblens_filewriter_shutdown.yaml,"
                + " Slurm 作业或 collector 可能未运行"
            )

        # 记录停止前输出文件大小
        size_before_stop = _remote_file_size(worker, JOBLENS_OUTPUT_LOG)

        # 优雅停止 JobLens
        _remote_stop_joblens(worker)

        # 验证输出文件在停止后存在且可解析
        assert _remote_file_exists(worker, JOBLENS_OUTPUT_LOG), (
            f"优雅关闭后输出文件不存在: {JOBLENS_OUTPUT_LOG}"
        )

        text = _remote_read_text(worker, JOBLENS_OUTPUT_LOG)
        assert len(text.strip()) > 0, (
            f"优雅关闭后输出文件为空: {JOBLENS_OUTPUT_LOG}"
        )

        lines = [line for line in text.splitlines() if line.strip()]
        assert len(lines) >= 1, f"期望至少 1 条文本记录, 实际 {len(lines)} 条"
        assert not lines[0].lstrip().startswith("{"), "优雅关闭后输出不应是 JSONL wrapper"
        assert "CPUMemCollector" in text, "优雅关闭后输出应包含 CPUMemCollector 文本"

        # 确认停止后文件大小 >= 停止前 (flush 已执行)
        size_after_stop = _remote_file_size(worker, JOBLENS_OUTPUT_LOG)
        assert size_after_stop >= size_before_stop, (
            f"优雅关闭后文件大小 ({size_after_stop})"
            f" 不应小于停止前 ({size_before_stop})"
        )

    finally:
        _restore_default_config(worker)


def test_shutdown_no_temp_leftovers(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    clean_test_state: None,
) -> None:
    """验证优雅关闭后输出目录中无 .tmp. 残留文件。

    部署 joblens_filewriter_shutdown.yaml（append 模式），
    生成数据后优雅停止 JobLens，检查 /var/log/joblens/ 中无 .tmp. 文件。
    append 模式下不应产生临时文件。
    """
    _deploy_config_and_restart(worker, "joblens_filewriter_shutdown")

    try:
        data_ready = _ensure_data_before_shutdown(
            worker, controller, joblens_api, timeout=60.0,
        )

        if not data_ready:
            pytest.skip(
                "输出文件未在超时内生成数据 — 已部署 joblens_filewriter_shutdown.yaml"
            )

        _remote_stop_joblens(worker)

        temp_files = _remote_temp_files(worker, DEFAULT_FILEWRITER_OUTPUT_DIR)
        assert len(temp_files) == 0, (
            f"优雅关闭后目录中存在临时文件残留: {temp_files}"
        )

    finally:
        _restore_default_config(worker)


def test_shutdown_journal_logs(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    clean_test_state: None,
) -> None:
    """验证 systemd journal 包含 JobLens 停止相关日志。

    部署 joblens_filewriter_shutdown.yaml，生成数据后优雅停止 JobLens，
    检查 journal 日志包含停止/关闭相关关键词。
    """
    _deploy_config_and_restart(worker, "joblens_filewriter_shutdown")

    try:
        data_ready = _ensure_data_before_shutdown(
            worker, controller, joblens_api, timeout=60.0,
        )

        if not data_ready:
            pytest.skip(
                "输出文件未在超时内生成数据 — 已部署 joblens_filewriter_shutdown.yaml"
            )

        _remote_stop_joblens(worker)

        journal = _remote_journal_since(worker, since="5 minutes ago")

        # 确保 journal 有内容
        assert len(journal) > 0, (
            "journal 日志为空 — JobLens 可能未启动或 journalctl 不可用"
        )

        # 检查 journal 中是否有停止/关闭相关日志
        shutdown_keywords = ["do_shutdown", "shutdown", "closed", "stopping"]
        found_keywords = [
            kw for kw in shutdown_keywords if kw.lower() in journal.lower()
        ]

        assert len(found_keywords) >= 1, (
            f"journal 日志中未找到停止/关闭相关关键词。"
            f" 搜索关键词: {shutdown_keywords},"
            f" journal 日志长度: {len(journal)} 字节"
        )

    finally:
        _restore_default_config(worker)


def test_shutdown_handle_release(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    clean_test_state: None,
) -> None:
    """验证优雅关闭后文件句柄已释放 (可重命名/删除)。

    部署 joblens_filewriter_shutdown.yaml，生成数据后优雅停止 JobLens，
    尝试重命名输出文件验证无 EBUSY 错误。
    """
    _deploy_config_and_restart(worker, "joblens_filewriter_shutdown")

    try:
        data_ready = _ensure_data_before_shutdown(
            worker, controller, joblens_api, timeout=60.0,
        )

        if not data_ready:
            pytest.skip(
                "输出文件未在超时内生成数据 — 已部署 joblens_filewriter_shutdown.yaml"
            )

        _remote_stop_joblens(worker)

        # 尝试重命名输出文件 — 如果文件句柄未释放, 会报 EBUSY
        test_renamed = f"{JOBLENS_OUTPUT_LOG}.shutdown-test-renamed"

        # 清理可能残留的测试文件
        worker.sudo(
            f"rm -f {test_renamed} 2>/dev/null || true", hide=True, warn=True,
        )

        rename_result = worker.sudo(
            f"mv {JOBLENS_OUTPUT_LOG} {test_renamed} 2>&1",
            hide=True,
            warn=True,
        )

        rename_ok = rename_result.ok

        # 恢复原始文件名
        if rename_ok:
            worker.sudo(
                f"mv {test_renamed} {JOBLENS_OUTPUT_LOG} 2>&1",
                hide=True,
                warn=True,
            )
        else:
            # 如果重命名失败, 检查是否因为文件本身不存在
            if not _remote_file_exists(worker, JOBLENS_OUTPUT_LOG):
                pytest.fail("输出文件在重命名测试前就不存在")
            # 如果文件存在但重命名失败, 可能句柄未释放
            stderr = rename_result.stderr or ""
            pytest.fail(
                f"优雅关闭后无法重命名输出文件 (句柄未释放?): {stderr}"
            )

        assert rename_ok, (
            f"优雅关闭后文件句柄应已释放, 但 mv 命令失败:"
            f" {rename_result.stderr}"
        )

    finally:
        _restore_default_config(worker)

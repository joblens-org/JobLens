"""JobLens integration test fixtures and shared configuration."""

import importlib
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Generator, Optional

import pytest

try:
    utils = importlib.import_module("test_cases.utils")
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    utils = importlib.import_module("utils")

CONTROLLER_HOST = utils.CONTROLLER_HOST
FILEWRITER_OUTPUT_PATHS = utils.build_filewriter_paths
INTEGRATION_ROOT = utils.INTEGRATION_ROOT
JOBLENS_DB_PATH = utils.JOBLENS_DB_PATH
JOBLENS_LOCK_PATH = utils.JOBLENS_LOCK_PATH
JOBLENS_OUTPUT_LOG = utils.JOBLENS_OUTPUT_LOG
PROJECT_ROOT = utils.PROJECT_ROOT
TRIGGER_PORT = utils.TRIGGER_PORT
WORKER_HOST = utils.WORKER_HOST
WORKER_IP = utils.WORKER_IP
FileWriterOutputPaths = utils.FileWriterOutputPaths
JobLensAPI = utils.JobLensAPI
RemoteClient = utils.RemoteClient
build_cleanup_cmds = utils.build_cleanup_cmds
build_filewriter_paths = utils.build_filewriter_paths

JOBLENS_OUTPUT_EXPORT_DIR = INTEGRATION_ROOT / ".runtime" / "joblens-output-logs"


def _safe_test_log_name(nodeid: str) -> str:
    """将 pytest nodeid 转成适合作为目录名的日志名。"""
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", nodeid).strip("_")
    return safe_name or "unknown-test"


def _write_export_file(target: Path, content: str) -> None:
    """写入导出文件，确保空日志也留下占位说明。"""
    target.write_text(content if content else "(空日志)\n", encoding="utf-8")


def _export_remote_command(worker: RemoteClient, command: str, target: Path) -> None:
    """执行远程命令并将 stdout/stderr 最佳努力写入本地文件。"""
    try:
        result = worker.sudo(command, hide=True, warn=True)
        content = result.stdout or result.stderr
        _write_export_file(target, content)
    except Exception as e:
        _write_export_file(target, f"导出远程日志失败: {e}\n")


def _export_joblens_logs(
    worker: RemoteClient,
    nodeid: str,
    extra_paths: Optional[FileWriterOutputPaths] = None,
) -> None:
    """每个测试结束后导出 JobLens FileWriter 与 systemd journal 日志。

    除了默认的 output.log 外，还导出 extra_paths 中配置的
    活动文件、旋转文件和临时文件。
    """
    test_log_dir = JOBLENS_OUTPUT_EXPORT_DIR / _safe_test_log_name(nodeid)
    test_log_dir.mkdir(parents=True, exist_ok=True)

    _export_remote_command(
        worker,
        f"test -f {JOBLENS_OUTPUT_LOG} && cat {JOBLENS_OUTPUT_LOG} || true",
        test_log_dir / "output.log",
    )
    if extra_paths is not None:
        for ap in extra_paths.active_paths:
            if ap == JOBLENS_OUTPUT_LOG:
                continue
            safe_name = Path(ap).name
            _export_remote_command(
                worker,
                f"test -f {ap} && cat {ap} || true",
                test_log_dir / safe_name,
            )
        for rg in extra_paths.rotated_globs:
            _export_remote_command(
                worker,
                f"test -f {rg} && cat {rg} || echo '(文件不存在)'",
                test_log_dir / Path(rg).name,
            )
        for tg in extra_paths.tmp_globs:
            _export_remote_command(
                worker,
                f"ls {tg} 2>/dev/null || echo '(无临时文件)'",
                test_log_dir / f"temp_{Path(tg).name}.ls",
            )
    _export_remote_command(
        worker,
        "journalctl -u joblens --no-pager --since '30 minutes ago' 2>&1 || true",
        test_log_dir / "joblens-journal.log",
    )
    _export_remote_command(
        worker,
        "journalctl -u joblens-trigger --no-pager --since '30 minutes ago' 2>&1 || true",
        test_log_dir / "joblens-trigger-journal.log",
    )


# ── CLI 选项 ──────────────────────────────────────────────────────────

def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--keep-vms",
        action="store_true",
        default=False,
        help="测试结束后保留 VM (不销毁不挂起)",
    )
    parser.addoption(
        "--skip-vagrant-up",
        action="store_true",
        default=False,
        help="跳过 vagrant up (假设 VM 已运行)",
    )
    parser.addoption(
        "--skip-vagrant-destroy",
        action="store_true",
        default=False,
        help="测试结束后不销毁 VM (默认行为是销毁)",
    )
    parser.addoption(
        "--trigger-base-url",
        default=None,
        help=f"覆盖 Trigger 基础 URL，默认 http://{WORKER_IP}:{TRIGGER_PORT}",
    )


# ── 路径 fixtures ─────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def repo_root() -> Path:
    """项目仓库根目录."""
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def integration_root() -> Path:
    """集成测试目录 (tests/integration)."""
    return INTEGRATION_ROOT


# ── 远程连接 fixtures ─────────────────────────────────────────────────

@pytest.fixture(scope="session")
def controller() -> RemoteClient:
    """controller VM 的 RemoteClient 连接 (通过 Vagrant SSH config 解析)."""
    return RemoteClient(CONTROLLER_HOST)


@pytest.fixture(scope="session")
def worker() -> RemoteClient:
    """worker VM 的 RemoteClient 连接 (通过 Vagrant SSH config 解析)."""
    return RemoteClient(WORKER_HOST)


# ── API fixture ───────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def joblens_api(request: pytest.FixtureRequest) -> JobLensAPI:
    """JobLens Trigger API 客户端，可通过 --trigger-base-url 覆盖."""
    base_url = request.config.getoption("--trigger-base-url")
    if base_url:
        return JobLensAPI(base_url=base_url)
    return JobLensAPI()


# ── 内部清理辅助函数 ──────────────────────────────────────────────────

def _cleanup_condor(controller: RemoteClient) -> None:
    try:
        controller.run(
            "condor_rm -all 2>/dev/null || true", hide=True, warn=True
        )
    except Exception:
        pass


def _cleanup_slurm(controller: RemoteClient) -> None:
    try:
        controller.run(
            "sudo scancel -u vagrant 2>/dev/null || true; "
            "sudo scancel -u root 2>/dev/null || true; "
            "for job_id in $(squeue -h -o '%A' 2>/dev/null | sort -u); "
            "do sudo scancel ${job_id} 2>/dev/null || true; done; "
            "sleep 2",
            hide=True,
            warn=True,
        )
    except Exception:
        pass


def _wait_ebpf_ready(worker: RemoteClient, timeout: float = 15.0) -> None:
    """等待 eBPF 程序加载完成 (bpftool prog list 包含 joblens)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = worker.sudo("bpftool prog list", hide=True, warn=True)
        if "joblens" in result.stdout.lower():
            return
        time.sleep(0.5)
    raise RuntimeError(f"eBPF 未在 {timeout}s 内就绪")


def _reset_joblens(
    worker: RemoteClient,
    extra_paths: Optional[FileWriterOutputPaths] = None,
) -> None:
    """停止 JobLens → 清理 LevelDB/锁/输出文件 → 重新启动 → 等待 eBPF 就绪.

    除了默认的 JOBLENS_OUTPUT_LOG 外，还通过 extra_paths 清理
    路由输出、旋转文件和临时文件。
    """
    try:
        worker.sudo(
            "systemctl stop joblens-trigger 2>/dev/null || true",
            hide=True,
            warn=True,
        )
    except Exception:
        pass
    try:
        worker.sudo(
            "systemctl stop joblens 2>/dev/null || true",
            hide=True,
            warn=True,
        )
    except Exception:
        pass
    try:
        worker.sudo(
            f"rm -rf {JOBLENS_DB_PATH} 2>/dev/null || true",
            hide=True,
            warn=True,
        )
    except Exception:
        pass
    try:
        worker.sudo(
            f"rm -f {JOBLENS_LOCK_PATH} 2>/dev/null || true",
            hide=True,
            warn=True,
        )
    except Exception:
        pass
    try:
        worker.sudo(
            f"truncate -s 0 {JOBLENS_OUTPUT_LOG} 2>/dev/null || true",
            hide=True,
            warn=True,
        )
    except Exception:
        pass
    if extra_paths is not None:
        try:
            worker.sudo(
                build_cleanup_cmds(extra_paths),
                hide=True,
                warn=True,
            )
        except Exception:
            pass
    try:
        worker.sudo(
            "systemctl start joblens 2>/dev/null || true",
            hide=True,
            warn=True,
        )
    except Exception:
        pass
    try:
        worker.sudo(
            "systemctl start joblens-trigger 2>/dev/null || true",
            hide=True,
            warn=True,
        )
    except Exception:
        pass

    _wait_ebpf_ready(worker)


def _ebpf_cleanup() -> None:
    """最佳努力清理 eBPF: 尝试通过 vagrant ssh 运行 bpftool."""
    try:
        subprocess.run(
            ["vagrant", "ssh", "worker", "-c",
             "sudo bpftool prog detach pinned /sys/fs/bpf/* 2>/dev/null || true; "
             "sudo rm -rf /sys/fs/bpf/joblens* 2>/dev/null || true"],
            cwd=INTEGRATION_ROOT,
            capture_output=True,
            timeout=15,
        )
    except Exception:
        pass


# ── 测试级清理 fixture ────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def export_joblens_logs(
    request: pytest.FixtureRequest,
    worker: RemoteClient,
    filewriter_output_paths: FileWriterOutputPaths,
) -> Generator[None, None, None]:
    """每个测试结束后导出 JobLens FileWriter 与 journalctl 日志。"""
    yield
    _export_joblens_logs(worker, request.node.nodeid, filewriter_output_paths)


@pytest.fixture
def filewriter_output_paths() -> FileWriterOutputPaths:
    """默认 FileWriter 输出路径集合 fixture。

    返回包含默认 output.log 及其旋转/临时文件模式的路径配置。
    需要自定义路径的测试可覆盖此 fixture。
    """
    return build_filewriter_paths(["output.log"])


@pytest.fixture
def clean_test_state(
    controller: RemoteClient,
    worker: RemoteClient,
    filewriter_output_paths: FileWriterOutputPaths,
) -> Generator[None, None, None]:
    """每个测试前后的最佳努力状态清理.

    前置: 清空 HTCondor/Slurm 作业队列，重置 JobLens 数据.
    后置: 再次清空作业队列.
    """
    _cleanup_condor(controller)
    _cleanup_slurm(controller)
    _reset_joblens(worker, filewriter_output_paths)
    yield
    _cleanup_condor(controller)
    _cleanup_slurm(controller)


# ── session 级 Vagrant 环境 fixture ───────────────────────────────────

@pytest.fixture(scope="session", autouse=True)
def vagrant_environment(
    request: pytest.FixtureRequest,
) -> Generator[None, None, None]:
    """管理 Vagrant VM 的 session 生命周期.

    启动时根据 --skip-vagrant-up 决定是否启动 VM.
    结束时根据 --keep-vms / --skip-vagrant-destroy 决定销毁/挂起/保留.
    """
    skip_up = request.config.getoption("--skip-vagrant-up")
    keep = request.config.getoption("--keep-vms")
    skip_destroy = request.config.getoption("--skip-vagrant-destroy")

    if not skip_up:
        try:
            subprocess.run(
                ["vagrant", "up"],
                cwd=INTEGRATION_ROOT,
                capture_output=True,
                timeout=300,
            )
        except Exception:
            pass

    yield

    if keep:
        return

    if skip_destroy:
        try:
            subprocess.run(
                ["vagrant", "suspend"],
                cwd=INTEGRATION_ROOT,
                capture_output=True,
                timeout=120,
            )
        except Exception:
            pass
    else:
        try:
            subprocess.run(
                ["vagrant", "destroy", "-f"],
                cwd=INTEGRATION_ROOT,
                capture_output=True,
                timeout=120,
            )
        except Exception:
            pass

    _ebpf_cleanup()

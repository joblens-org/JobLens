"""JobLens integration test fixtures and shared configuration."""

import subprocess
import time
from pathlib import Path
from typing import Generator

import pytest

from utils import (
    CONTROLLER_HOST,
    INTEGRATION_ROOT,
    JOBLENS_DB_PATH,
    JOBLENS_LOCK_PATH,
    JOBLENS_OUTPUT_LOG,
    PROJECT_ROOT,
    TRIGGER_PORT,
    WORKER_HOST,
    WORKER_IP,
    JobLensAPI,
    RemoteClient,
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
            "scancel -u vagrant 2>/dev/null || true; sudo scancel -u root 2>/dev/null || true",
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


def _reset_joblens(worker: RemoteClient) -> None:
    """停止 JobLens → 清理 LevelDB/锁/日志 → 重新启动 → 等待 eBPF 就绪."""
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
            cwd=PROJECT_ROOT,
            capture_output=True,
            timeout=15,
        )
    except Exception:
        pass


# ── 测试级清理 fixture ────────────────────────────────────────────────

@pytest.fixture
def clean_test_state(
    controller: RemoteClient, worker: RemoteClient
) -> Generator[None, None, None]:
    """每个测试前后的最佳努力状态清理.

    前置: 清空 HTCondor/Slurm 作业队列，重置 JobLens 数据.
    后置: 再次清空作业队列.
    """
    _cleanup_condor(controller)
    _cleanup_slurm(controller)
    _reset_joblens(worker)
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
                cwd=PROJECT_ROOT,
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
                cwd=PROJECT_ROOT,
                capture_output=True,
                timeout=120,
            )
        except Exception:
            pass
    else:
        try:
            subprocess.run(
                ["vagrant", "destroy", "-f"],
                cwd=PROJECT_ROOT,
                capture_output=True,
                timeout=120,
            )
        except Exception:
            pass

    _ebpf_cleanup()

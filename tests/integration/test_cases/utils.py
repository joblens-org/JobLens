"""JobLens integration test utility functions."""

# ── 1. stdlib imports ──────────────────────────────────────────────────
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

# ── 2. 路径常量 ─────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
INTEGRATION_ROOT = Path(__file__).resolve().parent.parent


# ── 3. 运行时预设环境加载 (必须在第三方 import 之前，确保 fast-fail 先于 ModuleNotFoundError) ──

def _load_preset_env() -> Dict[str, Any]:
    """从 tests/integration/.runtime/preset_env.json 加载预设环境配置。

    在模块导入时调用，确保所有常量在使用前已初始化。
    文件不存在或 JSON 解析失败时立即退出，不提供硬编码回退。

    Returns:
        解析并验证后的预设环境字典。

    Raises:
        SystemExit: 当环境文件不存在、JSON 解析失败或缺少必要字段时。
    """
    preset_path = INTEGRATION_ROOT / ".runtime" / "preset_env.json"

    if not preset_path.is_file():
        print(
            "FATAL: tests/integration/.runtime/preset_env.json 未找到。"
            "请先运行 run_preset.py",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        with open(preset_path, "r", encoding="utf-8") as f:
            preset = json.load(f)
    except json.JSONDecodeError as e:
        print(
            f"FATAL: tests/integration/.runtime/preset_env.json JSON 解析失败: {e}",
            file=sys.stderr,
        )
        sys.exit(1)

    # 验证顶层必要字段
    required_fields = [
        "preset_name", "controller", "worker",
        "trigger_url", "schedulers",
    ]
    for field in required_fields:
        if field not in preset:
            print(
                f"FATAL: tests/integration/.runtime/preset_env.json 缺少必要字段: {field}",
                file=sys.stderr,
            )
            sys.exit(1)

    # 验证 controller/worker 嵌套字段
    for role in ("controller", "worker"):
        role_data = preset.get(role, {})
        for subfield in ("host", "ip"):
            if subfield not in role_data:
                print(
                    f"FATAL: tests/integration/.runtime/preset_env.json "
                    f"{role} 缺少必要字段: {subfield}",
                    file=sys.stderr,
                )
                sys.exit(1)

    return preset


# 模块级执行：立即加载并校验环境文件
_preset = _load_preset_env()


# ── 4. 主机常量 (从 .runtime/preset_env.json 加载) ────────────────────────

PRESET_NAME = _preset["preset_name"]

CONTROLLER_HOST = _preset["controller"]["host"]
WORKER_HOST = _preset["worker"]["host"]
CONTROLLER_IP = _preset["controller"]["ip"]
WORKER_IP = _preset["worker"]["ip"]

TRIGGER_BASE_URL = _preset["trigger_url"]

# 从 trigger_url 提取端口号，保持与 TRIGGER_BASE_URL 一致
_parts = TRIGGER_BASE_URL.rsplit(":", 1)
if len(_parts) == 2 and _parts[1].isdigit():
    TRIGGER_PORT = int(_parts[1])
else:
    print(
        f"FATAL: trigger_url 格式无效，无法提取端口号: {TRIGGER_BASE_URL}",
        file=sys.stderr,
    )
    sys.exit(1)

JOBLENS_OUTPUT_LOG = "/var/log/joblens/output.log"
JOBLENS_DB_PATH = "/var/JobLens/job.db"
JOBLENS_LOCK_PATH = "/var/JobLens/JobLens.lock"


# ── 5. 第三方 imports (必须在 fast-fail 路径之后) ────────────────────────
import requests          # noqa: E402
from fabric import Connection  # noqa: E402


# ── 6. RemoteClient ────────────────────────────────────────────────────

class RemoteClient:
    """基于 fabric.Connection 的远程主机操作封装.

    host: 逻辑主机名（用于 SSH config 查找和日志）
    connect_host: 实际连接的 IP 地址（可选，不指定则用 host 做 DNS 解析）
    """

    def __init__(self, host: str, connect_host: str = "", **kwargs: Any) -> None:
        # 构建 connect_kwargs：用 IP 直连绕过 DNS 解析
        ck = kwargs.pop("connect_kwargs", {})
        if connect_host:
            ck["hostname"] = connect_host
        # 禁用 host key 校验（测试环境 VM 密钥不固定）
        ck.setdefault("look_for_keys", False)
        ck.setdefault("allow_agent", False)
        if "key_filename" not in ck and "pkey" not in ck:
            # 尝试 Vagrant 默认私钥
            vagrant_key = os.path.expanduser("~/.vagrant.d/insecure_private_key")
            if os.path.exists(vagrant_key):
                ck["key_filename"] = vagrant_key
        self._conn = Connection(host, connect_kwargs=ck, **kwargs)

    def run(self, command: str, **kwargs: Any) -> Any:
        """在远程主机上运行命令."""
        return self._conn.run(command, **kwargs)

    def sudo(self, command: str, **kwargs: Any) -> Any:
        """在远程主机上以 root 运行命令."""
        return self._conn.sudo(command, **kwargs)

    def read_text(self, path: str) -> str:
        """读取远程文本文件内容."""
        result = self._conn.run(f"cat {path}", hide=True)
        return result.stdout

    def put(self, local_path: str, remote_path: str) -> Any:
        """上传本地文件到远程主机."""
        return self._conn.put(local_path, remote_path)

    def close(self) -> None:
        """关闭 SSH 连接."""
        self._conn.close()


# ── 7. JobLensAPI ──────────────────────────────────────────────────────

class JobLensAPI:
    """JobLens Trigger REST API 封装，默认指向 worker VM."""

    def __init__(self, base_url: str = TRIGGER_BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()

    def get(self, path: str, **kwargs: Any) -> requests.Response:
        """通用 GET 请求."""
        return self._session.get(f"{self.base_url}{path}", **kwargs)

    def health_check(self) -> requests.Response:
        """GET /joblens/healthy — JobLens systemd 健康状态."""
        return self.get("/joblens/healthy")

    def rpc_health(self) -> requests.Response:
        """GET /joblens/rpc/health — RPC 健康检查含延迟."""
        return self.get("/joblens/rpc/health")

    def trigger_health(self) -> requests.Response:
        """GET /trigger/health — Trigger 组件健康详情."""
        return self.get("/trigger/health")

    def jobs_count(self) -> requests.Response:
        """GET /joblens/jobs/count — 已注册作业数量."""
        return self.get("/joblens/jobs/count")

    def jobs(self) -> requests.Response:
        """GET /joblens/jobs — 已注册作业列表."""
        return self.get("/joblens/jobs")

    def job_detail(self, job_id: int) -> requests.Response:
        """GET /joblens/jobs/{job_id} — 指定作业详情."""
        return self.get(f"/joblens/jobs/{job_id}")

    def rpc_functions(self) -> requests.Response:
        """GET /joblens/rpc/functions — 可用 RPC 方法列表."""
        return self.get("/joblens/rpc/functions")

    def collector_perf(self) -> requests.Response:
        """GET /joblens/collectors/perf — 采集器性能统计."""
        return self.get("/joblens/collectors/perf")

    def writer_info(self, writer_name: str) -> requests.Response:
        """GET /joblens/writers/{writer_name}/info — writer 基本信息."""
        return self.get(f"/joblens/writers/{writer_name}/info")

    def writer_perf(self) -> requests.Response:
        """GET /joblens/writers/perf — writer 性能统计."""
        return self.get("/joblens/writers/perf")


# ── 8. retry_with_backoff ──────────────────────────────────────────────

class RetryTimeoutError(TimeoutError):
    """重试超时错误，附带最后一次捕获的异常用于诊断."""

    def __init__(
        self, timeout: float, last_exception: Optional[Exception] = None
    ) -> None:
        self.last_exception = last_exception
        super().__init__(
            f"重试超时 ({timeout:.1f}s)，最后一次异常: {last_exception}"
        )


def retry_with_backoff(
    func: Callable[[], Any],
    timeout: float = 30.0,
    initial: float = 0.5,
    max_wait: float = 10.0,
) -> Any:
    """指数退避重试执行 func，超时抛出 RetryTimeoutError.

    Args:
        func: 每次重试调用的无参函数.
        timeout: 总超时时间(秒).
        initial: 初始等待时间(秒).
        max_wait: 最大单次等待时间(秒).
    """
    deadline = time.monotonic() + timeout
    wait = initial
    last_exc: Optional[Exception] = None

    while True:
        try:
            return func()
        except Exception as e:
            last_exc = e
            if time.monotonic() + wait > deadline:
                raise RetryTimeoutError(timeout, last_exc) from e
            time.sleep(wait)
            wait = min(wait * 2, max_wait)


# ── 9. parse_jsonl ─────────────────────────────────────────────────────

def parse_jsonl(path_or_text: Union[str, Path]) -> List[Dict[str, Any]]:
    """解析 JSONL 文件路径或原始 JSONL 文本.

    跳过空行，对无效 JSON 行抛出带行号的 ValueError.
    """
    if isinstance(path_or_text, Path):
        raw = path_or_text.read_text()
    elif os.path.isfile(str(path_or_text)):
        raw = Path(path_or_text).read_text()
    else:
        raw = str(path_or_text)

    records: List[Dict[str, Any]] = []
    for lineno, line in enumerate(raw.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            records.append(json.loads(stripped))
        except json.JSONDecodeError as e:
            raise ValueError(f"JSONL 第 {lineno} 行 JSON 无效: {e}") from e
    return records


# ── 10. validate_json_schema ───────────────────────────────────────────

def validate_json_schema(
    data: Dict[str, Any], required_fields: List[str]
) -> List[str]:
    """验证 data 包含 required_fields 中的所有点分路径字段.

    例如 required_fields=["job_info.pids"] 会检查 data["job_info"]["pids"] 是否存在.
    返回缺失字段名称列表 (非 pytest 断言).
    """
    missing: List[str] = []
    for field in required_fields:
        parts = field.split(".")
        current: Any = data
        found = True
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                found = False
                break
        if not found:
            missing.append(field)
    return missing


# ── 11. 作业提交工具 (供测试用例共享) ────────────────────────────────────

def submit_condor_job(
    controller: RemoteClient, sleep_seconds: int = 60
) -> str:
    """在 controller 节点上通过 condor_submit 提交最小 HTCondor sleep 作业，返回 ClusterId.

    condor_submit 必须在 scheduler 节点 (controller VM, 192.168.56.10) 上执行，
    因为只有 controller 运行了 condor_schedd 守护进程。
    """
    submit_text = f"""executable = /usr/bin/sleep
arguments = {sleep_seconds}
output = /tmp/condor_test.out
error  = /tmp/condor_test.err
log    = /tmp/condor_test.log
queue
"""
    controller.sudo("cat > /tmp/test_job.sub", stdin=submit_text)
    result = controller.run("condor_submit /tmp/test_job.sub", hide=True)
    for line in result.stdout.splitlines():
        if "submitted to cluster" in line:
            return line.strip().split()[-1].rstrip(".")
    return "unknown"


def submit_slurm_job(controller: RemoteClient) -> str:
    """在 controller 节点上通过 sbatch 提交最小 Slurm 作业，返回 JobID.

    sbatch 必须在 Slurm 控制器节点 (controller VM, 192.168.56.10) 上执行，
    因为只有 controller 运行了 slurmctld 守护进程。
    """
    result = controller.run(
        "sbatch --wrap='sleep 60' --output=/tmp/slurm_test.out",
        hide=True,
    )
    for line in result.stdout.splitlines():
        if "Submitted batch job" in line:
            return line.strip().split()[-1]
    return "unknown"


def wait_for_job_discovery(
    api: JobLensAPI,
    job_id: int,
    timeout: float = 30.0,
    initial: float = 1.0,
    max_wait: float = 5.0,
) -> Dict[str, Any]:
    """轮询等待 JobLens 发现指定 job_id，返回作业详情 dict."""

    def _check() -> Dict[str, Any]:
        resp = api.job_detail(job_id)
        if resp.status_code == 200:
            return resp.json()
        raise RuntimeError(f"job {job_id} 尚未发现 (HTTP {resp.status_code})")

    return retry_with_backoff(
        _check, timeout=timeout, initial=initial, max_wait=max_wait
    )

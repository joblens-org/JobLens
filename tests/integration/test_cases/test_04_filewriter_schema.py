import importlib
import sys
from pathlib import Path

try:
    _utils = importlib.import_module("test_cases.utils")
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    _utils = importlib.import_module("utils")

JOBLENS_OUTPUT_LOG = _utils.JOBLENS_OUTPUT_LOG
JobLensAPI = _utils.JobLensAPI
RemoteClient = _utils.RemoteClient
retry_with_backoff = _utils.retry_with_backoff
submit_slurm_job = _utils.submit_slurm_job


def _ensure_text_output(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    min_lines: int = 2,
) -> str:
    def _read_text() -> str:
        exists = worker.run(f"test -f {JOBLENS_OUTPUT_LOG}", hide=True, warn=True)
        if not exists.ok:
            raise RuntimeError(f"文本输出文件不存在: {JOBLENS_OUTPUT_LOG}")
        text = worker.read_text(JOBLENS_OUTPUT_LOG)
        lines = [line for line in text.splitlines() if line.strip()]
        if len(lines) >= min_lines:
            return text
        raise RuntimeError(f"文本输出记录不足: {len(lines)} < {min_lines}")

    try:
        return _read_text()
    except RuntimeError:
        job_id = submit_slurm_job(controller, sleep_seconds=180)
        if job_id == "unknown":
            raise RuntimeError("无法提交用于生成 FileWriter 文本输出的 Slurm 作业")

    def _wait_job_discovered() -> None:
        resp = joblens_api.jobs()
        if resp.status_code != 200:
            raise RuntimeError(f"jobs API 返回 HTTP {resp.status_code}")
        jobs = resp.json().get("jobs", [])
        for job in jobs:
            if "slurm" in str(job.get("subtype", "")).lower():
                return
        raise RuntimeError("用于生成 FileWriter 文本输出的 Slurm 作业尚未被 JobLens 发现")

    retry_with_backoff(
        _wait_job_discovered,
        timeout=60.0,
        initial=1.0,
        max_wait=5.0,
    )

    return retry_with_backoff(
        _read_text,
        timeout=90.0,
        initial=1.0,
        max_wait=5.0,
    )


def _nonempty_lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.strip()]


def test_text_file_exists_and_nonempty(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    text = _ensure_text_output(worker, controller, joblens_api)
    result = worker.run(f"test -f {JOBLENS_OUTPUT_LOG}", hide=True)
    assert result.ok, f"文本输出文件不存在: {JOBLENS_OUTPUT_LOG}"
    assert len(_nonempty_lines(text)) >= 2, "期望 FileWriter 至少输出 2 行纯文本记录"


def test_text_output_is_not_jsonl_wrapper(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    text = _ensure_text_output(worker, controller, joblens_api)
    first_line = _nonempty_lines(text)[0]
    assert not first_line.lstrip().startswith("{"), "FileWriter 不应再强制输出 JSONL wrapper"
    assert "@timestamp" not in text
    assert "collector_name" not in text
    assert "job_info" not in text


def test_cpumem_text_contains_collector_marker(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    text = _ensure_text_output(worker, controller, joblens_api)
    assert "CPUMemCollector" in text


def test_cpumem_text_contains_process_type(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    text = _ensure_text_output(worker, controller, joblens_api)
    assert "type=process" in text


def test_cpumem_text_contains_pid_field(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    text = _ensure_text_output(worker, controller, joblens_api)
    assert " pid=" in text


def test_cpumem_text_contains_cpu_field(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    text = _ensure_text_output(worker, controller, joblens_api)
    assert " cpuPercent=" in text


def test_cpumem_text_contains_memory_field(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    text = _ensure_text_output(worker, controller, joblens_api)
    assert " memoryPercent=" in text


def test_cpumem_text_contains_thread_field(
    worker: RemoteClient,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    text = _ensure_text_output(worker, controller, joblens_api)
    assert " numThreads=" in text

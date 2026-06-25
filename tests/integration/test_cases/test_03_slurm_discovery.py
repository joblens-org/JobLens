"""Slurm 作业自动发现集成测试。

测试覆盖:
  - Slurm 作业自动发现 (eBPF slurmstepd 子进程追踪)
  - 已发现作业的 PIDs 列表非空
  - 已发现作业的采集器列表包含 cpumem_collector
  - 已发现作业的 sub_attr 包含 job_id/step_id

流程:
  1. 在 controller 上通过 sbatch 提交最小 sleep 作业
  2. 轮询 worker 上 JobLens API，等待 SlurmJobWatcher (eBPF)
     自动发现该作业并注册到 JobRegistry
  3. 验证已发现作业的字段完整性

使用 conftest 提供的 clean_test_state, controller, worker, joblens_api fixtures。
"""

from typing import Any, Dict

from utils import (
    JobLensAPI,
    RemoteClient,
    retry_with_backoff,
    submit_slurm_job,
)


# ── 辅助函数 ────────────────────────────────────────────────────────────

def _wait_for_slurm_job(
    api: JobLensAPI,
    timeout: float = 60.0,
    initial: float = 1.0,
    max_wait: float = 10.0,
) -> Dict[str, Any]:
    """轮询 jobs() 直到找到 subtype 包含 'slurm' 的作业，返回作业数据 dict。

    Raises:
        RetryTimeoutError: 超时未发现 Slurm 作业。
    """

    def _find_slurm_job() -> Dict[str, Any]:
        resp = api.jobs()
        if resp.status_code != 200:
            raise RuntimeError(f"jobs API 返回 HTTP {resp.status_code}")
        data = resp.json()
        jobs = data.get("jobs", [])
        for job in jobs:
            subtype = str(job.get("subtype", "")).lower()
            if "slurm" in subtype:
                return job
        total = len(jobs)
        raise RuntimeError(
            f"未找到 Slurm 作业 (当前共 {total} 个作业)"
        )

    return retry_with_backoff(
        _find_slurm_job, timeout=timeout, initial=initial, max_wait=max_wait
    )


# ── test_slurm_auto_discovery ────────────────────────────────────────────

def test_slurm_auto_discovery(
    clean_test_state: None,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    """提交 Slurm 作业并验证 eBPF 自动发现。

    通过 sbatch 在 controller 上提交 sleep 作业，
    JobLens 上的 SlurmJobWatcher 通过 eBPF trace_slurm_stepd
    自动发现 slurmstepd 子进程并注册为作业。
    """
    # 提交 slurm sleep 作业
    job_id_str = submit_slurm_job(controller)
    assert job_id_str != "unknown", (
        f"sbatch 提交失败，期望返回数字 JobID，实际 '{job_id_str}'"
    )
    print(f"[test_slurm_auto_discovery] Slurm 作业已提交: JobID={job_id_str}")

    # 轮询等待自动发现 (最长 60s)
    slurm_job = _wait_for_slurm_job(joblens_api, timeout=60.0)

    subtype = str(slurm_job.get("subtype", ""))
    assert "slurm" in subtype.lower(), (
        f"期望作业 subtype 包含 'slurm'，实际 subtype='{subtype}'"
    )
    print(
        f"[test_slurm_auto_discovery] Slurm 作业已自动发现: "
        f"JobID={slurm_job.get('JobID')}, subtype={subtype}"
    )


# ── test_slurm_job_has_pids ──────────────────────────────────────────────

def test_slurm_job_has_pids(
    clean_test_state: None,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    """已发现的 Slurm 作业拥有非空的进程 PID 列表。

    SlurmJobWatcher 通过 eBPF 捕获 slurmstepd 子进程 exec 事件后,
    update_job_pids() 从 cgroup 中收集该作业的所有进程 PID。
    """
    submit_slurm_job(controller)
    slurm_job = _wait_for_slurm_job(joblens_api, timeout=60.0)

    pids = slurm_job.get("JobPIDs", [])
    assert isinstance(pids, list), (
        f"期望 JobPIDs 为 list 类型，实际 {type(pids).__name__}"
    )
    assert len(pids) > 0, (
        f"期望 JobPIDs 非空 (Slurm 作业应包含至少一个运行进程)，"
        f" 实际 pids={pids}"
    )
    print(f"[test_slurm_job_has_pids] Slurm 作业 PIDs: {pids}")


# ── test_slurm_job_has_collectors ────────────────────────────────────────

def test_slurm_job_has_collectors(
    clean_test_state: None,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    """已发现的 Slurm 作业的采集器列表包含 cpumem_collector。

    SlurmJobWatcher 构建作业时从 slurm_job_watcher.auto_add_collectors
    配置中读取默认采集器列表，并在注册时附加到作业上。
    """
    submit_slurm_job(controller)
    slurm_job = _wait_for_slurm_job(joblens_api, timeout=60.0)

    collectors = slurm_job.get("CollectorNames", [])
    assert "cpumem_collector" in collectors, (
        f"期望 CollectorNames 包含 'cpumem_collector'，"
        f" 实际 collectors={collectors}"
    )
    print(
        f"[test_slurm_job_has_collectors] Slurm 作业采集器: {collectors}"
    )


# ── test_slurm_job_sub_attr ──────────────────────────────────────────────

def test_slurm_job_sub_attr(
    clean_test_state: None,
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    """已发现的 Slurm 作业的 sub_attr 包含 job_id 和 step_id。

    SlurmJobAttr 结构体定义了 slurm 作业的专属子属性:
      - job_id: 从进程环境变量 SLURM_JOB_ID 解析
      - step_id: 从进程环境变量 SLURM_STEP_ID 解析

    通过 job_detail 端点获取完整作业详情后校验。
    """
    submit_slurm_job(controller)
    slurm_job = _wait_for_slurm_job(joblens_api, timeout=60.0)

    job_id = slurm_job.get("JobID")
    assert job_id is not None, "作业列表中缺少 JobID 字段"

    # 通过详情端点获取含 sub_attr 的完整数据
    resp = joblens_api.job_detail(job_id)
    assert resp.status_code == 200, (
        f"GET /joblens/jobs/{job_id} 期望 200, 实际 {resp.status_code}"
    )
    detail = resp.json()

    sub_attr = detail.get("sub_attr", {})
    assert isinstance(sub_attr, dict), (
        f"期望 sub_attr 为 dict 类型，实际 {type(sub_attr).__name__}，"
        f" detail keys={list(detail.keys())}"
    )
    assert sub_attr.get("job_id") is not None, (
        f"期望 sub_attr 包含 job_id，实际 sub_attr={sub_attr}"
    )
    assert "step_id" in sub_attr, (
        f"期望 sub_attr 包含 step_id，实际 sub_attr={sub_attr}"
    )
    print(
        f"[test_slurm_job_sub_attr] sub_attr: job_id={sub_attr.get('job_id')}, "
        f"step_id={sub_attr.get('step_id')}"
    )

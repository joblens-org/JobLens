"""HTCondor 作业自动发现集成测试。

测试覆盖:
  - 提交 HTCondor sleep 作业后 JobLens 在 60s 内自动发现
  - 发现作业的 pids 字段非空（eBPF condor_starter 追踪到的进程）
  - 发现作业的 collectors 包含 cpumem_collector
  - 发现作业的 sub_attr 包含 cluster_id 和 proc_id

全部使用 conftest 提供的 controller, joblens_api, clean_test_state fixtures。
每个测试通过 clean_test_state fixture 获得独立的状态隔离。
"""

from typing import Any, Dict

from utils import (
    JobLensAPI,
    RemoteClient,
    retry_with_backoff,
    submit_condor_job,
)


# ══════════════════════════════════════════════════════════════════════
# 辅助函数
# ══════════════════════════════════════════════════════════════════════

def _discover_condor_job(
    api: JobLensAPI,
    timeout: float = 60.0,
) -> Dict[str, Any]:
    """轮询等待 JobLens 自动发现一个 HTCondor 作业，返回其 job_detail 字典。

    策略：
      1. 轮询 /joblens/jobs/count 直到 job_count >= 1
      2. 遍历作业列表中的每个 JobID
      3. 优先检查列表项的 type 字段，否则通过 /joblens/jobs/{id} 获取详情
      4. 匹配 job_info.type 或 job_info.subtype 包含 "condor" 则返回

    Args:
        api: JobLensAPI 客户端实例。
        timeout: 总超时时间（秒），默认 60s。

    Returns:
        发现的第一个 HTCondor 作业的完整 job_detail 字典。

    Raises:
        RetryTimeoutError: 超时前未发现 condor 作业。
    """
    def _check() -> Dict[str, Any]:
        # 1. 检查作业计数是否达到最低要求
        count_resp = api.jobs_count()
        if count_resp.status_code != 200:
            raise RuntimeError(
                f"/joblens/jobs/count 返回 HTTP {count_resp.status_code}"
            )
        count_data = count_resp.json()
        if count_data.get("job_count", 0) < 1:
            raise RuntimeError("尚未发现任何作业 (job_count=0)")

        # 2. 获取作业列表
        jobs_resp = api.jobs()
        if jobs_resp.status_code != 200:
            raise RuntimeError(
                f"/joblens/jobs 返回 HTTP {jobs_resp.status_code}"
            )
        jobs_data = jobs_resp.json()
        jobs_list: list = jobs_data.get("jobs", [])

        # 3. 第一轮：依赖列表项自带的 type/subtype 字段快速匹配
        # dump_job() 返回: {"JobID": 1, "jobtype": "Job", "subtype": "Condor", ...}
        for job in jobs_list:
            job_type = (
                job.get("type", "")
                or job.get("job_type", "")
                or job.get("jobtype", "")
                or job.get("subtype", "")  # ← 添加 subtype 检查
            )
            if "condor" in str(job_type).lower():
                job_id = job.get("JobID")
                if job_id is not None:
                    detail_resp = api.job_detail(job_id)
                    if detail_resp.status_code == 200:
                        return detail_resp.json()

        # 4. 第二轮：逐个查询 job_detail 检查 subtype（API 直接返回 dump_job 输出）
        for job in jobs_list:
            job_id = job.get("JobID")
            if job_id is None:
                continue
            detail_resp = api.job_detail(job_id)
            if detail_resp.status_code != 200:
                continue
            detail = detail_resp.json()
            # API 返回 dump_job() 直接输出，subtype 在顶层而非 job_info 包裹
            subtype = (
                detail.get("type", "")
                or detail.get("subtype", "")
                or detail.get("job_info", {}).get("subtype", "")
            )
            if "condor" in str(subtype).lower():
                return detail

        # 5. 已发现作业但都不是 condor 类型，继续重试等待
        raise RuntimeError("已发现作业但未匹配到 condor 类型，继续等待...")

    return retry_with_backoff(
        _check, timeout=timeout, initial=1.0, max_wait=10.0,
    )


# ══════════════════════════════════════════════════════════════════════
# 测试函数 — HTCondor 自动发现
# ══════════════════════════════════════════════════════════════════════

def test_condor_auto_discovery(
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    clean_test_state: None,
) -> None:
    """提交 HTCondor sleep 作业后，JobLens 在 60s 内自动发现该作业。

    验证 job_info.subtype 包含 "condor"，表明 JobLens 正确识别了
    作业类型（通过 eBPF condor_starter 钩子自动注册）。
    """
    cluster_id = submit_condor_job(controller, sleep_seconds=120)
    assert cluster_id != "unknown", (
        f"condor_submit 未返回有效 ClusterId: stdout 包含 '{cluster_id}'"
    )

    job = _discover_condor_job(joblens_api, timeout=60.0)

    # API 返回 dump_job() 直接输出，字段在顶层
    subtype = (
        job.get("type", "")
        or job.get("subtype", "")
        or job.get("job_info", {}).get("subtype", "")
    )
    assert "condor" in str(subtype).lower(), (
        f"期望 type/subtype 包含 'condor'，实际 subtype='{subtype}'，"
        f"job keys={list(job.keys())}"
    )


def test_condor_job_has_pids(
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    clean_test_state: None,
) -> None:
    """自动发现的 HTCondor 作业的 pids 字段非空。

    验证 eBPF condor_starter 追踪器正确捕获了作业进程的 PID，
    这是后续 CPU/内存/IO 数据采集的前提。
    """
    cluster_id = submit_condor_job(controller, sleep_seconds=120)
    assert cluster_id != "unknown", (
        f"condor_submit 未返回有效 ClusterId: stdout 包含 '{cluster_id}'"
    )

    job = _discover_condor_job(joblens_api, timeout=60.0)
    # API 返回 dump_job() 直接输出，JobPIDs 在顶层
    pids = job.get("JobPIDs", [])

    assert isinstance(pids, list), (
        f"期望 JobPIDs 为 list 类型，实际 {type(pids).__name__}"
    )
    assert len(pids) > 0, (
        f"期望 JobPIDs 非空（至少包含作业进程 PID），实际 JobPIDs={pids}"
    )


def test_condor_job_has_collectors(
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    clean_test_state: None,
) -> None:
    """自动发现的 HTCondor 作业的 collectors 包含 cpumem_collector。

    验证 JobLens 默认为 condor 作业关联了 CPU/内存采集器，
    兼容 collectors 和 Lens 两种字段名。
    """
    cluster_id = submit_condor_job(controller, sleep_seconds=120)
    assert cluster_id != "unknown", (
        f"condor_submit 未返回有效 ClusterId: stdout 包含 '{cluster_id}'"
    )

    job = _discover_condor_job(joblens_api, timeout=60.0)
    # API 返回 dump_job() 直接输出，字段在顶层
    collectors = (
        job.get("CollectorNames", [])
        or job.get("collectors", [])
        or job.get("Lens", [])
    )

    assert isinstance(collectors, list), (
        f"期望 collectors/Lens 为 list 类型，实际 {type(collectors).__name__}"
    )
    assert "cpumem_collector" in collectors, (
        f"期望 collectors 包含 'cpumem_collector'，"
        f"实际 collectors={collectors}"
    )


def test_condor_job_sub_attr(
    controller: RemoteClient,
    joblens_api: JobLensAPI,
    clean_test_state: None,
) -> None:
    """自动发现的 HTCondor 作业的 sub_attr 包含 cluster_id 和 proc_id。

    验证 JobLens 从 HTCondor starter 追踪中正确提取了
    集群 ID 和进程 ID 标识，用于后续数据关联。
    """
    cluster_id = submit_condor_job(controller, sleep_seconds=120)
    assert cluster_id != "unknown", (
        f"condor_submit 未返回有效 ClusterId: stdout 包含 '{cluster_id}'"
    )

    job = _discover_condor_job(joblens_api, timeout=60.0)
    # API 返回 dump_job() 直接输出，sub_attr 在顶层
    sub_attr = job.get("sub_attr", {})

    assert isinstance(sub_attr, dict), (
        f"期望 sub_attr 为 dict 类型，实际 {type(sub_attr).__name__}"
    )
    assert "cluster_id" in sub_attr, (
        f"期望 sub_attr 包含 'cluster_id'，实际 keys={list(sub_attr.keys())}"
    )
    assert "proc_id" in sub_attr, (
        f"期望 sub_attr 包含 'proc_id'，实际 keys={list(sub_attr.keys())}"
    )

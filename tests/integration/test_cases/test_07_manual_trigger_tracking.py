import importlib
from typing import Any, Dict

_utils = importlib.import_module("utils")
JobLensAPI = _utils.JobLensAPI
RemoteClient = _utils.RemoteClient
retry_with_backoff = _utils.retry_with_backoff
submit_condor_job = _utils.submit_condor_job
submit_slurm_job = _utils.submit_slurm_job


def _jobs_matching_sub_attr(api: JobLensAPI, key: str, value: int) -> list[Dict[str, Any]]:
    resp = api.jobs()
    assert resp.status_code == 200, (
        f"GET /joblens/jobs 期望 200，实际 {resp.status_code}, body={resp.text[:300]}"
    )
    return [
        job for job in resp.json().get("jobs", [])
        if int(job.get("sub_attr", {}).get(key, -1)) == value
    ]


def _assert_no_automatic_condor_registration(api: JobLensAPI, cluster_id: int) -> None:
    matching_jobs = _jobs_matching_sub_attr(api, "cluster_id", cluster_id)
    assert matching_jobs == [], (
        "自动发现已关闭时，提交 HTCondor 作业后不应自动注册到 JobLens；"
        f"实际匹配到: {matching_jobs}"
    )


def _assert_no_automatic_slurm_registration(api: JobLensAPI, slurm_job_id: int) -> None:
    matching_jobs = _jobs_matching_sub_attr(api, "job_id", slurm_job_id)
    assert matching_jobs == [], (
        "自动发现已关闭时，提交 Slurm 作业后不应自动注册到 JobLens；"
        f"实际匹配到: {matching_jobs}"
    )


def _manual_add_condor_job(api: JobLensAPI, cluster_id: int) -> Dict[str, Any]:
    def _post() -> Dict[str, Any]:
        resp = api.add_condor_job(cluster_id)
        if resp.status_code != 200:
            raise RuntimeError(
                f"POST /joblens/condor_job 返回 HTTP {resp.status_code}: {resp.text[:500]}"
            )
        data = resp.json()
        if data.get("status") != "ok":
            raise RuntimeError(f"手动添加 HTCondor 作业响应异常: {data}")
        return data

    return retry_with_backoff(
        _post, timeout=60.0, initial=1.0, max_wait=5.0,
    )


def _manual_add_slurm_job(api: JobLensAPI, slurm_job_id: int) -> Dict[str, Any]:
    resp = api.add_slurm_job(slurm_job_id)
    assert resp.status_code == 200, (
        f"POST /joblens/slurm_job 期望 200，实际 {resp.status_code}, body={resp.text[:500]}"
    )
    data = resp.json()
    assert data.get("status") == "ok", (
        f"手动添加 Slurm 作业期望 status=ok，实际响应: {data}"
    )
    return data


def _wait_for_manual_condor_job(api: JobLensAPI, cluster_id: int) -> Dict[str, Any]:
    def _check() -> Dict[str, Any]:
        matching_jobs = _jobs_matching_sub_attr(api, "cluster_id", cluster_id)
        if matching_jobs:
            return matching_jobs[0]
        raise RuntimeError(f"尚未找到手动注册的 HTCondor 作业 {cluster_id}")

    return retry_with_backoff(
        _check, timeout=30.0, initial=1.0, max_wait=5.0,
    )


def _wait_for_manual_slurm_job(api: JobLensAPI, slurm_job_id: int) -> Dict[str, Any]:
    def _check() -> Dict[str, Any]:
        resp = api.jobs()
        if resp.status_code != 200:
            raise RuntimeError(f"jobs API 返回 HTTP {resp.status_code}")
        for job in resp.json().get("jobs", []):
            sub_attr = job.get("sub_attr", {})
            if int(sub_attr.get("job_id", -1)) == slurm_job_id:
                return job
        raise RuntimeError(f"尚未找到手动注册的 Slurm 作业 {slurm_job_id}")

    return retry_with_backoff(
        _check, timeout=30.0, initial=1.0, max_wait=5.0,
    )


def test_manual_trigger_adds_condor_job_by_default_slot(
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    controller.run("condor_rm -all 2>/dev/null || true", hide=True, warn=True)

    cluster_id_text = submit_condor_job(controller, sleep_seconds=180)
    assert cluster_id_text != "unknown", (
        f"condor_submit 提交失败，期望返回 ClusterId，实际 '{cluster_id_text}'"
    )
    cluster_id = int(cluster_id_text)

    _assert_no_automatic_condor_registration(joblens_api, cluster_id)
    _manual_add_condor_job(joblens_api, cluster_id)
    job = _wait_for_manual_condor_job(joblens_api, cluster_id)

    subtype = str(job.get("subtype", "")).lower()
    assert "condor" in subtype, (
        f"手动注册后期望 subtype 包含 condor，实际 job={job}"
    )

    pids = job.get("JobPIDs", [])
    assert isinstance(pids, list) and len(pids) > 0, (
        f"手动触发追踪后期望 JobPIDs 非空，实际 JobPIDs={pids}"
    )

    collectors = job.get("CollectorNames", [])
    assert "cpumem_collector" in collectors, (
        f"手动触发追踪后期望包含 cpumem_collector，实际 CollectorNames={collectors}"
    )

    sub_attr = job.get("sub_attr", {})
    assert int(sub_attr.get("cluster_id", -1)) == cluster_id, (
        f"期望 sub_attr.cluster_id={cluster_id}，实际 sub_attr={sub_attr}"
    )
    assert int(sub_attr.get("proc_id", -1)) == 0, (
        f"期望 sub_attr.proc_id=0，实际 sub_attr={sub_attr}"
    )


def test_manual_trigger_adds_slurm_job_by_scheduler_job_id(
    controller: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    controller.run(
        "scancel -u vagrant 2>/dev/null || true; sudo scancel -u root 2>/dev/null || true",
        hide=True,
        warn=True,
    )

    slurm_job_id_text = submit_slurm_job(controller, sleep_seconds=180)
    assert slurm_job_id_text != "unknown", (
        f"sbatch 提交失败，期望返回数字 JobID，实际 '{slurm_job_id_text}'"
    )
    slurm_job_id = int(slurm_job_id_text)

    _assert_no_automatic_slurm_registration(joblens_api, slurm_job_id)
    _manual_add_slurm_job(joblens_api, slurm_job_id)
    job = _wait_for_manual_slurm_job(joblens_api, slurm_job_id)

    subtype = str(job.get("subtype", "")).lower()
    assert "slurm" in subtype, (
        f"手动注册后期望 subtype 包含 slurm，实际 job={job}"
    )

    pids = job.get("JobPIDs", [])
    assert isinstance(pids, list) and len(pids) > 0, (
        f"手动触发追踪后期望 JobPIDs 非空，实际 JobPIDs={pids}"
    )

    collectors = job.get("CollectorNames", [])
    assert "cpumem_collector" in collectors, (
        f"手动触发追踪后期望包含 cpumem_collector，实际 CollectorNames={collectors}"
    )

    sub_attr = job.get("sub_attr", {})
    assert int(sub_attr.get("job_id", -1)) == slurm_job_id, (
        f"期望 sub_attr.job_id={slurm_job_id}，实际 sub_attr={sub_attr}"
    )
    assert "step_id" in sub_attr, (
        f"期望 sub_attr 包含 step_id，实际 sub_attr={sub_attr}"
    )

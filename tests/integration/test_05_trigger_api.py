"""Trigger REST API 端点验证测试 — 验证所有关键 HTTP 端点返回正确的状态码和响应结构."""

import pytest

from utils import JobLensAPI


# ══════════════════════════════════════════════════════════════════════
# 测试根端点与指标端点
# ══════════════════════════════════════════════════════════════════════

def test_root_endpoint(joblens_api: JobLensAPI) -> None:
    """GET / → 200, 响应 JSON 包含 'service' 字段."""
    resp = joblens_api.get("/")
    assert resp.status_code == 200, (
        f"GET / 期望 200, 实际 {resp.status_code}"
    )
    data = resp.json()
    assert "service" in data, (
        f"GET / 响应应包含 'service' 字段, 实际 keys: {list(data.keys())}"
    )


def test_metrics_endpoint(joblens_api: JobLensAPI) -> None:
    """GET /metrics → 200 (Prometheus 指标端点)."""
    resp = joblens_api.get("/metrics")
    assert resp.status_code == 200, (
        f"GET /metrics 期望 200, 实际 {resp.status_code}"
    )


# ══════════════════════════════════════════════════════════════════════
# 测试作业管理端点
# ══════════════════════════════════════════════════════════════════════

def test_jobs_list_endpoint(joblens_api: JobLensAPI) -> None:
    """GET /joblens/jobs → 200, 响应 JSON 中 status 字段为 'ok'."""
    resp = joblens_api.jobs()
    assert resp.status_code == 200, (
        f"GET /joblens/jobs 期望 200, 实际 {resp.status_code}"
    )
    data = resp.json()
    assert data.get("status") == "ok", (
        f"joblens/jobs status 期望 'ok', 实际 {data.get('status')}"
    )


def test_jobs_count_endpoint(joblens_api: JobLensAPI) -> None:
    """GET /joblens/jobs/count → 200, job_count 字段为 int 类型."""
    resp = joblens_api.jobs_count()
    assert resp.status_code == 200, (
        f"GET /joblens/jobs/count 期望 200, 实际 {resp.status_code}"
    )
    data = resp.json()
    assert isinstance(data.get("job_count"), int), (
        f"job_count 期望 int 类型, 实际 "
        f"{type(data.get('job_count'))} = {data.get('job_count')}"
    )


def test_job_detail_endpoint(joblens_api: JobLensAPI) -> None:
    """GET /joblens/jobs/{id} → 200，需存在至少一个已注册作业。

    若当前无已注册作业则跳过测试。
    """
    # 先检查是否有已注册的作业
    count_resp = joblens_api.jobs_count()
    assert count_resp.status_code == 200
    count_data = count_resp.json()

    if count_data.get("job_count", 0) == 0:
        pytest.skip("无已注册作业，跳过 job_detail 测试")

    # 取第一个作业的 ID 查询详情
    jobs_resp = joblens_api.jobs()
    assert jobs_resp.status_code == 200
    jobs_data = jobs_resp.json()
    jobs_list = jobs_data.get("jobs", [])
    if not jobs_list:
        pytest.skip("作业列表为空，跳过 job_detail 测试")

    first_job = jobs_list[0]
    job_id = first_job.get("JobID")
    if job_id is None:
        pytest.skip("作业对象中无 JobID 字段，跳过 job_detail 测试")

    resp = joblens_api.job_detail(job_id)
    assert resp.status_code == 200, (
        f"GET /joblens/jobs/{job_id} 期望 200, 实际 {resp.status_code}"
    )


def test_404_for_nonexistent_job(joblens_api: JobLensAPI) -> None:
    """GET /joblens/jobs/99999 → 404，查询不存在的作业 ID."""
    resp = joblens_api.job_detail(99999)
    assert resp.status_code == 404, (
        f"GET /joblens/jobs/99999 期望 404, 实际 {resp.status_code}"
    )


# ══════════════════════════════════════════════════════════════════════
# 测试配置与 RPC 端点
# ══════════════════════════════════════════════════════════════════════

def test_config_status_endpoint(joblens_api: JobLensAPI) -> None:
    """GET /joblens/config/status → 200（配置管理器状态端点）."""
    resp = joblens_api.get("/joblens/config/status")
    assert resp.status_code == 200, (
        f"GET /joblens/config/status 期望 200, 实际 {resp.status_code}"
    )


def test_rpc_functions_endpoint(joblens_api: JobLensAPI) -> None:
    """GET /joblens/rpc/functions → 200, functions 列表包含 'health'."""
    resp = joblens_api.rpc_functions()
    assert resp.status_code == 200, (
        f"GET /joblens/rpc/functions 期望 200, 实际 {resp.status_code}"
    )
    data = resp.json()
    functions = data.get("functions", [])
    assert "health" in functions, (
        f"rpc/functions 列表应包含 'health', 实际: {functions}"
    )

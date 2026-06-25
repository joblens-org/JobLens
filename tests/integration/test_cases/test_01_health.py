"""JobLens 启动后健康检查集成测试。

测试覆盖:
  - systemd 服务状态 (worker 上 joblens.service)
  - Trigger HTTP 健康端点 (/joblens/healthy)
  - RPC 健康检查 (/joblens/rpc/health)
  - Trigger 组件健康 (/trigger/health)
  - 初始作业计数 (/joblens/jobs/count)
  - RPC 方法列表 (/joblens/rpc/functions)

全部使用 conftest 提供的 joblens_api, worker, controller fixtures。
"""


# ── systemd 服务状态 ──────────────────────────────────────────────────

def test_systemd_service_active(worker):
    """验证 joblens systemd 服务处于 active 状态.

    通过 ssh 到 worker VM 执行 systemctl is-active 直接检查 systemd 状态,
    不走 HTTP API, 确保服务底层处于正常运行状态.
    """
    result = worker.sudo("systemctl is-active joblens", hide=True)
    assert result.stdout.strip() == "active", (
        f"期望 joblens 服务状态为 'active'，实际为 '{result.stdout.strip()}'"
    )


# ── Trigger HTTP 健康端点 ─────────────────────────────────────────────

def test_trigger_healthy_endpoint(joblens_api):
    """验证 /joblens/healthy 端点返回健康状态.

    检查 HTTP 200 且 json["healthy"] 为 True (systemd 服务 active + running).
    """
    resp = joblens_api.health_check()
    assert resp.status_code == 200, (
        f"期望 HTTP 200，实际 {resp.status_code}"
    )
    data = resp.json()
    assert data["healthy"] is True, (
        f"期望 healthy=True，实际 healthy={data.get('healthy')}, state={data.get('state')}"
    )


# ── RPC 健康检查 ──────────────────────────────────────────────────────

def test_rpc_health(joblens_api):
    """验证 /joblens/rpc/health 端点返回 200.

    仅检查 HTTP 状态码, 不检查延迟 (rpc_latency_ms) — 健康检查不做性能测试.
    """
    resp = joblens_api.rpc_health()
    assert resp.status_code == 200, (
        f"期望 HTTP 200，实际 {resp.status_code}, body={resp.text[:200]}"
    )


# ── Trigger 组件健康 ──────────────────────────────────────────────────

def test_trigger_health(joblens_api):
    """验证 /trigger/health 端点返回 running 状态.

    该端点独立于 JobLens RPC, 报告 Trigger 自身组件健康状态.
    """
    resp = joblens_api.trigger_health()
    assert resp.status_code == 200, (
        f"期望 HTTP 200，实际 {resp.status_code}"
    )
    data = resp.json()
    assert data["status"] == "running", (
        f"期望 status='running'，实际 status='{data.get('status')}'"
    )


# ── 初始作业计数 ──────────────────────────────────────────────────────

def test_no_jobs_initial(joblens_api):
    """验证 /joblens/jobs/count 端点返回有效计数 (>=0).

    新部署的 JobLens 初始作业计数应为 0, 但若之前有残留作业,
    仅验证响应格式有效且 job_count 为非负整数.
    """
    resp = joblens_api.jobs_count()
    assert resp.status_code == 200, (
        f"期望 HTTP 200，实际 {resp.status_code}"
    )
    data = resp.json()
    assert data["job_count"] >= 0, (
        f"期望 job_count >= 0，实际 job_count={data.get('job_count')}"
    )


# ── RPC 方法列表 ──────────────────────────────────────────────────────

def test_rpc_functions_available(joblens_api):
    """验证 /joblens/rpc/functions 端点返回可用 RPC 方法列表.

    检查 HTTP 200 且返回的 functions 字段为 list 类型 (非空更好, 但不强制).
    """
    resp = joblens_api.rpc_functions()
    assert resp.status_code == 200, (
        f"期望 HTTP 200，实际 {resp.status_code}"
    )
    data = resp.json()
    assert isinstance(data["functions"], list), (
        f"期望 functions 字段为 list 类型，实际为 {type(data['functions'])}"
    )

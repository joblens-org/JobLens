"""Contract tests for the job/collection Flask gateway.

Covers the collection-control request, job list/capabilities reads and the
legacy add/remove routes. The HTTP contract is documented in
doc/api_documentation.md section 12.

The Core is mocked at the single RPC seam in trigger/core/tools.py
(``call_core_rpc``), so these tests pass independently of C++ progress.
The Flask side must never allocate internal IDs, never fabricate a response when
the Core is unavailable, and never turn a Core error into HTTP 200.
"""

from typing import Any, Optional

import pytest
from flask import Flask

from trigger.api.routes import register_routes
from trigger.core import tools
from trigger.core.rpc_client import RPCError


class FakeCore:
    """Records forwarded RPC calls and replays one canned Core reply."""

    def __init__(self, response: object = None, error: Optional[Exception] = None) -> None:
        self.response = response
        self.error = error
        self.calls: list[tuple[str, object]] = []

    def __call__(
        self,
        method: str,
        params: object = None,
        rpc_client: object = None,
    ) -> object:
        self.calls.append((method, params))
        if self.error is not None:
            raise self.error
        return self.response


@pytest.fixture
def install_core(monkeypatch: pytest.MonkeyPatch):
    """Install a fake Core behind trigger.core.tools.call_core_rpc."""

    def _install(response: object = None, error: Optional[Exception] = None) -> FakeCore:
        fake = FakeCore(response=response, error=error)
        monkeypatch.setattr(tools, "call_core_rpc", fake)
        return fake

    return _install


def _app(rpc_client: object = None) -> Flask:
    """App with no live Core client: proves the tools.py seam is the only transport."""
    app = Flask(__name__)
    register_routes(app, rpc_client, None, None, None)
    return app


CONDOR_SELECTOR = {
    "scheduler_type": "condor",
    "cluster_name": "htcondor01",
    "cluster_tag": "schedd-a",
    "native_job_id": "172.0",
}


def _set_body(**overrides: Any) -> dict:
    body = {
        "selector": dict(CONDOR_SELECTOR),
        "scope": "instance",
        "ensure_registered": True,
        "operation": "set",
        "collectors": ["cpumem_collector"],
        "expected_revision": 0,
    }
    body.update(overrides)
    return body


def _target(**overrides: Any) -> dict:
    target = {
        "instance_id": "inst-condor-01",
        "internal_job_id": "42",
        "native_job_id": "172.0",
        # Accepted durable revision (the ONE record revision).
        "record_revision": 3,
        "mode": "manual",
        "desired_collectors": ["cpumem_collector"],
        "applied_collectors": [],
        # Actual runtime state, sampled: not proof the intent is attached.
        "applied_attach_token": 0,
        "draining": False,
        "errors": [],
    }
    target.update(overrides)
    return target


def _post_collection(client, body: object):
    return client.post("/joblens/collection", json=body)


# ==================== selector forms and error distinctions ====================

def test_selector_condor_native_selector_forwards_scope_and_ensure_registered(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": [_target()]})

    response = _post_collection(_app().test_client(), _set_body())

    assert response.status_code == 200
    assert fake.calls == [
        (
            "jobs/collection/set",
            {
                "selector": dict(CONDOR_SELECTOR),
                "scope": "instance",
                "ensure_registered": True,
                "operation": "set",
                "collectors": ["cpumem_collector"],
                "expected_revision": 0,
            },
        )
    ]
    body = response.get_json()
    assert body["status"] == "accepted"
    assert [target["instance_id"] for target in body["targets"]] == ["inst-condor-01"]
    assert body["targets"][0]["internal_job_id"] == "42"

    # 不猜身份、不分配内部 ID：转发体里只有契约字段，没有 JobID/internal_job_id 注入
    forwarded = fake.calls[0][1]
    assert set(forwarded) == {
        "selector",
        "scope",
        "ensure_registered",
        "operation",
        "collectors",
        "expected_revision",
    }
    assert "JobID" not in forwarded
    assert "internal_job_id" not in forwarded["selector"]


def test_selector_native_selector_defaults_ensure_registered_to_false(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": [_target()]})
    body = _set_body(ensure_registered=False)

    response = _post_collection(_app().test_client(), body)

    assert response.status_code == 200
    assert fake.calls[0][1]["ensure_registered"] is False


def test_selector_retired_instance_id_selector_is_rejected_at_the_boundary(
    install_core,
) -> None:
    """旧版不透明 instance_id 句柄已退役：边界显式拒绝，绝不下发 Core。"""
    fake = install_core()
    client = _app().test_client()

    response = _post_collection(
        client,
        {"selector": {"instance_id": "opaque-handle-9"}, "operation": "set", "collectors": []},
    )

    assert response.status_code == 400
    assert "instance_id" in response.get_json()["message"]
    # 边界拒绝：Core 从未被调用
    assert fake.calls == []


def test_selector_internal_job_id_selector_forwards_verbatim(install_core) -> None:
    """internal_job_id 实例句柄（唯一形式）：原样转发，不注入内部推导。"""
    fake = install_core(
        response={
            "status": "accepted",
            "targets": [_target(instance_id="opaque-handle-9", native_job_id=None)],
        }
    )

    response = _post_collection(
        _app().test_client(),
        {"selector": {"internal_job_id": "42"}, "operation": "set", "collectors": []},
    )

    assert response.status_code == 200
    forwarded = fake.calls[0][1]
    assert forwarded["selector"] == {"internal_job_id": "42"}
    assert forwarded["scope"] == "instance"
    # 透传边界：Trigger 不把一种句柄形式翻译成另一种
    assert "instance_id" not in forwarded["selector"]


def test_selector_internal_job_id_rejects_non_decimal(install_core) -> None:
    """非十进制 internal_job_id 在边界即拒绝，不打扰 Core。"""
    fake = install_core()
    client = _app().test_client()

    response = _post_collection(
        client,
        {"selector": {"internal_job_id": "4x2"}, "operation": "set", "collectors": []},
    )
    assert response.status_code == 400
    assert fake.calls == []


# ==================== job scope reports each observed step ====================


def test_selector_slurm_job_scope_reports_each_observed_step(install_core) -> None:
    targets = [
        _target(instance_id="inst-step-batch", native_job_id="9001", mode="auto"),
        _target(
            instance_id="inst-step-1",
            native_job_id="9001",
            internal_job_id="43",
            record_revision=1,
            applied_attach_token=1,
        ),
    ]
    fake = install_core(response={"status": "accepted", "targets": targets})

    response = _post_collection(
        _app().test_client(),
        {
            "selector": {
                "scheduler_type": "slurm",
                "cluster_name": "slurm-cluster",
                "native_job_id": "9001",
            },
            "scope": "job",
            "operation": "set",
            "collectors": ["cpumem_collector"],
        },
    )

    assert response.status_code == 200
    assert fake.calls[0][1]["scope"] == "job"
    assert "step_id" not in fake.calls[0][1]["selector"]
    body = response.get_json()
    assert body["status"] == "accepted"
    assert [target["instance_id"] for target in body["targets"]] == [
        "inst-step-batch",
        "inst-step-1",
    ]


def test_selector_slurm_step_selector_is_forwarded_as_native_string(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": [_target(native_job_id="9001")]})

    response = _post_collection(
        _app().test_client(),
        {
            "selector": {
                "scheduler_type": "slurm",
                "cluster_name": "slurm-cluster",
                "native_job_id": 9001,
                "step_id": 2,
            },
            "operation": "set",
            "collectors": ["cpumem_collector"],
        },
    )

    assert response.status_code == 200
    assert fake.calls[0][1]["selector"] == {
        "scheduler_type": "slurm",
        "cluster_name": "slurm-cluster",
        "native_job_id": "9001",
        "step_id": "2",
    }


def test_selector_missing_target_returns_404_and_no_fabricated_targets(install_core) -> None:
    install_core(
        response={
            "status": "error",
            "code": "no_verified_instance",
            "message": "no verified current instance for condor htcondor01 172.0",
        }
    )

    response = _post_collection(_app().test_client(), _set_body())

    assert response.status_code == 404
    assert response.get_json() == {
        "code": "no_verified_instance",
        "message": "no verified current instance for condor htcondor01 172.0",
    }


def test_selector_incomplete_native_namespace_is_rejected_before_rpc(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})

    response = _post_collection(
        _app().test_client(),
        {
            "selector": {"scheduler_type": "condor", "native_job_id": "172.0"},
            "operation": "set",
            "collectors": ["cpumem_collector"],
        },
    )

    assert response.status_code == 400
    assert fake.calls == []
    payload = response.get_json()
    assert payload["code"] == "invalid_request"
    assert "cluster_name" in payload["message"]


def test_selector_both_selector_forms_are_rejected(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})

    response = _post_collection(
        _app().test_client(),
        _set_body(selector={"instance_id": "inst-1", "native_job_id": "172.0"}),
    )

    assert response.status_code == 400
    assert fake.calls == []
    # 退役的 instance_id 键在任何组合里都是边界错误
    assert "instance_id" in response.get_json()["message"]


def test_selector_non_object_request_body_is_rejected(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})

    response = _post_collection(_app().test_client(), ["not", "an", "object"])

    assert response.status_code == 400
    assert fake.calls == []
    assert response.get_json()["code"] == "invalid_request"


def test_selector_invalid_scope_is_rejected(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})

    response = _post_collection(_app().test_client(), _set_body(scope="cluster"))

    assert response.status_code == 400
    assert fake.calls == []
    assert "scope" in response.get_json()["message"]


def test_selector_ambiguous_slurm_scope_returns_409_with_candidates(install_core) -> None:
    install_core(
        response={
            "status": "error",
            "code": "ambiguous_selector",
            "message": "native job 9001 matches instances from two scheduler namespaces",
            "candidates": ["inst-slurm-a", {"instance_id": "inst-slurm-b"}],
        }
    )

    response = _post_collection(
        _app().test_client(),
        _set_body(
            selector={
                "scheduler_type": "slurm",
                "cluster_name": "slurm-cluster",
                "native_job_id": "9001",
            }
        ),
    )

    assert response.status_code == 409
    assert response.get_json() == {
        "code": "ambiguous_selector",
        "message": "native job 9001 matches instances from two scheduler namespaces",
        "candidates": ["inst-slurm-a", "inst-slurm-b"],
    }


def test_selector_nested_core_error_envelope_preserves_code_and_candidates(install_core) -> None:
    install_core(
        response={
            "status": "error",
            "error": {"code": "ambiguous_selector", "candidate_instance_ids": ["inst-x"]},
            "message": "two local namespaces match native id 9001",
        }
    )

    response = _post_collection(
        _app().test_client(),
        _set_body(
            selector={
                "scheduler_type": "slurm",
                "cluster_name": "slurm-cluster",
                "native_job_id": "9001",
            }
        ),
    )

    assert response.status_code == 409
    assert response.get_json() == {
        "code": "ambiguous_selector",
        "message": "two local namespaces match native id 9001",
        "candidates": ["inst-x"],
    }


def test_selector_core_unavailable_returns_structured_503(install_core) -> None:
    install_core(error=RPCError("Socket file not found: /var/JobLens/rpc.sock"))

    response = _post_collection(_app().test_client(), _set_body())

    assert response.status_code == 503
    payload = response.get_json()
    assert payload["code"] == "rpc_unavailable"
    assert "Socket file not found" in payload["message"]
    assert "targets" not in payload


# ==================== set / stop / reset_auto and forwarded errors ====================

def test_control_set_distinguishes_accepted_from_applied(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": [_target()]})

    response = _post_collection(_app().test_client(), _set_body())

    assert response.status_code == 200
    assert fake.calls[0][1]["collectors"] == ["cpumem_collector"]
    body = response.get_json()
    assert body["status"] == "accepted"
    target = body["targets"][0]
    assert target["desired_collectors"] == ["cpumem_collector"]
    assert target["applied_collectors"] == []
    assert target["record_revision"] == 3
    assert target["applied_attach_token"] == 0
    assert target["mode"] == "manual"
    assert target["errors"] == []


def test_control_empty_collectors_forwarded_as_persistent_stop(install_core) -> None:
    fake = install_core(
        response={
            "status": "accepted",
            "targets": [_target(desired_collectors=[], applied_collectors=[])],
        }
    )

    response = _post_collection(
        _app().test_client(),
        _set_body(collectors=[]),
    )

    assert response.status_code == 200
    forwarded = fake.calls[0][1]
    assert "collectors" in forwarded
    assert forwarded["collectors"] == []
    assert response.get_json()["targets"][0]["desired_collectors"] == []


def test_control_reset_auto_omits_collectors(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": [_target(mode="auto")]})

    response = _post_collection(
        _app().test_client(),
        {
            "selector": dict(CONDOR_SELECTOR),
            "operation": "reset_auto",
        },
    )

    assert response.status_code == 200
    forwarded = fake.calls[0][1]
    assert forwarded["operation"] == "reset_auto"
    assert "collectors" not in forwarded
    assert response.get_json()["targets"][0]["mode"] == "auto"


def test_control_reset_auto_with_collectors_is_rejected(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})

    response = _post_collection(
        _app().test_client(),
        {
            "selector": dict(CONDOR_SELECTOR),
            "operation": "reset_auto",
            "collectors": [],
        },
    )

    assert response.status_code == 400
    assert fake.calls == []
    assert "must omit collectors" in response.get_json()["message"]


def test_control_missing_collectors_on_set_is_rejected(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})
    body = _set_body()
    del body["collectors"]

    response = _post_collection(_app().test_client(), body)

    assert response.status_code == 400
    assert fake.calls == []
    payload = response.get_json()
    assert payload["code"] == "invalid_request"
    assert "collectors" in payload["message"]


def test_control_null_collectors_on_set_is_rejected(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})

    response = _post_collection(_app().test_client(), _set_body(collectors=None))

    assert response.status_code == 400
    assert fake.calls == []
    assert "collectors" in response.get_json()["message"]


def test_control_unknown_collector_is_forwarded_as_400(install_core) -> None:
    install_core(
        response={
            "status": "error",
            "code": "unknown_collector",
            "message": "unknown collector: not_a_collector",
        }
    )

    response = _post_collection(
        _app().test_client(),
        _set_body(collectors=["not_a_collector"]),
    )

    assert response.status_code == 400
    assert response.get_json() == {
        "code": "unknown_collector",
        "message": "unknown collector: not_a_collector",
    }


def test_control_stale_expected_revision_is_forwarded_intact(install_core) -> None:
    install_core(
        response={
            "status": "error",
            "code": "stale_revision",
            "message": "binding revision 3 does not match expected 0",
        }
    )

    response = _post_collection(_app().test_client(), _set_body(expected_revision=0))

    assert response.status_code == 409
    body = response.get_json()
    assert body == {
        "code": "stale_revision",
        "message": "binding revision 3 does not match expected 0",
    }
    assert body.get("status") != "accepted"


def test_control_expected_revision_is_rejected_for_job_scope(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})

    response = _post_collection(
        _app().test_client(),
        _set_body(scope="job", expected_revision=2),
    )

    assert response.status_code == 400
    assert fake.calls == []
    assert "expected_revision" in response.get_json()["message"]


def test_control_instance_scope_forwards_expected_revision(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": [_target()]})

    response = _post_collection(
        _app().test_client(),
        {
            "selector": {"internal_job_id": "42"},
            "operation": "set",
            "collectors": ["cpumem_collector"],
            "expected_revision": 2,
        },
    )

    assert response.status_code == 200
    assert fake.calls[0][1]["expected_revision"] == 2
    assert fake.calls[0][1]["scope"] == "instance"


def test_control_core_down_returns_503_for_set_and_get(install_core) -> None:
    install_core(error=RPCError("Failed to connect to RPC server: [Errno 2]"))
    client = _app().test_client()

    set_response = _post_collection(client, _set_body())
    get_response = client.get("/joblens/collection?internal_job_id=42")

    assert set_response.status_code == 503
    assert get_response.status_code == 503
    assert set_response.get_json()["code"] == "rpc_unavailable"
    assert get_response.get_json()["code"] == "rpc_unavailable"


def test_control_get_forwards_internal_job_id_and_passes_selection_through(install_core) -> None:
    core_payload = {
        "status": "ok",
        "instance_id": "inst-condor-01",
        "mode": "manual",
        "record_revision": 3,
        "applied_attach_token": 0,
        "desired_collectors": ["cpumem_collector"],
        "applied_collectors": [],
        "errors": [{"code": "collector_init_failed", "collector": "cpumem_collector"}],
    }
    fake = install_core(response=core_payload)

    response = _app().test_client().get("/joblens/collection?internal_job_id=42")

    assert response.status_code == 200
    assert fake.calls == [("jobs/collection/get", {"internal_job_id": "42"})]
    assert response.get_json() == core_payload


def test_control_get_legacy_instance_id_route_is_adapted(install_core) -> None:
    """旧查询参数只保留在 Trigger 适配层，Core 只看到 internal_job_id。"""
    fake = install_core(response={"status": "ok", "internal_job_id": "42"})

    response = _app().test_client().get("/joblens/collection?instance_id=42")

    assert response.status_code == 200
    assert fake.calls == [("jobs/collection/get", {"internal_job_id": "42"})]


def test_control_get_requires_internal_job_id(install_core) -> None:
    fake = install_core(response={"status": "ok"})

    response = _app().test_client().get("/joblens/collection")

    assert response.status_code == 400
    assert fake.calls == []
    assert "internal_job_id" in response.get_json()["message"]


def test_control_get_missing_instance_returns_404(install_core) -> None:
    install_core(
        response={
            "status": "error",
            "code": "not_found",
            "message": "instance inst-gone is not a current job record",
        }
    )

    response = _app().test_client().get("/joblens/collection?internal_job_id=999")

    assert response.status_code == 404
    assert response.get_json()["code"] == "not_found"


def test_control_per_target_applied_error_is_not_masked(install_core) -> None:
    install_core(
        response={
            "status": "accepted",
            "targets": [
                _target(
                    desired_collectors=["cpumem_collector"],
                    applied_collectors=[],
                    errors=[{"code": "collector_init_failed", "message": "bpf load failed"}],
                )
            ],
        }
    )

    response = _post_collection(_app().test_client(), _set_body())

    assert response.status_code == 200
    target = response.get_json()["targets"][0]
    assert target["applied_collectors"] == []
    assert target["errors"] == [{"code": "collector_init_failed", "message": "bpf load failed"}]


# ==================== list / status / capabilities across health states ====================

def test_read_job_list_forwards_filters_and_paging(install_core) -> None:
    core_payload = {
        "snapshot_revision": 7,
        "records": [{"instance_id": "inst-condor-01", "internal_job_id": "42"}],
        "next_cursor": "cursor-2",
    }
    fake = install_core(response=core_payload)

    response = _app().test_client().get(
        "/joblens/jobs/list"
        "?scheduler_type=condor&native_job_id=172.0&state=running&limit=2&cursor=cursor-1"
    )

    assert response.status_code == 200
    assert fake.calls == [
        (
            "jobs/list",
            {
                "limit": 2,
                "scheduler_type": "condor",
                "native_job_id": "172.0",
                "state": "running",
                "cursor": "cursor-1",
            },
        )
    ]
    body = response.get_json()
    assert body == core_payload
    assert set(body) == {"snapshot_revision", "records", "next_cursor"}


def test_read_job_list_legacy_inventory_route_remains_an_alias(install_core) -> None:
    """旧 URL 只作为 Trigger 侧适配层保留，仍转发到同一个 Core 方法。"""
    fake = install_core(
        response={"snapshot_revision": 1, "records": [], "next_cursor": None}
    )

    response = _app().test_client().get("/joblens/inventory?limit=2")

    assert response.status_code == 200
    assert fake.calls == [("jobs/list", {"limit": 2})]


def test_read_job_list_defaults_limit_to_100(install_core) -> None:
    fake = install_core(
        response={"snapshot_revision": 1, "records": [], "next_cursor": None}
    )

    response = _app().test_client().get("/joblens/jobs/list")

    assert response.status_code == 200
    assert fake.calls[0][1] == {"limit": 100}
    assert response.get_json()["next_cursor"] is None


@pytest.mark.parametrize("limit", ["0", "1001", "abc", "-5"])
def test_read_job_list_rejects_out_of_range_limit(install_core, limit: str) -> None:
    fake = install_core(response={"snapshot_revision": 1, "records": [], "next_cursor": None})

    response = _app().test_client().get(f"/joblens/jobs/list?limit={limit}")

    assert response.status_code == 400
    assert fake.calls == []
    assert response.get_json()["code"] == "invalid_request"


def test_read_job_list_accepts_max_limit(install_core) -> None:
    fake = install_core(response={"snapshot_revision": 1, "records": [], "next_cursor": None})

    response = _app().test_client().get("/joblens/jobs/list?limit=1000")

    assert response.status_code == 200
    assert fake.calls[0][1] == {"limit": 1000}


def test_read_records_pass_through_decimal_string_ids_without_sentinel(install_core) -> None:
    records = [
        {"instance_id": "inst-1", "internal_job_id": "42", "state": "running"},
        {"instance_id": "inst-2", "internal_job_id": "9007199254740993", "state": "running"},
    ]
    install_core(
        response={"snapshot_revision": 11, "records": records, "next_cursor": None}
    )

    response = _app().test_client().get("/joblens/jobs/list")

    assert response.status_code == 200
    body = response.get_json()
    assert body["records"] == records
    assert all(isinstance(record["internal_job_id"], str) for record in body["records"])
    assert "0" not in [record["internal_job_id"] for record in body["records"]]
    assert len(body["records"]) == 2


def test_read_stale_cursor_returns_409_without_retry(install_core) -> None:
    fake = install_core(
        response={
            "status": "error",
            "code": "snapshot_changed",
            "message": "job snapshot 7 no longer matches cursor snapshot 6",
        }
    )

    response = _app().test_client().get("/joblens/jobs/list?cursor=cursor-1")

    assert response.status_code == 409
    assert response.get_json()["code"] == "snapshot_changed"
    assert len(fake.calls) == 1


@pytest.mark.parametrize(
    "core_status",
    [
        {
            "status": "initializing",
            "enabled": True,
            "reconciliation": {"state": "pending", "completed_scans": 0},
        },
        {
            "status": "degraded",
            "enabled": True,
            "sources": [{"scheduler_type": "condor", "state": "degraded"}],
            "queue": {"pending": 4},
        },
        {
            "status": "healthy",
            "enabled": True,
            "sources": [{"scheduler_type": "slurm", "state": "healthy"}],
            "persistence": {"state": "ok", "schema_version": 5},
        },
        {
            "status": "disabled",
            "enabled": False,
            "sources": [],
        },
    ],
)
def test_read_status_passes_through_lifecycle_states(install_core, core_status: dict) -> None:
    install_core(response=core_status)

    response = _app().test_client().get("/joblens/jobs/status")

    assert response.status_code == 200
    assert response.get_json() == core_status


def test_read_status_does_not_upgrade_disabled_registry_to_healthy(install_core) -> None:
    core_status = {"status": "disabled", "enabled": False, "sources": []}
    install_core(response=core_status)

    body = _app().test_client().get("/joblens/jobs/status").get_json()

    assert body["status"] == "disabled"
    assert body["enabled"] is False
    assert "healthy" not in body


def test_read_status_missing_registry_returns_404(install_core) -> None:
    install_core(
        response={
            "status": "error",
            "code": "not_found",
            "message": "job registry is not available on this core",
        }
    )

    response = _app().test_client().get("/joblens/jobs/status")

    assert response.status_code == 404
    assert response.get_json()["code"] == "not_found"


def test_read_capabilities_are_forwarded_verbatim(install_core) -> None:
    core_payload = {
        "status": "ok",
        "api_version": 2,
        "collection_control_version": 1,
        "native_identity_selectors": True,
        "job_scope": "current_local_instances",
        "db_schema_version": 5,
        "ready": True,
    }
    fake = install_core(response=core_payload)

    response = _app().test_client().get("/joblens/v2/capabilities")

    assert response.status_code == 200
    assert fake.calls == [("joblens/capabilities", {})]
    assert response.get_json() == core_payload


def test_v1_capabilities_keep_the_legacy_field_aliases(install_core) -> None:
    install_core(response={
        "status": "ok",
        "api_version": 2,
        "collection_control_version": 1,
        "native_identity_selectors": True,
        "job_scope": "current_local_instances",
        "db_schema_version": 5,
        "ready": True,
    })

    payload = _app().test_client().get("/joblens/capabilities").get_json()

    assert payload["api_version"] == 2
    assert payload["inventory_version"] == 1
    assert payload["inventory_ready"] is True


def test_read_capabilities_are_not_invented_when_core_lacks_them(install_core) -> None:
    install_core(error=RPCError("method not found: joblens/capabilities"))

    response = _app().test_client().get("/joblens/capabilities")

    assert response.status_code == 503
    body = response.get_json()
    assert body["code"] == "rpc_unavailable"
    assert "api_version" not in body
    assert "collection_control_version" not in body


def test_read_non_object_core_payload_is_not_reported_as_success(install_core) -> None:
    install_core(response=["unexpected", "shape"])

    response = _app().test_client().get("/joblens/jobs/list")

    assert response.status_code == 502
    assert response.get_json()["code"] == "core_response_invalid"


def test_read_contract_violating_collection_target_is_not_reported_as_success(
    install_core,
) -> None:
    install_core(
        response={
            "status": "accepted",
            "targets": [_target(internal_job_id=42)],
        }
    )

    response = _post_collection(_app().test_client(), _set_body())

    assert response.status_code == 502
    assert response.get_json()["code"] == "core_response_invalid"


# ==================== job_opt remove erase tier pass-through ====================


@pytest.mark.parametrize(
    "job_type,cluster_name,cluster_tag,sub_attr,core_error",
    [
        ("job.condor", "pool-a", "schedd#one@submit", {"cluster_id": 12, "proc_id": 3}, False),
        ("job.slurm", "cluster-a", "tag-a", {"job_id": 12, "step_id": 1}, False),
        ("job.condor", "pool-a", "schedd@submit",
         {"cluster_id": 12, "proc_id": 3, "collector_host": "pool-b"}, True),
        ("job.slurm", "cluster-a", "tag-a",
         {"job_id": 12, "step_id": 1, "cluster_name": "cluster-b"}, True),
    ],
)
def test_job_namespace_claims_reach_rpc_unchanged(
    monkeypatch, job_type, cluster_name, cluster_tag, sub_attr, core_error
) -> None:
    calls = []

    class FakeRPC:
        def __init__(self, socket_path, timeout):
            pass

        def get_function_list(self):
            return ["jobs/upsert", "jobs/stop", "jobs/erase"]

        def call(self, method, params):
            calls.append((method, params))
            if core_error:
                return {"status": "error", "msg": "namespace_conflict"}
            return {"status": "success"}

    monkeypatch.setattr(tools, "USE_MOCK_TOOLS", False)
    monkeypatch.setattr(tools, "use_rpc_opt", lambda: True)
    monkeypatch.setattr(tools, "config", {"lens_config": {"rpc_socket_path": "/tmp/test.sock"}})
    monkeypatch.setattr(tools, "RPCClient", FakeRPC)
    body = {"opt": "add", "type": job_type, "cluster_name": cluster_name,
            "cluster_tag": cluster_tag, "sub_attr": sub_attr}
    response = _app().test_client().post("/joblens/job", json=body)

    assert response.status_code == (500 if core_error else 200)
    assert len(calls) == 1 and calls[0][0] == "jobs/upsert"
    assert calls[0][1]["cluster_name"] == cluster_name
    assert calls[0][1]["cluster_tag"] == cluster_tag
    assert calls[0][1]["sub_attr"] == sub_attr


def test_erase_job_remove_forwards_the_erase_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """remove 的真删档必须原样透传给核心；默认不携带 erase。"""
    from trigger.api import routes as routes_module

    forwarded: dict[str, object] = {}

    def fake_job_opt(data: object) -> object:
        forwarded.clear()
        forwarded.update(data)  # type: ignore[arg-type]
        return {"status": "success", "mode": "erase", "deleted_count": 1}

    monkeypatch.setattr(routes_module, "job_opt", fake_job_opt)
    client = _app().test_client()

    response = client.post(
        "/joblens/job",
        json={"opt": "remove", "type": "job.condor", "JobID": 7, "erase": True},
    )

    assert response.status_code == 200
    assert forwarded.get("erase") is True

    # Default (absent): the stop tier, no erase key on the wire.
    response = client.post(
        "/joblens/job", json={"opt": "remove", "type": "job.condor", "JobID": 7}
    )

    assert response.status_code == 200
    assert "erase" not in forwarded


def test_erase_job_remove_erase_is_coerced_to_boolean(monkeypatch: pytest.MonkeyPatch) -> None:
    """pydantic 宽松模式：'true'/1 强转为布尔后透传，核心只见到布尔档位。"""
    from trigger.api import routes as routes_module

    forwarded: dict[str, object] = {}

    def fake_job_opt(data: object) -> object:
        forwarded.clear()
        forwarded.update(data)  # type: ignore[arg-type]
        return {"status": "success", "mode": "stop", "stopped_count": 1}

    monkeypatch.setattr(routes_module, "job_opt", fake_job_opt)
    client = _app().test_client()

    response = client.post(
        "/joblens/job",
        json={"opt": "remove", "type": "job.condor", "JobID": 7, "erase": "true"},
    )

    assert response.status_code == 200
    assert forwarded.get("erase") is True


# ==================== v2 resource API ====================


def _record(**overrides: Any) -> dict:
    record = {
        "internal_job_id": "42",
        "native_job_id": "172.0",
        "scheduler_type": "condor",
        "cluster_name": "htcondor01",
        "cluster_tag": "schedd-a",
        "state": "active",
        "desired_collectors": ["cpumem_collector"],
        "processes": [{"pid": 1235, "start_ticks": 1000, "boot_id": "boot"}],
        "attributes": {"owner": "alice"},
    }
    record.update(overrides)
    return record


def test_v2_job_list_is_the_resource_page(install_core) -> None:
    payload = {
        "status": "ok",
        "snapshot_revision": 7,
        "records": [_record()],
        "next_cursor": None,
    }
    fake = install_core(response=payload)

    response = _app().test_client().get("/joblens/v2/jobs?limit=1")

    assert response.status_code == 200
    assert fake.calls == [("jobs/list", {"limit": 1})]
    assert response.get_json() == payload


def test_v2_job_get_forwards_internal_job_id(install_core) -> None:
    fake = install_core(response={"status": "ok", **_record()})

    response = _app().test_client().get("/joblens/v2/jobs/42")

    assert response.status_code == 200
    assert fake.calls == [("jobs/get", {"internal_job_id": "42"})]
    assert response.get_json()["internal_job_id"] == "42"


def test_v2_job_upsert_omits_legacy_opt_field(install_core) -> None:
    fake = install_core(response={"status": "success", "internal_job_id": "42"})

    response = _app().test_client().put(
        "/joblens/v2/jobs",
        json={
            "type": "job.condor",
            "JobPIDs": [1235],
            "Lens": ["cpumem_collector"],
            "sub_attr": {"cluster_id": 172, "proc_id": 0},
        },
    )

    assert response.status_code == 200
    method, payload = fake.calls[0]
    assert method == "jobs/upsert"
    assert "opt" not in payload
    assert payload["sub_attr"] == {"cluster_id": 172, "proc_id": 0}


def test_v2_job_create_is_strict_and_forwards_jobs_create(install_core) -> None:
    fake = install_core(response={"status": "success", "internal_job_id": "42"})

    response = _app().test_client().post(
        "/joblens/v2/jobs",
        json={"type": "job.condor", "JobPIDs": [1235], "Lens": ["cpumem_collector"]},
    )

    assert response.status_code == 200
    assert fake.calls[0][0] == "jobs/create"
    assert "opt" not in fake.calls[0][1]


def test_v2_job_create_conflict_maps_to_409(install_core) -> None:
    install_core(response={
        "status": "error",
        "code": "already_exists",
        "message": "job instance 42 already exists",
        "internal_job_id": "42",
    })

    response = _app().test_client().post(
        "/joblens/v2/jobs",
        json={"type": "job.condor", "JobPIDs": [1235], "Lens": ["cpumem_collector"]},
    )

    assert response.status_code == 409
    assert response.get_json()["code"] == "already_exists"


def test_v2_job_erase_and_stop_are_distinct(install_core) -> None:
    fake = install_core(response={"status": "success", "mode": "stop"})
    client = _app().test_client()

    erase = client.delete("/joblens/v2/jobs/42")
    stop = client.post("/joblens/v2/jobs/42/stop")

    assert erase.status_code == 200
    assert stop.status_code == 200
    assert fake.calls == [
        ("jobs/erase", {"JobID": 42}),
        ("jobs/stop", {"JobID": 42}),
    ]


def test_v2_collection_subresource_verbs(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": [_target()]})
    client = _app().test_client()

    read = client.get("/joblens/v2/jobs/42/collection")
    put = client.put(
        "/joblens/v2/jobs/42/collection",
        json={"collectors": ["cpumem_collector"], "expected_revision": 3},
    )
    delete = client.delete("/joblens/v2/jobs/42/collection")

    assert read.status_code == 200
    assert put.status_code == 200
    assert delete.status_code == 200
    assert [call[0] for call in fake.calls] == [
        "jobs/collection/get",
        "jobs/collection/set",
        "jobs/collection/reset",
    ]
    assert fake.calls[1][1]["selector"] == {"internal_job_id": "42"}
    assert fake.calls[1][1]["operation"] == "set"
    assert fake.calls[1][1]["expected_revision"] == 3
    assert fake.calls[2][1]["operation"] == "reset_auto"


def test_v2_status_and_capabilities_are_shared(install_core) -> None:
    install_core(response={"status": "ok", "api_version": 2, "ready": True})
    client = _app().test_client()

    status = client.get("/joblens/v2/status")
    caps = client.get("/joblens/v2/capabilities")

    assert status.status_code == 200
    assert caps.status_code == 200


# ==================== v1 compatibility projection ====================


def test_legacy_jobs_list_projects_the_new_record_shape(install_core) -> None:
    fake = install_core(
        response={
            "status": "ok",
            "snapshot_revision": 1,
            "records": [_record()],
            "next_cursor": None,
        }
    )

    response = _app().test_client().get("/joblens/jobs")

    assert response.status_code == 200
    job = response.get_json()["jobs"][0]
    assert job["JobID"] == 42
    assert job["NativeJobID"] == "172.0"
    assert job["JobPIDs"] == [1235]
    assert job["CollectorNames"] == ["cpumem_collector"]
    assert job["sub_attr"] == {"owner": "alice"}
    assert fake.calls == [("jobs/list", {"limit": 1000})]


def test_legacy_job_count_uses_the_resource_count(install_core) -> None:
    fake = install_core(response={"status": "ok", "job_count": 3})

    response = _app().test_client().get("/joblens/jobs/count")

    assert response.status_code == 200
    assert response.get_json()["job_count"] == 3
    assert fake.calls == [("jobs/count", {})]


def test_legacy_pid_tracker_uses_diagnostics(install_core) -> None:
    fake = install_core(
        response={"status": "ok", "running": True, "pushed_events": 1, "dropped_events": 0}
    )

    response = _app().test_client().get("/joblens/pid_tracker/stats")

    assert response.status_code == 200
    assert fake.calls == [("diagnostics/pid_tracker", {})]


def test_v1_inventory_status_keeps_the_legacy_enabled_alias(install_core) -> None:
    install_core(response={
        "status": "healthy",
        "enabled": True,
        "sources": [],
    })

    payload = _app().test_client().get("/joblens/inventory/status").get_json()

    assert payload["enabled"] is True
    assert payload["inventory_enabled"] is True


def test_v1_inventory_records_keep_the_legacy_facts_revision_alias(install_core) -> None:
    install_core(response={
        "status": "ok",
        "snapshot_revision": 1,
        "records": [{"internal_job_id": "42", "record_revision": 7}],
        "next_cursor": None,
    })

    payload = _app().test_client().get("/joblens/inventory").get_json()

    assert payload["records"][0]["record_revision"] == 7
    assert payload["records"][0]["facts_revision"] == 7


def test_job_opt_adapter_maps_remove_to_stop_or_erase(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    class FakeRPC:
        def __init__(self, socket_path, timeout):
            pass

        def get_function_list(self):
            return ["jobs/upsert", "jobs/stop", "jobs/erase"]

        def call(self, method, params):
            calls.append((method, params))
            return {"status": "success"}

    monkeypatch.setattr(tools, "USE_MOCK_TOOLS", False)
    monkeypatch.setattr(tools, "config", {
        "lens_config": {"rpc_socket_path": "/tmp/test.sock", "rpc_timeout": 5},
    })
    monkeypatch.setattr(tools, "RPCClient", FakeRPC)

    tools.job_opt({"opt": "remove", "JobID": 7})
    tools.job_opt({"opt": "remove", "JobID": 7, "erase": True})

    assert [call[0] for call in calls] == ["jobs/stop", "jobs/erase"]
    assert all("erase" not in call[1] for call in calls)

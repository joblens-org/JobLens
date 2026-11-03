"""Section 9 contract tests for the inventory/collection Flask gateway.

Normative contract: doc/job_inventory_collection_decoupling_plan.md section 9
(9.1 new methods, 9.2 control request, 9.3 inventory/capabilities, 9.4 legacy).
Acceptance cases: doc/job_inventory_collection_decoupling_validation.md A30, A31, A33.

The Core is mocked at the single RPC seam in trigger/core/tools.py
(``call_inventory_rpc``), so these tests pass independently of C++ progress.
The Flask side must never allocate internal IDs, never fabricate a response when
the Core is unavailable, and never turn a Core error into HTTP 200.
"""

from typing import Any, Optional

import pytest
from flask import Flask

from trigger.api.routes import register_routes
from trigger.core import tools
from trigger.core.rpc_client import RPCError


class FakeInventoryCore:
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
    """Install a fake Core behind trigger.core.tools.call_inventory_rpc."""

    def _install(response: object = None, error: Optional[Exception] = None) -> FakeInventoryCore:
        fake = FakeInventoryCore(response=response, error=error)
        monkeypatch.setattr(tools, "call_inventory_rpc", fake)
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


# ==================== A30: selector forms and error distinctions ====================

def test_a30_condor_native_selector_forwards_scope_and_ensure_registered(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": [_target()]})

    response = _post_collection(_app().test_client(), _set_body())

    assert response.status_code == 200
    assert fake.calls == [
        (
            "JobCollection/set",
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

    # 不猜身份、不分配内部 ID：转发体里只有 §9.2 的字段，没有 JobID/internal_job_id 注入
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


def test_a30_native_selector_defaults_ensure_registered_to_false(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": [_target()]})
    body = _set_body(ensure_registered=False)

    response = _post_collection(_app().test_client(), body)

    assert response.status_code == 200
    assert fake.calls[0][1]["ensure_registered"] is False


def test_a30_instance_handle_selector_forwards_instance_id_only(install_core) -> None:
    fake = install_core(
        response={
            "status": "accepted",
            "targets": [_target(instance_id="opaque-handle-9", native_job_id=None)],
        }
    )

    response = _post_collection(
        _app().test_client(),
        {"selector": {"instance_id": "opaque-handle-9"}, "operation": "set", "collectors": []},
    )

    assert response.status_code == 200
    forwarded = fake.calls[0][1]
    assert forwarded["selector"] == {"instance_id": "opaque-handle-9"}
    assert forwarded["scope"] == "instance"
    assert response.get_json()["targets"][0]["instance_id"] == "opaque-handle-9"


def test_a30_slurm_job_scope_reports_each_observed_step(install_core) -> None:
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


def test_a30_slurm_step_selector_is_forwarded_as_native_string(install_core) -> None:
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


def test_a30_missing_target_returns_404_and_no_fabricated_targets(install_core) -> None:
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


def test_a30_incomplete_native_namespace_is_rejected_before_rpc(install_core) -> None:
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


def test_a30_both_selector_forms_are_rejected(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})

    response = _post_collection(
        _app().test_client(),
        _set_body(selector={"instance_id": "inst-1", "native_job_id": "172.0"}),
    )

    assert response.status_code == 400
    assert fake.calls == []
    assert "exactly one form" in response.get_json()["message"]


def test_a30_non_object_request_body_is_rejected(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})

    response = _post_collection(_app().test_client(), ["not", "an", "object"])

    assert response.status_code == 400
    assert fake.calls == []
    assert response.get_json()["code"] == "invalid_request"


def test_a30_invalid_scope_is_rejected(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})

    response = _post_collection(_app().test_client(), _set_body(scope="cluster"))

    assert response.status_code == 400
    assert fake.calls == []
    assert "scope" in response.get_json()["message"]


def test_a30_ambiguous_slurm_scope_returns_409_with_candidates(install_core) -> None:
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


def test_a30_nested_core_error_envelope_preserves_code_and_candidates(install_core) -> None:
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


def test_a30_core_unavailable_returns_structured_503(install_core) -> None:
    install_core(error=RPCError("Socket file not found: /var/JobLens/rpc.sock"))

    response = _post_collection(_app().test_client(), _set_body())

    assert response.status_code == 503
    payload = response.get_json()
    assert payload["code"] == "rpc_unavailable"
    assert "Socket file not found" in payload["message"]
    assert "targets" not in payload


# ==================== A31: set / stop / reset_auto and forwarded errors ====================

def test_a31_set_distinguishes_accepted_from_applied(install_core) -> None:
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


def test_a31_empty_collectors_forwarded_as_persistent_stop(install_core) -> None:
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


def test_a31_reset_auto_omits_collectors(install_core) -> None:
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


def test_a31_reset_auto_with_collectors_is_rejected(install_core) -> None:
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


def test_a31_missing_collectors_on_set_is_rejected(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})
    body = _set_body()
    del body["collectors"]

    response = _post_collection(_app().test_client(), body)

    assert response.status_code == 400
    assert fake.calls == []
    payload = response.get_json()
    assert payload["code"] == "invalid_request"
    assert "collectors" in payload["message"]


def test_a31_null_collectors_on_set_is_rejected(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})

    response = _post_collection(_app().test_client(), _set_body(collectors=None))

    assert response.status_code == 400
    assert fake.calls == []
    assert "collectors" in response.get_json()["message"]


def test_a31_unknown_collector_is_forwarded_as_400(install_core) -> None:
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


def test_a31_stale_expected_revision_is_forwarded_intact(install_core) -> None:
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


def test_a31_expected_revision_is_rejected_for_job_scope(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": []})

    response = _post_collection(
        _app().test_client(),
        _set_body(scope="job", expected_revision=2),
    )

    assert response.status_code == 400
    assert fake.calls == []
    assert "expected_revision" in response.get_json()["message"]


def test_a31_instance_scope_forwards_expected_revision(install_core) -> None:
    fake = install_core(response={"status": "accepted", "targets": [_target()]})

    response = _post_collection(
        _app().test_client(),
        {
            "selector": {"instance_id": "inst-condor-01"},
            "operation": "set",
            "collectors": ["cpumem_collector"],
            "expected_revision": 2,
        },
    )

    assert response.status_code == 200
    assert fake.calls[0][1]["expected_revision"] == 2
    assert fake.calls[0][1]["scope"] == "instance"


def test_a31_core_down_returns_503_for_set_and_get(install_core) -> None:
    install_core(error=RPCError("Failed to connect to RPC server: [Errno 2]"))
    client = _app().test_client()

    set_response = _post_collection(client, _set_body())
    get_response = client.get("/joblens/collection?instance_id=inst-condor-01")

    assert set_response.status_code == 503
    assert get_response.status_code == 503
    assert set_response.get_json()["code"] == "rpc_unavailable"
    assert get_response.get_json()["code"] == "rpc_unavailable"


def test_a31_get_forwards_instance_id_and_passes_selection_through(install_core) -> None:
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

    response = _app().test_client().get("/joblens/collection?instance_id=inst-condor-01")

    assert response.status_code == 200
    assert fake.calls == [("JobCollection/get", {"instance_id": "inst-condor-01"})]
    assert response.get_json() == core_payload


def test_a31_get_requires_instance_id(install_core) -> None:
    fake = install_core(response={"status": "ok"})

    response = _app().test_client().get("/joblens/collection")

    assert response.status_code == 400
    assert fake.calls == []
    assert "instance_id" in response.get_json()["message"]


def test_a31_get_missing_instance_returns_404(install_core) -> None:
    install_core(
        response={
            "status": "error",
            "code": "not_found",
            "message": "instance inst-gone is not a current inventory entry",
        }
    )

    response = _app().test_client().get("/joblens/collection?instance_id=inst-gone")

    assert response.status_code == 404
    assert response.get_json()["code"] == "not_found"


def test_a31_per_target_applied_error_is_not_masked(install_core) -> None:
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


# ==================== A33: list / status / capabilities across health states ====================

def test_a33_inventory_list_forwards_filters_and_paging(install_core) -> None:
    core_payload = {
        "snapshot_revision": 7,
        "records": [{"instance_id": "inst-condor-01", "internal_job_id": "42"}],
        "next_cursor": "cursor-2",
    }
    fake = install_core(response=core_payload)

    response = _app().test_client().get(
        "/joblens/inventory"
        "?scheduler_type=condor&native_job_id=172.0&state=running&limit=2&cursor=cursor-1"
    )

    assert response.status_code == 200
    assert fake.calls == [
        (
            "JobRegistry/inventory_list",
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


def test_a33_inventory_list_defaults_limit_to_100(install_core) -> None:
    fake = install_core(
        response={"snapshot_revision": 1, "records": [], "next_cursor": None}
    )

    response = _app().test_client().get("/joblens/inventory")

    assert response.status_code == 200
    assert fake.calls[0][1] == {"limit": 100}
    assert response.get_json()["next_cursor"] is None


@pytest.mark.parametrize("limit", ["0", "1001", "abc", "-5"])
def test_a33_inventory_list_rejects_out_of_range_limit(install_core, limit: str) -> None:
    fake = install_core(response={"snapshot_revision": 1, "records": [], "next_cursor": None})

    response = _app().test_client().get(f"/joblens/inventory?limit={limit}")

    assert response.status_code == 400
    assert fake.calls == []
    assert response.get_json()["code"] == "invalid_request"


def test_a33_inventory_list_accepts_max_limit(install_core) -> None:
    fake = install_core(response={"snapshot_revision": 1, "records": [], "next_cursor": None})

    response = _app().test_client().get("/joblens/inventory?limit=1000")

    assert response.status_code == 200
    assert fake.calls[0][1] == {"limit": 1000}


def test_a33_records_pass_through_decimal_string_ids_without_sentinel(install_core) -> None:
    records = [
        {"instance_id": "inst-1", "internal_job_id": "42", "state": "running"},
        {"instance_id": "inst-2", "internal_job_id": "9007199254740993", "state": "running"},
    ]
    install_core(
        response={"snapshot_revision": 11, "records": records, "next_cursor": None}
    )

    response = _app().test_client().get("/joblens/inventory")

    assert response.status_code == 200
    body = response.get_json()
    assert body["records"] == records
    assert all(isinstance(record["internal_job_id"], str) for record in body["records"])
    assert "0" not in [record["internal_job_id"] for record in body["records"]]
    assert len(body["records"]) == 2


def test_a33_stale_cursor_returns_409_without_retry(install_core) -> None:
    fake = install_core(
        response={
            "status": "error",
            "code": "snapshot_changed",
            "message": "inventory snapshot 7 no longer matches cursor snapshot 6",
        }
    )

    response = _app().test_client().get("/joblens/inventory?cursor=cursor-1")

    assert response.status_code == 409
    assert response.get_json()["code"] == "snapshot_changed"
    assert len(fake.calls) == 1


@pytest.mark.parametrize(
    "core_status",
    [
        {
            "status": "initializing",
            "inventory_enabled": True,
            "reconciliation": {"state": "pending", "completed_scans": 0},
        },
        {
            "status": "degraded",
            "inventory_enabled": True,
            "sources": [{"scheduler_type": "condor", "state": "degraded"}],
            "queue": {"pending": 4},
        },
        {
            "status": "healthy",
            "inventory_enabled": True,
            "sources": [{"scheduler_type": "slurm", "state": "healthy"}],
            "persistence": {"state": "ok", "schema_version": 4},
        },
        {
            "status": "disabled",
            "inventory_enabled": False,
            "sources": [],
        },
    ],
)
def test_a33_status_passes_through_lifecycle_states(install_core, core_status: dict) -> None:
    install_core(response=core_status)

    response = _app().test_client().get("/joblens/inventory/status")

    assert response.status_code == 200
    assert response.get_json() == core_status


def test_a33_status_does_not_upgrade_disabled_inventory_to_healthy(install_core) -> None:
    core_status = {"status": "disabled", "inventory_enabled": False, "sources": []}
    install_core(response=core_status)

    body = _app().test_client().get("/joblens/inventory/status").get_json()

    assert body["status"] == "disabled"
    assert body["inventory_enabled"] is False
    assert "healthy" not in body


def test_a33_status_missing_inventory_returns_404(install_core) -> None:
    install_core(
        response={
            "status": "error",
            "code": "not_found",
            "message": "inventory subsystem is not available on this core",
        }
    )

    response = _app().test_client().get("/joblens/inventory/status")

    assert response.status_code == 404
    assert response.get_json()["code"] == "not_found"


def test_a33_capabilities_are_forwarded_verbatim(install_core) -> None:
    core_payload = {
        "status": "ok",
        "inventory_version": 1,
        "collection_control_version": 1,
        "native_identity_selectors": True,
        "job_scope": "current_local_instances",
        "db_schema_version": 4,
    }
    fake = install_core(response=core_payload)

    response = _app().test_client().get("/joblens/capabilities")

    assert response.status_code == 200
    assert fake.calls == [("JobRegistry/capabilities", {})]
    assert response.get_json() == core_payload


def test_a33_capabilities_are_not_invented_when_core_lacks_them(install_core) -> None:
    install_core(error=RPCError("method not found: JobRegistry/capabilities"))

    response = _app().test_client().get("/joblens/capabilities")

    assert response.status_code == 503
    body = response.get_json()
    assert body["code"] == "rpc_unavailable"
    assert "inventory_version" not in body
    assert "collection_control_version" not in body


def test_a33_non_object_core_payload_is_not_reported_as_success(install_core) -> None:
    install_core(response=["unexpected", "shape"])

    response = _app().test_client().get("/joblens/inventory")

    assert response.status_code == 502
    assert response.get_json()["code"] == "core_response_invalid"


def test_a33_contract_violating_collection_target_is_not_reported_as_success(
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

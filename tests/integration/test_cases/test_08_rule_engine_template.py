from __future__ import annotations

import time
from collections.abc import Callable
from typing import cast

try:
    from test_cases.utils import (
        JobLensAPI,
        RemoteClient,
        retry_with_backoff,
        submit_condor_job,
        submit_slurm_job,
    )
except ModuleNotFoundError:
    from tests.integration.test_cases.utils import (
        JobLensAPI,
        RemoteClient,
        retry_with_backoff,
        submit_condor_job,
        submit_slurm_job,
    )


RULES_DIR = "/var/JobLens/integration-rules"
CONDOR_RULE = f"{RULES_DIR}/condor_job_template.lua"
SLURM_RULE = f"{RULES_DIR}/slurm_job_template.lua"
RULE_COLLECTOR = "cpumem_collector"
SANDBOX_WRITE_PROBE = "/tmp/joblens_rule_engine_sandbox_write_probe"


CONDOR_RULE_CODE = """
return {
  name = "integration-condor-rule-template",
  description = "集成测试模板：所有 Condor 作业通过规则引擎并追加采集器",
  priority = 10,
  condition = function(job)
    return {
      passed = job.subtype == "Condor" and job.sub_attr.cluster_id ~= nil,
      collectors = {"cpumem_collector"}
    }
  end
}
"""


SLURM_RULE_CODE = """
return {
  name = "integration-slurm-rule-template",
  description = "集成测试模板：所有 Slurm 作业通过规则引擎并追加采集器",
  priority = 10,
  condition = function(job)
    return {
      passed = job.subtype == "Slurm" and job.sub_attr.job_id ~= nil,
      collectors = {"cpumem_collector"}
    }
  end
}
"""


CONDOR_OWNER_RULE_CODE = """
return {
  name = "integration-condor-owner-rule",
  description = "集成测试模板：校验 Condor sub_attr.owner 字段可被多个规则共同读取",
  priority = 20,
  condition = function(job)
    return {
      passed = job.subtype == "Condor" and job.sub_attr.owner ~= nil,
      collectors = {"cpumem_collector"}
    }
  end
}
"""


CONDOR_NATIVE_ID_RULE_CODE = """
return {
  name = "integration-condor-native-id-rule",
  description = "集成测试模板：校验第二条 Condor 规则与第一条规则共同通过",
  priority = 10,
  condition = function(job)
    return {
      passed = job.NativeJobID ~= nil and job.NativeJobID ~= "",
      collectors = {"cpumem_collector"}
    }
  end
}
"""


SANDBOX_REQUIRE_OS_RULE_CODE = """
return {
  name = "integration-sandbox-require-os-rule",
  description = "集成测试模板：确认 require('os') 被沙箱阻断",
  priority = 20,
  condition = function(job)
    local ok = pcall(function() return require("os") end)
    return {
      passed = not ok,
      collectors = {"cpumem_collector"}
    }
  end
}
"""


SANDBOX_LOAD_RULE_CODE = """
return {
  name = "integration-sandbox-load-rule",
  description = "集成测试模板：确认 load 被沙箱移除",
  priority = 10,
  condition = function(job)
    local ok = pcall(function() return load("return true") end)
    return {
      passed = not ok,
      collectors = {"cpumem_collector"}
    }
  end
}
"""


SANDBOX_WRITE_RULE_CODE = f"""
return {{
  name = "integration-sandbox-write-rule",
  description = "集成测试模板：探测 Lua 是否能通过 io.open 写出沙箱",
  priority = 10,
  condition = function(job)
    local f = io.open("{SANDBOX_WRITE_PROBE}", "w")
    if f ~= nil then
      f:write("sandbox escape")
      f:close()
    end
    return {{
      passed = false,
      collectors = {{}}
    }}
  end
}}
"""


def _write_rule_engine_template(worker: RemoteClient, scheduler: str) -> str:
    if scheduler == "condor":
        _write_rules(worker, {"condor_job_template.lua": CONDOR_RULE_CODE})
        return "integration-condor-rule-template"
    elif scheduler == "slurm":
        _write_rules(worker, {"slurm_job_template.lua": SLURM_RULE_CODE})
        return "integration-slurm-rule-template"
    else:
        raise ValueError(f"未知调度器: {scheduler}")


def _write_rules(worker: RemoteClient, rules: dict[str, str]) -> None:
    worker.sudo(f"rm -rf {RULES_DIR} && mkdir -p {RULES_DIR}", hide=True)
    for filename, code in rules.items():
        rule_path = f"{RULES_DIR}/{filename}"
        write_rule_command = f"cat > {rule_path} <<'EOF'\n{code.strip()}\nEOF\n"
        worker.sudo(write_rule_command, hide=True)


def _restart_joblens_for_rule_engine(worker: RemoteClient) -> None:
    worker.sudo("systemctl stop joblens-trigger 2>/dev/null || true", hide=True, warn=True)
    worker.sudo("systemctl restart joblens", hide=True)
    worker.sudo("systemctl start joblens-trigger", hide=True)


def _restore_rule_engine_template(worker: RemoteClient) -> None:
    worker.sudo(f"rm -rf {RULES_DIR}", hide=True, warn=True)


def _sudo(worker: RemoteClient, command: str, *, hide: bool = True, warn: bool = False) -> object:
    sudo = cast(Callable[..., object], worker.sudo)
    return sudo(command, hide=hide, warn=warn)


def _stdout(result: object) -> str:
    value = getattr(result, "stdout", "")
    return value if isinstance(value, str) else ""


def _wait_for_rule_engine_ready(worker: RemoteClient, expected_rule_names: str | list[str]) -> None:
    rule_names = [expected_rule_names] if isinstance(expected_rule_names, str) else expected_rule_names

    def _check() -> str:
        result = _sudo(
            worker,
            "journalctl -u joblens --no-pager --since '2 minutes ago'",
            hide=True,
            warn=True,
        )
        log = _stdout(result)
        missing = [name for name in rule_names if f"Stored rule '{name}'" not in log]
        if missing:
            log_tail = log[-2000:]
            message = (
                "规则引擎尚未完成规则加载，缺失规则: "
                f"{missing}; 当前日志尾部:\n{log_tail}"
            )
            raise RuntimeError(message)
        return log

    retry_with_backoff(_check, timeout=30.0, initial=1.0, max_wait=5.0)


def _int_from_json(value: object, default: int = -1) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _response_json_object(response: object) -> dict[str, object]:
    json_func = cast(Callable[[], object], getattr(response, "json"))
    json_value = json_func()
    if isinstance(json_value, dict):
        return cast(dict[str, object], json_value)
    raise RuntimeError(f"响应 JSON 期望 object，实际 {type(json_value).__name__}")


def _dict_value(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return cast(dict[str, object], value)
    return {}


def _list_value(value: object) -> list[object]:
    if isinstance(value, list):
        return cast(list[object], value)
    return []


def _wait_for_job_with_sub_attr(
    api: JobLensAPI,
    key: str,
    value: int,
    timeout: float = 60.0,
) -> dict[str, object]:
    def _check() -> dict[str, object]:
        resp = api.jobs()
        if resp.status_code != 200:
            raise RuntimeError(
                f"GET /joblens/jobs 返回 HTTP {resp.status_code}: {resp.text[:500]}"
            )
        data = _response_json_object(resp)
        for job_value in _list_value(data.get("jobs", [])):
            job = _dict_value(job_value)
            sub_attr = _dict_value(job.get("sub_attr", {}))
            if _int_from_json(sub_attr.get(key, -1)) == value:
                return job
        raise RuntimeError(f"尚未找到 sub_attr.{key}={value} 的作业")

    return cast(
        dict[str, object],
        retry_with_backoff(_check, timeout=timeout, initial=1.0, max_wait=10.0),
    )


def _assert_rule_collector_attached(job: dict[str, object]) -> None:
    collectors = _list_value(job.get("CollectorNames", []))
    assert RULE_COLLECTOR in collectors, (
        "规则引擎模板应将 cpumem_collector 附加到作业；"
        f"实际 CollectorNames={collectors}, job={job}"
    )


def _remote_path_exists(worker: RemoteClient, path: str) -> bool:
    result = _sudo(worker, f"test -e {path}", hide=True, warn=True)
    exited = getattr(result, "exited", 1)
    return exited == 0


def _assert_no_job_with_sub_attr(api: JobLensAPI, key: str, value: int, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = api.jobs()
        if resp.status_code != 200:
            raise RuntimeError(
                f"GET /joblens/jobs 返回 HTTP {resp.status_code}: {resp.text[:500]}"
            )
        data = _response_json_object(resp)
        for job_value in _list_value(data.get("jobs", [])):
            job = _dict_value(job_value)
            sub_attr = _dict_value(job.get("sub_attr", {}))
            if _int_from_json(sub_attr.get(key, -1)) == value:
                raise AssertionError(f"期望规则阻断作业注册，但发现作业: {job}")
        time.sleep(2.0)


def test_condor_rule_engine_template_attaches_rule_collector(
    clean_test_state: None,
    controller: RemoteClient,
    worker: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    assert clean_test_state is None
    try:
        expected_rule_name = _write_rule_engine_template(worker, "condor")
        _restart_joblens_for_rule_engine(worker)
        _wait_for_rule_engine_ready(worker, expected_rule_name)

        cluster_id_text = submit_condor_job(controller, sleep_seconds=180)
        assert cluster_id_text != "unknown", (
            f"condor_submit 提交失败，期望返回 ClusterId，实际 '{cluster_id_text}'"
        )

        job = _wait_for_job_with_sub_attr(
            joblens_api,
            key="cluster_id",
            value=int(cluster_id_text),
        )
        assert "condor" in str(job.get("subtype", "")).lower(), (
            f"期望匹配到 Condor 作业，实际 job={job}"
        )
        _assert_rule_collector_attached(job)
    finally:
        _restore_rule_engine_template(worker)


def test_slurm_rule_engine_template_attaches_rule_collector(
    clean_test_state: None,
    controller: RemoteClient,
    worker: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    assert clean_test_state is None
    try:
        expected_rule_name = _write_rule_engine_template(worker, "slurm")
        _restart_joblens_for_rule_engine(worker)
        _wait_for_rule_engine_ready(worker, expected_rule_name)

        slurm_job_id_text = submit_slurm_job(controller, sleep_seconds=180)
        assert slurm_job_id_text != "unknown", (
            f"sbatch 提交失败，期望返回数字 JobID，实际 '{slurm_job_id_text}'"
        )

        job = _wait_for_job_with_sub_attr(
            joblens_api,
            key="job_id",
            value=int(slurm_job_id_text),
        )
        assert "slurm" in str(job.get("subtype", "")).lower(), (
            f"期望匹配到 Slurm 作业，实际 job={job}"
        )
        _assert_rule_collector_attached(job)
    finally:
        _restore_rule_engine_template(worker)


def test_condor_rule_engine_multiple_rules_all_pass(
    clean_test_state: None,
    controller: RemoteClient,
    worker: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    assert clean_test_state is None
    try:
        _write_rules(
            worker,
            {
                "condor_job_owner.lua": CONDOR_OWNER_RULE_CODE,
                "condor_job_native_id.lua": CONDOR_NATIVE_ID_RULE_CODE,
            },
        )
        _restart_joblens_for_rule_engine(worker)
        _wait_for_rule_engine_ready(
            worker,
            ["integration-condor-owner-rule", "integration-condor-native-id-rule"],
        )

        cluster_id_text = submit_condor_job(controller, sleep_seconds=180)
        assert cluster_id_text != "unknown", (
            f"condor_submit 提交失败，期望返回 ClusterId，实际 '{cluster_id_text}'"
        )
        job = _wait_for_job_with_sub_attr(
            joblens_api,
            key="cluster_id",
            value=int(cluster_id_text),
        )
        _assert_rule_collector_attached(job)
    finally:
        _restore_rule_engine_template(worker)


def test_condor_rule_engine_sandbox_blocks_require_and_load(
    clean_test_state: None,
    controller: RemoteClient,
    worker: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    assert clean_test_state is None
    try:
        _write_rules(
            worker,
            {
                "condor_job_sandbox_require_os.lua": SANDBOX_REQUIRE_OS_RULE_CODE,
                "condor_job_sandbox_load.lua": SANDBOX_LOAD_RULE_CODE,
            },
        )
        _restart_joblens_for_rule_engine(worker)
        _wait_for_rule_engine_ready(
            worker,
            ["integration-sandbox-require-os-rule", "integration-sandbox-load-rule"],
        )

        cluster_id_text = submit_condor_job(controller, sleep_seconds=180)
        assert cluster_id_text != "unknown", (
            f"condor_submit 提交失败，期望返回 ClusterId，实际 '{cluster_id_text}'"
        )
        job = _wait_for_job_with_sub_attr(
            joblens_api,
            key="cluster_id",
            value=int(cluster_id_text),
        )
        _assert_rule_collector_attached(job)
    finally:
        _restore_rule_engine_template(worker)


def test_condor_rule_engine_sandbox_prevents_filesystem_write(
    clean_test_state: None,
    controller: RemoteClient,
    worker: RemoteClient,
    joblens_api: JobLensAPI,
) -> None:
    assert clean_test_state is None
    try:
        worker.sudo(f"rm -f {SANDBOX_WRITE_PROBE}", hide=True, warn=True)
        _write_rules(worker, {"condor_job_sandbox_write.lua": SANDBOX_WRITE_RULE_CODE})
        _restart_joblens_for_rule_engine(worker)
        _wait_for_rule_engine_ready(worker, "integration-sandbox-write-rule")

        cluster_id_text = submit_condor_job(controller, sleep_seconds=180)
        assert cluster_id_text != "unknown", (
            f"condor_submit 提交失败，期望返回 ClusterId，实际 '{cluster_id_text}'"
        )
        _assert_no_job_with_sub_attr(
            joblens_api,
            key="cluster_id",
            value=int(cluster_id_text),
        )
        assert not _remote_path_exists(worker, SANDBOX_WRITE_PROBE), (
            f"Lua 沙箱应阻止 io.open 写文件，但发现 {SANDBOX_WRITE_PROBE} 已被创建"
        )
    finally:
        worker.sudo(f"rm -f {SANDBOX_WRITE_PROBE}", hide=True, warn=True)
        _restore_rule_engine_template(worker)

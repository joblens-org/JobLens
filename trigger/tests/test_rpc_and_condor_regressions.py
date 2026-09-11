import json
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest
from flask import Flask

from trigger.api.routes import register_routes
from trigger.core import tools
from trigger.core.rpc_client import RPCClient, RPCError


def _serve_once(socket_path: str, response: object) -> threading.Thread:
    def server() -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(socket_path)
            listener.listen(1)
            connection, _ = listener.accept()
            with connection:
                while connection.recv(4096):
                    pass
                connection.sendall(json.dumps(response).encode("utf-8"))

    thread = threading.Thread(target=server, daemon=True)
    thread.start()
    deadline = time.monotonic() + 2
    while not Path(socket_path).exists():
        if time.monotonic() > deadline:
            raise RuntimeError(f"socket 未在超时时间内创建: {socket_path}")
        time.sleep(0.01)
    return thread


def _call_with_response(tmp_path: Path, response: object) -> object:
    socket_path = str(tmp_path / f"rpc-{time.monotonic_ns()}.sock")
    thread = _serve_once(socket_path, response)
    try:
        return RPCClient(socket_path, timeout=1).call("test/method")
    finally:
        thread.join(timeout=2)


def _completed_process(stdout: str, stderr: str = "", returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["/usr/bin/ps", "-ax", "-o", "pid=,comm=,args="],
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def test_rpc_client_raises_rpc_error_when_legacy_error_envelope(tmp_path: Path) -> None:
    with pytest.raises(RPCError, match="method not found: es_writer/info"):
        _call_with_response(tmp_path, ["error", "method not found: es_writer/info"])


def test_rpc_client_preserves_legitimate_list_results(tmp_path: Path) -> None:
    result = _call_with_response(tmp_path, [{"name": "collector-a"}, {"name": "collector-b"}])

    assert result == [{"name": "collector-a"}, {"name": "collector-b"}]


def test_unknown_writer_info_returns_404_when_rpc_method_is_missing() -> None:
    class MissingMethodRPCClient:
        def call(self, method: str, params: object | None = None) -> None:
            raise RPCError("method not found: es_writer/info")

    app = Flask(__name__)
    register_routes(app, MissingMethodRPCClient(), None, None, None)

    response = app.test_client().get("/joblens/writers/es_writer/info")

    assert response.status_code == 404


def test_collectors_perf_returns_503_when_rpc_raises_legacy_error() -> None:
    class FailingRPCClient:
        def call(self, method: str, params: object | None = None) -> None:
            raise RPCError("collector perf failed")

    app = Flask(__name__)
    register_routes(app, FailingRPCClient(), None, None, None)

    response = app.test_client().get("/joblens/collectors/perf")

    assert response.status_code == 503


@pytest.mark.parametrize("path", ["/joblens/collectors/perf", "/joblens/writers/perf"])
def test_perf_routes_preserve_abort_status_for_rpc_error_dict(path: str) -> None:
    class ErrorStatusRPCClient:
        def call(self, method: str, params: object | None = None) -> dict[str, str]:
            return {"status": "error", "msg": "perf failed"}

    app = Flask(__name__)
    register_routes(app, ErrorStatusRPCClient(), None, None, None)

    response = app.test_client().get(path)
    body = response.get_data(as_text=True)

    assert response.status_code == 500
    assert "perf failed" in body
    assert "Unexpected error" not in body


def test_use_rpc_opt_falls_back_when_function_list_uses_legacy_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class LegacyFuncListRPCClient:
        def __init__(self, socket_path: str, timeout: float) -> None:
            self.socket_path = socket_path
            self.timeout = timeout

        def get_function_list(self) -> list[str]:
            raise RPCError("method not found: func_list")

    monkeypatch.setattr(tools, "config", {"lens_config": {"rpc_socket_path": "/tmp/rpc.sock", "rpc_timeout": 1}})
    monkeypatch.setattr(tools, "RPCClient", LegacyFuncListRPCClient)

    assert tools.use_rpc_opt() is False


def test_find_pids_by_slot_uses_exact_slot_not_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    ps_output = "\n".join(
        [
            "1170 condor_starter /usr/sbin/condor_starter -f slot1170@worker",
            "117 condor_starter /usr/sbin/condor_starter -f slot117@worker",
        ]
    )

    def fake_subprocess_run(
        args: list[str],
        text: bool,
        stdout: int,
        stderr: int,
    ) -> subprocess.CompletedProcess[str]:
        return _completed_process(ps_output)

    def fake_run(command: str, check: bool = True) -> str:
        if "pstree" in command and "1170" in command:
            return "condor_starter(1170)---wrong(9000)"
        if "pstree" in command and "117" in command:
            return "condor_starter(117)---python(3000)"
        return ps_output

    monkeypatch.setattr(tools.subprocess, "run", fake_subprocess_run)
    monkeypatch.setattr(tools, "run", fake_run)
    monkeypatch.setattr(tools, "_find_docker_container_by_slot", lambda slot, starter_pid: None)

    assert tools.find_pids_by_slot("slot117") == [3000]


def test_find_pids_by_slot_matches_dynamic_slot_exactly(monkeypatch: pytest.MonkeyPatch) -> None:
    ps_output = "\n".join(
        [
            "111 condor_starter /usr/sbin/condor_starter -f slot1@worker",
            "112 condor_starter /usr/sbin/condor_starter -f slot1_1@worker",
        ]
    )

    def fake_subprocess_run(
        args: list[str],
        text: bool,
        stdout: int,
        stderr: int,
    ) -> subprocess.CompletedProcess[str]:
        return _completed_process(ps_output)

    def fake_run(command: str, check: bool = True) -> str:
        return "condor_starter(112)---python(3001)"

    monkeypatch.setattr(tools.subprocess, "run", fake_subprocess_run)
    monkeypatch.setattr(tools, "run", fake_run)
    monkeypatch.setattr(tools, "_find_docker_container_by_slot", lambda slot, starter_pid: None)

    assert tools.find_pids_by_slot("slot1_1") == [3001]


def test_find_pids_by_slot_rejects_unrelated_command_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    ps_output = "999 bash /bin/bash -lc echo condor_starter slot117"

    def fake_subprocess_run(
        args: list[str],
        text: bool,
        stdout: int,
        stderr: int,
    ) -> subprocess.CompletedProcess[str]:
        return _completed_process(ps_output)

    monkeypatch.setattr(tools.subprocess, "run", fake_subprocess_run)

    with pytest.raises(RuntimeError, match="未找到 slot117"):
        tools.find_pids_by_slot("slot117")


def test_find_pids_by_slot_rejects_ambiguous_starters(monkeypatch: pytest.MonkeyPatch) -> None:
    ps_output = "\n".join(
        [
            "117 condor_starter /usr/sbin/condor_starter -f slot117@worker",
            "118 condor_starter /usr/sbin/condor_starter -f slot117@worker",
        ]
    )

    def fake_subprocess_run(
        args: list[str],
        text: bool,
        stdout: int,
        stderr: int,
    ) -> subprocess.CompletedProcess[str]:
        return _completed_process(ps_output)

    monkeypatch.setattr(tools.subprocess, "run", fake_subprocess_run)

    with pytest.raises(RuntimeError, match="对应多个 condor_starter"):
        tools.find_pids_by_slot("slot117")


def test_find_pids_by_slot_ignores_ps_stderr_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    ps_output = "117 condor_starter /usr/sbin/condor_starter -f slot117@worker"

    def fake_subprocess_run(
        args: list[str],
        text: bool,
        stdout: int,
        stderr: int,
    ) -> subprocess.CompletedProcess[str]:
        return _completed_process(ps_output, stderr="warning: ignored namespace")

    def fake_run(command: str, check: bool = True) -> str:
        return "condor_starter(117)---python(3000)"

    monkeypatch.setattr(tools.subprocess, "run", fake_subprocess_run)
    monkeypatch.setattr(tools, "run", fake_run)
    monkeypatch.setattr(tools, "_find_docker_container_by_slot", lambda slot, starter_pid: None)

    assert tools.find_pids_by_slot("slot117") == [3000]


def test_find_pids_by_slot_reports_ps_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_subprocess_run(
        args: list[str],
        text: bool,
        stdout: int,
        stderr: int,
    ) -> subprocess.CompletedProcess[str]:
        return _completed_process("", stderr="ps failed", returncode=1)

    monkeypatch.setattr(tools.subprocess, "run", fake_subprocess_run)

    with pytest.raises(RuntimeError, match="ps 查询 condor_starter 失败: ps failed"):
        tools.find_pids_by_slot("slot117")


def test_find_pids_by_slot_preserves_docker_container_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    ps_output = "117 condor_starter /usr/sbin/condor_starter -f slot117@worker"
    docker_top = "PID COMMAND\n5001 python\n5002 bash"

    def fake_subprocess_run(
        args: list[str],
        text: bool,
        stdout: int,
        stderr: int,
    ) -> subprocess.CompletedProcess[str]:
        return _completed_process(ps_output)

    def fake_run(command: str, check: bool = True) -> str:
        if "pstree" in command:
            return "condor_starter(117)---docker(222)"
        if "docker top" in command:
            return docker_top
        return ps_output

    def fake_cmdline(pid: int) -> str:
        if pid == 222:
            return "/usr/bin/docker start -a I0911"
        return "condor_starter -f slot117@worker"

    monkeypatch.setattr(tools.subprocess, "run", fake_subprocess_run)
    monkeypatch.setattr(tools, "run", fake_run)
    monkeypatch.setattr(tools, "_read_proc_cmdline", fake_cmdline)

    assert tools.find_pids_by_slot("slot117") == [5001, 5002]


def test_find_pids_by_slot_rejects_malformed_starter_pid(monkeypatch: pytest.MonkeyPatch) -> None:
    ps_output = "I0911 condor_starter /usr/sbin/condor_starter -f slot117@worker"

    def fake_subprocess_run(
        args: list[str],
        text: bool,
        stdout: int,
        stderr: int,
    ) -> subprocess.CompletedProcess[str]:
        return _completed_process(ps_output)

    monkeypatch.setattr(tools.subprocess, "run", fake_subprocess_run)

    with pytest.raises(RuntimeError, match="condor_starter"):
        tools.find_pids_by_slot("slot117")

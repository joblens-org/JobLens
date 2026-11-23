"""gunicorn.conf.py 监听地址解析的单测。

bind 的优先级：JOBLENS_TRIGGER_BIND > 配置 service.host/port
（JOBLENS_HOST/JOBLENS_PORT 覆盖配置）> 127.0.0.1:7592。
"""
import importlib.util
import pathlib

import pytest

CONF_PATH = pathlib.Path(__file__).resolve().parents[1] / "gunicorn.conf.py"

ENV_KEYS = (
    "JOBLENS_TRIGGER_BIND",
    "JOBLENS_HOST",
    "JOBLENS_PORT",
    "JOBLENS_TRIGGER_CONFIG_PATH",
)


def _load(tmp_path, monkeypatch, config_text=None, env=None):
    trigger_cfg = tmp_path / "trigger.yaml"
    if config_text is not None:
        trigger_cfg.write_text(config_text, encoding="utf-8")
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("JOBLENS_TRIGGER_CONFIG_PATH", str(trigger_cfg))
    for key, value in (env or {}).items():
        monkeypatch.setenv(key, value)
    spec = importlib.util.spec_from_file_location("joblens_gunicorn_conf_under_test", CONF_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bind_comes_from_config(tmp_path, monkeypatch):
    module = _load(tmp_path, monkeypatch, "service:\n  host: 0.0.0.0\n  port: 7592\n")
    assert module.bind == "0.0.0.0:7592"


def test_bind_defaults_to_loopback_without_config(tmp_path, monkeypatch):
    module = _load(tmp_path, monkeypatch)
    assert module.bind == "127.0.0.1:7592"


def test_bind_env_override_wins(tmp_path, monkeypatch):
    module = _load(
        tmp_path,
        monkeypatch,
        "service:\n  host: 0.0.0.0\n  port: 7592\n",
        {"JOBLENS_TRIGGER_BIND": "10.0.0.5:8000"},
    )
    assert module.bind == "10.0.0.5:8000"


def test_host_port_env_override_config(tmp_path, monkeypatch):
    module = _load(
        tmp_path,
        monkeypatch,
        "service:\n  host: 127.0.0.1\n  port: 7592\n",
        {"JOBLENS_HOST": "0.0.0.0", "JOBLENS_PORT": "9000"},
    )
    assert module.bind == "0.0.0.0:9000"


def test_malformed_port_falls_back(tmp_path, monkeypatch):
    module = _load(tmp_path, monkeypatch, "service:\n  host: 0.0.0.0\n  port: not-a-port\n")
    assert module.bind == "0.0.0.0:7592"


def test_loglevel_still_reads_config(tmp_path, monkeypatch):
    module = _load(tmp_path, monkeypatch, "logging:\n  level: debug\n")
    assert module.loglevel == "debug"

#   Copyright 2026 - 2026 wzycc
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
"""Etcd TLS/认证配置回归测试（不建立真实连接）。"""

import pytest

from trigger.core import etcd_client as mod
from trigger.core.etcd_client import EtcdClient


class FakeEtcd:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def get(self, key):
        return None, None


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in list(mod.TLS_ENV.values()) + [mod.TLS_REQUIRED_ENV]:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def captured(monkeypatch):
    calls = []

    def fake_client(**kwargs):
        calls.append(kwargs)
        return FakeEtcd(**kwargs)

    monkeypatch.setattr(mod.etcd3, "client", fake_client)
    return calls


def test_tls_required_by_default_blocks_plaintext(captured):
    client = EtcdClient(host="127.0.0.1", port=2379)
    assert client.is_connected() is False
    assert captured == []


def test_missing_ca_file_blocks_connection(captured, tmp_path):
    missing = tmp_path / "missing-ca.pem"
    client = EtcdClient(host="127.0.0.1", port=2379, ca_cert=str(missing))
    assert client.is_connected() is False
    assert captured == []


def test_cert_pair_must_be_complete(captured, tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("ca")
    client = EtcdClient(
        host="127.0.0.1", port=2379,
        ca_cert=str(ca), cert_key=str(tmp_path / "client.key"),
    )
    assert client.is_connected() is False
    assert captured == []


def test_tls_off_allows_plaintext_with_explicit_opt_out(captured, monkeypatch):
    monkeypatch.setenv(mod.TLS_REQUIRED_ENV, "off")
    client = EtcdClient(host="127.0.0.1", port=2379)
    assert client.is_connected() is True
    assert captured[0]["host"] == "127.0.0.1"
    assert "ca_cert" not in captured[0]


def test_tls_material_and_credentials_are_forwarded(captured, tmp_path, monkeypatch):
    ca = tmp_path / "ca.pem"
    key = tmp_path / "client.key"
    cert = tmp_path / "client.pem"
    for path in (ca, key, cert):
        path.write_text("x")
    monkeypatch.setenv(mod.TLS_ENV["user"], "joblens")
    monkeypatch.setenv(mod.TLS_ENV["password"], "secret")

    client = EtcdClient(
        host="etcd.example", port=2379,
        ca_cert=str(ca), cert_key=str(key), cert_cert=str(cert),
    )

    assert client.is_connected() is True
    kwargs = captured[0]
    assert kwargs["ca_cert"] == str(ca)
    assert kwargs["cert_key"] == str(key)
    assert kwargs["cert_cert"] == str(cert)
    assert kwargs["user"] == "joblens"
    assert kwargs["password"] == "secret"

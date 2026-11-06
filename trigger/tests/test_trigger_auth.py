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
"""Trigger HTTP 鉴权回归测试。"""

from flask import Flask
import pytest

from trigger.api import auth
from trigger.api.routes import register_routes

TOKEN = "test-token-do-not-use"


def _make_app() -> Flask:
    app = Flask(__name__)
    register_routes(app, None, None, None, None)
    return app


@pytest.fixture
def auth_on(monkeypatch):
    monkeypatch.setenv(auth.AUTH_ENV, "on")
    monkeypatch.setenv(auth.TOKEN_ENV, TOKEN)


def test_health_is_public(auth_on):
    client = _make_app().test_client()
    assert client.get("/trigger/health").status_code == 200


def test_write_requires_token(auth_on):
    client = _make_app().test_client()
    response = client.post("/joblens/job", json={})
    assert response.status_code == 401
    assert response.get_json()["code"] == "unauthorized"


def test_sensitive_read_requires_token(auth_on):
    client = _make_app().test_client()
    response = client.get("/joblens/jobs")
    assert response.status_code == 401


def test_wrong_token_is_forbidden(auth_on):
    client = _make_app().test_client()
    response = client.post(
        "/joblens/job",
        json={},
        headers={"Authorization": "Bearer wrong"},
    )
    assert response.status_code == 403
    assert response.get_json()["code"] == "forbidden"


def test_correct_token_passes_auth(auth_on):
    client = _make_app().test_client()
    response = client.post(
        "/joblens/job",
        json={},
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    assert response.status_code not in (401, 403)


def test_x_joblens_token_header_is_accepted(auth_on):
    client = _make_app().test_client()
    response = client.post(
        "/joblens/job",
        json={},
        headers={"X-JobLens-Token": TOKEN},
    )
    assert response.status_code not in (401, 403)


def test_unconfigured_token_fails_closed(monkeypatch):
    monkeypatch.setenv(auth.AUTH_ENV, "on")
    monkeypatch.delenv(auth.TOKEN_ENV, raising=False)
    client = _make_app().test_client()

    assert client.get("/trigger/health").status_code == 200
    response = client.post("/joblens/job", json={})
    assert response.status_code == 503
    assert response.get_json()["code"] == "auth_unconfigured"


def test_explicit_auth_off_allows_request(monkeypatch):
    monkeypatch.setenv(auth.AUTH_ENV, "off")
    monkeypatch.delenv(auth.TOKEN_ENV, raising=False)
    client = _make_app().test_client()
    response = client.post("/joblens/job", json={})
    assert response.status_code not in (401, 403)

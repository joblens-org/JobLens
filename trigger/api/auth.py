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
"""
Trigger HTTP 鉴权。

威胁模型：JobLens Trigger 以 root 运行在多用户登录节点上，任意本地用户都能
访问 127.0.0.1:7592，因此"只监听回环"不是安全边界，必须有令牌。

策略
----
- 健康/指标端点保持匿名，供 systemd、探针与 Prometheus 抓取：
  ``/``、``/trigger/health``、``/joblens/healthy``、``/metrics``。
- 其余端点（包括读取作业/命令行、规则、配置、写操作）都要求令牌。
- 令牌来自 ``JOBLENS_TRIGGER_TOKEN``；**未配置时保护端点直接失败**（503），
  绝不回退为放行。
- ``JOBLENS_TRIGGER_AUTH=off`` 是显式的开发/测试开关，会在启动时打 WARNING。
  生产环境不要设置。
- 请求头支持 ``Authorization: Bearer <token>`` 或 ``X-JobLens-Token``。
"""

from __future__ import annotations

import hmac
import logging
import os
from typing import Optional, Tuple

from flask import Flask, Response, jsonify, request

logger = logging.getLogger(__name__)

TOKEN_ENV = "JOBLENS_TRIGGER_TOKEN"
AUTH_ENV = "JOBLENS_TRIGGER_AUTH"

# 归一化后比较（去掉末尾斜杠）
PUBLIC_PATHS = frozenset({
    "/",
    "/metrics",
    "/trigger/health",
    "/joblens/healthy",
})

_FALSE_VALUES = {"0", "false", "no", "off", "disabled"}


def _auth_disabled() -> bool:
    return os.environ.get(AUTH_ENV, "on").strip().lower() in _FALSE_VALUES


def _expected_token() -> str:
    return os.environ.get(TOKEN_ENV, "").strip()


def _supplied_token() -> str:
    header = request.headers.get("Authorization", "") or ""
    if header.lower().startswith("bearer "):
        return header[7:].strip()
    return (request.headers.get("X-JobLens-Token", "") or "").strip()


def _normalized_path() -> str:
    path = request.path or "/"
    return path.rstrip("/") or "/"


def _check() -> Optional[Tuple[Response, int]]:
    if _auth_disabled():
        return None
    if request.method == "OPTIONS":
        return None
    if _normalized_path() in PUBLIC_PATHS:
        return None

    expected = _expected_token()
    if not expected:
        # fail closed：未配置令牌时不放行，也不静默降级
        logger.error(
            "Trigger auth is enabled but %s is not set; rejecting %s %s",
            TOKEN_ENV, request.method, request.path,
        )
        return jsonify({
            "error": "trigger auth token is not configured",
            "code": "auth_unconfigured",
        }), 503

    supplied = _supplied_token()
    if not supplied:
        return jsonify({
            "error": "authentication required",
            "code": "unauthorized",
        }), 401
    if not hmac.compare_digest(supplied, expected):
        logger.warning(
            "Rejected request with invalid token: %s %s from %s",
            request.method, request.path, request.remote_addr,
        )
        return jsonify({
            "error": "invalid token",
            "code": "forbidden",
        }), 403
    return None


def install_auth(app: Flask) -> None:
    """在 Flask 应用上安装统一鉴权。必须在注册业务路由之前调用。"""
    if _auth_disabled():
        logger.warning(
            "Trigger HTTP auth is DISABLED via %s; do not use this outside tests",
            AUTH_ENV,
        )
    elif not _expected_token():
        logger.warning(
            "Trigger HTTP auth is enabled but %s is empty: protected endpoints "
            "will return 503 until a token is configured", TOKEN_ENV,
        )
    else:
        logger.info("Trigger HTTP auth enabled (token from %s)", TOKEN_ENV)

    @app.before_request
    def _enforce_auth():
        return _check()

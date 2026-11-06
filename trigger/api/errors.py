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
HTTP 错误响应收敛。

路由里大量使用 ``abort(500, description=...)`` / ``abort(503, ...)`` 把
RPC 错误、文件路径、内部异常直接回显给调用方。5xx 不再返回这些细节：
统一返回 ``{"error": "internal error", "request_id": ...}``，细节只写日志，
便于排障又不泄露内部结构。
"""

from __future__ import annotations

import logging
import uuid

from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException

logger = logging.getLogger(__name__)


def install_error_handlers(app: Flask) -> None:
    @app.errorhandler(HTTPException)
    def _handle_http_exception(exc: HTTPException):
        status = exc.code or 500
        if status < 500:
            return exc

        request_id = uuid.uuid4().hex[:12]
        logger.error(
            "HTTP %s on %s %s (request_id=%s): %s",
            status, request.method, request.path, request_id, exc.description,
        )
        response = jsonify({
            "error": "internal error",
            "request_id": request_id,
        })
        response.status_code = status
        response.headers["X-Request-Id"] = request_id
        return response

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
"""trigger/tests 的公共夹具。

Trigger 默认开启 HTTP 鉴权；既有路由测试关注的是转发/错误映射，不是鉴权，
因此这里统一显式关闭。鉴权自身的测试在 test_trigger_auth.py 里单独打开。
"""

import pytest


@pytest.fixture(autouse=True)
def _disable_trigger_auth_for_route_tests(monkeypatch):
    monkeypatch.setenv("JOBLENS_TRIGGER_AUTH", "off")
    monkeypatch.delenv("JOBLENS_TRIGGER_TOKEN", raising=False)

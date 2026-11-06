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
"""规则文件路径校验：etcd/RPC 提供的规则名不得逃出规则目录。"""

from trigger.core.rule_manager import RuleManager


class FakeEtcd:
    def get(self, key):
        return None, None


def _manager(tmp_path):
    return RuleManager(
        etcd_client=FakeEtcd(),
        etcd_workdir="/work",
        etcd_role_path="/work/role",
        local_rules_dir=str(tmp_path / "rules"),
        etcd_priority=False,
    )


def test_valid_rule_name_stays_inside_rules_dir(tmp_path):
    manager = _manager(tmp_path)
    assert manager.update_local_rule("ok.lua", b"return {}") is True
    assert (tmp_path / "rules" / "ok.lua").exists()


def test_parent_traversal_is_rejected(tmp_path):
    manager = _manager(tmp_path)
    assert manager.update_local_rule("../escape.lua", b"x") is False
    assert not (tmp_path / "escape.lua").exists()
    assert manager.get_local_rule_path("../escape.lua") is None
    assert manager.get_local_rules_content("../escape.lua") == b""


def test_absolute_path_is_rejected(tmp_path):
    manager = _manager(tmp_path)
    assert manager.update_local_rule("/etc/passwd", b"x") is False
    assert manager.get_local_rule_path("/etc/passwd") is None


def test_nested_path_is_rejected(tmp_path):
    manager = _manager(tmp_path)
    assert manager.update_local_rule("a/b.lua", b"x") is False
    assert not (tmp_path / "rules" / "a").exists()

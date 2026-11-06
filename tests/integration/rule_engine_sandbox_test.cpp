/* Copyright 2026 - 2026 wzycc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

// RuleEngine 沙箱回归：不允许文件读写、动态加载、进程执行与无限循环。

#include "rule_engine/rule_engine.hpp"
#include "rule_engine/rule_path.hpp"
#include "core/collector_type.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include <unistd.h>

namespace fs = std::filesystem;

namespace {

int g_failures = 0;

#define CHECK(cond)                                                           \
    do {                                                                      \
        if (!(cond)) {                                                        \
            std::cerr << "FAIL " << __LINE__ << ": " #cond << "\n";           \
            ++g_failures;                                                     \
        }                                                                     \
    } while (0)

void write_file(const fs::path& path, const std::string& content) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out << content;
}

std::string rule_with_condition(const std::string& name, const std::string& body) {
    return "return { name = \"" + name + "\", description = \"t\", priority = 1, "
           "condition = function(job) " + body + " end }\n";
}

} // namespace

int main() {
    const fs::path root = fs::temp_directory_path() /
        ("joblens-rule-sandbox-" + std::to_string(::getpid()));
    fs::remove_all(root);
    fs::create_directories(root);

    Job job{};
    job.JobID = 42;
    job.subtype = JobSubType::Common;

    // 1) 正常规则仍可用
    write_file(root / "good.lua", rule_with_condition(
        "good",
        "return { passed = job.JobID == 42, collectors = {\"cpumem_collector\"} }"));
    // 2) io 不存在
    write_file(root / "io_write.lua",
        "return { name = \"io_write\", condition = function(job)\n"
        "  local f = io.open(\"" + (root / "pwned").string() + "\", \"w\")\n"
        "  if f ~= nil then f:write(\"x\") f:close() end\n"
        "  return { passed = false, collectors = {} }\n"
        "end }\n");
    // 3) require/package 不存在
    write_file(root / "require_os.lua", rule_with_condition(
        "require_os",
        "local ok = pcall(function() return require(\"os\") end)\n"
        "return { passed = not ok, collectors = {} }"));
    // 4) load 不存在
    write_file(root / "load_code.lua", rule_with_condition(
        "load_code",
        "local ok = pcall(function() return load(\"return true\") end)\n"
        "return { passed = not ok, collectors = {} }"));
    // 5) 条件里的无限循环必须被预算中断
    write_file(root / "loop_condition.lua", rule_with_condition(
        "loop_condition", "while true do end"));
    // 6) 加载期的无限循环同样必须被中断
    write_file(root / "loop_load.lua",
        "while true do end\nreturn { name = \"loop_load\", condition = function(job) "
        "return { passed = true, collectors = {} } end }\n");
    // 7) 超大规则文件必须被拒绝
    {
        std::string big = "return { name = \"too_big\", condition = function(job) "
                          "return { passed = true, collectors = {} } end }\n";
        big.append(300 * 1024, ' ');
        write_file(root / "too_big.lua", big);
    }

    RuleEngine engine;
    const auto t0 = std::chrono::steady_clock::now();
    const int loaded = engine.load_rules_from_directory(root.string());
    const auto load_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();

    // 只应加载 good / io_write / require_os / load_code / loop_condition
    // （loop_condition 的循环在 condition 内，加载期不执行）
    CHECK(loaded == 5);
    CHECK(load_ms < 5000);

    {
        auto [passed, collectors] = engine.evaluate("good.lua", job);
        CHECK(passed);
        CHECK(collectors.size() == 1 && collectors[0] == "cpumem_collector");
    }
    {
        auto [passed, _] = engine.evaluate("io_write.lua", job);
        CHECK(!passed);
        CHECK(!fs::exists(root / "pwned"));
    }
    {
        auto [passed, _] = engine.evaluate("require_os.lua", job);
        CHECK(passed);
        auto [passed2, __] = engine.evaluate("load_code.lua", job);
        CHECK(passed2);
    }
    {
        const auto start = std::chrono::steady_clock::now();
        auto [passed, _] = engine.evaluate("loop_condition.lua", job);
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        CHECK(!passed);
        CHECK(elapsed < 5000);
    }

    // 8) 规则目录之外的文件不得被加载
    const fs::path outside = fs::temp_directory_path() /
        ("joblens-rule-outside-" + std::to_string(::getpid()) + ".lua");
    write_file(outside, rule_with_condition(
        "outside", "return { passed = true, collectors = {} }"));
    CHECK(!engine.load_rule(outside.string()));

    // 9) 规则名/路径校验（与 RulesManager 内部使用的是同一组函数）
    {
        using namespace joblens::rules;
        CHECK(filename_ok("condor_rule-1.lua"));
        CHECK(!filename_ok("../escape"));
        CHECK(!filename_ok("/etc/passwd"));
        CHECK(!filename_ok("bad\nname"));
        CHECK(!filename_ok("a/b"));
        CHECK(!filename_ok(".."));
        CHECK(filename_ok("ok.lua"));

        const fs::path inside = root / "ok.lua";
        write_file(inside, "return {}\n");
        CHECK(path_within(inside, root));
        CHECK(!path_within(outside, root));
        CHECK(!path_within(root / ".." / "escape.lua", root));
    }

    fs::remove(outside);
    fs::remove_all(root);

    if (g_failures != 0) {
        std::cerr << g_failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "rule_engine_sandbox_test: OK\n";
    return 0;
}

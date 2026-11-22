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
// group_fields_test：组字段（sub_attr.groups）的解析 / 序列化兼容 / 视图 / 探测回归。
// 通过 run_group_fields_test.sh 以“换 main.cpp.o 重链生产对象”的方式运行。
#include "common/classad_decode.hpp"
#include "common/condor_job.hpp"
#include "common/slurm_job.hpp"
#include "core/collector_type.h"
#include "core/group_probe_stats.hpp"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <grp.h>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

static int g_failed = 0;
#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            ++g_failed;                                                      \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);  \
        }                                                                    \
    } while (0)

// ---------- 1. ClassAd 解码与组值校验 ----------
static void test_classad_decode() {
    using namespace ClassAdValue;
    CHECK(decode_string("\"JUNO.juno.default\"") == "JUNO.juno.default");
    CHECK(decode_string("  \"BES.physics.long\" \n") == "BES.physics.long");
    CHECK(decode_string("\"a\\\"b\"") == "a\"b");
    CHECK(decode_string("\"a\\\\b\"") == "a\\b");
    CHECK(decode_string("undefined") == "");
    CHECK(decode_string("UNDEFINED") == "");
    CHECK(decode_string("") == "");
    CHECK(decode_string("group_physics") == "group_physics");  // 无引号原样

    CHECK(valid_group_value("JUNO.juno.default", kGroupValueMaxLen));
    CHECK(valid_group_value("", kGroupValueMaxLen));  // 空串合法（“无值”由调用方按 empty 统计）
    CHECK(!valid_group_value(std::string(257, 'a'), kGroupValueMaxLen));
    CHECK(!valid_group_value("bad\nvalue", kGroupValueMaxLen));
    CHECK(!valid_group_value("bad\x01value", kGroupValueMaxLen));
    CHECK(valid_group_value(std::string(256, 'g'), kGroupValueMaxLen));
}

// ---------- 2. 序列化：新字段 round-trip、旧记录兼容、字段清单对齐 ----------
static void test_condor_attr_json() {
    CondorJobAttr a;
    a.auto_update_child = true;
    a.starter_pid = 4242;
    a.cluster_id = 24193;
    a.proc_id = 0;
    a.scheduler_host = "schedd07.ihep.ac.cn";
    a.scheduler_name = "scheduler@schedd07.ihep.ac.cn";
    a.slots_cgroup_path = "/sys/fs/cgroup/slot1";
    a.owner = "alice";
    a.job_ad_path = "/var/lib/condor/execute/dir_x/.job.ad";
    a.collector_host = "cm01.ihep.ac.cn";
    a.acct_group = "JUNO.juno.long";
    a.unix_group = "juno";

    nlohmann::json j = a;
    // 字段清单与 reflection() 对齐（12 项）——新增字段漏写 to_json/reflection 在此暴露
    CHECK(j.size() == 12);
    CHECK(j.contains("scheduler_name"));  // 行为变化锁定：旧宏漏写，手写序列化起入库
    CHECK(j.contains("acct_group"));
    CHECK(j.contains("unix_group"));

    auto back = j.get<CondorJobAttr>();
    CHECK(back.acct_group == "JUNO.juno.long");
    CHECK(back.unix_group == "juno");
    CHECK(back.scheduler_name == "scheduler@schedd07.ihep.ac.cn");
    CHECK(back.starter_pid == 4242);

    // 旧记录（无组字段、无 scheduler_name，宏时代格式）→ 容错读取，不抛异常
    const char* legacy = R"({
        "auto_update_child": true, "starter_pid": 100, "cluster_id": 123, "proc_id": 4,
        "scheduler_host": "h", "slots_cgroup_path": "/c", "owner": "bob",
        "job_ad_path": "/a", "collector_host": "cm"
    })";
    auto old = nlohmann::json::parse(legacy).get<CondorJobAttr>();
    CHECK(old.acct_group.empty());
    CHECK(old.unix_group.empty());
    CHECK(old.scheduler_name.empty());
    CHECK(old.cluster_id == 123);

    // null 值不炸（contains && !is_null 分支）
    auto nullable = nlohmann::json::parse(legacy);
    nullable["acct_group"] = nullptr;
    nullable["unix_group"] = nullptr;
    auto with_null = nullable.get<CondorJobAttr>();
    CHECK(with_null.acct_group.empty());
}

static void test_slurm_attr_json() {
    SlurmJobAttr a;
    a.stepd_pid = 99;
    a.job_id = 648949;
    a.step_id = 0;
    a.clusterName = "slurm_cluster_2";
    a.cgroup_path = "/sys/fs/cgroup/slurm/job648949";
    a.user = "carol";
    a.job_name = "Sim01";
    a.partition = "heps";
    a.acct_group = "gpupwa";
    a.unix_group = "physics";
    a.unix_gid = 290;

    nlohmann::json j = a;
    CHECK(j.size() == 12);
    CHECK(j.contains("clusterName"));  // 行为变化锁定：旧 reflection 漏写
    CHECK(j.contains("unix_gid"));
    auto back = j.get<SlurmJobAttr>();
    CHECK(back.acct_group == "gpupwa");
    CHECK(back.unix_gid == 290);

    // 旧记录（无组字段）
    auto old = nlohmann::json::parse(R"({
        "auto_update_child": true, "stepd_pid": 7, "job_id": 42, "step_id": 0,
        "cgroup_path": "/c", "user": "dave", "job_name": "n", "partition": "cpu",
        "clusterName": "cl"
    })").get<SlurmJobAttr>();
    CHECK(old.acct_group.empty());
    CHECK(old.unix_group.empty());
    CHECK(old.unix_gid == -1);

    // 大 gid（uint32 上限）不溢出、不与 -1 哨兵混淆
    j["unix_gid"] = 4294967294LL;
    auto big = j.get<SlurmJobAttr>();
    CHECK(big.unix_gid == 4294967294LL);
}

// ---------- 3. getGroups 访问器与对外 JSON（组只在 sub_attr 下，无顶层 groups） ----------
static void test_groups_view() {
    Job jc;
    jc.subtype = JobSubType::Condor;
    jc.sub_attr = CondorJobAttr{.acct_group = "JUNO.juno.default", .unix_group = ""};
    auto gc = getGroups(jc);
    CHECK(gc.acct_group == "JUNO.juno.default");
    CHECK(gc.unix_gid == -1);
    auto dj = dump_job(jc);
    CHECK(!dj.contains("groups"));  // 不再输出顶层 groups
    CHECK(dj["sub_attr"]["acct_group"] == "JUNO.juno.default");
    CHECK(!dj["sub_attr"].contains("unix_gid"));  // Condor 属性没有 gid 字段
    auto wj = job_to_json(jc);
    CHECK(!wj.contains("groups"));
    CHECK(wj["sub_attr"]["acct_group"] == "JUNO.juno.default");

    Job js;
    js.subtype = JobSubType::Slurm;
    js.sub_attr = SlurmJobAttr{.acct_group = "gpupwa", .unix_group = "physics", .unix_gid = 290};
    auto gs = getGroups(js);
    CHECK(gs.unix_group == "physics");
    CHECK(dump_job(js)["sub_attr"]["unix_gid"] == 290);
    // gid 未知的 Slurm 作业：sub_attr 里是 -1（数值，不输出 null）
    Job jn;
    jn.subtype = JobSubType::Slurm;
    jn.sub_attr = SlurmJobAttr{};
    CHECK(dump_job(jn)["sub_attr"]["unix_gid"] == -1);

    Job jm;
    jm.subtype = JobSubType::Common;
    jm.sub_attr = CommonJobAttr{};
    CHECK(getGroups(jm).empty());
    CHECK(!dump_job(jm).contains("groups"));
}

// ---------- 4. GroupProbeStats 计数与 JSON 形状 ----------
static void test_probe_stats() {
    auto& st = GroupProbeStats::instance();
    auto before = st.to_json();
    uint64_t a0 = before["by_source"]["condor_ad"]["attempts"].get<uint64_t>();
    uint64_t ne0 = before["by_source"]["condor_ad"]["non_empty"].get<uint64_t>();
    uint64_t seg0 = before["acct_group_segments"].value("3", 0);

    st.record_attempt(GroupProbeStats::Source::CondorAd);
    st.record_non_empty(GroupProbeStats::Source::CondorAd, "JUNO.juno.default");
    st.record_skipped(GroupProbeStats::Source::SlurmNss);
    st.record_truncated();

    auto after = st.to_json();
    CHECK(after["by_source"]["condor_ad"]["attempts"].get<uint64_t>() == a0 + 1);
    CHECK(after["by_source"]["condor_ad"]["non_empty"].get<uint64_t>() == ne0 + 1);
    CHECK(after["acct_group_segments"].value("3", 0) == seg0 + 1);
    CHECK(after["by_source"]["slurm_nss"]["skipped"].get<uint64_t>() >= 1);
    CHECK(after["truncated"].get<uint64_t>() >= 1);
    // NSS 来源的 non_empty 不进段数分布
    st.record_non_empty(GroupProbeStats::Source::SlurmNss, "physics");
    CHECK(GroupProbeStats::instance().to_json()["acct_group_segments"].value("1", 0) == 0);
}

// ---------- 5. Condor 探测：fake ad 文件 + 子进程 env ----------
static pid_t spawn_child_with_env(const std::vector<std::pair<std::string, std::string>>& envs) {
    for (auto& [k, v] : envs) setenv(k.c_str(), v.c_str(), 1);
    pid_t p = fork();
    if (p == 0) {
        execlp("sleep", "sleep", "10", (char*)nullptr);
        _exit(127);
    }
    for (auto& [k, v] : envs) unsetenv(k.c_str());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));  // 等 exec 完成
    return p;
}

static void write_fake_ad(const std::string& path) {
    std::ofstream f(path);
    f << "GlobalJobId = \"scheduler@inkcm.ihep.ac.cn#24193.0#1775723895\"\n";
    f << "Owner = \"liuqishan\"\n";
    f << "AcctGroup = \"JUNO.juno.long\"\n";
    f << "AccountingGroup = \"JUNO.juno.long.liuqishan\"\n";  // 应被忽略（派生物）
    f << "PrimaryUnixGroup = \"juno\"\n";
}

static void test_condor_probe(const std::string& work) {
    const std::string ad = work + "/group_fields_fake.job.ad";
    write_fake_ad(ad);

    // 5.1 正常路径：子进程 env 指向 ad 文件（starter 缺失 → 用户路径 + 来源校验）
    pid_t child = spawn_child_with_env({{"_CONDOR_JOB_AD", ad}});
    Job job;
    job.subtype = JobSubType::Condor;
    job.JobID = 24193;
    set_pids(job, {child});
    job.JobNativeID = "24193.0";
    job.sub_attr = CondorJobAttr{.starter_pid = 0, .cluster_id = 24193, .proc_id = 0};
    CHECK(CondorJob::probe_groups(job));
    CHECK(job.group_probed);
    auto& attr = std::get<CondorJobAttr>(job.sub_attr);
    CHECK(attr.acct_group == "JUNO.juno.long");   // 解码去引号；AccountingGroup 未被采集
    CHECK(attr.unix_group == "juno");
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);

    // 5.2 GlobalJobId 串号：NativeJobID 不一致 → 整组丢弃 + 计失败 + 不置 probed
    child = spawn_child_with_env({{"_CONDOR_JOB_AD", ad}});
    Job bad;
    bad.subtype = JobSubType::Condor;
    bad.JobID = 999;
    set_pids(bad, {child});
    bad.JobNativeID = "999.9";
    bad.sub_attr = CondorJobAttr{.starter_pid = 0};
    CHECK(!CondorJob::probe_groups(bad));
    CHECK(!bad.group_probed);
    CHECK(bad.group_probe_attempts == 1);
    CHECK(std::get<CondorJobAttr>(bad.sub_attr).acct_group.empty());
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);

    // 5.3 ad 路径为符号链接 → 拒绝
    const std::string link = work + "/group_fields_fake.link";
    unlink(link.c_str());
    symlink(ad.c_str(), link.c_str());
    child = spawn_child_with_env({{"_CONDOR_JOB_AD", link}});
    Job sym;
    sym.subtype = JobSubType::Condor;
    set_pids(sym, {child});
    sym.JobNativeID = "24193.0";
    sym.sub_attr = CondorJobAttr{.starter_pid = 0};
    CHECK(!CondorJob::probe_groups(sym));
    CHECK(std::get<CondorJobAttr>(sym.sub_attr).acct_group.empty());
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);
    unlink(link.c_str());
    unlink(ad.c_str());
}

// ---------- 6. Slurm 探测：子进程 env + NSS 解析 ----------
static void test_slurm_probe() {
    gid_t my_gid = getgid();
    struct group* g = getgrgid(my_gid);
    std::string expect_name = g && g->gr_name ? g->gr_name : "";

    pid_t child = spawn_child_with_env({
        {"SLURM_JOB_ACCOUNT", "gpupwa"},
        {"SLURM_JOB_GID", std::to_string((long long)my_gid)},
    });
    Job job;
    job.subtype = JobSubType::Slurm;
    job.JobID = 648949;
    set_pids(job, {child});
    job.JobNativeID = "648949";
    job.sub_attr = SlurmJobAttr{.stepd_pid = 0, .job_id = 648949};
    CHECK(SlurmJob::probe_groups(job));
    CHECK(job.group_probed);
    auto& attr = std::get<SlurmJobAttr>(job.sub_attr);
    CHECK(attr.acct_group == "gpupwa");
    CHECK(attr.unix_gid == (int64_t)my_gid);
    CHECK(attr.unix_group == expect_name);  // NSS 解析本组 gid
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);

    // 坏值：account 含控制字符 → 拒绝并计失败
    child = spawn_child_with_env({{"SLURM_JOB_ACCOUNT", "bad\ngrp"}});
    Job bad;
    bad.subtype = JobSubType::Slurm;
    set_pids(bad, {child});
    bad.sub_attr = SlurmJobAttr{};
    CHECK(!SlurmJob::probe_groups(bad));
    CHECK(!bad.group_probed);
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);

    // gid 解析边界：env 无 SLURM_JOB_GID → -1（仅在测试环境未注入该变量时断言）
    if (getenv("SLURM_JOB_GID") == nullptr) {
        CHECK(SlurmJob::getGid(getpid()) == -1);
    }
}

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    test_classad_decode();
    test_condor_attr_json();
    test_slurm_attr_json();
    test_groups_view();
    test_probe_stats();
    test_condor_probe(argv[1]);
    test_slurm_probe();

    if (g_failed == 0) {
        printf("group_fields_test: all passed\n");
        return 0;
    }
    printf("group_fields_test: %d check(s) failed\n", g_failed);
    return 1;
}

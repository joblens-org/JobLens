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
 * limitations under the License.
 *
 * PowerCollector — eBPF + RAPL 逐作业能耗归因引擎
 *
 * 架构:
 *   内核态: eBPF钩住sched_switch → 纳秒级per-(pid,cpu)运行时间追踪
 *   用户态: 读RAPL ΔE_pkg + sysfs CPU频率 + eBPF任务运行时间
 *          → E_job = ΔE_pkg × Σ(time_c × freq_c)_job / Σ(time_c × freq_c)_all
 *
 * 归因公式:
 *   分母 total_weighted = Δt × 10^9 × Σ_cpu freq_mhz(cpu)
 *                      (所有CPU核心在ΔT内的理论最大频率加权时间)
 *   分子 对每个(pid, cpu): weighted_ns = runtime_ns × freq_mhz(cpu)
 *         → 按pid2job分组求和 → job_weighted = Σ weighted_ns
 *   归因 E_job = ΔE_pkg × job_weighted / total_weighted
 *
 * Collector是System-scoped: 追踪本机所有PID，通过pid2job BPF map
 * 将能耗归因到JobLens管理的作业。未被任何作业覆盖的CPU时间能耗
 * 归入system_overhead。
 */
#pragma once
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include "core/collector_type.h"
#include "icollector.h"
#include "ebpf/trace_sched_runtime.h"

#include <bpf/bpf.h>
#include <bpf/libbpf.h>

#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <fstream>

/* ── 每个PID的能耗归因结果 (按Job分组前的中间层) ─────────────────────── */
struct PowerPerPid {
    pid_t pid;                  // 进程ID (线程级别)
    u64   tgid;                 // 线程组ID (进程级别)
    u64   runtime_ns_total;     // 所有CPU核心上的累计运行时间 (ns)
    double weighted_ns_total;   // Σ_cpu (runtime_ns × freq_mhz)
    double energy_j;            // 归因到这个PID的能耗 (焦耳)
    double cpu_pct;             // 相当于单核的百分比 (可>100%, 表示多核)
};

/* ── 每个Job的能耗归因结果 (输出单位) ───────────────────────────────── */
struct PowerPerJob {
    uint64_t job_id;            // JobLens内部JobID
    std::string native_job_id;  // 调度器原生作业ID (如Condor "123.0")
    double energy_j;            // 归因到这个Job的总能耗 (J)
    double weighted_ns_total;   // 这个Job的频率加权时间和
    std::vector<PowerPerPid> pids; // 每个进程的详细能耗
};

/* ── 一次完整采集的快照 ─────────────────────────────────────────────── */
struct PowerSnapshot {
    std::chrono::system_clock::time_point ts; // 采集时间戳
    double interval_s;           // 距上次采集的墙钟间隔 (秒)
    double delta_rapl_uj;        // RAPL能耗增量 (微焦耳)
    double delta_rapl_j;         // RAPL能耗增量 (焦耳)
    int    core_count;           // CPU核心数
    std::vector<double> core_freqs_mhz;   // 每核心瞬时频率 (MHz)
    double total_weighted_ns;    // 归因分母 = Δt × Σ freq
    double system_overhead_j;    // 未归因到任何作业的能耗 (idle CPU + 系统进程)
    std::vector<PowerPerJob> jobs; // 各作业能耗明细

    /* ── Gap 1: CPU调速器 (对标DESY) ─────────────────────────────── */
    std::vector<std::string> core_governors; // 每核心 scaling_governor

    /* ── Gap 2: 累计电量 (对标GLASGOW power_plus.py) ────────────── */
    double cumulative_kwh_total = 0.0;  // 累计总电量 (kWh)
    std::unordered_map<uint64_t, double> cumulative_kwh_by_job; // 各Job累计

    /* ── Gap 3: IPMI交叉验证 (对标GLASGOW power_plus.py) ────────── */
    double ipmi_power_w = 0.0;  // 同期IPMI整机功率读数 (W)

    /* ── 基础指标 ────────────────────────────────────────────────── */
    double avg_power_w = 0.0;   // 平均CPU Package功率 (ΔE_pkg / Δt)
};

/* ── power_bench校准参考基线 (可选的运行时验证数据) ────────────────── */
struct CalibrationRef {
    bool     valid = false;         // 是否成功加载了有效校准数据
    double   rapl_idle_w = 0.0;     // 空闲时RAPL功率 (W)
    double   ipmi_idle_w = 0.0;     // 空闲时IPMI功率 (W)
    double   static_overhead_w = 0.0; // IPMI − RAPL 静态开销 (主板+内存+风扇)
    double   per_core_w = 0.0;      // 每核心功耗增量 (full−idle)/cores
    double   full_rapl_w = 0.0;     // 满载时RAPL功率 (W)
    int      core_count = 0;        // 校准时的核心数
    std::string source_path;        // 校准数据来源 (summary.json路径)
};

class PowerCollector : public ICollector {
public:
    /* ── ICollector 生命周期 ──────────────────────────────────────── */
    bool init(const nlohmann::json& cfg) override;   // 加载eBPF、检测RAPL、配置参数
    void deinit() noexcept override;                  // 卸载eBPF、清理资源

    /* Job-scoped: 全局缓存后按Job逐个归因 (对标IO/Net Collector) */
    CollectResult collect(const Job& job) override;   // 读缓存→归因单个Job

    /* Writer适配器: 将PowerSnapshot转成各Writer可消费的格式 */
    CollectDataParseFunc get_writer_parser(const std::string& writer_type) override;

private:
    /* ── eBPF管理 ──────────────────────────────────────────────────── */
    bool load_ebpf();              // 加载power_collect.bpf.o
    void unload_ebpf() noexcept;   // 卸载eBPF程序
    void update_pid2job_map();     // 从JobRegistry同步PID→JobID到BPF map
    std::vector<task_cpu_runtime> dump_task_cpu_time(); // 批量导出+清空BPF map
    std::vector<task_cpu_runtime> dump_task_cpu_time_single(int fd, struct bpf_map* map, size_t key_sz, size_t val_sz); // fallback

    /* ── RAPL (CPU能耗计数器) ──────────────────────────────────────── */
    std::string detect_rapl_base(); // 自动探测Intel/AMD RAPL路径
    uint64_t read_rapl_uj();        // 读硬件energy_uj计数器的累加值

    /* ── CPU频率与调速器 ──────────────────────────────────────────── */
    std::vector<double> read_cpu_freqs_mhz();          // 读每核瞬时频率
    std::vector<std::string> read_cpu_governors();     // 读每核调速器模式
    int detect_core_count();                            // 检测CPU核心数

    /* ── IPMI交叉验证 ──────────────────────────────────────────────── */
    bool detect_ipmi();           // 检测ipmi-dcmi或ipmitool是否可用
    double read_ipmi_watts();     // 读IPMI整机瞬时功率 (W)

    /* ── 全局缓存 (对标IO/Net: 全量读一次, 按Job切片) ───────────── */
    void refresh_global_cache();  // 刷新缓存: RAPL + BPF dump + CPU freq
    PowerSnapshot extract_job_energy(const std::vector<task_cpu_runtime>& tasks,
                                     uint64_t delta_uj, double interval_s,
                                     const std::vector<double>& freqs,
                                     uint64_t target_job_id);

    /* ── 能耗计算核心 ──────────────────────────────────────────────── */
    PowerSnapshot compute_energy(const std::vector<task_cpu_runtime>& tasks,
                                 uint64_t delta_rapl_uj,
                                 double interval_s,
                                 const std::vector<double>& freqs);

    /* ── 状态变量 ──────────────────────────────────────────────────── */
    bool inited_ = false;

    /* eBPF */
    struct bpf_object*  bpf_obj_ = nullptr;
    std::vector<struct bpf_link*> bpf_links_;

    /* RAPL */
    std::string rapl_base_;         // RAPL sysfs路径 (如/sys/class/powercap/intel-rapl)
    uint64_t last_rapl_uj_ = 0;     // 上次读取的RAPL计数器值
    bool     rapl_valid_   = false; // RAPL是否可用

    /* CPU */
    int core_count_ = 0;            // CPU核心数

    /* 时间追踪 */
    std::chrono::steady_clock::time_point last_collect_ts_;

    /* ── power_bench 校准引用 ────────────────────────────────────── */
    CalibrationRef cal_ref_;
    void load_calibration_ref(const nlohmann::json& cfg); // 加载基准配置
    void validate_against_baseline(const PowerSnapshot& snap); // 运行时校验

    /* ── Gap 2: 累计电量 ─────────────────────────────────────────── */
    double cumulative_kwh_total_ = 0.0; // 累计总电量 (kWh)
    std::unordered_map<uint64_t, double> cumulative_kwh_by_job_; // 各Job累计

    /* ── Gap 3: IPMI ──────────────────────────────────────────────── */
    bool    ipmi_available_ = false;   // IPMI是否可用
    std::string ipmi_cmd_;             // IPMI命令 (ipmi-dcmi或ipmitool)

    /* ── Job-scoped 全局缓存 ─────────────────────────────────────── */
    std::vector<task_cpu_runtime> cached_tasks_;
    uint64_t cached_rapl_start_uj_ = 0;
    uint64_t cached_rapl_end_uj_   = 0;
    double   cached_interval_s_    = 0.0;
    std::vector<double> cached_freqs_;
    std::chrono::steady_clock::time_point cache_ts_;
    std::unordered_set<uint64_t> processed_in_cycle_;
    static constexpr double CACHE_TTL_S = 2.0;
};

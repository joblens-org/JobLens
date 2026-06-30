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
 * PowerCollector — eBPF + RAPL per-job energy attribution
 *
 * Architecture:
 *   Kernel: eBPF hooks sched_switch → per-(pid,cpu) runtime in ns
 *   User:   reads RAPL  E_pkg + sysfs CPU freq + eBPF task runtime
 *           → E_job =  E_pkg ×  (time_c × freq_c)[job] /   (time_c × freq_c)[all]
 *
 * The collector is System-scoped: it tracks every PID on the host and
 * attributes energy to JobLens-managed jobs via the pid2job BPF map.
 */
#pragma once
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include "core/collector_type.h"
#include "icollector.h"
#include "ebpf/power_collect.h"

#include <bpf/bpf.h>
#include <bpf/libbpf.h>

#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <string>
#include <unordered_map>
#include <vector>
#include <fstream>

/* ── Per-PID energy result (intermediate, before job-level grouping) ────── */
struct PowerPerPid {
    pid_t pid;
    u64   tgid;
    u64   runtime_ns_total;    // Σ_cpu runtime
    double weighted_ns_total;   // Σ_cpu (runtime × freq_mhz)
    double energy_j;            // attributed energy
    double cpu_pct;             // % of one core during the interval
};

/* ── Per-Job energy result (output unit) ────────────────────────────────── */
struct PowerPerJob {
    uint64_t job_id;
    std::string native_job_id;
    double energy_j;
    double weighted_ns_total;
    std::vector<PowerPerPid> pids;
};

/* ── Full collection snapshot ────────────────────────────────────────────── */
struct PowerSnapshot {
    std::chrono::system_clock::time_point ts;
    double interval_s;           // wall-clock interval since last collection
    double delta_rapl_uj;        // RAPL  E in microjoules
    double delta_rapl_j;         //    "    in joules
    int    core_count;
    std::vector<double> core_freqs_mhz;
    double total_weighted_ns;    // denominator =  t × Σ freq
    double system_overhead_j;    // RAPL energy not attributed to any job
    std::vector<PowerPerJob> jobs;
};

class PowerCollector : public ICollector {
public:
    /* ── ICollector lifecycle ──────────────────────────────────────── */
    bool init(const nlohmann::json& cfg) override;
    void deinit() noexcept override;

    /* system-scoped collector — Job parameter is ignored */
    CollectResult collect(const Job& /*job*/) override { return collect(); }
    CollectResult collect() override;

    /* writer adapters */
    CollectDataParseFunc get_writer_parser(const std::string& writer_type) override;

private:
    /* ── eBPF ──────────────────────────────────────────────────────── */
    bool load_ebpf();
    void unload_ebpf() noexcept;
    void update_pid2job_map();
    std::vector<task_cpu_runtime> dump_task_cpu_time();

    /* ── RAPL ──────────────────────────────────────────────────────── */
    std::string detect_rapl_base();
    uint64_t read_rapl_uj();

    /* ── CPU frequency ─────────────────────────────────────────────── */
    std::vector<double> read_cpu_freqs_mhz();
    int detect_core_count();

    /* ── Energy computation ────────────────────────────────────────── */
    PowerSnapshot compute_energy(const std::vector<task_cpu_runtime>& tasks,
                                 uint64_t delta_rapl_uj,
                                 double interval_s,
                                 const std::vector<double>& freqs);

    /* ── State ─────────────────────────────────────────────────────── */
    bool inited_ = false;

    /* eBPF */
    struct bpf_object*  bpf_obj_ = nullptr;
    std::vector<struct bpf_link*> bpf_links_;

    /* RAPL */
    std::string rapl_base_;
    uint64_t last_rapl_uj_ = 0;
    bool     rapl_valid_   = false;

    /* CPU */
    int core_count_ = 0;

    /* timing */
    std::chrono::steady_clock::time_point last_collect_ts_;
};

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
    std::string vo;              // Virtual Organization (Gap 2: from scheduler metadata)
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

    /* ── WLCG standard metrics ───────────────────────────────────────── */
    double avg_power_w = 0.0;        // ΔE_pkg / interval_s  (average package watts)
    double pue_corrected_j = 0.0;    // ΔE_pkg × PUE  (total facility energy estimate)
    double co2_equivalent_g = 0.0;   // facility energy → CO₂  (g)
    double hs23_per_watt = 0.0;      // HS23 score / watt  (if HS23 score provided)

    /* ── Gap 1: CPU governor (对标 DESY) ─────────────────────────────── */
    std::vector<std::string> core_governors;

    /* ── Gap 2: Cumulative kWh accumulator (对标 GLASGOW) ────────────── */
    double cumulative_kwh_total = 0.0;       // running total energy (kWh)
    std::unordered_map<uint64_t, double> cumulative_kwh_by_job;  // per-job kWh

    /* ── Gap 3: IPMI cross-validation (对标 GLASGOW power_plus.py) ───── */
    double ipmi_power_w = 0.0;               // concurrent IPMI reading (W)
};

/* ── power_bench calibration reference (optional validation baseline) ───── */
struct CalibrationRef {
    bool     valid = false;
    double   rapl_idle_w = 0.0;      // RAPL idle power (W)
    double   ipmi_idle_w = 0.0;      // IPMI idle power (W)
    double   static_overhead_w = 0.0; // IPMI − RAPL gap
    double   per_core_w = 0.0;       // (full − idle) / cores
    double   full_rapl_w = 0.0;      // RAPL full load power (W)
    int      core_count = 0;
    std::string source_path;         // path to the summary.json that was loaded
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

    /* ── CPU frequency & governor ──────────────────────────────────── */
    std::vector<double> read_cpu_freqs_mhz();
    std::vector<std::string> read_cpu_governors();
    int detect_core_count();

    /* ── IPMI cross-validation (Gap 3) ──────────────────────────────── */
    bool detect_ipmi();
    double read_ipmi_watts();

    /* ── VO extraction (Gap 2) ──────────────────────────────────────── */
    std::string extract_vo_from_job(const Job& job);

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

    /* ── WLCG / site-level parameters ──────────────────────────────── */
    double pue_ = 1.0;                      // Power Usage Effectiveness (default: 1.0 = no correction)
    double carbon_intensity_g_per_kwh_ = 0.0; // grid carbon intensity (g CO₂/kWh)
    double hs23_score_ = 0.0;              // HS23 benchmark score (0 = not available)

    /* ── power_bench calibration reference ────────────────────────── */
    CalibrationRef cal_ref_;
    void load_calibration_ref(const nlohmann::json& cfg);
    void validate_against_baseline(const PowerSnapshot& snap);

    /* ── Gap 2: Cumulative kWh ─────────────────────────────────────── */
    double cumulative_kwh_total_ = 0.0;
    std::unordered_map<uint64_t, double> cumulative_kwh_by_job_;

    /* ── Gap 3: IPMI ────────────────────────────────────────────────── */
    bool    ipmi_available_ = false;
    std::string ipmi_cmd_;   // "ipmi-dcmi ..." or "ipmitool dcmi power reading"
};

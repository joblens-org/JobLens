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
 * PowerCollector — eBPF sched_switch + RAPL 逐作业能耗归因
 *
 * 采集生命周期:
 *   init()      → 加载eBPF程序、探测RAPL硬件、读取初始能量计数器基准
 *   collect()   → 导出eBPF任务运行时数据、读RAPL ΔE_pkg、读CPU频率、
 *                  用频率加权公式计算每个Job的归因能耗
 *   deinit()    → 卸载eBPF程序、清理资源
 *
 * 归因公式 (用户态计算):
 *   对每个作业J:
 *     weighted_J = Σ_{p∈J} Σ_{cpu} runtime_ns(p,cpu) × freq_mhz(cpu)
 *   total_weighted = interval_s × 1e9 × Σ_{cpu} freq_mhz(cpu)
 *   E_job = ΔE_pkg × weighted_J / total_weighted
 *   system_overhead = ΔE_pkg − Σ E_job
 *
 * 数据流:
 *   内核eBPF (每上下文切换) → task_cpu_time map
 *   用户态collect() (每ΔT) → dump map → 读RAPL → 读频率 → 归因计算 → Writer输出
 */
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include "collector/power_collector.hpp"

#include "core/collector_registry.hpp"
#include "core/job_registry.hpp"
#include "common/ebpf_common.hpp"
#include "common/utils.hpp"
#include "writer/prometheus_exporter_writer.hpp"

#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <optional>
#include <regex>
#include <sstream>
#include <unistd.h>

/* ── Auto-registration ─────────────────────────────────────────────────── */
AUTO_REGISTER_JOB_COLLECTOR(
    PowerCollector,
    "Per-job energy attribution via eBPF sched_switch + RAPL. "
    "Tracks per-task per-CPU runtime at ns precision, reads RAPL package "
    "energy counters and per-core CPU frequency, then attributes energy "
    "proportional to frequency-weighted CPU time: "
    "E_job =  E_pkg ×  (t_c × f_c)_job /  (t_c × f_c)_all.",
    ConfigParams{
        {"freq",                      "Sampling frequency in Hz (default 1.0 = 1s intervals)"},
        {"use_writers",               "Writer names for output (e.g. file_writer, PrometheusExporterWriter)"},
        {"auto_start",                "Auto-start on daemon launch (true/false, default true)"},
    }
)

using json = nlohmann::json;

/* ═══════════════════════════════════════════════════════════════════════════
 * RAPL (Running Average Power Limit) 硬件接口
 * 读取CPU Package的能量计数器: /sys/class/powercap/intel-rapl:N/energy_uj
 * 只读顶层package域 (intel-rapl:\d+$)，避免子域(cores/uncore)重复计数
 * ═══════════════════════════════════════════════════════════════════════════ */

/* 自动探测Intel/AMD RAPL路径 */
std::string PowerCollector::detect_rapl_base()
{
    /* Probe for Intel or AMD RAPL. ARM / unknown → empty. */
    DIR* dp = opendir("/sys/class/powercap");
    if (!dp) return "";

    std::string found;
    struct dirent* entry;
    while ((entry = readdir(dp)) != nullptr) {
        std::string name(entry->d_name);
        if (name.rfind("intel-rapl", 0) == 0) {
            found = "/sys/class/powercap/intel-rapl"; break;
        }
        if (name.rfind("amd_energy", 0) == 0) {
            found = "/sys/class/powercap/amd_energy"; break;
        }
    }
    closedir(dp);
    return found;
}

uint64_t PowerCollector::read_rapl_uj()
{
    if (rapl_base_.empty()) return 0;

    uint64_t total = 0;

    /* Only sum top-level package domains (intel-rapl:N, not intel-rapl:N:M)
     * to avoid double-counting sub-domains (core, uncore, dram). */
    std::regex pkg_re(R"(intel-rapl:(\d+)$)");
    std::regex amd_re(R"(amd_energy:(\d+)$)");

    DIR* dp = opendir("/sys/class/powercap");
    if (!dp) return 0;

    struct dirent* entry;
    while ((entry = readdir(dp)) != nullptr) {
        std::string name(entry->d_name);
        bool is_pkg = std::regex_match(name, pkg_re) ||
                      std::regex_match(name, amd_re);
        if (!is_pkg) continue;

        std::string path = "/sys/class/powercap/" + name + "/energy_uj";
        std::ifstream f(path);
        if (!f.is_open()) continue;

        uint64_t val = 0;
        f >> val;
        total += val;
    }
    closedir(dp);
    return total;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * CPU频率与调速器采集
 * 读取每个核心的瞬时频率(scaling_cur_freq)和调速器模式(scaling_governor)
 * 回退链: cur_freq → cpuinfo_max_freq → 2000MHz默认值
 * ═══════════════════════════════════════════════════════════════════════════ */

/* 检测CPU核心数 (sysconf _SC_NPROCESSORS_ONLN) */
int PowerCollector::detect_core_count()
{
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return (n > 0) ? static_cast<int>(n) : 1;
}

std::vector<double> PowerCollector::read_cpu_freqs_mhz()
{
    std::vector<double> freqs;
    freqs.reserve(core_count_);

    for (int cpu = 0; cpu < core_count_; ++cpu) {
        double mhz = 0.0;
        std::string path = "/sys/devices/system/cpu/cpu"
                         + std::to_string(cpu)
                         + "/cpufreq/scaling_cur_freq";
        std::ifstream f(path);
        if (f.is_open()) {
            unsigned long khz = 0;
            f >> khz;
            mhz = static_cast<double>(khz) / 1000.0;
        }
        /* fallback: try cpuinfo_max_freq (base frequency) */
        if (mhz <= 0.0) {
            std::string base_path = "/sys/devices/system/cpu/cpu"
                                  + std::to_string(cpu)
                                  + "/cpufreq/cpuinfo_max_freq";
            std::ifstream fb(base_path);
            if (fb.is_open()) {
                unsigned long khz = 0;
                fb >> khz;
                mhz = static_cast<double>(khz) / 1000.0;
            }
        }
        freqs.push_back(mhz > 0.0 ? mhz : 2000.0);  // last-resort default: 2 GHz
    }
    return freqs;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * eBPF 程序管理
 * RPM安装路径: /usr/lib64/joblens/bpf_obj/power_collect.bpf.o
 * 开发构建:    build/bpf_obj/power_collect.bpf.o (fallback)
 * ═══════════════════════════════════════════════════════════════════════════ */

bool PowerCollector::load_ebpf()
{
    /* Path to the compiled BPF object.
     * CMake places .bpf.o files under ${CMAKE_BINARY_DIR}/bpf_obj/,
     * and the installed RPM places them under /usr/<lib>/joblens/bpf_obj/.
     */
    std::string bpf_path = std::string("/usr/") + JOBLENS_INSTALL_LIBDIR
                         + "/joblens/bpf_obj/power_collect.bpf.o";
    /* If we're running from a build tree, try the build directory first */
    if (access(bpf_path.c_str(), R_OK) != 0) {
        /* Fallback: running from build directory (development) */
        bpf_path = "bpf_obj/power_collect.bpf.o";
    }

    bpf_obj_ = EbpfCommon::load_bpf_obj(bpf_path, bpf_links_);
    if (!bpf_obj_) {
        spdlog::error("PowerCollector: failed to load eBPF object from {}", bpf_path);
        return false;
    }

    spdlog::info("PowerCollector: eBPF object loaded, {} links attached",
                 bpf_links_.size());
    return true;
}

void PowerCollector::unload_ebpf() noexcept
{
    if (bpf_obj_) {
        EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
        bpf_obj_ = nullptr;
        spdlog::info("PowerCollector: eBPF unloaded");
    }
}

void PowerCollector::update_pid2job_map()
{
    if (!bpf_obj_) return;

    /* Get all active jobs from JobRegistry and push their PIDs into the
     * pid2job BPF map so the collector can group runtime by JobID. */
    auto jobs = JobRegistry::instance().snapshot();

    int total_pids = 0;
    for (const auto& job : jobs) {
        if (job.JobPIDs.empty()) continue;
        EbpfCommon::update_pid_in_kernel(
            bpf_obj_, "pid2job",
            static_cast<uint64_t>(job.JobID),
            job.JobPIDs);
        total_pids += static_cast<int>(job.JobPIDs.size());
    }
    spdlog::debug("PowerCollector: updated pid2job map with {} PIDs from {} jobs",
                  total_pids, jobs.size());
}

std::vector<task_cpu_runtime> PowerCollector::dump_task_cpu_time()
{
    std::vector<task_cpu_runtime> result;

    if (!bpf_obj_) return result;

    struct bpf_map* map = bpf_object__find_map_by_name(bpf_obj_, "task_cpu_time");
    if (!map) {
        spdlog::error("PowerCollector: task_cpu_time map not found");
        return result;
    }

    int fd = bpf_map__fd(map);
    if (fd < 0) return result;

    size_t key_sz = bpf_map__key_size(map);
    size_t val_sz = bpf_map__value_size(map);
    size_t max_entries = bpf_map__max_entries(map);

    /* 批量读取: bpf_map_lookup_batch 一次系统调用拿全部 */
    std::vector<uint8_t> keys_buf(max_entries * key_sz, 0);
    std::vector<uint8_t> vals_buf(max_entries * val_sz, 0);
    uint32_t count = max_entries;

    int ret = bpf_map_lookup_batch(fd, nullptr, nullptr,
                                    keys_buf.data(), vals_buf.data(),
                                    &count, nullptr);
    if (ret != 0 && ret != -ENOENT) {
        /* batch not supported, fallback to single-key dump */
        spdlog::debug("PowerCollector: batch lookup not supported (ret={}), using single-key fallback", ret);
        return dump_task_cpu_time_single(fd, map, key_sz, val_sz);
    }

    /* 解析读取到的 entry */
    for (uint32_t i = 0; i < count; ++i) {
        struct task_cpu_key tck;
        std::memcpy(&tck, keys_buf.data() + i * key_sz, sizeof(tck));

        u64 val = 0;
        std::memcpy(&val, vals_buf.data() + i * val_sz, sizeof(val));

        task_cpu_runtime tcr;
        tcr.pid_tgid   = tck.pid_tgid;
        tcr.cpu        = tck.cpu;
        tcr.runtime_ns = val;
        result.push_back(tcr);
    }

    /* 批量删除 */
    if (count > 0) {
        bpf_map_delete_batch(fd, keys_buf.data(), &count, nullptr);
    }

    spdlog::debug("PowerCollector: dumped {} task-cpu runtime entries (batch)", result.size());
    return result;
}

/* 逐条读取 fallback (batch 不支持时使用) */
std::vector<task_cpu_runtime> PowerCollector::dump_task_cpu_time_single(
    int fd, struct bpf_map* map, size_t key_sz, size_t val_sz)
{
    std::vector<task_cpu_runtime> result;
    std::vector<std::vector<uint8_t>> keys;
    std::vector<u64> values;
    std::vector<uint8_t> cur_key(key_sz, 0);
    std::vector<uint8_t> next_key(key_sz, 0);

    while (bpf_map_get_next_key(fd, cur_key.data(), next_key.data()) == 0) {
        u64 val = 0;
        std::vector<uint8_t> val_buf(val_sz, 0);
        if (bpf_map_lookup_elem(fd, next_key.data(), val_buf.data()) == 0) {
            std::memcpy(&val, val_buf.data(), sizeof(val));
        }
        keys.push_back(next_key);
        values.push_back(val);
        cur_key = next_key;
    }

    for (const auto& k : keys) bpf_map_delete_elem(fd, k.data());

    for (size_t i = 0; i < keys.size(); ++i) {
        struct task_cpu_key tck;
        std::memcpy(&tck, keys[i].data(), sizeof(tck));
        task_cpu_runtime tcr;
        tcr.pid_tgid = tck.pid_tgid;
        tcr.cpu = tck.cpu;
        tcr.runtime_ns = values[i];
        result.push_back(tcr);
    }
    return result;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * 能耗归因核心计算
 * 1. 分母: total_weighted_ns = Δt × 1e9 × Σ freq_mhz (理论最大加权时间)
 * 2. 按pid_tgid聚合eBPF运行时 → weighted_ns = runtime_ns × freq_mhz
 * 3. 通过pid2job分组 → job_weighted = Σ pid_weighted
 * 4. E_job = ΔE_pkg × job_weighted / total_weighted_ns
 * 5. E_pid = E_job × pid_weighted / job_weighted (二级分配)
 * 6. system_overhead = ΔE_pkg - Σ E_job
 * ═══════════════════════════════════════════════════════════════════════════ */

PowerSnapshot PowerCollector::compute_energy(
    const std::vector<task_cpu_runtime>& tasks,
    uint64_t delta_rapl_uj,
    double interval_s,
    const std::vector<double>& freqs)
{
    PowerSnapshot snap;
    snap.ts               = std::chrono::system_clock::now();
    snap.interval_s        = interval_s;
    snap.delta_rapl_uj     = static_cast<double>(delta_rapl_uj);
    snap.delta_rapl_j      = snap.delta_rapl_uj / 1e6;
    snap.core_count        = core_count_;
    snap.core_freqs_mhz    = freqs;

    /* ── total_weighted = interval_s × Σ_cpu freq_mhz ── */
    double sum_freq = 0.0;
    for (double f : freqs) sum_freq += f;
    /* total_weighted in ns·MHz: interval_s * 1e9 ns/s × sum_freq_mhz */
    double total_weighted_ns = interval_s * 1e9 * sum_freq;
    snap.total_weighted_ns = total_weighted_ns;

    if (total_weighted_ns <= 0.0 || tasks.empty()) {
        snap.system_overhead_j = snap.delta_rapl_j;
        return snap;
    }

    /* ── Group by (tgid, pid) and sum weighted_ns ── */
    /* Key: pid_tgid (u64) → {pid, tgid, runtime_ns_total, weighted_ns} */
    struct PidAccum {
        pid_t pid;
        u64   tgid;
        u64   runtime_ns_total = 0;
        double weighted_ns     = 0.0;
    };
    std::unordered_map<u64, PidAccum> pid_map;

    for (const auto& t : tasks) {
        u64 pt = t.pid_tgid;
        u32 pid = static_cast<u32>(pt & 0xFFFFFFFF);
        u32 tgid = static_cast<u32>(pt >> 32);
        u32 cpu = t.cpu;
        u64 ns  = t.runtime_ns;

        double freq_mhz = (cpu < freqs.size()) ? freqs[cpu] : 2000.0;

        auto& acc = pid_map[pt];
        acc.pid  = pid;
        acc.tgid = tgid;
        acc.runtime_ns_total += ns;
        acc.weighted_ns      += static_cast<double>(ns) * freq_mhz;
    }

    /* ── Build pid2job lookup from JobRegistry ── */
    std::unordered_map<pid_t, uint64_t> pid2job;
    std::unordered_map<uint64_t, std::string> job_native_ids;
    {
        auto jobs = JobRegistry::instance().snapshot();
        for (const auto& job : jobs) {
            job_native_ids[job.JobID] = job.NativeJobID;
            for (pid_t p : job.JobPIDs) {
                pid2job[p] = job.JobID;
            }
        }
    }

    /* ── Sum weighted_ns per job ── */
    struct JobAccum {
        uint64_t job_id = 0;
        std::string native_job_id;
        double weighted_ns = 0.0;
        std::vector<PowerPerPid> pids;
    };
    std::unordered_map<uint64_t, JobAccum> job_map;

    double attributed_weighted_ns = 0.0;

    for (const auto& [pt, acc] : pid_map) {
        uint64_t jid = 0;
        std::string native_jid;

        auto it_j = pid2job.find(acc.pid);
        if (it_j != pid2job.end()) {
            jid = it_j->second;
            auto it_nj = job_native_ids.find(jid);
            if (it_nj != job_native_ids.end()) {
                native_jid = it_nj->second;
            }
        }

        if (jid == 0) continue;  // system process → overhead

        auto& ja = job_map[jid];
        ja.job_id        = jid;
        ja.native_job_id = native_jid;
        ja.weighted_ns  += acc.weighted_ns;

        PowerPerPid pp;
        pp.pid              = acc.pid;
        pp.tgid             = acc.tgid;
        pp.runtime_ns_total = acc.runtime_ns_total;
        pp.weighted_ns_total = acc.weighted_ns;
        pp.cpu_pct           = (interval_s > 0.0)
            ? (100.0 * static_cast<double>(acc.runtime_ns_total) / (interval_s * 1e9))
            : 0.0;
        /* energy assigned below after total is known */
        ja.pids.push_back(pp);

        attributed_weighted_ns += acc.weighted_ns;
    }

    /* ── Assign energy per job ── */
    for (auto& [jid, ja] : job_map) {
        PowerPerJob pj;
        pj.job_id           = ja.job_id;
        pj.native_job_id    = ja.native_job_id;
        pj.weighted_ns_total = ja.weighted_ns;
        pj.energy_j         = (total_weighted_ns > 0.0)
            ? snap.delta_rapl_j * ja.weighted_ns / total_weighted_ns
            : 0.0;

        /* Assign energy to each PID proportionally */
        for (auto& pp : ja.pids) {
            pp.energy_j = (ja.weighted_ns > 0.0)
                ? pj.energy_j * pp.weighted_ns_total / ja.weighted_ns
                : 0.0;
        }
        pj.pids = std::move(ja.pids);
        snap.jobs.push_back(std::move(pj));
    }

    snap.system_overhead_j = snap.delta_rapl_j;
    for (const auto& j : snap.jobs) {
        snap.system_overhead_j -= j.energy_j;
    }
    if (snap.system_overhead_j < 0.0) snap.system_overhead_j = 0.0;

    /* ── Basic metric ──────────────────────────────────────────────────── */
    snap.avg_power_w = (interval_s > 0.0) ? snap.delta_rapl_j / interval_s : 0.0;

    /* ── Gap 2: Cumulative kWh accumulator (对标 GLASGOW power_plus.py) ── */
    /* ΔE_pkg (J) → kWh */
    double delta_kwh = snap.delta_rapl_j / 3.6e6;
    cumulative_kwh_total_ += delta_kwh;
    snap.cumulative_kwh_total = cumulative_kwh_total_;
    for (auto& job : snap.jobs) {
        double job_kwh = job.energy_j / 3.6e6;
        cumulative_kwh_by_job_[job.job_id] += job_kwh;
        snap.cumulative_kwh_by_job[job.job_id] = cumulative_kwh_by_job_[job.job_id];
    }

    /* ── Gap 3: IPMI cross-validation log ─────────────────────────────── */
    /* (the actual IPMI value is set in collect() → snap.ipmi_power_w;
     *   here we just log the comparison as a soft validation) */
    if (snap.ipmi_power_w > 0.0 && snap.avg_power_w > 0.0) {
        double ratio = snap.ipmi_power_w / snap.avg_power_w;
        if (ratio < 0.8 || ratio > 2.5) {
            spdlog::warn("PowerCollector: IPMI/RAPL ratio={:.2f} (IPMI={:.0f}W, RAPL_avg={:.1f}W) — "
                         "possible IPMI stale reading or RAPL counter issue",
                         ratio, snap.ipmi_power_w, snap.avg_power_w);
        }
        spdlog::debug("PowerCollector: IPMI cross-check — IPMI={:.0f}W, RAPL_avg={:.1f}W, ratio={:.2f}",
                      snap.ipmi_power_w, snap.avg_power_w, ratio);
    }

    spdlog::debug("PowerCollector: computed energy —  E={:.2f} J, jobs={}, overhead={:.2f} J, interval={:.3f} s",
                  snap.delta_rapl_j, snap.jobs.size(), snap.system_overhead_j, interval_s);

    return snap;
}

/* ── power_bench calibration reference ───────────────────────────────────── */

void PowerCollector::load_calibration_ref(const nlohmann::json& cfg)
{
    if (!cfg.contains("calibration_ref")) return;

    const auto& cr = cfg["calibration_ref"];
    if (!cr.is_object()) return;

    /* Check if calibration is explicitly disabled */
    if (cr.contains("enabled")) {
        bool enabled = false;
        if (cr["enabled"].is_boolean()) enabled = cr["enabled"].get<bool>();
        else if (cr["enabled"].is_string()) {
            auto s = cr["enabled"].get<std::string>();
            enabled = (s == "true" || s == "1");
        }
        if (!enabled) {
            spdlog::info("PowerCollector: calibration_ref disabled in config");
            return;
        }
    }

    /* Try loading from bench_summary file path */
    if (cr.contains("bench_summary") && cr["bench_summary"].is_string()) {
        std::string path = cr["bench_summary"].get<std::string>();
        std::ifstream f(path);
        if (f.is_open()) {
            try {
                json summary = json::parse(f);
                // power_bench summary.json structure:
                // {"idle": {"pkg": {"avg": 66.2, ...}, "ipmi": {"avg": 118, ...}}, "full": {...}}
                auto parse_stats = [&summary](const std::string& label,
                                               const std::string& kind) -> double {
                    if (summary.contains(label) && summary[label].contains(kind)
                        && summary[label][kind].contains("avg"))
                        return summary[label][kind]["avg"].get<double>();
                    return 0.0;
                };
                cal_ref_.rapl_idle_w = parse_stats("idle", "pkg");
                cal_ref_.ipmi_idle_w = parse_stats("idle", "ipmi");
                cal_ref_.full_rapl_w = parse_stats("full", "pkg");
                cal_ref_.static_overhead_w = cal_ref_.ipmi_idle_w - cal_ref_.rapl_idle_w;
                int bench_cores = 0;
                if (summary.contains("idle") && summary["idle"].contains("pkg")
                    && summary["idle"]["pkg"].contains("n"))
                    bench_cores = core_count_; // use current core count as fallback
                // Infer per_core from full - idle delta
                cal_ref_.per_core_w = (bench_cores > 0 && cal_ref_.full_rapl_w > 0.0)
                    ? (cal_ref_.full_rapl_w - cal_ref_.rapl_idle_w) / bench_cores
                    : 0.0;
                cal_ref_.core_count = bench_cores;
                cal_ref_.source_path = path;
                cal_ref_.valid = (cal_ref_.rapl_idle_w > 0.0);
            } catch (const std::exception& e) {
                spdlog::warn("PowerCollector: failed to parse calibration_ref summary '{}': {}",
                             path, e.what());
                return;
            }
        } else {
            spdlog::warn("PowerCollector: calibration_ref bench_summary '{}' not found", path);
            return;
        }
    }

    /* Allow inline override/addition of specific values */
    auto try_get = [&cr](const char* key) -> std::optional<double> {
        if (!cr.contains(key)) return std::nullopt;
        const auto& v = cr[key];
        if (v.is_number()) return v.get<double>();
        if (v.is_string()) {
            try { return std::stod(v.get<std::string>()); }
            catch (...) { return std::nullopt; }
        }
        return std::nullopt;
    };
    if (auto v = try_get("rapl_idle_w"))    cal_ref_.rapl_idle_w = *v;
    if (auto v = try_get("ipmi_idle_w"))    cal_ref_.ipmi_idle_w = *v;
    if (auto v = try_get("static_overhead_w")) cal_ref_.static_overhead_w = *v;
    if (auto v = try_get("per_core_w"))     cal_ref_.per_core_w = *v;
    if (auto v = try_get("full_rapl_w"))    cal_ref_.full_rapl_w = *v;
    if (auto v = try_get("core_count"))     cal_ref_.core_count = static_cast<int>(*v);

    /* Mark valid if at least RAPL idle baseline is set */
    if (cal_ref_.rapl_idle_w > 0.0) cal_ref_.valid = true;

    if (cal_ref_.valid) {
        spdlog::info("PowerCollector: calibration_ref loaded — "
                     "RAPL_idle={:.1f}W IPMI_idle={:.1f}W static={:.1f}W per_core={:.1f}W source={}",
                     cal_ref_.rapl_idle_w, cal_ref_.ipmi_idle_w,
                     cal_ref_.static_overhead_w, cal_ref_.per_core_w,
                     cal_ref_.source_path.empty() ? "(inline)" : cal_ref_.source_path);
    }
}

void PowerCollector::validate_against_baseline(const PowerSnapshot& snap)
{
    if (!cal_ref_.valid || snap.interval_s < 0.5) return; // skip first short sample

    double current_w = snap.avg_power_w;
    double baseline_w = cal_ref_.rapl_idle_w;

    if (baseline_w <= 0.0) return;

    double deviation_pct = 100.0 * std::abs(current_w - baseline_w) / baseline_w;

    /* Warn if idle-like power deviates >30% from power_bench baseline */
    if (deviation_pct > 30.0) {
        spdlog::warn("PowerCollector: avg_power={:.1f}W deviates {:.0f}% from power_bench "
                     "baseline RAPL_idle={:.1f}W — possible hardware change or misconfiguration",
                     current_w, deviation_pct, baseline_w);
    }

    /* Also check per_core increment if jobs are running */
    if (cal_ref_.per_core_w > 0.0 && !snap.jobs.empty()) {
        for (const auto& job : snap.jobs) {
            double job_w = (snap.interval_s > 0.0) ? job.energy_j / snap.interval_s : 0.0;
            int pid_count = static_cast<int>(job.pids.size());
            if (pid_count == 0) continue;
            double per_pid_w = job_w / pid_count;

            /* Range check: per-PID watt should be within [0.1×, 5×] of baseline per-core */
            if (per_pid_w > cal_ref_.per_core_w * 5.0) {
                spdlog::warn("PowerCollector: Job {} per-PID power {:.1f}W > 5× baseline "
                             "per_core {:.1f}W — possible frequency anomaly",
                             job.job_id, per_pid_w, cal_ref_.per_core_w);
            }
        }
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Gap 1: CPU调速器记录 (对标DESY log-power-consumption.sh)
 * DESY每次IPMI采样时同时记录cpu0的scaling_governor。
 * 我们遍历所有CPU核心读取governor，输出各模式的数量分布。
 * ═══════════════════════════════════════════════════════════════════════════ */

std::vector<std::string> PowerCollector::read_cpu_governors()
{
    std::vector<std::string> governors;
    governors.reserve(core_count_);

    for (int cpu = 0; cpu < core_count_; ++cpu) {
        std::string path = "/sys/devices/system/cpu/cpu"
                         + std::to_string(cpu)
                         + "/cpufreq/scaling_governor";
        std::ifstream f(path);
        std::string gov = "unknown";
        if (f.is_open()) {
            std::getline(f, gov);
            if (gov.empty()) gov = "unknown";
        }
        governors.push_back(gov);
    }
    return governors;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Gap 3: IPMI在线交叉验证 (对标GLASGOW power_plus.py)
 * GLASGOW用离线脚本对比估算功率vs真实IPMI。
 * 我们每个采集周期同时读一次IPMI瞬时功率，在线比较IPMI/RAPL ratio。
 * 比例超出[0.8, 2.5]范围时发WARN日志。
 * IPMI通过BMC读取，~1s响应延迟，对毫秒级负载波动不敏感。
 * ═══════════════════════════════════════════════════════════════════════════ */

bool PowerCollector::detect_ipmi()
{
    /* Try ipmi-dcmi first (newer, better output), then ipmitool */
    if (system("which ipmi-dcmi >/dev/null 2>&1") == 0) {
        ipmi_cmd_ = "ipmi-dcmi --get-system-power-statistics";
        ipmi_available_ = true;
    } else if (system("which ipmitool >/dev/null 2>&1") == 0) {
        ipmi_cmd_ = "ipmitool dcmi power reading";
        ipmi_available_ = true;
    } else {
        ipmi_available_ = false;
    }

    spdlog::info("PowerCollector: IPMI {} (cmd={})",
                 ipmi_available_ ? "available" : "unavailable",
                 ipmi_available_ ? ipmi_cmd_ : "n/a");
    return ipmi_available_;
}

double PowerCollector::read_ipmi_watts()
{
    if (!ipmi_available_) return 0.0;

    double w = 0.0;
    std::string cmd = ipmi_cmd_ + " 2>/dev/null";
    FILE* fp = popen(cmd.c_str(), "r");
    if (!fp) return 0.0;

    char buf[256];
    std::string output;
    while (fgets(buf, sizeof(buf), fp)) output += buf;
    pclose(fp);

    /* Parse: "Current Power : 285 Watts" or "Instantaneous power reading: 285 Watts" */
    if (ipmi_cmd_.find("ipmi-dcmi") != std::string::npos) {
        auto pos = output.find("Current Power");
        if (pos != std::string::npos) {
            /* Find digits after "Current Power" */
            const char* p = output.c_str() + pos + 13; // skip "Current Power"
            while (*p && !std::isdigit(*p)) ++p;
            w = std::atof(p);
        }
    } else {
        auto pos = output.find("Instantaneous power reading");
        if (pos != std::string::npos) {
            /* Split by spaces, 4th token is the number */
            std::istringstream iss(output.substr(pos));
            std::string token;
            for (int i = 0; i < 4; ++i) iss >> token;
            w = std::atof(token.c_str());
        }
    }
    return w;
}

/* ── ICollector lifecycle ───────────────────────────────────────────────── */

bool PowerCollector::init(const nlohmann::json& cfg)
{
    if (inited_) {
        spdlog::warn("PowerCollector: already initialized");
        return false;
    }

    spdlog::info("PowerCollector: init with config: {}", cfg.dump());

    /* Detect RAPL */
    rapl_base_ = detect_rapl_base();
    if (rapl_base_.empty()) {
        spdlog::warn("PowerCollector: no RAPL interface found — energy data will be zero");
    } else {
        rapl_valid_ = true;
        spdlog::info("PowerCollector: RAPL base = {}", rapl_base_);
    }

    /* Detect core count */
    core_count_ = detect_core_count();
    spdlog::info("PowerCollector: {} CPU cores detected", core_count_);

    /* Load eBPF */
    if (!load_ebpf()) {
        spdlog::error("PowerCollector: eBPF load failed");
        return false;
    }

    /* Detect IPMI for cross-validation (Gap 3) */
    detect_ipmi();

    /* Seed first RAPL reading */
    if (rapl_valid_) {
        last_rapl_uj_ = read_rapl_uj();
    }

    /* Load power_bench calibration reference (optional) */
    load_calibration_ref(cfg);

    last_collect_ts_ = std::chrono::steady_clock::now();

    inited_ = true;
    return true;
}

void PowerCollector::deinit() noexcept
{
    if (!inited_) return;
    unload_ebpf();
    inited_ = false;
    spdlog::info("PowerCollector: deinit complete");
}

/* ── 刷新全局缓存 (对标 IO/Net: 全量读一次, 后续 collect(Job) 切片取) ── */
void PowerCollector::refresh_global_cache()
{
    update_pid2job_map();

    auto now = std::chrono::steady_clock::now();
    double interval_s = std::chrono::duration<double>(now - last_collect_ts_).count();
    last_collect_ts_ = now;
    if (interval_s > 60.0 || interval_s <= 0.0) interval_s = 1.0;

    cached_rapl_start_uj_ = last_rapl_uj_;
    uint64_t rapl_now = rapl_valid_ ? read_rapl_uj() : 0;
    cached_rapl_end_uj_ = rapl_now;
    if (!(rapl_valid_ && rapl_now >= last_rapl_uj_)) {
        spdlog::debug("PowerCollector: RAPL wraparound");
    }
    last_rapl_uj_ = rapl_now;

    cached_interval_s_ = interval_s;
    cached_freqs_ = read_cpu_freqs_mhz();
    cached_tasks_ = dump_task_cpu_time();
    cache_ts_ = now;
    processed_in_cycle_.clear();
}

/* ── 从全局缓存中提取单个Job的能耗 ── */
PowerSnapshot PowerCollector::extract_job_energy(
    const std::vector<task_cpu_runtime>& tasks, uint64_t delta_uj,
    double interval_s, const std::vector<double>& freqs, uint64_t target_job_id)
{
    PowerSnapshot full = compute_energy(tasks, delta_uj, interval_s, freqs);

    PowerSnapshot snap;
    snap.ts                = full.ts;
    snap.interval_s         = full.interval_s;
    snap.delta_rapl_uj      = full.delta_rapl_uj;
    snap.delta_rapl_j       = full.delta_rapl_j;
    snap.core_count         = full.core_count;
    snap.core_freqs_mhz     = full.core_freqs_mhz;
    snap.total_weighted_ns  = full.total_weighted_ns;
    snap.avg_power_w        = full.avg_power_w;
    snap.cumulative_kwh_total = full.cumulative_kwh_total;
    snap.ipmi_power_w       = full.ipmi_power_w;
    snap.core_governors     = full.core_governors;

    double extracted_j = 0.0;
    for (auto& j : full.jobs) {
        if (j.job_id == target_job_id) {
            extracted_j = j.energy_j;
            snap.jobs.push_back(std::move(j));
        }
    }
    snap.system_overhead_j = snap.delta_rapl_j - extracted_j;
    if (snap.system_overhead_j < 0.0) snap.system_overhead_j = 0.0;

    return snap;
}

/* ── Job-scoped collect: 缓存过期刷新, 提取当前Job归因 ── */
CollectResult PowerCollector::collect(const Job& job)
{
    if (!inited_) {
        spdlog::warn("PowerCollector: collect(job) called before init");
        return PowerSnapshot{};
    }

    auto now = std::chrono::steady_clock::now();
    double age = std::chrono::duration<double>(now - cache_ts_).count();

    /* 缓存过期 → 刷新全局数据 */
    if (age >= CACHE_TTL_S || cached_tasks_.empty()) {
        refresh_global_cache();
    }

    /* 去重: 本周期已处理过 → 跳过 */
    if (processed_in_cycle_.count(job.JobID)) {
        spdlog::debug("PowerCollector: collect(job#{}) skipped (already processed)", job.JobID);
        return PowerSnapshot{};
    }
    processed_in_cycle_.insert(job.JobID);

    /* 从缓存提取本 Job 的能耗 */
    uint64_t delta_uj = 0;
    if (cached_rapl_end_uj_ >= cached_rapl_start_uj_) {
        delta_uj = cached_rapl_end_uj_ - cached_rapl_start_uj_;
    }

    PowerSnapshot snap = extract_job_energy(cached_tasks_, delta_uj,
                                            cached_interval_s_, cached_freqs_,
                                            job.JobID);

    double ipmi_w = ipmi_available_ ? read_ipmi_watts() : 0.0;
    snap.ipmi_power_w = ipmi_w;
    validate_against_baseline(snap);

    spdlog::debug("PowerCollector: collect(job#{}) age={:.1f}s E={:.2f}J cache={} entries",
                  job.JobID, age,
                  snap.jobs.empty() ? 0.0 : snap.jobs[0].energy_j,
                  cached_tasks_.size());
    return snap;
}

/* ── Writer parsers ─────────────────────────────────────────────────────── */

CollectDataParseFunc PowerCollector::get_writer_parser(const std::string& writer_type)
{
    if (writer_type == "FileWriter") {
        return [this](std::any data) -> std::any {
            if (!data.has_value()) {
                json err;
                err["error"] = "empty data";
                return err;
            }

            PowerSnapshot snap;
            try {
                snap = std::any_cast<PowerSnapshot>(data);
            } catch (const std::bad_any_cast& e) {
                json err;
                err["error"] = std::string("PowerCollector: bad any_cast — ") + e.what();
                return err;
            }

            json j;
            j["interval_s"]        = snap.interval_s;
            j["delta_rapl_uj"]     = snap.delta_rapl_uj;
            j["delta_rapl_j"]      = snap.delta_rapl_j;
            j["core_count"]        = snap.core_count;
            j["total_weighted_ns"] = snap.total_weighted_ns;
            j["system_overhead_j"] = snap.system_overhead_j;

            j["avg_power_w"]       = snap.avg_power_w;

            /* ── Gap 1: CPU governors ── */
            if (!snap.core_governors.empty()) {
                j["core_governors"] = snap.core_governors;
                /* summary: count unique governor modes */
                std::unordered_map<std::string, int> gov_count;
                for (const auto& g : snap.core_governors) gov_count[g]++;
                json jg = json::object();
                for (const auto& [gov, cnt] : gov_count) jg[gov] = cnt;
                j["governor_summary"] = jg;
            }

            /* ── Gap 2: Cumulative kWh ── */
            j["cumulative_kwh_total"] = snap.cumulative_kwh_total;
            if (!snap.cumulative_kwh_by_job.empty()) {
                json ck = json::object();
                for (const auto& [jid, kwh] : snap.cumulative_kwh_by_job) {
                    ck[std::to_string(jid)] = kwh;
                }
                j["cumulative_kwh_by_job"] = ck;
            }

            /* ── Gap 3: IPMI cross-validation ── */
            if (snap.ipmi_power_w > 0.0) {
                j["ipmi_power_w"] = snap.ipmi_power_w;
            }

            /* calibration reference (if loaded) */
            if (cal_ref_.valid) {
                json cr;
                cr["rapl_idle_w"]      = cal_ref_.rapl_idle_w;
                cr["ipmi_idle_w"]      = cal_ref_.ipmi_idle_w;
                cr["static_overhead_w"] = cal_ref_.static_overhead_w;
                cr["per_core_w"]       = cal_ref_.per_core_w;
                cr["source"]           = cal_ref_.source_path;
                j["calibration_ref"]  = cr;
            }

            /* average core frequency */
            if (!snap.core_freqs_mhz.empty()) {
                double avg = 0.0;
                for (double f : snap.core_freqs_mhz) avg += f;
                j["avg_core_freq_mhz"] = avg / snap.core_freqs_mhz.size();
            }

            j["jobs"] = json::array();
            for (const auto& job : snap.jobs) {
                json jj;
                jj["job_id"]            = job.job_id;
                jj["native_job_id"]     = job.native_job_id;
                jj["energy_j"]          = job.energy_j;
                jj["weighted_ns_total"] = job.weighted_ns_total;

                jj["pids"] = json::array();
                for (const auto& pp : job.pids) {
                    json jp;
                    jp["pid"]              = pp.pid;
                    jp["tgid"]             = pp.tgid;
                    jp["runtime_ns_total"] = pp.runtime_ns_total;
                    jp["weighted_ns_total"]= pp.weighted_ns_total;
                    jp["energy_j"]         = pp.energy_j;
                    jp["cpu_pct"]          = pp.cpu_pct;
                    jj["pids"].push_back(jp);
                }
                j["jobs"].push_back(jj);
            }

            return j;
        };
    }

    if (writer_type == "PrometheusExporterWriter") {
        return [](std::any data) -> std::any {
            if (!data.has_value()) {
                PrometheusExporterWriter::prometheus_job_state empty;
                return empty;
            }

            PowerSnapshot snap;
            try {
                snap = std::any_cast<PowerSnapshot>(data);
            } catch (const std::bad_any_cast& e) {
                spdlog::error("PowerCollector: Prometheus parser bad any_cast — {}", e.what());
                PrometheusExporterWriter::prometheus_job_state empty;
                return empty;
            }

            PrometheusExporterWriter::prometheus_job_state ret;
            ret.JobID = 0;  // system-scoped, no single JobID

            double total_energy = snap.delta_rapl_j;
            for (const auto& job : snap.jobs) {
                PrometheusExporterWriter::prometheus_process_state state = {};
                /* Semantic field mapping for power metrics:
                 *   job_id / pid → job identifier
                 *   name         → native_job_id (scheduler-native ID string)
                 *   cpu_usage_percent → energy_j  (attributed energy in Joules)
                 *   mem_usage_percent → energy_share (this job's fraction of total)
                 *   mem_rss_kb    → avg_power_w (J / interval_s)
                 *   mem_vm_kb     → weighted_ns (ns·MHz total for this job)
                 *   threads_cnt   → pid_count (number of processes in this job)
                 */
                state.job_id = static_cast<uint32_t>(job.job_id);
                state.pid    = static_cast<pid_t>(job.job_id);
                state.name   = job.native_job_id.empty()
                                   ? "job_" + std::to_string(job.job_id)
                                   : job.native_job_id;
                state.cpu_usage_percent = job.energy_j;
                state.mem_usage_percent = (total_energy > 0.0)
                    ? 100.0 * job.energy_j / total_energy
                    : 0.0;
                state.mem_rss_kb = (snap.interval_s > 0.0)
                    ? static_cast<int64_t>(job.energy_j / snap.interval_s)
                    : 0;
                state.mem_vm_kb   = static_cast<int64_t>(job.weighted_ns_total);
                state.threads_cnt = static_cast<int32_t>(job.pids.size());

                ret.processes_state.push_back(state);
            }

            return ret;
        };
    }

    if (writer_type == "ESWriter") {
        return [this](std::any data) -> std::any {
            /* Reuse the FileWriter JSON formatter from this instance */
            return get_writer_parser("FileWriter")(data);
        };
    }

    spdlog::warn("PowerCollector: no parser for writer type '{}'", writer_type);
    return nullptr;
}

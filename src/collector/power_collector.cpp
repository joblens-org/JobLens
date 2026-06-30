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
 * PowerCollector — eBPF sched_switch + RAPL per-job energy attribution
 *
 * Collector life-cycle:
 *   init()      → load eBPF, detect RAPL, seed last-energy counter
 *   collect()   → dump eBPF runtime maps, read  E_pkg, read CPU freqs,
 *                  compute E_job =  E_pkg × Σ(time×freq)_job / Σ(time×freq)_all
 *   deinit()    → unload eBPF
 *
 * Energy formula (user-space):
 *   For each job J:
 *     weighted_J = Σ_{p  J} Σ_{cpu} runtime_ns(p,cpu) × freq_mhz(cpu)
 *   total_weighted = interval_s × Σ_{cpu} freq_mhz(cpu)
 *   E_job = ΔE_pkg × weighted_J / total_weighted
 *   System overhead = ΔE_pkg − Σ E_job
 */
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include "collector/power_collector.hpp"

#include "core/collector_registry.hpp"
#include "core/job_registry.hpp"
#include "common/ebpf_common.hpp"
#include "common/utils.hpp"

#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <cmath>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <regex>
#include <sstream>
#include <unistd.h>

/* ── Auto-registration ─────────────────────────────────────────────────── */
AUTO_REGISTER_SYSTEM_COLLECTOR(
    PowerCollector,
    "Per-job energy attribution via eBPF sched_switch + RAPL. "
    "Tracks per-task per-CPU runtime at ns precision, reads RAPL package "
    "energy counters and per-core CPU frequency, then attributes energy "
    "proportional to frequency-weighted CPU time: "
    "E_job =  E_pkg ×  (t_c × f_c)_job /  (t_c × f_c)_all.",
    ConfigParams{
        {"freq",         "Sampling frequency in Hz (default 1.0 = 1s intervals)"},
        {"use_writers",  "Writer names for output (e.g. file_writer)"},
        {"auto_start",   "Auto-start on daemon launch (true/false, default true)"},
    }
)

using json = nlohmann::json;

/* ── RAPL helpers ───────────────────────────────────────────────────────── */

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

/* ── CPU helpers ────────────────────────────────────────────────────────── */

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

/* ── eBPF management ───────────────────────────────────────────────────── */

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

    /* Collect keys and values, then batch-delete */
    std::vector<std::vector<uint8_t>> keys;
    std::vector<u64> values;

    std::vector<uint8_t> cur_key(key_sz, 0);
    std::vector<uint8_t> next_key(key_sz, 0);

    bool first = true;
    while (true) {
        int ret;
        if (first) {
            ret = bpf_map_get_next_key(fd, nullptr, next_key.data());
            first = false;
        } else {
            ret = bpf_map_get_next_key(fd, cur_key.data(), next_key.data());
        }
        if (ret != 0) break;  // no more keys

        /* Read value */
        u64 val = 0;
        std::vector<uint8_t> val_buf(val_sz, 0);
        int lookup_ret = bpf_map_lookup_elem(fd, next_key.data(), val_buf.data());
        if (lookup_ret == 0) {
            std::memcpy(&val, val_buf.data(), sizeof(val));
        }

        keys.push_back(next_key);
        values.push_back(val);

        cur_key = next_key;
    }

    /* Delete all entries we've read */
    for (const auto& k : keys) {
        bpf_map_delete_elem(fd, k.data());
    }

    /* Parse keys into task_cpu_runtime structs */
    for (size_t i = 0; i < keys.size(); ++i) {
        struct task_cpu_key tck;
        std::memcpy(&tck, keys[i].data(), sizeof(tck));

        task_cpu_runtime tcr;
        tcr.pid_tgid   = tck.pid_tgid;
        tcr.cpu        = tck.cpu;
        tcr.runtime_ns = values[i];
        result.push_back(tcr);
    }

    spdlog::debug("PowerCollector: dumped {} task-cpu runtime entries", result.size());
    return result;
}

/* ── Energy computation ─────────────────────────────────────────────────── */

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
    std::unordered_map<uint64_t, const Job*> job_by_id;
    {
        auto jobs = JobRegistry::instance().snapshot();
        for (const auto& job : jobs) {
            job_by_id[job.JobID] = &job;
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
            auto it_jb = job_by_id.find(jid);
            if (it_jb != job_by_id.end()) {
                native_jid = it_jb->second->NativeJobID;
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

    spdlog::debug("PowerCollector: computed energy —  E={:.2f} J, jobs={}, overhead={:.2f} J, interval={:.3f} s",
                  snap.delta_rapl_j, snap.jobs.size(), snap.system_overhead_j, interval_s);

    return snap;
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

    /* Seed first RAPL reading */
    if (rapl_valid_) {
        last_rapl_uj_ = read_rapl_uj();
    }

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

CollectResult PowerCollector::collect()
{
    PowerSnapshot snap;

    if (!inited_) {
        spdlog::warn("PowerCollector: collect() called before init");
        return snap;
    }

    auto now = std::chrono::steady_clock::now();
    double interval_s = std::chrono::duration<double>(now - last_collect_ts_).count();
    last_collect_ts_ = now;

    /* Clamp interval to reasonable range (avoid giant values on first call) */
    if (interval_s > 60.0 || interval_s <= 0.0) {
        interval_s = 1.0;  // default 1s for first call or after long pause
    }

    /* ── 1. Update pid2job map ── */
    update_pid2job_map();

    /* ── 2. Read RAPL delta ── */
    uint64_t rapl_now = rapl_valid_ ? read_rapl_uj() : 0;
    uint64_t delta_uj = 0;
    if (rapl_valid_ && rapl_now >= last_rapl_uj_) {
        delta_uj = rapl_now - last_rapl_uj_;
    } else if (rapl_valid_) {
        /* Wraparound — skip this round */
        spdlog::debug("PowerCollector: RAPL counter wraparound detected, resetting baseline");
        delta_uj = 0;
    }
    last_rapl_uj_ = rapl_now;

    /* ── 3. Read CPU frequencies ── */
    auto freqs = read_cpu_freqs_mhz();

    /* ── 4. Dump eBPF task runtime ── */
    auto tasks = dump_task_cpu_time();

    /* ── 5. Compute per-job energy ── */
    snap = compute_energy(tasks, delta_uj, interval_s, freqs);

    return snap;
}

/* ── Writer parsers ─────────────────────────────────────────────────────── */

CollectDataParseFunc PowerCollector::get_writer_parser(const std::string& writer_type)
{
    if (writer_type == "FileWriter") {
        return [](std::any data) -> std::any {
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

    if (writer_type == "ESWriter") {
        return [](std::any data) -> std::any {
            /* Reuse the same JSON formatter as FileWriter */
            auto file_parser = PowerCollector().get_writer_parser("FileWriter");
            return file_parser(data);
        };
    }

    spdlog::warn("PowerCollector: no parser for writer type '{}'", writer_type);
    return nullptr;
}

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
#include "collector/new_io_usage_collector.hpp"
#include "common/mountinfo_utils.hpp"
#include "common/welford_utils.hpp"
#include "core/collector_registry.hpp"
#include "common/utils.hpp"
#include "common/ebpf_common.hpp"
#include "ebpf/job_pid_track.h"
#include "writer/prometheus_exporter_writer.hpp"
#include <sys/stat.h>
#include <unistd.h>
#include <nlohmann/json.hpp>
#include <limits>
#include <optional>
#include <sstream>
#include <unordered_map>
#include <utility>

AUTO_REGISTER_JOB_COLLECTOR(
    NewIOUsageCollector,
    "Collect IO usage aggregated by Job and file (eBPF job-level + latency distribution)",
    ConfigParams{
        {"freq", "Sampling frequency in Hz"},
        {"include_process_details", "Whether to include per-process and per-file details (true/false), default true"}
    }
)

using json = nlohmann::json;

namespace {

std::string make_file_identity_key(const std::string& mount_point, const std::string& fs_type, const std::string& path)
{
    // NUL 分隔避免字符串拼接歧义；写出时只使用 FileIOStat::path。
    std::string key;
    key.reserve(mount_point.size() + fs_type.size() + path.size() + 2);
    key.append(mount_point);
    key.push_back('\0');
    key.append(fs_type);
    key.push_back('\0');
    key.append(path);
    return key;
}

bool add_saturating_u64(u64& target, u64 value)
{
    if (value > std::numeric_limits<u64>::max() - target) {
        target = std::numeric_limits<u64>::max();
        return true;
    }
    target += value;
    return false;
}

struct IoAggregationStatus {
    bool byte_saturated{false};
    WelfordUtils::MergeStatus welford;

    void absorb(const WelfordUtils::MergeStatus& status) noexcept
    {
        welford.absorb(status);
    }

    bool needs_warning() const noexcept
    {
        return byte_saturated || !welford.ok();
    }
};

void merge_rw_stat_into_io(IoCounters& target, const rw_stat& source, IoAggregationStatus& status)
{
    status.byte_saturated = add_saturating_u64(target.rchar, source.read_bytes) || status.byte_saturated;
    status.byte_saturated = add_saturating_u64(target.wchar, source.write_bytes) || status.byte_saturated;

    WelfordUtils::Aggregate read_target{target.syscr, target.read_mean, target.read_variance};
    const WelfordUtils::Aggregate read_source{source.read_count, source.read_mean, source.read_variance};
    status.absorb(WelfordUtils::merge_into(read_target, read_source));
    target.syscr = read_target.count;
    target.read_mean = read_target.mean;
    target.read_variance = read_target.m2;

    WelfordUtils::Aggregate write_target{target.syscw, target.write_mean, target.write_variance};
    const WelfordUtils::Aggregate write_source{source.write_count, source.write_mean, source.write_variance};
    status.absorb(WelfordUtils::merge_into(write_target, write_source));
    target.syscw = write_target.count;
    target.write_mean = write_target.mean;
    target.write_variance = write_target.m2;
}

json io_counters_to_json(const IoCounters& io)
{
    return {
        {"rchar", io.rchar}, {"wchar", io.wchar},
        {"syscr", io.syscr}, {"syscw", io.syscw},
        {"read_mean", io.read_mean}, {"write_mean", io.write_mean},
        {"read_variance", io.read_variance}, {"write_variance", io.write_variance},
        {"rchar_speed", io.rchar_speed}, {"wchar_speed", io.wchar_speed}
    };
}

}

bool NewIOUsageCollector::init(const json& cfg){
    include_process_details = !cfg.contains("include_process_details") ||
        cfg["include_process_details"].get<std::string>() == "true";

    if (!init_ebpf()){
        spdlog::error("NewIOUsageCollector: init ebpf error");
        deinit_ebpf();
        return false;
    }
    return true;
}

bool NewIOUsageCollector::init_ebpf(){
    auto path = EbpfCommon::resolve_bpf_obj("job_io_new.bpf.o");
    if (path.empty()) return false;
    bpf_obj_ = EbpfCommon::load_bpf_obj_pinned(path, bpf_links_, JOBLENS_BPF_PIN_ROOT);
    return bpf_obj_ != nullptr;
}

void NewIOUsageCollector::deinit_ebpf(){
    EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
}

void NewIOUsageCollector::deinit() noexcept{
    deinit_ebpf();
    dump_keys_.clear();
    dump_vals_.clear();
    known_pids_.clear();
    last_job_time_.clear();
    last_proc_io_.clear();
    last_file_io_.clear();
    last_file_proc_io_.clear();
    last_job_io_.clear();
    last_job_latency_.clear();
    spdlog::info("NewIOUsageCollector deinit");
}

void NewIOUsageCollector::refresh_dump_cache_if_needed(){
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_dump_time_).count();
    if (elapsed < DUMP_TTL_MS && !dump_keys_.empty()) return;

    EbpfCommon::lookup_hashmap_batch<job_pid_fd_key, rw_stat>(
        bpf_obj_, jobfdstat_map_name, dump_keys_, dump_vals_);
    last_dump_time_ = now;
}

// 清理已死且已输出过的短命进程的 eBPF 条目（保证"至少输出一次"后延迟清理）
void NewIOUsageCollector::cleanup_dead_pids(uint64_t job_id){
    auto known_it = known_pids_.find(job_id);
    if (known_it == known_pids_.end()) return;

    std::vector<pid_t> to_cleanup;
    for (const auto& [pid, st] : known_it->second){
        if (!st.alive && st.output_count >= 1){
            to_cleanup.push_back(pid);
        }
    }
    bool cache_changed = false;
    for (pid_t pid : to_cleanup){
        // 删除该 pid 在本 job 下所有 job_fd_stat 条目（本采集器私有 map）。
        // pid2job 为共享 map, 其删除由内核 exit hook 统一负责, 此处不再触碰。
        for (size_t i = 0; i < dump_keys_.size(); ++i){
            if (dump_keys_[i].job_id == job_id && dump_keys_[i].pid == static_cast<u32>(pid)){
                EbpfCommon::delete_hashmap_elem<job_pid_fd_key, rw_stat>(
                    bpf_obj_, jobfdstat_map_name, dump_keys_[i]);
                cache_changed = true;
            }
        }
        known_it->second.erase(pid);
    }
    if (known_it->second.empty()){
        known_pids_.erase(known_it);
    }
    if (cache_changed){
        dump_keys_.clear();
        dump_vals_.clear();
        last_dump_time_ = std::chrono::steady_clock::time_point{};
    }
}

CollectResult NewIOUsageCollector::collect(const Job& job){
    JobIOStat result;
    result.job_id = job.JobID;

    // pid2job / cgroup2job 由 JobRegistry + 内核 fork/exit hook 统一维护, 本处只读。

    // 2. Job 级总量（权威，含短命进程）
    if (auto js = EbpfCommon::lookup_hashmap_elem<uint64_t, rw_stat>(bpf_obj_, jobstat_map_name, job.JobID)){
        result.job_total.rchar = js->read_bytes;
        result.job_total.wchar = js->write_bytes;
        result.job_total.syscr = js->read_count;
        result.job_total.syscw = js->write_count;
        result.job_total.read_mean = js->read_mean;
        result.job_total.write_mean = js->write_mean;
        result.job_total.read_variance = js->read_variance;
        result.job_total.write_variance = js->write_variance;
    }

    // 3. 时延直方图（64 桶 × 读/写 逐桶查询）
    for (uint32_t b = 0; b < LATENCY_BUCKETS; ++b){
        struct latency_key rk = {.job_id = job.JobID, .bucket = b, .is_write = 0};
        if (auto v = EbpfCommon::lookup_hashmap_elem<latency_key, u64>(bpf_obj_, latency_map_name, rk))
            result.job_latency.read_hist[b] = *v;
        struct latency_key wk = {.job_id = job.JobID, .bucket = b, .is_write = 1};
        if (auto v = EbpfCommon::lookup_hashmap_elem<latency_key, u64>(bpf_obj_, latency_map_name, wk))
            result.job_latency.write_hist[b] = *v;
    }

    // 4. 刷新周期缓存 + 聚合进程/文件
    refresh_dump_cache_if_needed();
    std::unordered_map<pid_t, std::optional<MountInfoUtils::MountTable>> mount_tables;
    IoAggregationStatus io_status;
    for (size_t i = 0; i < dump_keys_.size(); ++i){
        if (dump_keys_[i].job_id != job.JobID) continue;
        pid_t pid = static_cast<pid_t>(dump_keys_[i].pid);
        u32 fd = dump_keys_[i].fd;
        const rw_stat& s = dump_vals_[i];

        // 进程级聚合（含短命进程）
        auto& proc = result.processes[pid];
        proc.pid = pid;
        merge_rw_stat_into_io(proc.io, s, io_status);
        proc.alive = Utils::is_process_running(pid);
        proc.source = proc.alive ? "alive" : "ephemeral";

        // 文件级聚合（仅存活进程能 fd→path；短命进程死后无法反查文件路径）。
        // fd→path 反查（mountinfo/readlink/stat）是明细路径的主要开销，受开关控制。
        if (include_process_details && proc.alive){
            auto mount_table_it = mount_tables.find(pid);
            if (mount_table_it == mount_tables.end()) {
                mount_table_it = mount_tables.emplace(pid, MountInfoUtils::read_for_pid(pid)).first;
            }
            std::string fdpath = "/proc/" + std::to_string(pid) + "/fd/" + std::to_string(fd);
            char buf[512];
            ssize_t n = ::readlink(fdpath.c_str(), buf, sizeof(buf) - 1);
            if (n > 0){
                buf[n] = '\0';
                std::string path(buf);
                if (!path.empty() && path[0] == '/'){
                    struct stat stb;
                    if (::stat(path.c_str(), &stb) == 0 && (S_ISREG(stb.st_mode) || S_ISBLK(stb.st_mode))){
                        std::string mount_point;
                        std::string fs_type;
                        if (mount_table_it->second) {
                            if (auto mount_entry = MountInfoUtils::resolve(*mount_table_it->second, path)) {
                                mount_point = mount_entry->mount_point;
                                fs_type = mount_entry->fs_type;
                            }
                        }
                        const auto file_key = make_file_identity_key(mount_point, fs_type, path);
                        auto& finfo = result.files[file_key];
                        finfo.path = path;
                        finfo.mount_point = mount_point;
                        finfo.fs_type = fs_type;
                        finfo.pos = static_cast<unsigned long long>(stb.st_size);
                        merge_rw_stat_into_io(finfo.total, s, io_status);
                        auto& pfinfo = finfo.processes[pid];
                        pfinfo.pid = pid;
                        pfinfo.alive = true;
                        merge_rw_stat_into_io(pfinfo.io, s, io_status);
                    }
                }
            }
        }
    }
    if (io_status.needs_warning()){
        spdlog::warn(
            "NewIOUsageCollector: invalid or saturated fd aggregate while collecting job_id={} invalid_m2={} count_saturated={} mean_saturated={} m2_saturated={} byte_saturated={}",
            job.JobID,
            io_status.welford.invalid_input,
            io_status.welford.count_saturated,
            io_status.welford.mean_saturated,
            io_status.welford.m2_saturated,
            io_status.byte_saturated);
    }

    // 5. 更新短命进程状态 + 延迟清理
    auto& job_known_pids = known_pids_[job.JobID];
    for (const auto& [pid, proc] : result.processes){
        auto& st = job_known_pids[pid];
        st.output_count++;
        st.alive = proc.alive;
    }
    cleanup_dead_pids(job.JobID);

    // 清空需在状态机/清理之后（它们依赖 processes 的存活信息）；
    // 第 6 步的明细差分与基线更新随空 map 自然跳过
    if (!include_process_details){
        result.processes.clear();
        result.files.clear();
    }

    // 6. speed 差分（Job 级）
    auto now = std::chrono::steady_clock::now();
    auto it_time = last_job_time_.find(job.JobID);
    if (it_time != last_job_time_.end()){
        double period = std::chrono::duration<double>(now - it_time->second).count();
        result.collect_period = period;
        auto it_io = last_job_io_.find(job.JobID);
        if (it_io != last_job_io_.end() && period > 0){
            result.job_total.rchar_speed = (result.job_total.rchar >= it_io->second.rchar)
                ? (result.job_total.rchar - it_io->second.rchar) / period : 0;
            result.job_total.wchar_speed = (result.job_total.wchar >= it_io->second.wchar)
                ? (result.job_total.wchar - it_io->second.wchar) / period : 0;
        }

        if (period > 0){
            auto proc_snapshot_it = last_proc_io_.find(job.JobID);
            auto file_snapshot_it = last_file_io_.find(job.JobID);
            auto file_proc_snapshot_it = last_file_proc_io_.find(job.JobID);
            for (auto& [pid, proc] : result.processes){
                if (proc_snapshot_it != last_proc_io_.end()){
                    auto it_proc_io = proc_snapshot_it->second.find(pid);
                    if (it_proc_io == proc_snapshot_it->second.end()) continue;
                    proc.io.rchar_speed = (proc.io.rchar >= it_proc_io->second.rchar)
                        ? (proc.io.rchar - it_proc_io->second.rchar) / period : 0;
                    proc.io.wchar_speed = (proc.io.wchar >= it_proc_io->second.wchar)
                        ? (proc.io.wchar - it_proc_io->second.wchar) / period : 0;
                }
            }

            for (auto& [file_key, file] : result.files){
                if (file_snapshot_it != last_file_io_.end()){
                    auto it_file_io = file_snapshot_it->second.find(file_key);
                    if (it_file_io != file_snapshot_it->second.end()){
                        file.total.rchar_speed = (file.total.rchar >= it_file_io->second.rchar)
                            ? (file.total.rchar - it_file_io->second.rchar) / period : 0;
                        file.total.wchar_speed = (file.total.wchar >= it_file_io->second.wchar)
                            ? (file.total.wchar - it_file_io->second.wchar) / period : 0;
                    }
                }
                if (file_proc_snapshot_it == last_file_proc_io_.end()) continue;
                auto file_proc_it = file_proc_snapshot_it->second.find(file_key);
                if (file_proc_it == file_proc_snapshot_it->second.end()) continue;
                for (auto& [pid, proc] : file.processes){
                    auto it_file_proc_io = file_proc_it->second.find(pid);
                    if (it_file_proc_io != file_proc_it->second.end()){
                        proc.io.rchar_speed = (proc.io.rchar >= it_file_proc_io->second.rchar)
                            ? (proc.io.rchar - it_file_proc_io->second.rchar) / period : 0;
                        proc.io.wchar_speed = (proc.io.wchar >= it_file_proc_io->second.wchar)
                            ? (proc.io.wchar - it_file_proc_io->second.wchar) / period : 0;
                    }
                }
            }
        }
    }
    last_job_time_[job.JobID] = now;
    last_job_io_[job.JobID] = result.job_total;

    ProcessIoSnapshot process_snapshot;
    for (const auto& [pid, proc] : result.processes){
        process_snapshot[pid] = proc.io;
    }
    last_proc_io_[job.JobID] = std::move(process_snapshot);

    FileIoSnapshot file_snapshot;
    FileProcessIoSnapshot file_process_snapshot;
    for (const auto& [file_key, file] : result.files){
        file_snapshot[file_key] = file.total;
        auto& process_snapshot_for_file = file_process_snapshot[file_key];
        for (const auto& [pid, proc] : file.processes){
            process_snapshot_for_file[pid] = proc.io;
        }
    }
    last_file_io_[job.JobID] = std::move(file_snapshot);
    last_file_proc_io_[job.JobID] = std::move(file_process_snapshot);

    return result;
}

CollectDataParseFunc NewIOUsageCollector::get_writer_parser(const std::string& writer_type){
    if (writer_type.compare("ESWriter") == 0){
        return [](std::any data) -> std::any {
            if (!data.has_value()){
                json j;
                j["error"] = "empty data";
                return j;
            }
            auto s = std::any_cast<JobIOStat>(data);
            json j;
            j["job_id"] = s.job_id;
            j["collect_period"] = s.collect_period;
            j["job_total"] = io_counters_to_json(s.job_total);
            j["job_latency"] = {
                {"read_hist", std::vector<u64>(std::begin(s.job_latency.read_hist), std::end(s.job_latency.read_hist))},
                {"write_hist", std::vector<u64>(std::begin(s.job_latency.write_hist), std::end(s.job_latency.write_hist))}
            };
            j["files"] = json::array();
            for (const auto& file_entry : s.files){
                const auto& f = file_entry.second;
                json fj;
                fj["path"] = f.path;
                fj["mount_point"] = f.mount_point;
                fj["fs_type"] = f.fs_type;
                fj["pos"] = f.pos;
                fj["total"] = io_counters_to_json(f.total);
                fj["processes"] = json::array();
                for (const auto& [pid, p] : f.processes){
                    json pj = io_counters_to_json(p.io);
                    pj["pid"] = pid;
                    pj["alive"] = p.alive;
                    fj["processes"].push_back(pj);
                }
                j["files"].push_back(fj);
            }
            j["processes"] = json::array();
            for (const auto& [pid, p] : s.processes){
                json pj = io_counters_to_json(p.io);
                pj["pid"] = pid;
                pj["source"] = p.source;
                pj["alive"] = p.alive;
                j["processes"].push_back(pj);
            }
            return j;
        };
    }

    if (writer_type.compare("FileWriter") == 0){
        return [](std::any data) -> std::any {
            if (!data.has_value()){
                return std::string("NewIOUsageCollector error=empty_data\n");
            }
            auto s = std::any_cast<JobIOStat>(data);
            std::ostringstream out;
            out << "NewIOUsageCollector job_id=" << s.job_id
                << " collect_period=" << s.collect_period
                << " rchar=" << s.job_total.rchar
                << " wchar=" << s.job_total.wchar
                << " syscr=" << s.job_total.syscr
                << " syscw=" << s.job_total.syscw
                << " read_mean=" << s.job_total.read_mean
                << " write_mean=" << s.job_total.write_mean
                << " read_variance=" << s.job_total.read_variance
                << " write_variance=" << s.job_total.write_variance
                << " rchar_speed=" << s.job_total.rchar_speed
                << " wchar_speed=" << s.job_total.wchar_speed
                << '\n';
            for (size_t i = 0; i < LATENCY_BUCKETS; ++i){
                if (s.job_latency.read_hist[i] != 0 || s.job_latency.write_hist[i] != 0){
                    out << "NewIOUsageCollector latency_bucket"
                        << " job_id=" << s.job_id
                        << " bucket=" << i
                        << " read_count=" << s.job_latency.read_hist[i]
                        << " write_count=" << s.job_latency.write_hist[i]
                        << '\n';
                }
            }
            for (const auto& [pid, p] : s.processes){
                out << "NewIOUsageCollector process"
                    << " job_id=" << s.job_id
                    << " pid=" << pid
                    << " source=" << p.source
                    << " alive=" << p.alive
                    << " rchar=" << p.io.rchar
                    << " wchar=" << p.io.wchar
                    << " syscr=" << p.io.syscr
                    << " syscw=" << p.io.syscw
                    << " read_mean=" << p.io.read_mean
                    << " write_mean=" << p.io.write_mean
                    << " read_variance=" << p.io.read_variance
                    << " write_variance=" << p.io.write_variance
                    << " rchar_speed=" << p.io.rchar_speed
                    << " wchar_speed=" << p.io.wchar_speed
                    << '\n';
            }
            for (const auto& file_entry : s.files){
                const auto& f = file_entry.second;
                out << "NewIOUsageCollector file"
                    << " job_id=" << s.job_id
                    << " path=" << f.path
                    << " mount_point=" << f.mount_point
                    << " fs_type=" << f.fs_type
                    << " pos=" << f.pos
                    << " rchar=" << f.total.rchar
                    << " wchar=" << f.total.wchar
                    << " syscr=" << f.total.syscr
                    << " syscw=" << f.total.syscw
                    << " read_mean=" << f.total.read_mean
                    << " write_mean=" << f.total.write_mean
                    << " read_variance=" << f.total.read_variance
                    << " write_variance=" << f.total.write_variance
                    << " rchar_speed=" << f.total.rchar_speed
                    << " wchar_speed=" << f.total.wchar_speed
                    << '\n';
                for (const auto& [pid, p] : f.processes){
                    out << "NewIOUsageCollector file_process"
                        << " job_id=" << s.job_id
                        << " path=" << f.path
                        << " pid=" << pid
                        << " alive=" << p.alive
                        << " rchar=" << p.io.rchar
                        << " wchar=" << p.io.wchar
                        << " syscr=" << p.io.syscr
                        << " syscw=" << p.io.syscw
                        << " read_mean=" << p.io.read_mean
                        << " write_mean=" << p.io.write_mean
                        << " read_variance=" << p.io.read_variance
                        << " write_variance=" << p.io.write_variance
                        << " rchar_speed=" << p.io.rchar_speed
                        << " wchar_speed=" << p.io.wchar_speed
                        << '\n';
                }
            }
            return out.str();
        };
    }

    if (writer_type.compare("PrometheusExporterWriter") == 0){
        return [](std::any data) -> std::any {
            PrometheusExporterWriter::prometheus_job_state ret;
            if (!data.has_value()){
                ret.JobID = 0;
                return ret;
            }
            auto s = std::any_cast<JobIOStat>(data);
            ret.JobID = static_cast<int>(s.job_id);
            ret.io_rchar_total = static_cast<int64_t>(s.job_total.rchar);
            ret.io_wchar_total = static_cast<int64_t>(s.job_total.wchar);
            ret.io_rchar_per_sec = s.job_total.rchar_speed;
            ret.io_wchar_per_sec = s.job_total.wchar_speed;
            PrometheusExporterWriter::prometheus_process_state jobstate;
            jobstate.pid = 0;
            jobstate.io_read_bytes_total = static_cast<int64_t>(s.job_total.rchar);
            jobstate.io_write_bytes_total = static_cast<int64_t>(s.job_total.wchar);
            jobstate.io_read_bytes_per_sec = s.job_total.rchar_speed;
            jobstate.io_write_bytes_per_sec = s.job_total.wchar_speed;
            ret.processes_state.push_back(jobstate);
            for (const auto& [pid, p] : s.processes){
                PrometheusExporterWriter::prometheus_process_state ps;
                ps.pid = pid;
                ps.io_read_bytes_total = static_cast<int64_t>(p.io.rchar);
                ps.io_write_bytes_total = static_cast<int64_t>(p.io.wchar);
                ret.processes_state.push_back(ps);
            }
            return ret;
        };
    }

    return nullptr;
}

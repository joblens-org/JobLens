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
#include "core/collector_registry.hpp"
#include "common/utils.hpp"
#include "common/ebpf_common.hpp"
#include "writer/prometheus_exporter_writer.hpp"
#include <sys/stat.h>
#include <unistd.h>
#include <nlohmann/json.hpp>
#include <sstream>

AUTO_REGISTER_JOB_COLLECTOR(
    NewIOUsageCollector,
    "Collect IO usage aggregated by Job and file (eBPF job-level + latency distribution)",
    ConfigParams{
        {"freq", "Sampling frequency in Hz"}
    }
)

using json = nlohmann::json;

// Slurm 用 cgroup_path，Condor 用 slots_cgroup_path；Common job 无 cgroup 返回空串。
static std::string get_job_cgroup_path(const Job& job){
    try{
        if (job.subtype == JobSubType::Slurm)
            return std::get<SlurmJobAttr>(job.sub_attr).cgroup_path;
        if (job.subtype == JobSubType::Condor)
            return std::get<CondorJobAttr>(job.sub_attr).slots_cgroup_path;
    }catch(const std::bad_variant_access&){
    }
    return {};
}

bool NewIOUsageCollector::init(const json& cfg){
    (void)cfg;
    if (!init_ebpf()){
        spdlog::error("NewIOUsageCollector: init ebpf error");
        deinit_ebpf();
        return false;
    }
    return true;
}

bool NewIOUsageCollector::init_ebpf(){
    auto path = Utils::JobLensRootDir() + bpf_o_path;
    bpf_obj_ = EbpfCommon::load_bpf_obj(path, bpf_links_);
    return bpf_obj_ != nullptr;
}

void NewIOUsageCollector::deinit_ebpf(){
    EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
}

void NewIOUsageCollector::deinit() noexcept{
    deinit_ebpf();
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
void NewIOUsageCollector::cleanup_dead_pids(){
    std::vector<pid_t> to_cleanup;
    for (const auto& [pid, st] : known_pids_){
        if (!st.alive && st.output_count >= 1){
            to_cleanup.push_back(pid);
        }
    }
    for (pid_t pid : to_cleanup){
        // 删除该 pid 在本 job 下所有 job_fd_stat 条目
        for (size_t i = 0; i < dump_keys_.size(); ++i){
            if (dump_keys_[i].pid == pid){
                EbpfCommon::delete_hashmap_elem<job_pid_fd_key, rw_stat>(
                    bpf_obj_, jobfdstat_map_name, dump_keys_[i]);
            }
        }
        EbpfCommon::delete_hashmap_elem<pid_t, uint64_t>(bpf_obj_, pid2jobid_map_name, pid);
        known_pids_.erase(pid);
    }
}

CollectResult NewIOUsageCollector::collect(const Job& job){
    JobIOStat result;
    result.job_id = job.JobID;

    // 1. 更新内核过滤表（pid + cgroup）
    EbpfCommon::update_pid_in_kernel(bpf_obj_, pid2jobid_map_name, job.JobID, job.JobPIDs);
    auto cg_path = get_job_cgroup_path(job);
    if (!cg_path.empty()){
        if (auto cgid = EbpfCommon::cgroup_path_to_id(cg_path)){
            if (!EbpfCommon::update_cgroup_in_kernel(bpf_obj_, cgroup2jobid_map_name, job.JobID, {*cgid})){
                spdlog::warn("NewIOUsageCollector: update cgroup in kernel error, job={} path={}", job.JobID, cg_path);
            }
        }
    }

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
    for (size_t i = 0; i < dump_keys_.size(); ++i){
        if (dump_keys_[i].job_id != job.JobID) continue;
        pid_t pid = dump_keys_[i].pid;
        u32 fd = dump_keys_[i].fd;
        const rw_stat& s = dump_vals_[i];

        // 进程级聚合（含短命进程）
        auto& proc = result.processes[pid];
        proc.pid = pid;
        proc.io.rchar += s.read_bytes;
        proc.io.wchar += s.write_bytes;
        proc.io.syscr += s.read_count;
        proc.io.syscw += s.write_count;
        proc.alive = Utils::is_process_running(pid);
        proc.source = proc.alive ? "alive" : "ephemeral";

        // 文件级聚合（仅存活进程能 fd→path；短命进程死后无法反查文件路径）
        if (proc.alive){
            std::string fdpath = "/proc/" + std::to_string(pid) + "/fd/" + std::to_string(fd);
            char buf[512];
            ssize_t n = ::readlink(fdpath.c_str(), buf, sizeof(buf) - 1);
            if (n > 0){
                buf[n] = '\0';
                std::string path(buf);
                if (!path.empty() && path[0] == '/'){
                    struct stat stb;
                    if (::stat(path.c_str(), &stb) == 0 && (S_ISREG(stb.st_mode) || S_ISBLK(stb.st_mode))){
                        auto& finfo = result.files[path];
                        finfo.path = path;
                        finfo.pos = static_cast<unsigned long long>(stb.st_size);
                        finfo.total.rchar += s.read_bytes;
                        finfo.total.wchar += s.write_bytes;
                        finfo.total.syscr += s.read_count;
                        finfo.total.syscw += s.write_count;
                        auto& pfinfo = finfo.processes[pid];
                        pfinfo.pid = pid;
                        pfinfo.alive = true;
                        pfinfo.io.rchar += s.read_bytes;
                        pfinfo.io.wchar += s.write_bytes;
                        pfinfo.io.syscr += s.read_count;
                        pfinfo.io.syscw += s.write_count;
                    }
                }
            }
        }
    }

    // 5. 更新短命进程状态 + 延迟清理
    for (const auto& [pid, proc] : result.processes){
        auto& st = known_pids_[pid];
        st.job_id = job.JobID;
        st.output_count++;
        st.alive = proc.alive;
    }
    cleanup_dead_pids();

    // 6. speed 差分（Job 级）
    auto now = std::chrono::steady_clock::now();
    auto it_time = last_job_time_.find(job.JobID);
    if (it_time != last_job_time_.end()){
        double period = std::chrono::duration_cast<std::chrono::seconds>(now - it_time->second).count();
        result.collect_period = period;
        auto it_io = last_job_io_.find(job.JobID);
        if (it_io != last_job_io_.end() && period > 0){
            result.job_total.rchar_speed = (result.job_total.rchar >= it_io->second.rchar)
                ? (result.job_total.rchar - it_io->second.rchar) / period : 0;
            result.job_total.wchar_speed = (result.job_total.wchar >= it_io->second.wchar)
                ? (result.job_total.wchar - it_io->second.wchar) / period : 0;
        }

        if (period > 0){
            for (auto& [pid, proc] : result.processes){
                auto it_proc_io = last_proc_io_.find(pid);
                if (it_proc_io != last_proc_io_.end()){
                    proc.io.rchar_speed = (proc.io.rchar >= it_proc_io->second.rchar)
                        ? (proc.io.rchar - it_proc_io->second.rchar) / period : 0;
                    proc.io.wchar_speed = (proc.io.wchar >= it_proc_io->second.wchar)
                        ? (proc.io.wchar - it_proc_io->second.wchar) / period : 0;
                }
            }

            for (auto& [path, file] : result.files){
                auto it_file_io = last_file_io_.find(path);
                if (it_file_io != last_file_io_.end()){
                    file.total.rchar_speed = (file.total.rchar >= it_file_io->second.rchar)
                        ? (file.total.rchar - it_file_io->second.rchar) / period : 0;
                    file.total.wchar_speed = (file.total.wchar >= it_file_io->second.wchar)
                        ? (file.total.wchar - it_file_io->second.wchar) / period : 0;
                }
                for (auto& [pid, proc] : file.processes){
                    auto key = path + "#" + std::to_string(pid);
                    auto it_file_proc_io = last_file_io_.find(key);
                    if (it_file_proc_io != last_file_io_.end()){
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
    for (const auto& [pid, proc] : result.processes){
        last_proc_io_[pid] = proc.io;
    }
    for (const auto& [path, file] : result.files){
        last_file_io_[path] = file.total;
        for (const auto& [pid, proc] : file.processes){
            last_file_io_[path + "#" + std::to_string(pid)] = proc.io;
        }
    }

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
            j["job_total"] = {
                {"rchar", s.job_total.rchar}, {"wchar", s.job_total.wchar},
                {"syscr", s.job_total.syscr}, {"syscw", s.job_total.syscw},
                {"rchar_speed", s.job_total.rchar_speed}, {"wchar_speed", s.job_total.wchar_speed}
            };
            j["job_latency"] = {
                {"read_hist", std::vector<u64>(std::begin(s.job_latency.read_hist), std::end(s.job_latency.read_hist))},
                {"write_hist", std::vector<u64>(std::begin(s.job_latency.write_hist), std::end(s.job_latency.write_hist))}
            };
            j["files"] = json::array();
            for (const auto& [path, f] : s.files){
                json fj;
                fj["path"] = path;
                fj["mount_point"] = f.mount_point;
                fj["fs_type"] = f.fs_type;
                fj["pos"] = f.pos;
                fj["total"] = {{"rchar", f.total.rchar}, {"wchar", f.total.wchar},
                               {"syscr", f.total.syscr}, {"syscw", f.total.syscw},
                               {"rchar_speed", f.total.rchar_speed}, {"wchar_speed", f.total.wchar_speed}};
                fj["processes"] = json::array();
                for (const auto& [pid, p] : f.processes){
                    fj["processes"].push_back({
                        {"pid", pid}, {"alive", p.alive},
                        {"rchar", p.io.rchar}, {"wchar", p.io.wchar},
                        {"rchar_speed", p.io.rchar_speed}, {"wchar_speed", p.io.wchar_speed}
                    });
                }
                j["files"].push_back(fj);
            }
            j["processes"] = json::array();
            for (const auto& [pid, p] : s.processes){
                j["processes"].push_back({
                    {"pid", pid}, {"source", p.source}, {"alive", p.alive},
                    {"rchar", p.io.rchar}, {"wchar", p.io.wchar},
                    {"syscr", p.io.syscr}, {"syscw", p.io.syscw},
                    {"rchar_speed", p.io.rchar_speed}, {"wchar_speed", p.io.wchar_speed}
                });
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
                << " rchar=" << s.job_total.rchar
                << " wchar=" << s.job_total.wchar
                << " syscr=" << s.job_total.syscr
                << " syscw=" << s.job_total.syscw
                << " rchar_speed=" << s.job_total.rchar_speed
                << " wchar_speed=" << s.job_total.wchar_speed
                << '\n';
            for (const auto& [path, f] : s.files){
                out << "NewIOUsageCollector file path=" << path
                    << " rchar=" << f.total.rchar
                    << " wchar=" << f.total.wchar
                    << " rchar_speed=" << f.total.rchar_speed
                    << " wchar_speed=" << f.total.wchar_speed
                    << '\n';
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

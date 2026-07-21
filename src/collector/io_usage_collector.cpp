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
#include "collector/io_usage_collector.hpp"
#include <chrono>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>
#include <nlohmann/json.hpp>
#include "common/utils.hpp"
#include "common/ebpf_common.hpp"
#include "core/collector_registry.hpp"
#include "writer/prometheus_exporter_writer.hpp"
// #include "ebpf/job_fd_basic.h"

AUTO_REGISTER_JOB_COLLECTOR(
    IOUsageCollector, 
    "Collect IO usage statistics from /proc/[pid]/io",
    ConfigParams{
        {"freq", "Sampling frequency in Hz, e.g., 0.2 for once every 5 seconds"},
        {"summary", "Whether to summarize data across all processes (true/false), default false"},
        {"use_ebpf", "Whether to use ebpf for collecting file io data"}
    }
)

using json = nlohmann::json;
using namespace std::chrono;

bool IOUsageCollector::init_ebpf(){
    auto path = Utils::JobLensRootDir() + bpf_o_path;
    // event_front_ = std::make_unique<std::queue<event>>();
    // event_back_ = std::make_unique<std::queue<event>>();
    bpf_obj_ = EbpfCommon::load_bpf_obj(path, bpf_links_);
    ring_buffer_sample_fn fn  = [](void *ctx, void *data, size_t size){
        spdlog::debug("IOUsageCollector: ebpf got io event");
        // auto ptr = static_cast<IOUsageCollector*>(ctx);
        // auto event_ptr = static_cast<struct event*>(data);
        // auto event_copy = *event_ptr;
        // {
        //     ptr->event_swap_mtx_.lock();
        //     ptr->event_front_->push(event_copy);
        //     ptr->event_swap_mtx_.unlock();
        // }
        return 0;
    };

    bpf_rb_ = EbpfCommon::new_rb(bpf_obj_, rb_name, fn, static_cast<void*>(this));
    if(!bpf_obj_ && !bpf_rb_){
        return false;
    }
    // poll_thread = std::make_unique<std::thread>([this](){
    //     spdlog::debug("IOUsageCollector: ebpf poll thread started");
    //     while(polling_running){
    //         int err = ring_buffer__poll(bpf_rb_, 100 /* ms */);
    //         if (err == -EINTR){
    //             spdlog::error("JobRegistry: error EINTR");
    //             break;
    //         }
    //         if (err < 0){
    //             spdlog::error("JobRegistry: error {}", err);
    //         }
    //     }
    // });
    return true;
}

void IOUsageCollector::deinit_ebpf(){
    for(auto& kv: pidfd_stat_map){
        pid_fd_key key = {.pid=kv.first.pid, .fd=kv.first.fd};
        EbpfCommon::delete_hashmap_elem<pid_t, uint64_t>(bpf_obj_, pid2jobid_map_name, kv.first.pid);
        EbpfCommon::delete_hashmap_elem<pid_fd_key, rw_stat>(bpf_obj_, pid2fdstat_map_name, key);
    }
    pidfd_stat_map.clear();
    EbpfCommon::free_rb(bpf_rb_);
    EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
    spdlog::info("IOUsageCollector: deinit_ebpf");
}

// std::optional<IOUsageCollector::user_rw_stat> IOUsageCollector::get_fd_stat(pid_t pid, uint32_t fd)
// {
    
//     event_swap_mtx_.lock(); // 使用自旋锁，减轻损耗
//     event_front_.swap(event_back_);
//     event_swap_mtx_.unlock();

//     while(!event_back_->empty()){
//         auto event = event_back_->front();
//         event_back_->pop();
//         auto& stat = pidfd_stat_map[{event.pid, event.fd}];
//         if (event.is_write){
//             stat.write_bytes += event.cnt;
//             // stat.write_time = event.ktimestamp;
//         }else{
//             stat.read_bytes += event.cnt;
//             // stat.read_time = event.ktimestamp;
//         }
//     }

//     user_pidfd_key usr_key = { .pid = static_cast<u32>(pid), .fd = fd };
//     user_rw_stat& usr = pidfd_stat_map[usr_key]; // 上次用户态记录
//     auto now = std::chrono::steady_clock::now();
//     // 按照调用作为采样时间点计算速度
//     usr.read_speed = usr.read_bytes - usr.last_read_bytes / (std::chrono::duration<double>(now - usr.last_read_time).count());
//     usr.write_speed = usr.write_bytes - usr.last_write_bytes / (std::chrono::duration<double>(now - usr.last_write_time).count());
//     usr.last_read_bytes = usr.read_bytes;
//     usr.last_write_bytes = usr.write_bytes;
//     usr.last_read_time = now;
//     usr.last_write_time = now;
    
//     return usr;
// }

std::optional<IOUsageCollector::user_rw_stat> IOUsageCollector::get_fd_stat(pid_t pid, uint32_t fd)
{
    struct pid_fd_key key = { .pid = static_cast<u32>(pid), .fd = fd };

    /* 1. 从 eBPF map 里取当前内核统计值 */
    spdlog::debug("IOUsageCollector: get_fd_stat called for pid:{} fd:{}", pid, fd);
    auto ret = EbpfCommon::lookup_hashmap_elem<pid_fd_key, rw_stat>(
        bpf_obj_, pid2fdstat_map_name, key);
    if (ret == std::nullopt) {
        // spdlog::warn("IOUsageCollector: lookup_hashmap_elem in get_fd_stat failed, pid={} fd={}", key.pid, key.fd);
        return std::nullopt;
    } 
    auto& stat = ret.value();          // 当前内核记录
    user_pidfd_key usr_key = { .pid = static_cast<u32>(pid), .fd = fd };
    auto& usr = pidfd_stat_map[usr_key]; // 上次用户态记录

    // double dt_ns = static_cast<double>(stat.read_ktimestamp - usr.read_ktimestamp);
    // double dt_s  = dt_ns / 1'000'000'000.0;
    // if(dt_ns >= 0){
    //     if (usr.read_ktimestamp == 0) {
    //         usr.read_speed  = 0;
    //     }else{
    //         int64_t delta_read  = static_cast<int64_t>(stat.read_bytes - usr.read_bytes);
    //         usr.read_speed  = static_cast<uint64_t>(std::max<int64_t>(0, delta_read)  / dt_s);
    //     }
        
    //     if (usr.write_ktimestamp == 0) {
    //         usr.write_speed = 0;
    //     }else{
    //         int64_t delta_write = static_cast<int64_t>(stat.write_bytes - usr.write_bytes);
    //         usr.write_speed = static_cast<uint64_t>(std::max<int64_t>(0, delta_write) / dt_s);
    //     }
    // }
   
    auto now = std::chrono::steady_clock::now();
    // 按照调用作为采样时间点计算速度
    usr.read_speed = (usr.read_bytes - usr.last_read_bytes) / (std::chrono::duration<double>(now - usr.last_read_time).count());
    usr.write_speed = (usr.write_bytes - usr.last_write_bytes) / (std::chrono::duration<double>(now - usr.last_write_time).count());
    usr.last_read_bytes = usr.read_bytes;
    usr.last_write_bytes = usr.write_bytes;
    usr.last_read_time = now;
    usr.last_write_time = now;

    // usr.read_ktimestamp  = stat.read_ktimestamp;
    usr.read_bytes  = stat.read_bytes;
    usr.read_count  = stat.read_count;
    usr.read_variance = stat.read_variance;
    usr.read_mean = stat.read_mean;
    // usr.write_ktimestamp = stat.write_ktimestamp;
    usr.write_bytes = stat.write_bytes;
    usr.write_count = stat.write_count;
    usr.write_variance = stat.write_variance;
    usr.write_mean = stat.write_mean;

    return usr;
}

bool IOUsageCollector::init(const nlohmann::json& cfg){
    if(cfg.contains("summary") && cfg["summary"].get<std::string>() == "true"){
        summary = true;
    }else{
        summary = false;
    }

    if(cfg.contains("use_ebpf") && cfg["use_ebpf"].get<std::string>() == "true"){
        use_ebpf = true;
        if(!init_ebpf()){
            spdlog::error("IOUsageCollector: init ebpf error");
            deinit_ebpf();
            use_ebpf = false;
        };
    }

    return true;
}

void IOUsageCollector::deinit() noexcept{
    pid_state_dict.clear();
    if (use_ebpf) {
        deinit_ebpf();
    }
    
    spdlog::info("IOUsageCollector deinit");
}

// --------------- 工具函数 ---------------
static bool read_proc_io(int pid, unsigned long long &rb, unsigned long long &wb,
                         unsigned long long &rch, unsigned long long &wch,
                         unsigned long long &scr, unsigned long long &scw)
{
    std::ifstream ifs("/proc/" + std::to_string(pid) + "/io");
    if (!ifs) return false;
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.rfind("read_bytes:", 0) == 0)      rb  = std::stoull(line.substr(11));
        else if (line.rfind("write_bytes:", 0)==0) wb  = std::stoull(line.substr(12));
        else if (line.rfind("rchar:", 0) == 0)     rch = std::stoull(line.substr(6));
        else if (line.rfind("wchar:", 0) == 0)     wch = std::stoull(line.substr(6));
        else if (line.rfind("syscr:", 0) == 0)     scr = std::stoull(line.substr(6));
        else if (line.rfind("syscw:", 0) == 0)     scw = std::stoull(line.substr(6));
    }
    return true;
}

// 取 fd 对应的绝对路径
static std::string fd_to_path(int pid, int fd)
{
    char buf[512];
    std::string p = "/proc/" + std::to_string(pid) + "/fd/" + std::to_string(fd);
    ssize_t n = readlink(p.c_str(), buf, sizeof(buf)-1);
    if (n < 0) return {};
    buf[n] = '\0';
    return std::string(buf);
}

// 根据绝对路径找挂载点和文件系统类型
// static bool path_to_mount(const std::string &abs, std::string &mnt, std::string &fst)
// {
//     std::ifstream ifs("/proc/self/mountinfo");
//     if (!ifs) return false;
//     std::string line;
//     std::string best_mnt;
//     std::string best_fst;
//     size_t best_len = 0;
//     while (std::getline(ifs, line)) {
//         std::istringstream iss(line);
//         int dummy; std::string dev, root, mntpoint, opts;
//         iss >> dummy >> dummy >> dummy >> dev >> root >> mntpoint;
//         std::getline(iss, opts);          // 剩下的忽略
//         if (abs.find(mntpoint) == 0 && mntpoint.size() > best_len) {
//             best_len = mntpoint.size();
//             best_mnt = mntpoint;
//             // 简单解析：倒数第二段就是 fs-type
//             auto pos = line.rfind(' ');
//             if (pos != std::string::npos) {
//                 auto pos2 = line.rfind(' ', pos-1);
//                 if (pos2 != std::string::npos)
//                     best_fst = line.substr(pos2+1, pos-pos2-1);
//             }
//         }
//     }
//     if (best_len) { mnt = best_mnt; fst = best_fst; return true; }
//     return false;
// }

static bool path_to_mount(const std::string &abs,
                          std::string &mnt,
                          std::string &fst)
{
    /* 缓存项 */
    struct MountEntry {
        std::string mntpoint;
        std::string fstype;
        bool operator<(const MountEntry &rhs) const {
            return mntpoint.size() > rhs.mntpoint.size(); // 长的放前面
        }
    };
    /* 静态缓存：解析后的挂载表 + 时间戳 */
    static std::vector<MountEntry>  s_cache;
    static time_t                   s_cache_mtime = 0;

    /* 1. 判断是否需要重新加载：缓存空 或 文件更新 */
    struct stat st{};
    if (stat("/proc/self/mountinfo", &st) != 0) return false;      // 文件都打不开
    bool need_reload = s_cache.empty() || st.st_mtime > s_cache_mtime;

    if (need_reload) {
        s_cache.clear();
        std::ifstream ifs("/proc/self/mountinfo");
        if (!ifs) return false;

        std::string line;
        while (std::getline(ifs, line)) {
            std::istringstream iss(line);
            int d1, d2, d3;
            std::string dev, root, mntpoint;
            if (!(iss >> d1 >> d2 >> d3 >> dev >> root >> mntpoint))
                continue;

            // 简单取 fs-type：倒数第二段
            std::string fstype;
            auto pos = line.rfind(' ');
            if (pos != std::string::npos) {
                auto pos2 = line.rfind(' ', pos - 1);
                if (pos2 != std::string::npos)
                    fstype = line.substr(pos2 + 1, pos - pos2 - 1);
            }
            s_cache.push_back({std::move(mntpoint), std::move(fstype)});
        }
        /* 按挂载点长度降序，保证最长前缀先匹配 */
        std::sort(s_cache.begin(), s_cache.end());
        s_cache_mtime = st.st_mtime;
    }

    /* 2. 在缓存里找最长前缀 */
    for (const auto &e : s_cache) {
        if (abs.find(e.mntpoint) == 0) {
            mnt = e.mntpoint;
            fst = e.fstype;
            return true;
        }
    }

    /* 3. 缓存里找不到 -> 视为“失效”，下次重新读（可选） */
    s_cache.clear();   // 强制下次重载
    return false;
}

// --------------- CPUMemCollector ---------------
CollectResult IOUsageCollector::collect(const Job &job)
{
    std::vector<IOUsageInfo> result;
    if (use_ebpf){
        if (!EbpfCommon::update_pid_in_kernel(bpf_obj_, pid2jobid_map_name, job.JobID, job.JobPIDs)){
            spdlog::error("IOUsageCollector: update pid in kernel error");
            ring_buffer__consume(bpf_rb_);
            // 触发一次fetch，之后执行完其他内容的时候再分析fetch内容或者执行多cpu数据聚合（这里fetch是无堵塞的，所以需要这样）
        }
    }
    for (int pid : job.JobPIDs) {
        // 提高鲁棒性
        if (! Utils::is_process_running(pid)){
            continue;
        }
        IOUsageInfo info{};
        info.pid = pid;
        /* 1. 读 /proc/pid/io */
        unsigned long long rb=0, wb=0, rch=0, wch=0, scr=0, scw=0;

        if (!read_proc_io(pid, rb, wb, rch, wch, scr, scw)) {
            // 进程可能已退出，跳过
            continue;
        }
        info.read_bytes  = rb;
        info.write_bytes = wb;
        info.rchar       = rch;
        info.wchar       = wch;
        info.syscr       = scr;
        info.syscw       = scw;
        spdlog::debug("IOUsageCollector: pid={} read_bytes={} write_bytes={} rchar={} wchar={} syscr={} syscw={}",
                      pid, rb, wb, rch, wch, scr, scw);
        auto now = steady_clock::now();

        /* 2. 计算速度 */
        auto it = pid_state_dict.find(pid);
        if (it != pid_state_dict.end()) {
            const pid_state &st = it->second;
            collect_period = std::chrono::duration_cast<std::chrono::seconds>(now - st.last_time).count();
            info.read_speed  = (rb  >= st.last_read_bytes)  ? (rb  - st.last_read_bytes)  / collect_period : 0;
            info.write_speed = (wb  >= st.last_write_bytes) ? (wb  - st.last_write_bytes) / collect_period : 0;
            info.rchar_speed = (rch >= st.last_rchar)       ? (rch - st.last_rchar)       / collect_period : 0;
            info.wchar_speed = (wch >= st.last_wchar)       ? (wch - st.last_wchar)       / collect_period : 0;
            spdlog::debug("IOUsageCollector: pid={} read_speed={} write_speed={} rchar_speed={} wchar_speed={}",
                          pid, info.read_speed, info.write_speed, info.rchar_speed, info.wchar_speed);
        }else {
            info.read_speed  = 0;
            info.write_speed = 0;
            info.rchar_speed = 0;
            info.wchar_speed = 0;
        }
        pid_state_dict[pid] = pid_state{ now, rb, wb, rch, wch };

        /* 3. 扫描 /proc/pid/fd/ 收集打开的文件 */
        std::string fd_dir = "/proc/" + std::to_string(pid) + "/fd/";
        DIR *dir = ::opendir(fd_dir.c_str());
        if (dir) {
            struct dirent *ent;
            while ((ent = ::readdir(dir))) {
                if (ent->d_name[0] == '.') continue;
                int fd = std::atoi(ent->d_name);
                std::string path = fd_to_path(pid, fd);
                if (path.empty() || path[0] != '/') continue; // 只保留真实文件系统路径
                struct stat stb;
                if (::stat(path.c_str(), &stb) < 0) continue;
                if (!S_ISREG(stb.st_mode) && !S_ISBLK(stb.st_mode)) continue;

                FileIOInfo finfo;
                finfo.path = path;
                finfo.pos  = static_cast<unsigned long long>(stb.st_size);
                path_to_mount(path, finfo.mount_point, finfo.fs_type);
                if (use_ebpf){
                    auto stat = get_fd_stat(pid, fd);
                    if (stat != std::nullopt) {
                        auto stat_v = stat.value();
                        finfo.read_bytes = stat_v.read_bytes;
                        finfo.write_bytes = stat_v.write_bytes;
                        finfo.read_count = stat_v.read_count;
                        finfo.write_count = stat_v.write_count;
                        finfo.read_mean = stat_v.read_mean;
                        finfo.write_mean = stat_v.write_mean;
                        finfo.read_speed = stat_v.read_speed;
                        finfo.write_speed = stat_v.write_speed;
                        finfo.read_variance = stat_v.read_variance;
                        finfo.write_variance = stat_v.write_variance;
                    } else {
                        finfo.read_bytes = 0;
                        finfo.write_bytes = 0;
                        finfo.read_count = 0;
                        finfo.write_count = 0;
                        finfo.read_mean = 0;
                        finfo.write_mean = 0;
                        finfo.read_speed = 0;
                        finfo.write_speed = 0;
                        finfo.read_variance = 0;
                        finfo.write_variance = 0;
                    }
                }
                info.file_info[fd] = std::move(finfo);
                
            }
            ::closedir(dir);
        }

        result.push_back(info);
    }
    
    

    if (summary){
        IOUsageInfo info_summary{};   
        info_summary.pid = 0;
        for (const auto& info : result){
            info_summary.read_bytes += info.read_bytes;
            info_summary.write_bytes += info.write_bytes;
            info_summary.rchar += info.rchar;
            info_summary.wchar += info.wchar;
            info_summary.syscr += info.syscr;
            info_summary.syscw += info.syscw;
        }
        if (job_summary_state_dict.count(job.JobID) == 0){
            job_summary_state_dict[job.JobID] = pid_state{
                .last_time = steady_clock::now(),
                .last_read_bytes = info_summary.read_bytes, 
                .last_write_bytes = info_summary.write_bytes,
                .last_rchar = info_summary.rchar,
                .last_wchar = info_summary.wchar};
            info_summary.wchar_speed = 0;
            info_summary.rchar_speed = 0;
            info_summary.read_speed = 0;
            info_summary.write_speed = 0;
        } else{
            auto& state = job_summary_state_dict[job.JobID];
            auto now = steady_clock::now();
            collect_period = std::chrono::duration_cast<std::chrono::seconds>(now - state.last_time).count();
            info_summary.read_speed  = (info_summary.read_bytes  >= state.last_read_bytes)  ? (info_summary.read_bytes  - state.last_read_bytes)  / collect_period : 0;
            info_summary.write_speed = (info_summary.write_bytes  >= state.last_write_bytes) ? (info_summary.write_bytes  - state.last_write_bytes) / collect_period : 0;
            info_summary.rchar_speed = (info_summary.rchar >= state.last_rchar)       ? (info_summary.rchar - state.last_rchar)       / collect_period : 0;
            info_summary.wchar_speed = (info_summary.wchar >= state.last_wchar)       ? (info_summary.wchar - state.last_wchar)       / collect_period : 0;
            state.last_read_bytes   = info_summary.read_bytes;
            state.last_write_bytes  = info_summary.write_bytes;
            state.last_rchar        = info_summary.rchar;
            state.last_wchar        = info_summary.wchar;
            state.last_time         = now;
        }
        result.push_back(info_summary);
    }
    return result;
}


CollectDataParseFunc IOUsageCollector::get_writer_parser(const std::string& writer_type) {
    CollectDataParseFunc func = nullptr;
    
    if(writer_type.compare("ESWriter") == 0){
        func = [this](std::any data)->std::any{
            nlohmann::json ret;
            if(data.has_value() == false) {
                spdlog::warn("IOUsageInfo: error writer parser, empty data");
                ret["error"] = "empty data";
                return ret;
            }
            ret["process_data"] = nlohmann::json::array();
            auto parsed = std::any_cast<std::vector<IOUsageInfo>>(data);
            for (const auto& info : parsed) {
                nlohmann::json j;
                j["pid"] = info.pid;
                j["read_bytes"] = info.read_bytes;
                j["writer_bytes"] = info.write_bytes;
                j["read_speed"] = info.read_speed;
                j["write_speed"] = info.write_speed;
                j["rchar"] = info.rchar;
                j["wchar"] = info.wchar;
                j["rchar_speed"] = info.rchar_speed;
                j["wchar_speed"] = info.wchar_speed;
                j["syscr"] = info.syscr;
                j["syscw"] = info.syscw;
                nlohmann::json files = nlohmann::json::array();
                for (const auto& [fd, finfo] : info.file_info) {
                    nlohmann::json fj;
                    fj["fd"] = fd;
                    fj["path"] = finfo.path;
                    fj["mount_point"] = finfo.mount_point;
                    fj["fs_type"] = finfo.fs_type;
                    fj["pos"] = finfo.pos;
                    if (use_ebpf) {
                        fj["read_bytes"] = finfo.read_bytes;
                        fj["read_speed"] = finfo.read_speed;
                        fj["write_bytes"] = finfo.write_bytes;
                        fj["write_speed"] = finfo.write_speed;
                        fj["read_mean"] = finfo.read_mean;
                        fj["write_mean"] = finfo.write_mean;
                        fj["read_variance"] = finfo.read_variance;
                        fj["write_variance"] = finfo.write_variance;
                        fj["write_count"] = finfo.write_count;
                        fj["read_count"] = finfo.read_count;
                    }
                    files.push_back(fj);
                }
                j["files"] = files;
                if (info.pid == 0){
                    if (summary) ret["summary"] = j;
                }else{
                    ret["process_data"].push_back(j);
                }
            }
            return ret;
        };
    }

    if(writer_type.compare("FileWriter") == 0){
        func = [this](std::any data)->std::any{
            if(data.has_value() == false) {
                spdlog::warn("IOUsageInfo: error FileWriter parser, empty data");
                return std::string("IOUsageCollector error=empty_data\n");
            }
            auto parsed = std::any_cast<std::vector<IOUsageInfo>>(data);
            std::ostringstream out;
            for (const auto& info : parsed) {
                out << "IOUsageCollector"
                    << " type=" << (info.pid == 0 ? "summary" : "process")
                    << " pid=" << info.pid
                    << " read_bytes=" << info.read_bytes
                    << " write_bytes=" << info.write_bytes
                    << " read_speed=" << info.read_speed
                    << " write_speed=" << info.write_speed
                    << " rchar=" << info.rchar
                    << " wchar=" << info.wchar
                    << " rchar_speed=" << info.rchar_speed
                    << " wchar_speed=" << info.wchar_speed
                    << " syscr=" << info.syscr
                    << " syscw=" << info.syscw
                    << '\n';
                for (const auto& [fd, finfo] : info.file_info) {
                    out << "IOUsageCollector file"
                        << " pid=" << info.pid
                        << " fd=" << fd
                        << " path=" << finfo.path
                        << " mount_point=" << finfo.mount_point
                        << " fs_type=" << finfo.fs_type
                        << " pos=" << finfo.pos;
                    if (use_ebpf) {
                        out << " read_bytes=" << finfo.read_bytes
                            << " read_speed=" << finfo.read_speed
                            << " write_bytes=" << finfo.write_bytes
                            << " write_speed=" << finfo.write_speed
                            << " read_mean=" << finfo.read_mean
                            << " write_mean=" << finfo.write_mean
                            << " read_variance=" << finfo.read_variance
                            << " write_variance=" << finfo.write_variance
                            << " write_count=" << finfo.write_count
                            << " read_count=" << finfo.read_count;
                    }
                    out << '\n';
                }
            }
            return out.str();
        };
    }

    if(writer_type.compare("PrometheusExporterWriter") == 0){
        func = [this](std::any data)->std::any{
            PrometheusExporterWriter::prometheus_job_state ret;
            if(data.has_value() == false) {
                spdlog::warn("IOUsageCollector: error writer parser, empty data");
                ret.JobID = 0;
                return ret;
            }
            auto parsed = std::any_cast<std::vector<IOUsageInfo>>(data);
            
            for (const auto& info : parsed) {
                PrometheusExporterWriter::prometheus_process_state state;
                state.pid = info.pid;
                state.io_read_bytes_per_sec = info.read_speed;
                state.io_read_bytes_total = info.read_bytes;
                state.io_write_bytes_per_sec = info.write_speed;
                state.io_write_bytes_total = info.write_bytes;
                
                ret.processes_state.push_back(state);
            }
            return ret;
        };
    }
    
    return func;
}

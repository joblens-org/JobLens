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
#pragma once
#include <array>
#include <cstdio>
#include <string>
#include <filesystem>
#include <unistd.h>
#include <climits>
#include <cerrno>
#include <cstdint>
#include <string>
#include <vector>
#include <fstream>
#include <xxhash.h>
#include <signal.h>
#include <execinfo.h>
#include <spdlog/spdlog.h>
#include <exception>
#include <memory>
#include <yaml-cpp/yaml.h>
#include <nlohmann/json.hpp>
#include <string>
#include <string_view>
#include <unordered_map>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace Utils
{
    // =============== 进程信息相关工具函数 ===============
    
    /**
     * @brief 获取指定pid的父进程pid
     * @param pid 进程ID
     * @return 父进程ID，出错返回-1
     */
    inline pid_t get_ppid_of(pid_t pid)
    {
        std::string path = "/proc/" + std::to_string(pid) + "/stat";
        std::ifstream stat(path);
        if (!stat)
            return -1;

        // 格式：pid (comm) S ppid ...
        std::string line;
        if (!std::getline(stat, line))
            return -1;

        // 从右括号后开始找第 4 个字段
        std::size_t last_rparen = line.rfind(')');
        if (last_rparen == std::string::npos)
            return -1;

        std::istringstream iss(line.substr(last_rparen + 1));
        pid_t ppid;
        std::string dummy;
        iss >> dummy >> dummy >> ppid;   // skip state, then ppid
        return ppid;
    }

    /**
     * @brief 获取指定pid的命令行（空格分隔）
     * @param pid 进程ID
     * @return 命令行字符串，失败返回空串
     */
    inline std::string get_cmdline_of(pid_t pid)
    {
        std::string path = "/proc/" + std::to_string(pid) + "/cmdline";
        std::ifstream cmdline(path, std::ios::binary);
        if (!cmdline)
            return {};

        std::string content;
        content.assign(std::istreambuf_iterator<char>(cmdline),
                       std::istreambuf_iterator<char>());

        // 把 '\0' 换成空格
        for (char &c : content)
            if (c == '\0')
                c = ' ';

        // 去掉末尾多余空格
        if (!content.empty() && content.back() == ' ')
            content.pop_back();

        return content;
    }

    /**
     * @brief 从/proc/<pid>/environ中读取指定环境变量
     * @param pid 进程ID
     * @param key 环境变量名
     * @return 环境变量值，失败返回空串
     */
    inline std::string get_env_field(pid_t pid, const std::string &key)
    {
        std::ifstream file(fs::path("/proc") / std::to_string(pid) / "environ", std::ios::binary);
        if (!file) return "";

        std::string raw{std::istreambuf_iterator<char>(file),
                        std::istreambuf_iterator<char>()};
        size_t pos = 0;
        while (pos < raw.size()) {
            size_t end = raw.find('\0', pos);
            if (end == std::string::npos) end = raw.size();
            std::string kv(raw.substr(pos, end - pos));
            size_t eq = kv.find('=');
            if (eq != std::string::npos && kv.substr(0, eq) == key)
                return kv.substr(eq + 1);
            pos = end + 1;
        }
        return "";
    }

    /**
     * @brief 获取指定进程的cgroup v2绝对路径
     * @param pid 进程ID，默认为0表示当前进程
     * @return cgroup绝对路径，失败返回空串
     */
    inline std::string v2_cgroup_absolute_path(pid_t pid = 0){
        const fs::path proc = (pid == 0) ? "/proc/self/cgroup"
                                         : "/proc/" + std::to_string(pid) + "/cgroup";

        std::ifstream fin(proc);
        if (!fin)
            return {};                 // 进程不存在或权限不足

        std::string line;
        while (std::getline(fin, line)){
            // v2 统一层级格式：0::/relative/path
            if (line.compare(0,3,"0::") == 0){
                std::string rel = line.substr(3);          // "/system.slice/xxx"
                // 拼到挂载点即可。v2 统一层级挂载点 99% 是 /sys/fs/cgroup
                fs::path mount = "/sys/fs/cgroup";
                return (mount / fs::path(rel).lexically_normal()).string();
            }
        }
        return {};   // 不是 v2 或没找到
    }

    /**
     * @brief 获取指定cgroup目录中的所有进程PID
     * @param cgroup_dir cgroup目录路径
     * @return PID列表
     */
    inline std::vector<pid_t> get_pids_in_cgroup(const std::string& cgroup_dir){
        std::vector<pid_t> pids;
        fs::path procs = fs::path(cgroup_dir) / "cgroup.procs";
        std::ifstream fin(procs);
        if (!fin)
            return pids;   // 文件不存在或权限不足

        pid_t pid;
        while (fin >> pid)          // 每行一个 PID
            pids.push_back(pid);
        return pids;
    }
    inline std::string executableDir() {
        char buf[PATH_MAX + 1] = {};
        ssize_t len = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
        if (len == -1) throw std::runtime_error("readlink /proc/self/exe failed");
        fs::path p(std::string(buf, len));
        return p.parent_path().string() + '/';
    }

    inline std::string parentDir(const std::string& path) {
        fs::path p(path);
        if (p.has_relative_path()) p = fs::absolute(p);
        return p.parent_path().string() + '/';
    }

    inline std::string JobLensRootDir(){
        // 结构上为.../.../JobLens/bin/JobLens
        // 利用static做缓存
        static auto path = parentDir(executableDir());
        return path;
    }

    inline bool ensure_directory_exists(const std::string& file_path) {
    // 从文件路径中提取目录部分
        fs::path path(file_path);
        fs::path dir = path.parent_path();
        
        // 如果路径没有目录部分（如 "filename.txt"），直接返回成功
        if (dir.empty()) {
            return true;
        }
        
        // 如果目录已存在，检查是否为目录类型
        if (fs::exists(dir)) {
            return fs::is_directory(dir);
        }
        
        // 递归创建所有必要的父目录
        std::error_code ec;
        bool result = fs::create_directories(dir, ec);

        if (ec) {
            spdlog::error("Utils: failed to create directories for path {}: {}", dir.string(), ec.message());
        }
        
        // 如果创建失败，检查是否因为并发创建导致
        if (!result && ec) {
            // 再次检查目录是否存在（可能被其他线程/进程创建）
            return fs::exists(dir) && fs::is_directory(dir);
        }
        
        return result;
    }

    inline std::string error_message_from_errno(int errnum) {
        char buf[256];
        char* msg = strerror_r(errnum, buf, sizeof(buf));
        return std::string(msg);
    }

    /// 惰性获取 HTCondor COLLECTOR_HOST 作为集群名称。
    /// 首次调用时执行 condor_config_val 并缓存，后续直接返回（线程安全）。
    /// 返回值自动剥离端口号，保留完整域名。
    inline std::string get_condor_collector_host() {
        static const std::string cached = []() -> std::string {
            std::array<char, 256> buf{};
            std::string result;
            std::unique_ptr<FILE, decltype(&pclose)> pipe(
                popen("condor_config_val COLLECTOR_HOST 2>/dev/null", "r"),
                pclose);
            if (!pipe) return "";
            while (fgets(buf.data(), static_cast<int>(buf.size()), pipe.get()))
                result += buf.data();
            if (!result.empty() && result.back() == '\n')
                result.pop_back();
            // 剥离端口号: "host.domain:9618" -> "host.domain"
            auto pos = result.rfind(':');
            if (pos != std::string::npos) {
                bool all_digits = true;
                for (size_t i = pos + 1; i < result.size(); ++i)
                    if (!std::isdigit(static_cast<unsigned char>(result[i]))) {
                        all_digits = false; break;
                    }
                if (all_digits) result.resize(pos);
            }
            return result;
        }();
        return cached;
    }

    inline std::uint64_t file_hash_xxh64(const std::string& path) {
        std::ifstream f(path, std::ios::ate | std::ios::binary);
        if (!f)
            throw std::runtime_error("cannot open " + path);

        const std::size_t n = f.tellg();
        f.seekg(0);

        std::vector<char> buf(n);
        if (!f.read(buf.data(), static_cast<std::streamsize>(n)))
            throw std::runtime_error("read failed: " + path);

        return XXH64(buf.data(), n, 0);   // seed = 0
    }

    inline bool is_process_running(pid_t pid) {
        return kill(pid, 0) == 0;
    }

    inline std::string get_backtrace() {
        constexpr int max_frames = 32;
        void* buffer[max_frames];
        
        // 获取栈帧
        int n = backtrace(buffer, max_frames);
        
        // 解析符号
        char** symbols = backtrace_symbols(buffer, n);
        if (!symbols) return "Failed to get symbols";
        
        std::string result;
        for (int i = 0; i < n; ++i) {
            result += fmt::format("{}\n", symbols[i]);
        }
        
        free(symbols);
        return result;
    }

    // 递归将 YAML 节点转换为 JSON
    inline nlohmann::json yamlToJson(const YAML::Node& node) {
        if (node.IsNull()) {
            return nullptr;
        } else if (node.IsScalar()) {
            return node.as<std::string>(); // 可根据需要转换类型
        } else if (node.IsSequence()) {
            nlohmann::json arr = nlohmann::json::array();
            for (const auto& item : node) {
                arr.push_back(yamlToJson(item));
            }
            return arr;
        } else if (node.IsMap()) {
            nlohmann::json obj = nlohmann::json::object();
            for (const auto& kv : node) {
                obj[kv.first.as<std::string>()] = yamlToJson(kv.second);
            }
            return obj;
        }
        return nullptr;
    }

    
    // 内部递归实现
    inline void flatten_impl(const json& j, std::string_view prefix,
                            std::unordered_map<std::string, std::string>& out,
                            char sep = '.') {
        // 处理对象：遍历键值对
        if (j.is_object()) {
            for (const auto& [key, val] : j.items()) {  // C++17 structured binding
                std::string new_key = prefix.empty() ? std::string(key)
                                                    : std::string(prefix) + sep + std::string(key);
                flatten_impl(val, new_key, out, sep);
            }
        }
        // 处理数组：下标作为 key
        else if (j.is_array()) {
            for (size_t i = 0; i < j.size(); ++i) {
                std::string new_key = prefix.empty() ? std::to_string(i)
                                                    : std::string(prefix) + sep + std::to_string(i);
                flatten_impl(j[i], new_key, out, sep);
            }
        }
        // 叶子节点：转换为字符串
        else {
            std::string value;
            if (j.is_string())       value = j.get<std::string>();
            else if (j.is_null())    value = "";
            else if (j.is_boolean()) value = j.get<bool>() ? "true" : "false";
            else if (j.is_number_integer()) value = std::to_string(j.get<int64_t>());
            else if (j.is_number_unsigned())  value = std::to_string(j.get<uint64_t>());
            else if (j.is_number_float())    value = std::to_string(j.get<double>());
            else value = j.dump();  // 兜底（如 binary, discarded 等）
            
            out.emplace(std::string(prefix), std::move(value));
        }
    }

    // 对外接口：扁平化 JSON 为 unordered_map<string, string>
    // 参数 separator 可自定义连接符，默认为 '.'
    inline std::unordered_map<std::string, std::string> flatten_json(const json& j, 
                                                                char separator = '.') {
        std::unordered_map<std::string, std::string> result;
        // 仅当输入为对象或数组时进行扁平化；标量输入则 key 为空字符串
        if (j.is_structured()) {
            flatten_impl(j, "", result, separator);
        } else {
            flatten_impl(j, "", result, separator);
        }
        return result;
    }


    inline bool has_template_vars(std::string_view sv) noexcept {
        const char* s = sv.data();
        const char* end = s + sv.size();
        
        while (s + 4 <= end) {  // 剩余至少4字符才可能闭合
            if (s[0] == '[' && s[1] == '[') {
                // 查找 ]]，手动推进指针
                const char* p = s + 2;
                while (p + 1 < end) {
                    if (p[0] == ']' && p[1] == ']') {
                        if (p > s + 2) return true; // 非空
                        break; // 空标记，跳出内层继续外层
                    }
                    ++p;
                }
                s += 2; // 跳过当前 [[
            } else {
                ++s;
            }
        }
        return false;
    }


    inline std::string render_bracket(std::string_view tmpl,
                                    const std::unordered_map<std::string, std::string>& vars) {
        std::string out;
        out.reserve(tmpl.size());
        size_t pos = 0;
        
        while (pos < tmpl.size()) {
            size_t open = tmpl.find("[[", pos);
            if (open == std::string_view::npos) {
                out += tmpl.substr(pos);
                break;
            }
            out += tmpl.substr(pos, open - pos);
            
            size_t close = tmpl.find("]]", open + 2);
            if (close == std::string_view::npos) {
                out += tmpl.substr(open);
                break;
            }
            
            std::string_view key = tmpl.substr(open + 2, close - open - 2);
            if (auto it = vars.find(std::string(key)); it != vars.end()) {
                out += it->second;
            } else {
                out += tmpl.substr(open, close - open + 2);
            }
            pos = close + 2;
        }
        return out;
    }

} 

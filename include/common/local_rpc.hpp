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

#include <string>
#include <functional>
#include <unordered_map>
#include <memory>
#include <system_error>
#include <vector>
#include <mutex>
#include <nlohmann/json.hpp>

using json = nlohmann::json;


class RPCError : public std::runtime_error {
public:
    explicit RPCError(const std::string& msg) : std::runtime_error(msg) {}
};

class RPCServer {
public:
    // 获取单例实例
    static RPCServer& instance(const std::string& socket_path="");
    
    // 禁用拷贝和移动
    RPCServer(const RPCServer&) = delete;
    RPCServer& operator=(const RPCServer&) = delete;
    
    // 注册方法 - 使用json作为参数和返回值
    void register_method(const std::string& name, std::function<json(const json&)> func);

    void start();
    void run();
    void stop();
    bool is_running() const;
    
    // 获取服务句柄信息
    std::string get_socket_path() const;
    std::vector<std::string> get_registered_methods() const;

private:
    RPCServer(const std::string& socket_path);
    ~RPCServer();
    
    class Impl;
    std::unique_ptr<Impl> impl_;
};

// // 显式实例化常见函数类型
// template void RPCServer::register_method<std::function<std::string(const std::string&)>>(
//     const std::string&, std::function<std::string(const std::string&)>);


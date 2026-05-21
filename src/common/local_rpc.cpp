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
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include "common/local_rpc.hpp"
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <poll.h>
#include <thread>
#include <atomic>
#include <algorithm>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <iostream>
#include "common/utils.hpp"

// 公共工具函数
namespace {
    void send_all(int fd, const std::string& data) {
        size_t sent = 0;
        while (sent < data.size()) {
            ssize_t n = send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
            if (n < 0) {
                spdlog::error("LocalRPC: send failed, errno: {}", errno);
            }
            sent += n;
        }
        spdlog::trace("LocalRPC: sent {} bytes to client", sent);
    }

    std::string recv_all(int fd) {
        std::string data;
        char buffer[4096];
        
        // 设置接收超时
        struct timeval tv{1, 0}; // 1秒超时
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        
        ssize_t n = recv(fd, buffer, sizeof(buffer), 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                spdlog::debug("LocalRPC: receive timeout");
            }
            spdlog::error("LocalRPC: recv failed, errno: {}", errno);
            return "";
        }
        if (n == 0) {
            spdlog::debug("LocalRPC: client closed connection");
            return "";
        }
        data.append(buffer, n);
        spdlog::trace("LocalRPC: received {} bytes from client", data.size());
        return data;
    }
}

// 服务器实现
class RPCServer::Impl {
public:
    Impl(const std::string& socket_path) 
        : socket_path_(socket_path), 
          server_fd_(-1), 
          running_(false) {
        
        spdlog::info("LocalRPC: initializing server with socket path: {}", socket_path);
        
        // 注册默认方法
        register_default_methods();
    }
    
    ~Impl() {
        spdlog::info("LocalRPC: shutting down server");
        stop();
    }
    
    void register_default_methods() {
        // health检查方法
        methods_["health"] = [this](const json& params) {
            json response;
            response["status"] = "healthy";
            response["timestamp"] = time(nullptr);
            response["methods_count"] = methods_.size();
            response["running"] = running_.load();
            response["socket_path"] = socket_path_;
            return response;
        };
        
        // 函数列表方法
        methods_["func_list"] = [this](const json& params) {
            std::vector<std::string> method_names;
            for (const auto& pair : methods_) {
                method_names.push_back(pair.first);
            }
            // 按字母顺序排序
            std::sort(method_names.begin(), method_names.end());
            
            json response = method_names;
            return response;
        };
    }
    
    void start() {
        int ret;

        if (running_) {
            spdlog::warn("LocalRPC: server is already running");
            return;
        }
        
        spdlog::info("LocalRPC: starting server on socket: {}", socket_path_);
        
        server_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
        if (server_fd_ < 0) {
            spdlog::error("LocalRPC: create socket failed, err: {}", Utils::error_message_from_errno(errno));
            throw RPCError("create socket failed");
        }
        
        // 删除可能存在的旧socket文件
        if (access(socket_path_.c_str(), F_OK) == 0) {
            unlink(socket_path_.c_str());
        }

        if (!Utils::ensure_directory_exists(socket_path_))
            spdlog::error("LocalRPC: failed to ensure directory exists for socket: {}", socket_path_);

        struct sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, socket_path_.c_str(), sizeof(addr.sun_path) - 1);
        
        ret = bind(server_fd_, (struct sockaddr*)&addr, sizeof(addr));
        if (ret < 0) {
            spdlog::error("LocalRPC: bind failed for socket: {}, err: {}", socket_path_, Utils::error_message_from_errno(errno));
            close(server_fd_);
            throw RPCError("bind failed");
        }
        
        ret = listen(server_fd_, 5);
        if (ret < 0) {
            spdlog::error("LocalRPC: listen failed, err: {}", Utils::error_message_from_errno(errno));
            close(server_fd_);
            throw RPCError("listen failed");
        }

        std::thread(&Impl::handle_loop, this).detach();

        running_ = true;
        spdlog::info("LocalRPC: server started successfully");
        
    }
    
    void stop() {
        spdlog::info("LocalRPC: stopping server");
        running_ = false;
        if (server_fd_ >= 0) {
            close(server_fd_);
            server_fd_ = -1;
            spdlog::debug("LocalRPC: closed server socket");
        }
        unlink(socket_path_.c_str());
        spdlog::debug("LocalRPC: removed socket file: {}", socket_path_);
    }
    
    void handle_loop() {
        spdlog::info("LocalRPC: entering main server loop");
        
        while (running_) {
            struct pollfd pfd{server_fd_, POLLIN, 0};
            int ret = poll(&pfd, 1, 100); // 100ms超时
            
            if (ret > 0 && (pfd.revents & POLLIN)) {
                int client_fd = accept(server_fd_, nullptr, nullptr);
                if (client_fd >= 0) {
                    spdlog::debug("LocalRPC: accepted new client connection");
                    handle_client(client_fd);
                } else {
                    spdlog::error("LocalRPC: accept failed");
                }
            } else if (ret < 0) {
                spdlog::error("LocalRPC: poll failed");
            }
        }
        
        spdlog::info("LocalRPC: server loop ended");
    }

    void register_method(const std::string& name, std::function<json(const json&)> func) {
        spdlog::info("LocalRPC: registering method: {}", name);
        methods_[name] = std::move(func);
        spdlog::debug("LocalRPC: total registered methods: {}", methods_.size());
    }
    
    bool is_running() const {
        return running_;
    }
    
    std::string get_socket_path() const {
        return socket_path_;
    }
    
    std::vector<std::string> get_registered_methods() const {
        std::vector<std::string> method_names;
        for (const auto& pair : methods_) {
            method_names.push_back(pair.first);
        }
        std::sort(method_names.begin(), method_names.end());
        return method_names;
    }
    
private:
    void handle_client(int client_fd) {
        try {
            spdlog::debug("LocalRPC: handling client request");
            
            std::string request_data = recv_all(client_fd);
            if (request_data.empty()) {
                spdlog::warn("LocalRPC: received empty request from client");
                close(client_fd);
                return;
            }
            spdlog::debug("LocalRPC: received request data: {}", request_data);
            spdlog::debug("LocalRPC: received request data ({} bytes)", request_data.size());
            
            auto request = json::parse(request_data);
            std::string method = request["method"];
            json params = request["params"];
            
            spdlog::info("LocalRPC: processing method call: {}", method);
            
            json result;
            if (methods_.count(method)) {
                result = methods_[method](params);
                spdlog::debug("LocalRPC: method {} executed successfully", method);
            } else {
                result = {"error", "method not found: " + method};
            }
            send_all(client_fd, result.dump());
            spdlog::debug("LocalRPC: sent response ({} bytes)", result.dump().size());
            
        } catch (const std::exception& e) {
            spdlog::error("LocalRPC: error handling client request: {}", e.what());
            json error_response = {"error", e.what()};
            send_all(client_fd, error_response.dump());
        }
        close(client_fd);
        spdlog::debug("LocalRPC: closed client connection");
    }
    
    std::string socket_path_;
    int server_fd_ = -1;
    std::atomic<bool> running_;
    std::unordered_map<std::string, std::function<json(const json&)>> methods_;
};


// RPCServer 公共接口实现
RPCServer& RPCServer::instance(const std::string& socket_path) {
    static bool instance_inited = false;
    if(socket_path.empty() && !instance_inited){
        spdlog::error("RPCServer: socket_path must be provided on first call");
        throw std::runtime_error("RPCServer: socket_path must be provided on first call");
    }

    static RPCServer instance(socket_path);
    instance_inited = true;

    return instance;
}

RPCServer::RPCServer(const std::string& socket_path) 
    : impl_(std::make_unique<Impl>(socket_path)) {}

RPCServer::~RPCServer() = default;

void RPCServer::register_method(const std::string& name, std::function<nlohmann::json(const nlohmann::json&)> func) {
    impl_->register_method(name, [func = std::move(func)](const nlohmann::json& params) {
        return func(params);
    });
}

void RPCServer::start() { impl_->start(); }
void RPCServer::stop() { impl_->stop(); }
bool RPCServer::is_running() const { return impl_->is_running(); }

std::string RPCServer::get_socket_path() const { 
    return impl_->get_socket_path(); 
}

std::vector<std::string> RPCServer::get_registered_methods() const { 
    return impl_->get_registered_methods(); 
}


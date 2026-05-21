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
#include "common/streamer_watcher.hpp"
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <atomic>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <spdlog/spdlog.h>
#include <filesystem>
namespace {

int create_and_bind_tcp(const std::string& host_port) {
    size_t colon = host_port.find(':');
    if (colon == std::string::npos) throw std::runtime_error("bad host:port");
    std::string host = host_port.substr(0, colon);
    int port = std::stoi(host_port.substr(colon + 1));

    int sock = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (sock < 0) throw std::runtime_error("socket");

    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (host == "*") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    }

    if (bind(sock, (sockaddr*)&addr, sizeof(addr)) < 0)
        throw std::runtime_error("bind");

    if (listen(sock, 10) < 0) throw std::runtime_error("listen");
    return sock;
}

int open_fifo(const std::string& path) {
    /* 1. 目录不存在则递归创建 */
    auto dir = std::filesystem::path(path).parent_path();
    if (!dir.empty() && !std::filesystem::exists(dir)) {
        std::error_code ec;
        std::filesystem::create_directories(dir, ec);
        if (ec) {
            spdlog::error("StreamWatcher: open_fifo: failed to create dir {}: {}", dir.c_str(), ec.message());
            throw std::runtime_error("create_directories");
        }
        spdlog::info("StreamWatcher: open_fifo: created directory {}", dir.c_str());
    }

    /* 2. 文件已存在则删除（避免旧 fifo 残留导致 open 阻塞） */
    if (std::filesystem::exists(path)) {
        std::error_code ec;
        std::filesystem::remove(path, ec);
        if (ec) {
            spdlog::error("StreamWatcher: open_fifo: failed to remove old fifo {}: {}", path, ec.message());
            throw std::runtime_error("remove old fifo");
        }
        spdlog::warn("StreamWatcher: open_fifo: removed stale fifo {}", path);
    }

    /* 3. 创建新 fifo */
    if (mkfifo(path.c_str(), 0666) != 0) {
        spdlog::error("StreamWatcher: open_fifo: mkfifo {} failed: {}", path, strerror(errno));
        throw std::runtime_error("mkfifo");
    }
    spdlog::info("StreamWatcher: open_fifo: created fifo {}", path);

    /* 4. 非阻塞打开 */
    int fd = open(path.c_str(), O_RDONLY | O_NONBLOCK);
    if (fd < 0) {
        spdlog::error("StreamWatcher: open_fifo: open {} failed: {}", path, strerror(errno));
        throw std::runtime_error("open fifo");
    }
    return fd;
}
int open_file(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("open file");
    // 直接跳到末尾，后面用 inotify 检测追加
    lseek(fd, 0, SEEK_END);
    return fd;
}

} // namespace

class StreamWatcher::Impl {
public:
    Impl(const Config& cfg, Callback cb) : cfg_(cfg), cb_(std::move(cb)) {
        if (cfg_.type == Type::TCP) {
            fd_ = create_and_bind_tcp(cfg_.path);
        } else if (cfg_.type == Type::FIFO) {
            fd_ = open_fifo(cfg_.path);
        } else if (cfg_.type == Type::FILE) {
            fd_ = open_file(cfg_.path);
        }
        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ < 0){
            ::close(fd_);
            spdlog::error("epoll_create1 failed: {}", strerror(errno));
            throw std::runtime_error("epoll_create1");
        }

        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = fd_;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd_, &ev) < 0){
            ::close(fd_);
            ::close(epoll_fd_);
            spdlog::error("epoll_ctl add failed: {}", strerror(errno));
            throw std::runtime_error("epoll_ctl add");
        }
    }

    ~Impl() {
        stop();
        ::close(fd_);
        ::close(epoll_fd_);
    }

    void start() {
        stop_flag_ = false;
        thread_ = std::thread([this]() {
            constexpr int max_events = 64;
            epoll_event events[max_events];
            while (!stop_flag_) {
                int nf = epoll_wait(epoll_fd_, events, max_events, -1);
                if (nf < 0) {
                    if (errno == EINTR) continue;
                    spdlog::error("epoll_wait failed: {}", strerror(errno));
                    break;
                }
                for (int i = 0; i < nf; ++i) {
                    handle_event(events[i]);
                }
            }
        });
    }

    void stop() {
        stop_flag_ = true;
        if (thread_.joinable()) thread_.join();
    }

private:
    void handle_event(const epoll_event& ev) {
        if (cfg_.type == Type::TCP) {
            if (ev.data.fd == fd_) {
                // 监听 socket 可读 => 新连接
                int conn = accept4(fd_, nullptr, nullptr, SOCK_NONBLOCK);
                if (conn < 0) return;
                epoll_event new_ev{};
                new_ev.events = EPOLLIN | EPOLLET; // 边缘触发
                new_ev.data.fd = conn;
                epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, conn, &new_ev);
                return;
            }
        }
        spdlog::debug("StreamWatcher: enter an event");
        // 普通数据可读
        char buf[4096];
        ssize_t n = read(ev.data.fd, buf, sizeof(buf));
        if (n <= 0) {
            if (cfg_.type == Type::TCP) {
                epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, ev.data.fd, nullptr);
                ::close(ev.data.fd);
            }
            return;
        }
        spdlog::debug("StreamWatcher: read {} bytes from fd {}", n, ev.data.fd);
        cb_(buf, static_cast<std::size_t>(n));
    }

    Config cfg_;
    Callback cb_;
    int fd_;
    int epoll_fd_;
    std::thread thread_;
    std::atomic<bool> stop_flag_{false};
};

// public 接口转发
StreamWatcher::StreamWatcher(const Config& cfg, Callback cb)
    : pImpl_(std::make_unique<Impl>(cfg, std::move(cb))) {

    spdlog::info("StreamWatcher: started watching {}",
                cfg.type == Type::TCP ? ("tcp:" + cfg.path) :
                cfg.type == Type::FIFO ? ("fifo:" + cfg.path) :
                ("file:" + cfg.path));
    }
StreamWatcher::~StreamWatcher() = default;
void StreamWatcher::start() { pImpl_->start(); }
void StreamWatcher::stop() { pImpl_->stop(); }
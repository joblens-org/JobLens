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
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include <functional>
#include <memory>
#include <string>

class StreamWatcher {
public:
    // 统一回调签名：buf 为本次可读到的数据，len 为数据长度
    using Callback = std::function<void(const char* buf, std::size_t len)>;

    enum class Type {
        TCP,      // 监听某个 TCP 端口，有连接到达后监视该连接的 socket
        FIFO,     // 监听 Linux 管道（mkfifo）
        FILE      // 监视一个普通文件，检测追加内容（inotify）
    };

    struct Config {
        Type type;
        std::string path;   // 对于 FIFO/FILE 是文件路径；对于 TCP 是 "host:port"
    };

    StreamWatcher(const Config& cfg, Callback cb);
    ~StreamWatcher();

    void start();   // 启动事件循环（内部线程）
    void stop();    // 停止事件循环，可重复 start/stop

private:
    class Impl;
    std::unique_ptr<Impl> pImpl_;
};
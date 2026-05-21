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
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <sys/inotify.h>
#include <sys/select.h>
#include <unistd.h>
#include <fcntl.h>
#include <filesystem>
#include <functional>
#include <cstring>
#include <spdlog/spdlog.h>

// 用于inotify事件的缓冲区计算
#define EVENT_SIZE (sizeof(struct inotify_event))
#define BUF_LEN (1024 * (EVENT_SIZE + 16))

class DirectoryWatcher {
public:
    // 事件回调函数类型
    using EventCallback = std::function<void(const std::string& path, uint32_t mask)>;
    
    explicit DirectoryWatcher(const std::string& rootPath);
    ~DirectoryWatcher();
    
    // 禁止拷贝，允许移动
    DirectoryWatcher(const DirectoryWatcher&) = delete;
    DirectoryWatcher& operator=(const DirectoryWatcher&) = delete;
    
    // 开始监控
    void start();
    
    // 停止监控
    void stop();
    
    // 设置事件回调
    void setEventCallback(EventCallback callback);
    
    // 获取目录下所有文件列表（线程安全）
    std::vector<std::string> getAllFiles() const;
    
    // 检查是否正在运行
    bool isRunning() const { return running; }

private:
    // 私有成员变量
    std::string rootPath;                          // 监控根目录
    int inotifyFd;                                 // inotify文件描述符
    std::atomic<bool> running{false};              // 运行状态
    std::thread watchThread;                       // 监控线程
    mutable std::mutex mutex;                      // 保护文件列表的互斥锁
    std::unordered_map<int, std::string> watchMap; // wd到路径的映射
    std::vector<std::string> allFiles;             // 缓存的文件列表
    EventCallback eventCallback;                   // 事件回调
    std::atomic<bool> needUpdate{true};            // 文件列表需要更新标志
    
    // 私有方法
    void watchThreadFunc();                       // 监控线程主函数
    void recursiveAddWatch(const std::string& path); // 递归添加监控
    void handleEvent(struct inotify_event* event); // 处理inotify事件
    void collectAllFiles();                       // 收集所有文件
    void addWatchForPath(const std::string& path); // 为单个路径添加监控
};

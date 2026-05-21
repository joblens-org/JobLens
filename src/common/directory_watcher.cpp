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
#include "common/directory_watcher.hpp"


DirectoryWatcher::DirectoryWatcher(const std::string& rootPath) 
    : rootPath(rootPath), inotifyFd(-1) {
    // 创建inotify实例
    inotifyFd = inotify_init1(IN_NONBLOCK);
    if (inotifyFd < 0) {
        spdlog::error("DirectoryWatcher: Failed to initialize inotify: {}", strerror(errno));
        throw std::runtime_error("inotify_init1 failed");
    }
    
    // 递归添加初始监控
    recursiveAddWatch(rootPath);
}

DirectoryWatcher::~DirectoryWatcher() {
    stop();
    if (inotifyFd >= 0) {
        close(inotifyFd);
    }
}

void DirectoryWatcher::start() {
    if (running) return;
    
    running = true;
    needUpdate = true;
    
    // 启动监控线程
    watchThread = std::thread(&DirectoryWatcher::watchThreadFunc, this);
}

void DirectoryWatcher::stop() {
    if (!running) return;
    
    running = false;
    
    // 通知线程退出
    if (watchThread.joinable()) {
        watchThread.join();
    }
}

void DirectoryWatcher::setEventCallback(EventCallback callback) {
    std::lock_guard<std::mutex> lock(mutex);
    eventCallback = callback;
}

std::vector<std::string> DirectoryWatcher::getAllFiles() const {
    std::lock_guard<std::mutex> lock(mutex);
    
    // 如果需要更新或文件列表为空，则重新收集
    if (needUpdate || allFiles.empty()) {
        const_cast<DirectoryWatcher*>(this)->collectAllFiles();
        const_cast<DirectoryWatcher*>(this)->needUpdate = false;
    }
    
    return allFiles;
}

void DirectoryWatcher::recursiveAddWatch(const std::string& path) {
    try {
        // 遍历目录树
        for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
            if (entry.is_directory()) {
                addWatchForPath(entry.path().string());
            }
        }
        // 添加根目录本身
        addWatchForPath(path);
    } catch (const std::exception& e) {
        std::cerr << "Error traversing directory: " << e.what() << std::endl;
    }
}

void DirectoryWatcher::addWatchForPath(const std::string& path) {
    // 添加监控
    int wd = inotify_add_watch(inotifyFd, path.c_str(), 
                               IN_CREATE | IN_DELETE | IN_MODIFY | IN_MOVED_FROM | 
                               IN_MOVED_TO | IN_CLOSE_WRITE);
    
    if (wd < 0) {
        std::cerr << "Failed to add watch for " << path << ": " 
                  << strerror(errno) << std::endl;
        return;
    }
    
    std::lock_guard<std::mutex> lock(mutex);
    watchMap[wd] = path;
}

void DirectoryWatcher::watchThreadFunc() {
    fd_set rfds;
    struct timeval tv;
    char buffer[BUF_LEN];
    
    while (running) {
        FD_ZERO(&rfds);
        FD_SET(inotifyFd, &rfds);
        
        // 设置超时时间
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        
        // 使用select等待事件
        int retval = select(inotifyFd + 1, &rfds, nullptr, nullptr, &tv);
        
        if (retval < 0) {
            if (errno == EINTR) continue; // 被信号中断
            std::cerr << "select() error: " << strerror(errno) << std::endl;
            break;
        } else if (retval == 0) {
            // 超时，继续循环
            continue;
        }
        
        // 有事件发生，读取事件
        int length = read(inotifyFd, buffer, BUF_LEN);
        if (length < 0) {
            if (errno != EAGAIN) {
                std::cerr << "read() error: " << strerror(errno) << std::endl;
            }
            continue;
        }
        
        // 处理所有事件
        int i = 0;
        while (i < length) {
            struct inotify_event* event = (struct inotify_event*)&buffer[i];
            handleEvent(event);
            i += EVENT_SIZE + event->len;
        }
    }
}

void DirectoryWatcher::handleEvent(struct inotify_event* event) {
    std::lock_guard<std::mutex> lock(mutex);
    
    auto it = watchMap.find(event->wd);
    if (it == watchMap.end()) return;
    
    std::string parentPath = it->second;
    std::string fullPath;
    
    if (event->len > 0) {
        fullPath = parentPath + "/" + event->name;
    } else {
        fullPath = parentPath;
    }
    
    // 标记需要更新文件列表
    needUpdate = true;
    
    // 处理不同类型的事件
    if (event->mask & IN_CREATE) {
        // 新文件或目录创建
        if (eventCallback) {
            eventCallback(fullPath, IN_CREATE);
        }
        
        // 如果是新目录，递归添加监控
        if (event->mask & IN_ISDIR) {
            // 延迟一下确保目录创建完成
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            addWatchForPath(fullPath);
        }
    }
    
    if (event->mask & IN_DELETE) {
        if (eventCallback) {
            eventCallback(fullPath, IN_DELETE);
        }
    }
    
    if (event->mask & IN_MODIFY) {
        if (eventCallback) {
            eventCallback(fullPath, IN_MODIFY);
        }
    }
    
    if (event->mask & IN_MOVED_FROM) {
        if (eventCallback) {
            eventCallback(fullPath, IN_MOVED_FROM);
        }
    }
    
    if (event->mask & IN_MOVED_TO) {
        if (eventCallback) {
            eventCallback(fullPath, IN_MOVED_TO);
        }
        
        // 如果是目录，添加监控
        if (event->mask & IN_ISDIR) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            addWatchForPath(fullPath);
        }
    }
    
    if (event->mask & IN_CLOSE_WRITE) {
        if (eventCallback) {
            eventCallback(fullPath, IN_CLOSE_WRITE);
        }
    }
}

void DirectoryWatcher::collectAllFiles() {
    allFiles.clear();
    
    try {
        // 使用C++17 filesystem递归遍历
        for (const auto& entry : std::filesystem::recursive_directory_iterator(rootPath)) {
            if (entry.is_regular_file()) {
                allFiles.push_back(entry.path().string());
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error collecting files: " << e.what() << std::endl;
    }
}
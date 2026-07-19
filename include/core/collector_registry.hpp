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
// collector_registry.h
#pragma once
#include <unordered_map>
#include <functional>
#include <memory>
#include <mutex>
#include <atomic>
#include <limits>
#include <thread>
#include "common/print_fmt.hpp"
#include "collector/icollector.h"
#include "common/config.hpp"
#include "common/perf_counter.hpp"


class CollectorRegistry {
public:
    // 单例（可选）；也可 main() 手动构造
    static CollectorRegistry& instance();

    // 统一的数据结构，包含采集器的所有信息
    struct CollectorInfo {
        using Factory = std::function<std::unique_ptr<ICollector>()>;
        
        Factory factory;                    // 工厂函数
        CollectorScope scope;               // 作用域
        CollectorHelpInfo help_info;        // 帮助信息
        
        CollectorInfo() = default;
        CollectorInfo(Factory f, CollectorScope s, CollectorHelpInfo help) 
            : factory(std::move(f)), scope(s), help_info(std::move(help)) {}
    };

    // 统一的注册函数
    template <typename T>
    void registerCollector(const std::string& name, CollectorScope scope, const CollectorHelpInfo& help_info) {
        collectors_.emplace(name, CollectorInfo{
            []() -> std::unique_ptr<ICollector> { return std::make_unique<T>(); },
            scope,
            help_info
        });
    }

    // 重载版本：使用可变参数创建帮助信息
    template <typename T>
    void registerCollector(const std::string& name, CollectorScope scope, 
                          const std::string& help_text, 
                          std::initializer_list<ConfigParam> config_params = {}) {
        CollectorHelpInfo help_info;
        help_info.help_text = help_text;
        help_info.config_params = config_params;
        
        registerCollector<T>(name, scope, help_info);
    }

    // 重载版本：接受ConfigParams（vector）
    template <typename T>
    void registerCollector(const std::string& name, CollectorScope scope, 
                          const std::string& help_text, 
                          const ConfigParams& config_params) {
        CollectorHelpInfo help_info;
        help_info.help_text = help_text;
        help_info.config_params = config_params;
        
        registerCollector<T>(name, scope, help_info);
    }

    CollectorScope getScope(const std::string& name) const;

    // 获取帮助信息
    const CollectorHelpInfo* getHelp(const std::string& name) const;

    const std::string getFmtHelp(const std::string& name, int width) const;

    // 工厂：根据名称 + JSON 配置生成一个"已初始化"的可调用对象
    // 返回 nullptr 表示失败
    CollectorHandle createCollector(const std::string& type, const std::string& name);

    // 列举已注册采集器（调试用）
    std::vector<std::string> list() const;

    CollectDataParseFunc getCollectorParser(const std::string& collector_name, const std::string& writer_type);

    CollectDataParseFuncV2 getCollectorParserV2(const std::string& collector_name, const std::string& writer_type);

    // 便捷方法：按 V2 → V1 → nullptr 回退顺序解析最佳 parser
    // writer 调用点应使用此方法，避免因 FileWriter raw string fallback 绕过 V2-only parser
    CollectDataParseFuncV2 resolveBestParserV2(const std::string& collector_name, const std::string& writer_type);

    std::string getCollectorType(const std::string& collector_name);

    void shutdown() {
        // 清理所有实例
        collector_instances_.clear();
        collectors_.clear();
    }

private:
    CollectorRegistry() = default;
    std::unordered_map<std::string, CollectorInfo> collectors_;  // 统一的数据结构
    std::unordered_map<std::string, std::unique_ptr<ICollector>> collector_instances_;  
    // collector perf
    void initCollectorPerf();
    CollectFunc makePerfFunc(std::string name, CollectFunc func);
    std::mutex perf_mtx_;
    bool enable_collector_perf;
    bool perf_inited;
    int perf_win_size;
    std::unordered_map<std::string, std::unique_ptr<PerfCounter>> collector_perf_;
};

template <typename T>
struct AutoReg {
    AutoReg(const char* name) {
        CollectorRegistry::instance().registerCollector<T>(name);
    }
};


// 完全兼容现有使用方式的宏定义
#define AUTO_REGISTER_JOB_COLLECTOR(CollectorClass, ...)                     \
namespace {                                                              \
    struct AutoReg_##CollectorClass {                                    \
        AutoReg_##CollectorClass() {                                     \
            CollectorRegistry::instance()                                \
                .registerCollector<CollectorClass>(#CollectorClass, CollectorScope::Job, __VA_ARGS__); \
        }                                                                \
    };                                                                   \
    static AutoReg_##CollectorClass _auto_reg_##CollectorClass;          \
}

// 带作用域参数的宏，用于需要指定作用域的情况
#define AUTO_REGISTER_SYSTEM_COLLECTOR(CollectorClass, ...)   \
namespace {                                                              \
    struct AutoReg_##CollectorClass {                                    \
        AutoReg_##CollectorClass() {                                     \
            CollectorRegistry::instance()                                \
                .registerCollector<CollectorClass>(#CollectorClass, CollectorScope::System, __VA_ARGS__); \
        }                                                                \
    };                                                                   \
    static AutoReg_##CollectorClass _auto_reg_##CollectorClass;          \
}


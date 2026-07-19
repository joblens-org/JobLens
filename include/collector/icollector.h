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
// icollector.h
#pragma once
#include <string>
#include "core/collector_type.h"
#include <nlohmann/json.hpp>

class ICollector {
public:
    virtual ~ICollector() = default;
    const CollectorScope scope() const { return scope_; }
    // 生命周期
    virtual bool init(const nlohmann::json& config)      = 0;   // 返回 false 表示失败
    virtual CollectResult collect(const Job& job){       // 传入 Job 信息进行采集
        return {};
    }          
    virtual CollectResult collect(){                     // 无参采集，适用于系统级采集器
        return {};
    }                        
    virtual void deinit() noexcept                       = 0;
    virtual CollectDataParseFunc get_writer_parser(const std::string& writer_type) = 0;

    /// V2 解析器适配器 — 提供默认实现，将旧 V1 parser 包装为 V2 兼容格式
    /// 子类可覆写以提供原生 V2 实现；默认实现忽略 WriterParseContext，仅委托给 V1 parser
    virtual CollectDataParseFuncV2 get_writer_parser_v2(const std::string& writer_type) {
        auto legacy = get_writer_parser(writer_type);
        if (!legacy) {
            return nullptr;
        }
        return [legacy](const WriterParseContext& /*ctx*/, std::any data) {
            return legacy(data);
        };
    }

    void set_type(std::string type){
        type_ = type;
    }
    void set_name(std::string name){
        name_ = name;
    }
    std::string get_name(){
        return name_;
    }
    std::string get_type(){
        return type_;
    }
protected:
    CollectorScope scope_;
private:
    std::string name_;
    std::string type_;
};


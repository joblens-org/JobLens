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


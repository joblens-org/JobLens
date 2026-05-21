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
#include "rule_engine/rules_manager.hpp"
#include "common/local_rpc.hpp"
#include <fstream>


RulesManager::RulesManager(const std::string& name, const std::string& rules_dir, const std::string& rules_prefix) : 
name_(name), rule_engine_(), rules_dir_(rules_dir), rules_prefix_(rules_prefix), dir_watcher_(rules_dir) {
    fs::path dir_path(rules_dir);
    if (!fs::exists(dir_path)) {
        fs::create_directories(dir_path);
    }
    rule_engine_.load_rules_from_directory(rules_dir);

    dir_watcher_.setEventCallback(
        [this](const std::string& path, uint32_t mask) {
            auto filename = fs::path(path).filename().string();
            if (filename.find(rules_prefix_) != 0) {
                return;
            }
            if (mask & (IN_CLOSE_WRITE)) {
                spdlog::info("RulesManager: Detected change in rule file: {}", path);
                rule_engine_.load_rule(path);
            } else if (mask & IN_DELETE) {
                spdlog::info("RulesManager: Detected deletion of rule file: {}", path);
                rule_engine_.remove_rule(path);
            }
        }
    );
    dir_watcher_.start();
}

RulesManager::~RulesManager() {
    spdlog::info("RulesManager: Stopping directory watcher for '{}'", name_);
    dir_watcher_.stop();
    spdlog::info("RulesManager: Directory watcher stopped for '{}'", name_);
}

void RulesManager::register_rpc_methods() {
    RPCServer::instance().register_method(
        name_ + "/list_rules",
        [this](const nlohmann::json& params) -> nlohmann::json {
            return rule_engine_.list_rules();
        }
    );

    RPCServer::instance().register_method(
        name_ + "/add_rule",
        [this](const nlohmann::json& params) -> nlohmann::json {
            if (!params.contains("rule_filename") || !params.contains("lua_code")) {
                nlohmann::json res;
                res["error"] = "missing 'rule_filename' or 'lua_code' parameter";
                return res;
            }
            std::string rule_filename = params["rule_filename"].get<std::string>();
            std::string lua_code = params["lua_code"].get<std::string>();
            nlohmann::json res;
            if (add_rule(rule_filename, lua_code)) {
                res["status"] = "success";
            } else {
                res["error"] = "failed to add rule";
            }
            return res;
        }
    );

    RPCServer::instance().register_method(
        name_ + "/remove_rule",
        [this](const nlohmann::json& params) -> nlohmann::json {
            if (!params.contains("rule_filename")) {
                nlohmann::json res;
                res["error"] = "missing 'rule_filename' parameter";
                return res;
            }
            std::string rule_filename = params["rule_filename"].get<std::string>();
            nlohmann::json res;
            if (remove_rule(rule_filename)) {
                res["status"] = "success";
            } else {
                res["error"] = "failed to remove rule";
            }
            return res;
        }
    );

    RPCServer::instance().register_method(
        name_ + "/update_rules",
        [this](const nlohmann::json& params) -> nlohmann::json {
            nlohmann::json res;
            int updated = rule_engine_.update_rules();
            res["updated_rules"] = updated;
            return res;
        }
    );

    RPCServer::instance().register_method(
        name_ + "/set_rule_enabled",
        [this](const nlohmann::json& params) -> nlohmann::json {
            nlohmann::json res;
            if (!params.contains("rule_filename") ) {
                res["error"] = "missing 'rule_filename' parameter";
                return res;
            }
            std::string rule_filename = params["rule_filename"].get<std::string>();
            rule_engine_.set_enabled(rule_filename, true);
            res["status"] = "success";
            return res;
        }
    );

    RPCServer::instance().register_method(
        name_ + "/set_rule_disabled",
        [this](const nlohmann::json& params) -> nlohmann::json {
            nlohmann::json res;
            if (!params.contains("rule_filename")) {
                res["error"] = "missing 'rule_filename' parameter";
                return res;
            }
            std::string rule_filename = params["rule_filename"].get<std::string>();
            rule_engine_.set_enabled(rule_filename, false);
            res["status"] = "success";
            return res;
        }
    );
}


bool RulesManager::add_rule(const std::string& rule_filename, const std::string& lua_code) {
    auto is_valid = rule_engine_.validate_rule_code(lua_code);
    if (!is_valid) {
        spdlog::error("RulesManager: Invalid Lua code for rule '{}'", rule_filename);
        return false;
    }
    // 写入到文件中，利用watch机制自动加载
    auto path = fs::path(rule_filename).parent_path().string();
    auto filename = fs::path(rule_filename).filename().string();
    if (filename.find(rules_prefix_) != 0) {
        filename = rules_prefix_ + filename;
    }
    
    if (path.empty()) {
        path = rules_dir_;
    }
    else if (path[0] != '/') {
        path = rules_dir_ + "/" + path;
    } else {
        spdlog::error("RulesManager: Cannot add rule outside of rules directory: '{}'", rule_filename);
        return false;
    }
    auto full_path = path + "/" + filename;
    std::ofstream file(full_path);
    file << lua_code;
    file.close();
    return true;
}


bool RulesManager::remove_rule(const std::string& rule_filename) {
    if (std::remove(rule_filename.c_str()) != 0) {
        spdlog::error("RulesManager: Failed to remove rule file '{}'", rule_filename);
        return false;
    }
    return true;
}


bool RulesManager::update_rule(const std::string& rule_filename, const std::string& lua_code) {
    if(remove_rule(rule_filename)){
        return add_rule(rule_filename, lua_code);
    }else{
        return false;
    }
}

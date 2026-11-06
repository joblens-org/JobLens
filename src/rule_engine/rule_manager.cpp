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
#include "rule_engine/rule_path.hpp"
#include "common/local_rpc.hpp"
#include <fstream>
#include <system_error>


RulesManager::RulesManager(const std::string& name, const std::string& rules_dir, const std::string& rules_prefix) : 
name_(name), rule_engine_(), rules_dir_(rules_dir), rules_prefix_(rules_prefix), dir_watcher_(rules_dir) {
    fs::path dir_path(rules_dir);
    if (!fs::exists(dir_path)) {
        std::error_code ec;
        fs::create_directories(dir_path, ec);
        if (ec) {
            spdlog::error("RulesManager: failed to create rules dir {}: {}", rules_dir, ec.message());
        }
    }
    // 规则目录只有 root/服务用户可写；否则任何本地用户都能改以 root 执行的规则。
    {
        std::error_code ec;
        fs::permissions(dir_path,
                        fs::perms::owner_all | fs::perms::group_read | fs::perms::group_exec,
                        fs::perm_options::replace, ec);
        if (ec) {
            spdlog::warn("RulesManager: failed to tighten rules dir {}: {}", rules_dir, ec.message());
        }
        const auto st = fs::status(dir_path, ec);
        if (!ec && (st.permissions() & fs::perms::others_write) != fs::perms::none) {
            spdlog::error("RulesManager: rules dir {} is world-writable; "
                          "any local user can inject Lua that runs as root", rules_dir);
        }
    }
    rule_engine_.load_rules_from_directory(rules_dir);

    dir_watcher_.setEventCallback(
        [this](const std::string& path, uint32_t mask) {
            auto filename = fs::path(path).filename().string();
            if (filename.find(rules_prefix_) != 0) {
                return;
            }
            if (fs::path(path).extension() != ".lua") {
                spdlog::warn("RulesManager: ignoring non-.lua path: {}", path);
                return;
            }
            if (!joblens::rules::path_within(path, rules_dir_)) {
                spdlog::error("RulesManager: ignoring rule outside rules dir: {}", path);
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
    // 规则名可来自 RPC/etcd：只接受平坦目录下的安全文件名，禁止路径穿越。
    auto filename = fs::path(rule_filename).filename().string();
    if (!joblens::rules::filename_ok(filename)) {
        spdlog::error("RulesManager: invalid rule filename '{}'", rule_filename);
        return false;
    }
    if (lua_code.size() > 256 * 1024) {
        spdlog::error("RulesManager: rule '{}' is too large ({} bytes)", filename, lua_code.size());
        return false;
    }
    if (filename.find(rules_prefix_) != 0) {
        filename = rules_prefix_ + filename;
    }
    const fs::path root(rules_dir_);
    const fs::path full_path = root / filename;
    if (!joblens::rules::path_within(full_path, root)) {
        spdlog::error("RulesManager: refusing to write outside rules dir: '{}'", full_path.string());
        return false;
    }

    auto is_valid = rule_engine_.validate_rule_code(lua_code);
    if (!is_valid) {
        spdlog::error("RulesManager: Invalid Lua code for rule '{}'", rule_filename);
        return false;
    }
    // 写入到文件中，利用watch机制自动加载
    std::ofstream file(full_path);
    if (!file.is_open()) {
        spdlog::error("RulesManager: failed to open rule file for write: {}", full_path.string());
        return false;
    }
    file << lua_code;
    file.close();
    return true;
}


bool RulesManager::remove_rule(const std::string& rule_filename) {
    auto filename = fs::path(rule_filename).filename().string();
    if (!joblens::rules::filename_ok(filename)) {
        spdlog::error("RulesManager: invalid rule filename '{}'", rule_filename);
        return false;
    }
    const fs::path root(rules_dir_);
    const fs::path full_path = root / filename;
    if (!joblens::rules::path_within(full_path, root)) {
        spdlog::error("RulesManager: refusing to remove outside rules dir: '{}'", full_path.string());
        return false;
    }
    if (std::remove(full_path.c_str()) != 0) {
        spdlog::error("RulesManager: Failed to remove rule file '{}'", full_path.string());
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

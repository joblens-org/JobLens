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
#include <iostream>
#include <spdlog/spdlog.h>
#include <csignal>

#include "common/main_utils.hpp"

#include "common/print_fmt.hpp"
#include "core/collector_registry.hpp"
#include "core/writer_manager.hpp"
#include "core/collector_scheduler.hpp"
#include "common/utils.hpp"

/* ---------- collector 子命令 ---------- */
int cmdCollector(const std::vector<std::string>& args)
{
    cxxopts::Options opt("JobLens collector", "Manage collectors");
    opt.add_options()
        ("l,list", "List all available collectors")
        ("d,doc", "Show help for a specific collector", cxxopts::value<std::string>()->default_value(""))
        ;
    auto [argc, argv] = MainUtils::makeArgs(args);
    auto result = opt.parse(argc, argv.data());

    if (result.count("list")) {
        auto collectors = CollectorRegistry::instance().list();
        std::cout << "Available collectors:\n";
        for (const auto& name : collectors) std::cout << " - " << name << '\n';
        return 0;
    }
    if (result.count("doc") && !result["doc"].as<std::string>().empty()) {
        auto name = result["doc"].as<std::string>();
        auto width = getTerminalWidth();
        std::cout << CollectorRegistry::instance().getFmtHelp(name, width > 0 ? width : 80);
        return 0;
    }
    std::cout << opt.help() << '\n';
    return 0;
}

/* ---------- writer 子命令 ---------- */
int cmdWriter(const std::vector<std::string>& args)
{
    cxxopts::Options opt("JobLens writer", "Manage writers");
    opt.add_options()
        ("l,list", "List all available writers")
        ("d,doc", "Show help for a specific writer", cxxopts::value<std::string>()->default_value(""))
        ;
    auto [argc, argv] = MainUtils::makeArgs(args);
    auto result = opt.parse(argc, argv.data());

    if (result.count("list")) {
        auto writers = WriterManager::instance().list();
        std::cout << "Available writers:\n";
        for (const auto& name : writers) std::cout << " - " << name << '\n';
        return 0;
    }
    if (result.count("doc") && !result["doc"].as<std::string>().empty()) {
        auto name = result["doc"].as<std::string>();
        auto width = getTerminalWidth();
        std::cout << WriterManager::instance().getFmtHelp(name, width > 0 ? width : 80);
        return 0;
    }
    std::cout << opt.help() << '\n';
    return 0;
}

int runJobLensService(const cxxopts::ParseResult& result){
    auto mode = result["mode"].as<std::string>();
    std::transform(mode.begin(), mode.end(), mode.begin(), ::tolower);
    auto config_path = result["config"].as<std::string>();
    MainUtils::init(config_path);

    if (mode.compare("service") == 0){
        if(MainUtils::already_running()){
            spdlog::error("Main: Anothor JobLens has already started");
            return 0;
        }
        MainUtils::onBecomeService();
    }
    
    // 优雅退出机制
    static std::atomic<bool> should_exit{false};
    static std::mutex mtx;
    static std::condition_variable cv;

    // 设置信号处理器
    auto signal_handler = [](int signal) {
        spdlog::info("Main: Received signal {}, initiating shutdown", signal);
        should_exit = true;
        cv.notify_all();
    };

    // 注册信号处理器
    ::signal(SIGINT, signal_handler);   // Ctrl+C
    ::signal(SIGTERM, signal_handler);  // kill命令
    
    spdlog::info("Main: JobLens service is running.");
    
    // 等待退出信号
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [] { 
        return should_exit.load(); 
    });
    
    spdlog::info("Main: Shutting down JobLens service...");
    
    // 执行清理操作
    if (mode.compare("service") == 0){
        CollectorScheduler::instance().shutdown();
        WriterManager::instance().shutdown();
        CollectorRegistry::instance().shutdown();
    }
    
    spdlog::info("Main: JobLens service stopped successfully");
    return 0;
}

int main(int argc, char* argv[]) {
    try{
        cxxopts::Options global_opt("JobLens", "A job monitor system");
        global_opt.add_options()
            ("h,help", "Show help")
            ("c,config", "Configuration file path", cxxopts::value<std::string>()->default_value("config.yaml"))
            ("m,mode", "run mode (default: service)", cxxopts::value<std::string>()->default_value("service"))
            ("v,version", "Show the version")
            ;
        global_opt.allow_unrecognised_options();
        
        auto result = global_opt.parse(argc, argv);
        auto unmatched = result.unmatched();
        if (argc < 2) {
            std::cout << global_opt.help() << std::endl;
            return 0;
        }

        if (result.count("help") && unmatched.empty()) {
            std::cout << global_opt.help() << "\n\n"
                      << "Sub-commands:\n"
                      << "  collector  Manage collectors\n"
                      << "  writer     Manage writers\n";
            return 0;
        }

        if (result.count("version")) return MainUtils::version();

        if (!unmatched.empty()) {
            std::string cmd = unmatched[0];
            std::vector<std::string> subArgs{ argv[0] };  // 保留程序名
            subArgs.insert(subArgs.end(), unmatched.begin() + 1, unmatched.end());

            if (cmd.compare("collector") == 0) return cmdCollector(subArgs);
            if (cmd.compare("writer") == 0)    return cmdWriter(subArgs);
            std::cerr << "error: unknown sub-command \"" << cmd << "\"\n";
            return 1;
        }

        return runJobLensService(result);
        
    } catch (const std::exception& ex) {
        spdlog::critical("Main: Unhandled exception: {}\nStack trace:\n{}", 
                         ex.what(), Utils::get_backtrace());
        throw;
    }
}

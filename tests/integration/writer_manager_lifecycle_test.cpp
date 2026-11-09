// Writer status snapshots must stay valid across a concurrent shutdown: a
// query copies the writer set under the manager lock and uses it outside, so
// every writer it saw must live until the snapshot is finished. The manager
// hands out shared ownership for exactly that reason; before the fix the
// snapshot held raw pointers and shutdown could free a writer mid-query.
//
// The interleaving is forced by shape, not by luck: many writers make one
// snapshot call long (a hundred sequential status reads under contention),
// many query threads keep snapshots in flight at every instant, and the
// destroyer swaps and frees the set right in the middle of them. On the
// pre-fix raw-pointer snapshot a sanitizer build of this suite reports a
// deterministic heap-use-after-free; with shared ownership every query
// finishes on a live set and the free waits for the last snapshot.
#include "core/writer_manager.hpp"
#include "writer/base_writer.hpp"
#include "core/output_record.h"
#include "common/local_rpc.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr int kWriterCount = 100;
constexpr int kQueryThreads = 16;

void check(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}

// Destruction timing, owned by the test: the log proves the swapped-out set
// was destroyed exactly once.
struct DestructionLog {
    std::mutex mutex;
    std::condition_variable cv;
    int destroyed = 0;
};

class ProbeWriter : public BaseWriter {
public:
    ProbeWriter(std::string name, std::string type, std::string config_name)
        : BaseWriter(std::move(name), std::move(type), std::move(config_name)) {}
    ~ProbeWriter() override {
        shutdown();
        auto& log = log_();
        std::lock_guard lock(log.mutex);
        ++log.destroyed;
        log.cv.notify_all();
    }
    static DestructionLog& log_() {
        static DestructionLog value;
        return value;
    }
    bool accepts(OutputKind kind) const override { return kind == OutputKind::Text; }
protected:
    bool flush_impl(const std::vector<OutputRecordPtr>&) override { return true; }
};

// Every snapshot must be well formed: either the full writer set or none of
// it, and each reported writer must carry its identity and a lifecycle state.
void query_once(WriterManager& manager) {
    const auto status = manager.output_status();
    check(status.at("status") == "ok", "a concurrent query must return a valid snapshot");
    const auto count = status.at("writer_count").get<size_t>();
    check(count == 0 || count == kWriterCount,
          "a snapshot must see the whole set or the post-shutdown emptiness");
    if (count == kWriterCount) {
        const auto& writers = status.at("writers");
        check(writers.at(0).at("name") == "wm_probe_0" &&
                  writers.at(kWriterCount - 1).at("name") == "wm_probe_" +
                      std::to_string(kWriterCount - 1),
              "the snapshot must carry the writer identities in order");
        for (const auto& entry : writers) {
            const auto lifecycle = entry.at("status").get<std::string>();
            check(lifecycle == "running" || lifecycle == "closing",
                  "the snapshot must carry a lifecycle status");
        }
    }
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    Config::instance(argv[1]);
    // initialize() registers its RPC methods, so the server must exist first.
    auto& rpc = RPCServer::instance(std::string(argv[1]) + ".sock");
    rpc.start();
    spdlog::set_level(spdlog::level::warn);
    auto& manager = WriterManager::instance();
    manager.registerWriter<ProbeWriter>("ProbeWriter");
    manager.initialize();
    query_once(manager);

    std::atomic<bool> stop{false};
    std::atomic<int> warmed{0};
    // The queries must be inside their snapshot calls when the destroyer
    // swaps: the loops only end when the test stops them.
    const auto storm = [&] {
        int served = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            query_once(manager);
            if (++served == 3) ++warmed;
        }
    };
    std::vector<std::thread> queriers;
    for (int i = 0; i < kQueryThreads; ++i) queriers.emplace_back(storm);
    for (int i = 0; i < 100 && warmed.load() < kQueryThreads; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    check(warmed.load() == kQueryThreads,
          "every query thread must be running before shutdown starts");

    // Shut every writer down before the destroyer runs: the manager's own
    // shutdown loop then early-returns per writer, so the gap between the
    // swap and the free shrinks to microseconds and lands squarely inside
    // the snapshots that copied the set before it.
    for (int i = 0; i < kWriterCount; ++i) {
        auto* writer = manager.find("wm_probe_" + std::to_string(i));
        check(writer != nullptr, "every configured writer must be registered");
        writer->shutdown();
    }
    std::thread destroyer([&] { manager.shutdown(); });
    destroyer.join();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    stop.store(true);
    for (auto& querier : queriers) querier.join();

    auto& log = ProbeWriter::log_();
    std::unique_lock lock(log.mutex);
    check(log.cv.wait_for(lock, std::chrono::seconds(2),
                          [&] { return log.destroyed == kWriterCount; }),
          "the writer set must be destroyed exactly once");

    std::cout << "PASS: writer status snapshots stay alive across concurrent shutdown\n";
}

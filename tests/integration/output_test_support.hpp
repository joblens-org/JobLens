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

#include "writer/base_writer.hpp"
#include "core/output_context.hpp"
#include "core/output_router.hpp"

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace output_test {

inline void check(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}

inline std::shared_ptr<SampleMetadata> make_sample(const std::string& collector,
                                                   const std::string& type,
                                                   uint64_t job_id = 1,
                                                   const std::string& sample_id = "run:1") {
    auto sample = std::make_shared<SampleMetadata>();
    sample->sample_id = sample_id;
    sample->collector_name = collector;
    sample->collector_type = type;
    sample->job.JobID = job_id;
    sample->job.NativeJobID = std::to_string(job_id);
    sample->sampled_at = std::chrono::system_clock::time_point(std::chrono::milliseconds(1700000000000LL));
    return sample;
}

inline OutputRecordPtr make_record(const std::string& channel, OutputPayload payload,
                                   const std::string& collector = "collector",
                                   uint64_t job_id = 1,
                                   const std::string& sample_id = "run:1") {
    auto record = std::make_shared<OutputRecord>();
    record->sample = make_sample(collector, "TestCollector", job_id, sample_id);
    record->channel = channel;
    record->sequence = 0;
    record->record_id = sample_id + ":0";
    record->payload = std::move(payload);
    record->accounted_bytes = estimate_output_bytes(record->payload);
    return record;
}

/// 可配置的接收端。默认接受所有 payload 类型。
class NativeSink : public BaseWriter {
public:
    explicit NativeSink(std::string name, std::string config_name = "sink_cfg")
        : BaseWriter(std::move(name), "NativeSink", std::move(config_name)) {}

    ~NativeSink() override { shutdown(); }

    bool accepts(OutputKind) const override { return accept_all; }

    bool accept_all = true;

    void block(bool value) {
        std::lock_guard lock(m_);
        blocked_ = value;
        if (!blocked_) cv_.notify_all();
    }

    void wait_entered() {
        std::unique_lock lock(m_);
        check(cv_.wait_for(lock, std::chrono::seconds(5), [&] { return entered_; }),
              "sink flush did not start");
    }

    std::vector<OutputRecordPtr> delivered() const {
        std::lock_guard lock(m_);
        return delivered_;
    }

    std::vector<OutputRecordPtr> wait_delivered(size_t count) {
        std::unique_lock lock(m_);
        const bool ok = cv_.wait_for(lock, std::chrono::seconds(5),
                                     [&] { return delivered_.size() >= count; });
        check(ok, "sink did not deliver the expected records");
        return delivered_;
    }

    size_t delivered_count() const {
        std::lock_guard lock(m_);
        return delivered_.size();
    }

    void allow_unsupported() { accept_all = true; }

protected:
    bool flush_impl(const std::vector<OutputRecordPtr>& batch) override {
        std::unique_lock lock(m_);
        entered_ = true;
        cv_.notify_all();
        cv_.wait(lock, [&] { return !blocked_; });
        for (const auto& record : batch) delivered_.push_back(record);
        cv_.notify_all();
        return true;
    }

private:
    mutable std::mutex m_;
    std::condition_variable cv_;
    bool blocked_ = false;
    bool entered_ = false;
    std::vector<OutputRecordPtr> delivered_;
};

} // namespace output_test

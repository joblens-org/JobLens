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
#include <atomic>
#include <boost/circular_buffer.hpp>


struct PerfCounter {
    /* 基本计数器 */
    std::atomic<uint64_t> call_cnt{0};
    std::atomic<uint64_t> err_cnt{0};

    /* 增量式均值 & 方差 */
    std::atomic<double> mean{0.0};
    std::atomic<double> variance{0.0};   // M2 / (n-1)

    /* 极值 */
    std::atomic<double> min_us{std::numeric_limits<double>::max()};
    std::atomic<double> max_us{0.0};

    /* 原始样本循环缓冲区 */
    boost::circular_buffer<double> buf;

    explicit PerfCounter(size_t win) : buf(win) {}

    struct Snapshot {
        uint64_t call_cnt = 0;
        uint64_t err_cnt  = 0;
        double   min_us   = 0;
        double   max_us   = 0;
        double   mean_us  = 0;
        double   variance = 0;
    };

    Snapshot snapshot() const {
        Snapshot s;
        s.call_cnt = call_cnt.load(std::memory_order_relaxed);
        s.err_cnt  = err_cnt.load(std::memory_order_relaxed);
        s.min_us   = (s.call_cnt ? min_us.load(std::memory_order_relaxed) : 0);
        s.max_us   = (s.call_cnt ? max_us.load(std::memory_order_relaxed) : 0);
        s.mean_us  = mean.load(std::memory_order_relaxed);
        s.variance  = variance.load(std::memory_order_relaxed);
        return s;
    }

    /* 线程安全：每采完一次调用 */
    void append(double us) {
        /* 1. 极值 */
        double old_min = min_us.load(std::memory_order_relaxed);
        double old_max = max_us.load(std::memory_order_relaxed);
        while (us < old_min &&
               !min_us.compare_exchange_weak(old_min, us, std::memory_order_relaxed));
        while (us > old_max &&
               !max_us.compare_exchange_weak(old_max, us, std::memory_order_relaxed));

        /* 2. 缓冲区 */
        buf.push_back(us);

        /* 3. 增量式均值 & 方差 (Welford) */
        uint64_t n = call_cnt.fetch_add(1, std::memory_order_relaxed) + 1;
        double delta = us - mean.load(std::memory_order_relaxed);
        double new_mean = mean.load(std::memory_order_relaxed) + delta / n;
        mean.store(new_mean, std::memory_order_relaxed);

        double delta2 = us - new_mean;
        double new_m2 = variance.load(std::memory_order_relaxed) * (n - 1) + delta * delta2;
        if (n > 1) variance.store(new_m2 / (n - 1), std::memory_order_relaxed);
    }
};
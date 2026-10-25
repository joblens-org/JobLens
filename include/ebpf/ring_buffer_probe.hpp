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

#include <bpf/libbpf.h>

#include <cerrno>
#include <cstddef>
#include <string>
#include <utility>

#include "core/collector_execution_context.hpp"

// The shared poller watches libbpf's own epoll fd. The collector keeps
// ownership of the ring_buffer and must cancel the registration before
// ring_buffer__free() closes that fd. Drain on the worker with
// ring_buffer__consume_n(rb, budget.max_events); never call
// ring_buffer__poll() from the poll thread.
inline ProbeDescriptor make_ring_buffer_probe(const struct ring_buffer* ring_buffer,
                                              std::string name = {})
{
    return ProbeDescriptor{
        ProbeKind::RingBuffer,
        ring_buffer ? ring_buffer__epoll_fd(ring_buffer) : -1,
        std::move(name),
    };
}

// ---------------------------------------------------------------------------
// Bounded worker-side consumption.
//
// ring_buffer__consume_n() requires libbpf >= 1.5. Older toolchains must not
// silently fall back to the unbounded ring_buffer__consume(); they get an
// explicit -ENOSYS so the collector can report the unsupported dependency.
// A collector targeting older libbpf supplies its own tested bounded adapter
// (sample-callback budget) instead of using this helper unguarded.
// ---------------------------------------------------------------------------

struct BoundedConsumeResult {
    size_t consumed = 0; // records delivered to the registered sample callback
    bool more = false;   // the turn bound was reached; data may remain
    int error = 0;       // libbpf error unrelated to the turn bound
};

inline bool ring_buffer_consume_n_available()
{
#if defined(LIBBPF_MAJOR_VERSION) && defined(LIBBPF_MINOR_VERSION)
    return LIBBPF_MAJOR_VERSION > 1 || (LIBBPF_MAJOR_VERSION == 1 && LIBBPF_MINOR_VERSION >= 5);
#else
    return false;
#endif
}

inline BoundedConsumeResult bounded_ring_buffer_consume(struct ring_buffer* ring_buffer,
                                                        size_t max_events)
{
    BoundedConsumeResult result;
    if (!ring_buffer || max_events == 0) {
        result.error = -EINVAL;
        return result;
    }
#if defined(LIBBPF_MAJOR_VERSION) && defined(LIBBPF_MINOR_VERSION) && \
    (LIBBPF_MAJOR_VERSION > 1 || (LIBBPF_MAJOR_VERSION == 1 && LIBBPF_MINOR_VERSION >= 5))
    const long count = ring_buffer__consume_n(ring_buffer, max_events);
    if (count < 0) {
        result.error = static_cast<int>(count);
        return result;
    }
    result.consumed = static_cast<size_t>(count);
    // No public pending-size API for kernel-attached rings: an exactly-full
    // turn conservatively reports more work, costing one extra empty turn.
    result.more = result.consumed == max_events;
    return result;
#else
    (void)ring_buffer;
    (void)max_events;
    result.error = -ENOSYS;
    return result;
#endif
}

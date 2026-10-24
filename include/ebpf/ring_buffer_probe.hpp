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

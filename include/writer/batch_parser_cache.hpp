#pragma once

#include "core/collector_registry.hpp"
#include <unordered_map>

// A flush owns this cache: a later batch must observe parser registrations again.
// Keep empty results too, so the writer's fallback also resolves once per collector.
class BatchParserCache {
public:
    explicit BatchParserCache(const std::string& writer_type) : writer_type_(writer_type) {}

    const CollectDataParseFuncV2& get(const std::string& collector_name) {
        auto found = parsers_.find(collector_name);
        if (found == parsers_.end()) {
            found = parsers_.emplace(collector_name,
                CollectorRegistry::instance().resolveBestParserV2(collector_name, writer_type_)).first;
        }
        return found->second;
    }

private:
    const std::string& writer_type_;
    std::unordered_map<std::string, CollectDataParseFuncV2> parsers_;
};

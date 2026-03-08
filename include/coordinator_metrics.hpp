#pragma once

#include "coordinator_event_sink.hpp"

#include <array>
#include <cstdint>
#include <cstdio>

namespace hft {

struct CoordinatorMetrics {
  std::array<uint64_t, 6> event_type_counts{};
  std::array<uint64_t, 4> reject_reason_counts{};
};

struct CoordinatorMetricsSnapshot {
  std::array<uint64_t, 6> event_type_counts{};
  std::array<uint64_t, 4> reject_reason_counts{};
  uint64_t dropped_events{};
  uint64_t queued_events{};
};


std::size_t event_type_index(CoordinatorEventType type);
std::size_t reject_reason_index(ExecRejectReason reason);

uint64_t event_count(const CoordinatorMetrics& metrics, CoordinatorEventType type);
uint64_t reject_count(const CoordinatorMetrics& metrics, ExecRejectReason reason);

const char* event_type_name(CoordinatorEventType type);


CoordinatorMetricsSnapshot snapshot(const CoordinatorMetrics& metrics,const QueueEventSink& event_sink);

uint64_t drain_and_report(QueueEventSink& event_sink,CoordinatorMetrics& metrics,FILE* out = stdout);

} 
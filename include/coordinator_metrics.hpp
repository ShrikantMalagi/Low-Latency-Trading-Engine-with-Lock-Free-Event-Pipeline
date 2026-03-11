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
  uint64_t coordinator_dropped_events{};
  uint64_t coordinator_queued_events{};
  uint64_t journal_enqueued_events{};
  uint64_t journal_flushed_events{};
  uint64_t journal_dropped_events{};
  uint64_t journal_queue_depth{};
  uint64_t recovery_replay_attempted{};
  uint64_t recovery_replay_succeeded{};
  uint64_t recovery_records_replayed{};
  uint64_t recovery_error_code{};
  uint64_t recovery_error_line{};
};


std::size_t event_type_index(CoordinatorEventType type);
std::size_t reject_reason_index(ExecRejectReason reason);

uint64_t event_count(const CoordinatorMetrics& metrics, CoordinatorEventType type);
uint64_t reject_count(const CoordinatorMetrics& metrics, ExecRejectReason reason);

const char* event_type_name(CoordinatorEventType type);


CoordinatorMetricsSnapshot snapshot(
    const CoordinatorMetrics& metrics,
    const QueueEventSink& event_sink,
    const JournalSink* journal_sink = nullptr,
    const RecoveryStatus* recovery_status = nullptr);

uint64_t drain_and_report(QueueEventSink& event_sink,CoordinatorMetrics& metrics,FILE* out = stdout);

} 

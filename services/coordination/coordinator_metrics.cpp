#include "coordinator_metrics.hpp"

namespace hft {

std::size_t event_type_index(CoordinatorEventType type) {
  return static_cast<std::size_t>(type);
}

std::size_t reject_reason_index(ExecRejectReason reason) {
  const auto raw = static_cast<std::size_t>(reason);
  return raw > 0 ? raw - 1 : 0;
}

uint64_t event_count(const CoordinatorMetrics& metrics, CoordinatorEventType type) {
  return metrics.event_type_counts[event_type_index(type)];
}

uint64_t reject_count(const CoordinatorMetrics& metrics, ExecRejectReason reason) {
  return metrics.reject_reason_counts[reject_reason_index(reason)];
}

const char* event_type_name(CoordinatorEventType type) {
  switch (type) {
    case CoordinatorEventType::NewAccepted: return "NewAccepted";
    case CoordinatorEventType::NewRejected: return "NewRejected";
    case CoordinatorEventType::FillEmitted: return "FillEmitted";
    case CoordinatorEventType::CancelAccepted: return "CancelAccepted";
    case CoordinatorEventType::CancelRejected: return "CancelRejected";
    case CoordinatorEventType::InternalError: return "InternalError";
    default: return "Unknown";
  }
}

static void print_drain_totals(
    const CoordinatorMetrics& metrics,
    uint64_t drained,
    std::size_t dropped_events,
    FILE* out) {
  std::fprintf(
      out,
      "[coordinator] drained=%llu totals: new_ack=%llu new_rej=%llu fill=%llu "
      "cancel_ack=%llu cancel_rej=%llu internal=%llu rej_invalid_order=%llu "
      "rej_dup_id=%llu rej_unknown_id=%llu rej_invalid_transition=%llu dropped=%llu\n",
      static_cast<unsigned long long>(drained),
      static_cast<unsigned long long>(event_count(metrics, CoordinatorEventType::NewAccepted)),
      static_cast<unsigned long long>(event_count(metrics, CoordinatorEventType::NewRejected)),
      static_cast<unsigned long long>(event_count(metrics, CoordinatorEventType::FillEmitted)),
      static_cast<unsigned long long>(event_count(metrics, CoordinatorEventType::CancelAccepted)),
      static_cast<unsigned long long>(event_count(metrics, CoordinatorEventType::CancelRejected)),
      static_cast<unsigned long long>(event_count(metrics, CoordinatorEventType::InternalError)),
      static_cast<unsigned long long>(reject_count(metrics, ExecRejectReason::InvalidOrder)),
      static_cast<unsigned long long>(reject_count(metrics, ExecRejectReason::DuplicateOrderId)),
      static_cast<unsigned long long>(reject_count(metrics, ExecRejectReason::UnknownOrderId)),
      static_cast<unsigned long long>(reject_count(metrics, ExecRejectReason::InvalidTransition)),
      static_cast<unsigned long long>(dropped_events));
}

CoordinatorMetricsSnapshot snapshot(
    const CoordinatorMetrics& metrics,
    const QueueEventSink& event_sink,
    const JournalSink* journal_sink,
    const RecoveryStatus* recovery_status) {
  return CoordinatorMetricsSnapshot{
      .event_type_counts = metrics.event_type_counts,
      .reject_reason_counts = metrics.reject_reason_counts,
      .coordinator_dropped_events = static_cast<uint64_t>(event_sink.dropped_events()),
      .coordinator_queued_events = static_cast<uint64_t>(event_sink.size()),
      .journal_enqueued_events = journal_sink ? journal_sink->enqueued() : 0,
      .journal_flushed_events = journal_sink ? journal_sink->flushed() : 0,
      .journal_dropped_events = journal_sink ? journal_sink->dropped() : 0,
      .journal_queue_depth = journal_sink ? journal_sink->queue_depth() : 0,
      .recovery_replay_attempted = recovery_status && recovery_status->replay_attempted ? 1u : 0u,
      .recovery_replay_succeeded = recovery_status && recovery_status->replay_succeeded ? 1u : 0u,
      .recovery_records_replayed = recovery_status ? recovery_status->records_replayed : 0u,
      .recovery_error_code = recovery_status ? recovery_status->replay_error_code : 0u,
      .recovery_error_line = recovery_status ? recovery_status->replay_error_line : 0u,
  };
}


uint64_t drain_and_report(QueueEventSink& event_sink, CoordinatorMetrics& metrics, FILE* out) {
  uint64_t drained = 0;
  while (auto envelope = event_sink.try_pop()) {
    const auto& event = envelope->event;

    const auto t = event_type_index(event.type);
    if (t < metrics.event_type_counts.size()) {
      ++metrics.event_type_counts[t];
    }

    if (event.reject_reason.has_value()) {
      const auto r = reject_reason_index(*event.reject_reason);
      if (r < metrics.reject_reason_counts.size()) {
        ++metrics.reject_reason_counts[r];
      }
    }

    std::fprintf(
        out,
        "[coordinator] event_id=%llu ts_ns=%llu type=%s order_id=%llu\n",
        static_cast<unsigned long long>(envelope->event_id),
        static_cast<unsigned long long>(envelope->timestamp_ns),
        event_type_name(event.type),
        static_cast<unsigned long long>(event.order_id));

    ++drained;
  }

  if (drained > 0) {
    print_drain_totals(metrics, drained, event_sink.dropped_events(), out);
    std::fflush(out);
  }

  return drained;
}

}

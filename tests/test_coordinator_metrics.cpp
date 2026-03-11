#include "coordinator_metrics.hpp"

#include <gtest/gtest.h>

#include <cstdio>

namespace {

using hft::CoordinatorEvent;
using hft::CoordinatorEventType;
using hft::CoordinatorMetrics;
using hft::ExecRejectReason;
using hft::QueueEventSink;

TEST(CoordinatorMetrics, DrainAndReportReturnsZeroWhenQueueIsEmpty) {
  QueueEventSink sink;
  CoordinatorMetrics metrics{};

  FILE* out = std::tmpfile();
  ASSERT_NE(out, nullptr);

  const auto drained = hft::drain_and_report(sink, metrics, out);
  EXPECT_EQ(drained, 0u);
  EXPECT_EQ(hft::event_count(metrics, CoordinatorEventType::NewAccepted), 0u);
  EXPECT_EQ(hft::reject_count(metrics, ExecRejectReason::InvalidOrder), 0u);

  std::fclose(out);
}

TEST(CoordinatorMetrics, DrainAndReportAccumulatesEventTypeAndRejectReasonCounters) {
  QueueEventSink sink;
  CoordinatorMetrics metrics{};

  sink.on_event(CoordinatorEvent{
      .type = CoordinatorEventType::NewAccepted,
      .order_id = 1001,
  });
  sink.on_event(CoordinatorEvent{
      .type = CoordinatorEventType::NewRejected,
      .order_id = 1002,
      .reject_reason = ExecRejectReason::DuplicateOrderId,
  });
  sink.on_event(CoordinatorEvent{
      .type = CoordinatorEventType::CancelRejected,
      .order_id = 1003,
      .reject_reason = ExecRejectReason::UnknownOrderId,
  });
  sink.on_event(CoordinatorEvent{
      .type = CoordinatorEventType::InternalError,
      .order_id = 1004,
      .reject_reason = ExecRejectReason::InvalidTransition,
  });

  FILE* out = std::tmpfile();
  ASSERT_NE(out, nullptr);

  const auto drained = hft::drain_and_report(sink, metrics, out);
  EXPECT_EQ(drained, 4u);
  EXPECT_EQ(sink.size(), 0u);

  EXPECT_EQ(hft::event_count(metrics, CoordinatorEventType::NewAccepted), 1u);
  EXPECT_EQ(hft::event_count(metrics, CoordinatorEventType::NewRejected), 1u);
  EXPECT_EQ(hft::event_count(metrics, CoordinatorEventType::CancelRejected), 1u);
  EXPECT_EQ(hft::event_count(metrics, CoordinatorEventType::InternalError), 1u);
  EXPECT_EQ(hft::event_count(metrics, CoordinatorEventType::FillEmitted), 0u);

  EXPECT_EQ(hft::reject_count(metrics, ExecRejectReason::DuplicateOrderId), 1u);
  EXPECT_EQ(hft::reject_count(metrics, ExecRejectReason::UnknownOrderId), 1u);
  EXPECT_EQ(hft::reject_count(metrics, ExecRejectReason::InvalidTransition), 1u);
  EXPECT_EQ(hft::reject_count(metrics, ExecRejectReason::InvalidOrder), 0u);

  std::fclose(out);
}

TEST(CoordinatorMetrics, DrainAndReportIsCumulativeAcrossCalls) {
  QueueEventSink sink;
  CoordinatorMetrics metrics{};

  sink.on_event(CoordinatorEvent{
      .type = CoordinatorEventType::NewAccepted,
      .order_id = 2001,
  });
  sink.on_event(CoordinatorEvent{
      .type = CoordinatorEventType::FillEmitted,
      .order_id = 2002,
  });

  FILE* out = std::tmpfile();
  ASSERT_NE(out, nullptr);

  EXPECT_EQ(hft::drain_and_report(sink, metrics, out), 2u);
  EXPECT_EQ(hft::event_count(metrics, CoordinatorEventType::NewAccepted), 1u);
  EXPECT_EQ(hft::event_count(metrics, CoordinatorEventType::FillEmitted), 1u);

  sink.on_event(CoordinatorEvent{
      .type = CoordinatorEventType::CancelRejected,
      .order_id = 2003,
      .reject_reason = ExecRejectReason::InvalidOrder,
  });

  EXPECT_EQ(hft::drain_and_report(sink, metrics, out), 1u);
  EXPECT_EQ(hft::event_count(metrics, CoordinatorEventType::CancelRejected), 1u);
  EXPECT_EQ(hft::reject_count(metrics, ExecRejectReason::InvalidOrder), 1u);

  std::fclose(out);
}

TEST(CoordinatorMetrics, SnapshotIncludesDroppedAndQueuedEvents) {
    QueueEventSink sink(/*max_queue_size=*/2);
    CoordinatorMetrics metrics{};
  
    sink.on_event(CoordinatorEvent{.type = CoordinatorEventType::NewAccepted, .order_id = 1});
    sink.on_event(CoordinatorEvent{.type = CoordinatorEventType::NewAccepted, .order_id = 2});
    sink.on_event(CoordinatorEvent{.type = CoordinatorEventType::NewAccepted, .order_id = 3}); // drops oldest
  
    const auto snap_before = hft::snapshot(metrics, sink);
    EXPECT_EQ(snap_before.coordinator_dropped_events, 1u);
    EXPECT_EQ(snap_before.coordinator_queued_events, 2u);
  
    FILE* out = std::tmpfile();
    ASSERT_NE(out, nullptr);
    EXPECT_EQ(hft::drain_and_report(sink, metrics, out), 2u);
    std::fclose(out);
  
    const auto snap_after = hft::snapshot(metrics, sink);
    EXPECT_EQ(snap_after.coordinator_queued_events, 0u);
    EXPECT_EQ(snap_after.coordinator_dropped_events, 1u);
    EXPECT_EQ(snap_after.event_type_counts[hft::event_type_index(CoordinatorEventType::NewAccepted)], 2u);
  }

} 

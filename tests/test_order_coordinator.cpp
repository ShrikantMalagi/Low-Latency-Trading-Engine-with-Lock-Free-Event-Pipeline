#include "order_coordinator.hpp"
#include "coordinator_event_sink.hpp"
#include "recovery.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <string>
#include <vector>
#include <unistd.h>

using namespace hft;

namespace {

Order MakeBuy(uint64_t order_id, int64_t price, int64_t qty) {
  return Order{
      .order_id = order_id,
      .side = Side::Buy,
      .price = price,
      .qty = qty,
  };
}

Order MakeSell(uint64_t order_id, int64_t price, int64_t qty) {
  return Order{
      .order_id = order_id,
      .side = Side::Sell,
      .price = price,
      .qty = qty,
  };
}

struct RecordingSink : CoordinatorEventSink {
  std::vector<CoordinatorEvent> events;

  void on_event(const CoordinatorEvent& event) override {
    events.push_back(event);
  }
};

struct BackpressureJournalSink : JournalSink {
  JournalWriteResult write(const OmsJournalEvent&) override {
    return JournalWriteResult::Backpressure;
  }
};

std::string make_temp_journal_path() {
  char tmp[] = "/tmp/hft_journal_XXXXXX";
  const int fd = ::mkstemp(tmp);
  if (fd >= 0) {
    ::close(fd);
  }
  return std::string(tmp);
}

std::vector<int> read_journal_event_types(const std::string& path) {
  std::ifstream in(path);
  std::vector<int> types;
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) {
      continue;
    }
    const auto type_pos = line.find("|type=");
    if (type_pos == std::string::npos) {
      continue;
    }
    const auto value_start = type_pos + 6;
    const auto value_end = line.find('|', value_start);
    const std::string type_name =
        value_end == std::string::npos ? line.substr(value_start) : line.substr(value_start, value_end - value_start);

    if (type_name == "SubmitNew") {
      types.push_back(static_cast<int>(OmsEventType::SubmitNew));
    } else if (type_name == "NewAck") {
      types.push_back(static_cast<int>(OmsEventType::NewAck));
    } else if (type_name == "NewReject") {
      types.push_back(static_cast<int>(OmsEventType::NewReject));
    } else if (type_name == "SubmitCancel") {
      types.push_back(static_cast<int>(OmsEventType::SubmitCancel));
    } else if (type_name == "CancelAck") {
      types.push_back(static_cast<int>(OmsEventType::CancelAck));
    } else if (type_name == "CancelReject") {
      types.push_back(static_cast<int>(OmsEventType::CancelReject));
    } else if (type_name == "Fill") {
      types.push_back(static_cast<int>(OmsEventType::Fill));
    }
  }
  return types;
}

}

TEST(OrderCoordinator, SubmitNewRejectsInvalidOrder) {
  Oms oms;
  Exchange exchange;
  OrderCoordinator coordinator(oms, exchange);

  auto result = coordinator.submit_new(MakeBuy(1, 100, 0));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().order_id, 1u);
  EXPECT_EQ(result.error().reason, ExecRejectReason::InvalidOrder);
}

TEST(OrderCoordinator, SubmitNewMapsDuplicateOrderIdFromOms) {
  Oms oms;
  Exchange exchange;
  OrderCoordinator coordinator(oms, exchange);

  ASSERT_TRUE(coordinator.submit_new(MakeBuy(10, 100, 5)).has_value());

  auto result = coordinator.submit_new(MakeSell(10, 101, 3));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().order_id, 10u);
  EXPECT_EQ(result.error().reason, ExecRejectReason::DuplicateOrderId);
}

TEST(OrderCoordinator, CrossingOrderReturnsAckAndFillAndUpdatesBothSidesInOms) {
  Oms oms;
  Exchange exchange;
  OrderCoordinator coordinator(oms, exchange);

  auto maker_submit = coordinator.submit_new(MakeSell(100, 101, 4));
  ASSERT_TRUE(maker_submit.has_value());
  EXPECT_TRUE(maker_submit->ack);
  EXPECT_TRUE(maker_submit->fills.empty());

  auto taker_submit = coordinator.submit_new(MakeBuy(200, 105, 4));
  ASSERT_TRUE(taker_submit.has_value());
  EXPECT_TRUE(taker_submit->ack);
  ASSERT_EQ(taker_submit->fills.size(), 1u);
  EXPECT_EQ(taker_submit->fills[0].maker_order_id, 100u);
  EXPECT_EQ(taker_submit->fills[0].taker_order_id, 200u);
  EXPECT_EQ(taker_submit->fills[0].price, 101);
  EXPECT_EQ(taker_submit->fills[0].qty, 4);

  auto maker = oms.get(100);
  ASSERT_TRUE(maker.has_value());
  EXPECT_EQ(maker->status, OrderStatus::Filled);
  EXPECT_EQ(maker->remaining_qty, 0);

  auto taker = oms.get(200);
  ASSERT_TRUE(taker.has_value());
  EXPECT_EQ(taker->status, OrderStatus::Filled);
  EXPECT_EQ(taker->remaining_qty, 0);
}

TEST(OrderCoordinator, SubmitCancelUnknownOrderMapsToUnknownOrderId) {
  Oms oms;
  Exchange exchange;
  OrderCoordinator coordinator(oms, exchange);

  auto result = coordinator.submit_cancel(999);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().order_id, 999u);
  EXPECT_EQ(result.error().reason, ExecRejectReason::UnknownOrderId);
}

TEST(OrderCoordinator, SubmitCancelHappyPathReturnsAckAndCancelsInOms) {
  Oms oms;
  Exchange exchange;
  OrderCoordinator coordinator(oms, exchange);

  ASSERT_TRUE(coordinator.submit_new(MakeBuy(300, 100, 6)).has_value());

  auto cancel_result = coordinator.submit_cancel(300);
  ASSERT_TRUE(cancel_result.has_value());
  EXPECT_TRUE(cancel_result->ack);
  EXPECT_TRUE(cancel_result->fills.empty());

  auto rec = oms.get(300);
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->status, OrderStatus::Canceled);
}

TEST(OrderCoordinator, CancelNotFoundOnExchangeReturnsUnknownOrderAndRestoresOmsState) {
  Oms oms;
  Exchange exchange;
  OrderCoordinator coordinator(oms, exchange);

  ASSERT_TRUE(coordinator.submit_new(MakeBuy(400, 100, 6)).has_value());

  // Simulate an external book mutation to force exchange.cancel(...) == false.
  ASSERT_TRUE(exchange.cancel(400));

  auto cancel_result = coordinator.submit_cancel(400);
  ASSERT_FALSE(cancel_result.has_value());
  EXPECT_EQ(cancel_result.error().order_id, 400u);
  EXPECT_EQ(cancel_result.error().reason, ExecRejectReason::UnknownOrderId);

  auto rec = oms.get(400);
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->status, OrderStatus::Live);
}
TEST(OrderCoordinator, SubmitNewWithTrackedMakerUpdatesBothOmsRecords) {
  Oms oms;
  Exchange exchange;
  OrderCoordinator coordinator(oms, exchange);

  // Resting maker sell
  auto maker_submit = coordinator.submit_new(MakeSell(7001, 101, 5));
  ASSERT_TRUE(maker_submit.has_value());
  ASSERT_TRUE(maker_submit->ack);
  ASSERT_TRUE(maker_submit->fills.empty());

  // Crossing taker buy
  auto taker_submit = coordinator.submit_new(MakeBuy(7002, 105, 5));
  ASSERT_TRUE(taker_submit.has_value());
  ASSERT_TRUE(taker_submit->ack);
  ASSERT_EQ(taker_submit->fills.size(), 1u);

  const auto& fill = taker_submit->fills[0];
  EXPECT_EQ(fill.maker_order_id, 7001u);
  EXPECT_EQ(fill.taker_order_id, 7002u);
  EXPECT_EQ(fill.price, 101);
  EXPECT_EQ(fill.qty, 5);

  auto maker = oms.get(7001);
  auto taker = oms.get(7002);
  ASSERT_TRUE(maker.has_value());
  ASSERT_TRUE(taker.has_value());

  EXPECT_EQ(maker->status, OrderStatus::Filled);
  EXPECT_EQ(maker->filled_qty, 5);
  EXPECT_EQ(maker->remaining_qty, 0);

  EXPECT_EQ(taker->status, OrderStatus::Filled);
  EXPECT_EQ(taker->filled_qty, 5);
  EXPECT_EQ(taker->remaining_qty, 0);
}

TEST(OrderCoordinator, InvalidNewOrderEmitsNewRejectedEvent) {
  Oms oms;
  Exchange exchange;
  RecordingSink sink;
  OrderCoordinator coordinator(oms, exchange, &sink);

  auto result = coordinator.submit_new(MakeBuy(9001, 100, 0));
  ASSERT_FALSE(result.has_value());

  ASSERT_EQ(sink.events.size(), 1u);
  EXPECT_EQ(sink.events[0].type, CoordinatorEventType::NewRejected);
  EXPECT_EQ(sink.events[0].order_id, 9001u);
  ASSERT_TRUE(sink.events[0].reject_reason.has_value());
  EXPECT_EQ(*sink.events[0].reject_reason, ExecRejectReason::InvalidOrder);
}

TEST(OrderCoordinator, CrossingFlowEmitsAcceptedThenFillEvents) {
  Oms oms;
  Exchange exchange;
  RecordingSink sink;
  OrderCoordinator coordinator(oms, exchange, &sink);

  ASSERT_TRUE(coordinator.submit_new(MakeSell(9002, 101, 4)).has_value());
  ASSERT_TRUE(coordinator.submit_new(MakeBuy(9003, 105, 4)).has_value());

  ASSERT_EQ(sink.events.size(), 3u);
  EXPECT_EQ(sink.events[0].type, CoordinatorEventType::NewAccepted);
  EXPECT_EQ(sink.events[0].order_id, 9002u);
  EXPECT_EQ(sink.events[1].type, CoordinatorEventType::NewAccepted);
  EXPECT_EQ(sink.events[1].order_id, 9003u);
  EXPECT_EQ(sink.events[2].type, CoordinatorEventType::FillEmitted);
  EXPECT_EQ(sink.events[2].order_id, 9003u);
  ASSERT_TRUE(sink.events[2].contra_order_id.has_value());
  EXPECT_EQ(*sink.events[2].contra_order_id, 9002u);
  ASSERT_TRUE(sink.events[2].price.has_value());
  EXPECT_EQ(*sink.events[2].price, 101);
  ASSERT_TRUE(sink.events[2].qty.has_value());
  EXPECT_EQ(*sink.events[2].qty, 4);
}

TEST(OrderCoordinator, CancelSuccessEmitsCancelAcceptedEvent) {
  Oms oms;
  Exchange exchange;
  RecordingSink sink;
  OrderCoordinator coordinator(oms, exchange, &sink);

  ASSERT_TRUE(coordinator.submit_new(MakeBuy(9004, 100, 5)).has_value());
  ASSERT_TRUE(coordinator.submit_cancel(9004).has_value());

  ASSERT_GE(sink.events.size(), 2u);
  EXPECT_EQ(sink.events.back().type, CoordinatorEventType::CancelAccepted);
  EXPECT_EQ(sink.events.back().order_id, 9004u);
}

TEST(OrderCoordinator, CancelNotFoundEmitsCancelRejectedEvent) {
  Oms oms;
  Exchange exchange;
  RecordingSink sink;
  OrderCoordinator coordinator(oms, exchange, &sink);

  ASSERT_TRUE(coordinator.submit_new(MakeBuy(9005, 100, 6)).has_value());
  ASSERT_TRUE(exchange.cancel(9005));

  auto cancel_result = coordinator.submit_cancel(9005);
  ASSERT_FALSE(cancel_result.has_value());

  ASSERT_GE(sink.events.size(), 2u);
  EXPECT_EQ(sink.events.back().type, CoordinatorEventType::CancelRejected);
  EXPECT_EQ(sink.events.back().order_id, 9005u);
  ASSERT_TRUE(sink.events.back().reject_reason.has_value());
  EXPECT_EQ(*sink.events.back().reject_reason, ExecRejectReason::UnknownOrderId);
}

TEST(OrderCoordinator, InternalErrorEmittedWhenMakerFillTransitionFails) {
  Oms oms;
  Exchange exchange;
  RecordingSink sink;
  OrderCoordinator coordinator(oms, exchange, &sink);

  const Order maker = MakeSell(9101, 101, 3);
  const Order taker = MakeBuy(9102, 105, 3);

  // Intentionally desync OMS and Exchange:
  // maker stays on the exchange book, but OMS marks it PendingCancel.
  ASSERT_TRUE(exchange.add_order(maker).empty());
  ASSERT_TRUE(oms.submit_new(maker).has_value());
  ASSERT_TRUE(oms.on_new_ack(maker.order_id).has_value());
  ASSERT_TRUE(oms.submit_cancel(maker.order_id).has_value());

  auto result = coordinator.submit_new(taker);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().order_id, maker.order_id);
  EXPECT_EQ(result.error().reason, ExecRejectReason::InvalidTransition);

  ASSERT_GE(sink.events.size(), 2u);
  EXPECT_EQ(sink.events.back().type, CoordinatorEventType::InternalError);
  EXPECT_EQ(sink.events.back().order_id, maker.order_id);
  ASSERT_TRUE(sink.events.back().reject_reason.has_value());
  EXPECT_EQ(*sink.events.back().reject_reason, ExecRejectReason::InvalidTransition);
  ASSERT_TRUE(sink.events.back().message.has_value());
}

TEST(OrderCoordinator, JournalBackpressureEmitsInternalError) {
  Oms oms;
  Exchange exchange;
  RecordingSink sink;
  BackpressureJournalSink journal_sink;
  OrderCoordinator coordinator(oms, exchange, &sink, &journal_sink);

  auto result = coordinator.submit_new(MakeBuy(9150, 100, 2));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().order_id, 9150u);
  EXPECT_EQ(result.error().reason, ExecRejectReason::InvalidTransition);
  EXPECT_EQ(result.error().message, "journal backpressure");

  ASSERT_EQ(sink.events.size(), 1u);
  EXPECT_EQ(sink.events.back().type, CoordinatorEventType::InternalError);
  EXPECT_EQ(sink.events.back().order_id, 9150u);
  ASSERT_TRUE(sink.events.back().message.has_value());
  EXPECT_EQ(*sink.events.back().message, "journal backpressure");
}

TEST(OrderCoordinator, QueueSinkCapturesAcceptedEvent) {
  Oms oms;
  Exchange exchange;
  QueueEventSink sink;
  OrderCoordinator coordinator(oms, exchange, &sink);

  auto result = coordinator.submit_new(MakeBuy(9201, 100, 2));
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(sink.size(), 1u);

  auto event = sink.try_pop();
  ASSERT_TRUE(event.has_value());
  EXPECT_EQ(event->event.type, CoordinatorEventType::NewAccepted);
  EXPECT_EQ(event->event.order_id, 9201u);
  EXPECT_EQ(sink.size(), 0u);
}

TEST(OrderCoordinator, QueueSinkPreservesCrossingEventOrder) {
  Oms oms;
  Exchange exchange;
  QueueEventSink sink;
  OrderCoordinator coordinator(oms, exchange, &sink);

  ASSERT_TRUE(coordinator.submit_new(MakeSell(9202, 101, 3)).has_value());
  ASSERT_TRUE(coordinator.submit_new(MakeBuy(9203, 105, 3)).has_value());

  auto e1 = sink.try_pop();
  auto e2 = sink.try_pop();
  auto e3 = sink.try_pop();
  auto e4 = sink.try_pop();

  ASSERT_TRUE(e1.has_value());
  ASSERT_TRUE(e2.has_value());
  ASSERT_TRUE(e3.has_value());
  ASSERT_FALSE(e4.has_value());

  EXPECT_EQ(e1->event.type, CoordinatorEventType::NewAccepted);
  EXPECT_EQ(e1->event.order_id, 9202u);
  EXPECT_EQ(e2->event.type, CoordinatorEventType::NewAccepted);
  EXPECT_EQ(e2->event.order_id, 9203u);
  EXPECT_EQ(e3->event.type, CoordinatorEventType::FillEmitted);
  EXPECT_EQ(e3->event.order_id, 9203u);
  ASSERT_TRUE(e3->event.contra_order_id.has_value());
  EXPECT_EQ(*e3->event.contra_order_id, 9202u);
}

TEST(OrderCoordinator, QueueSinkTryPopReturnsEmptyWhenNoEvents) {
  QueueEventSink sink;
  auto event = sink.try_pop();
  EXPECT_FALSE(event.has_value());
}

TEST(OrderCoordinator, QueueSinkEnvelopeHasMonotonicIdsAndTimestamps) {
  Oms oms;
  Exchange exchange;
  QueueEventSink sink;
  OrderCoordinator coordinator(oms, exchange, &sink);

  ASSERT_TRUE(coordinator.submit_new(MakeBuy(9301, 100, 2)).has_value());
  ASSERT_TRUE(coordinator.submit_new(MakeSell(9302, 101, 1)).has_value());

  auto first = sink.try_pop();
  auto second = sink.try_pop();

  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());

  EXPECT_EQ(first->source, "order_coordinator");
  EXPECT_EQ(second->source, "order_coordinator");

  EXPECT_GT(first->event_id, 0u);
  EXPECT_GT(second->event_id, first->event_id);

  EXPECT_GT(first->timestamp_ns, 0u);
  EXPECT_GT(second->timestamp_ns, 0u);
  EXPECT_GE(second->timestamp_ns, first->timestamp_ns);
}

TEST(OrderCoordinator, QueueSinkDropsOldestWhenMaxQueueSizeReached) {
  QueueEventSink sink(/*max_queue_size=*/2);

  sink.on_event(CoordinatorEvent{
      .type = CoordinatorEventType::NewAccepted,
      .order_id = 9401,
  });
  sink.on_event(CoordinatorEvent{
      .type = CoordinatorEventType::NewAccepted,
      .order_id = 9402,
  });
  sink.on_event(CoordinatorEvent{
      .type = CoordinatorEventType::NewAccepted,
      .order_id = 9403,
  });

  EXPECT_EQ(sink.max_queue_size(), 2u);
  EXPECT_EQ(sink.size(), 2u);
  EXPECT_EQ(sink.dropped_events(), 1u);

  auto first = sink.try_pop();
  auto second = sink.try_pop();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(first->event.order_id, 9402u);
  EXPECT_EQ(second->event.order_id, 9403u);
}

TEST(OrderCoordinator, QueueSinkTreatsZeroMaxQueueSizeAsOne) {
  QueueEventSink sink(/*max_queue_size=*/0);
  EXPECT_EQ(sink.max_queue_size(), 1u);

  sink.on_event(CoordinatorEvent{
      .type = CoordinatorEventType::NewAccepted,
      .order_id = 9501,
  });
  sink.on_event(CoordinatorEvent{
      .type = CoordinatorEventType::NewAccepted,
      .order_id = 9502,
  });

  EXPECT_EQ(sink.size(), 1u);
  EXPECT_EQ(sink.dropped_events(), 1u);

  auto only = sink.try_pop();
  ASSERT_TRUE(only.has_value());
  EXPECT_EQ(only->event.order_id, 9502u);
}

TEST(OrderCoordinator, JournalsSubmitNewAckAndFillEventsForCrossingOrder) {
  const std::string journal_path = make_temp_journal_path();
  ASSERT_EQ(std::remove(journal_path.c_str()), 0);

  Oms oms;
  Exchange exchange;
  SyncJournalSink sink(journal_path);
  OrderCoordinator coordinator(oms, exchange, nullptr, &sink);

  ASSERT_TRUE(coordinator.submit_new(MakeSell(9601, 101, 3)).has_value());
  ASSERT_TRUE(coordinator.submit_new(MakeBuy(9602, 105, 3)).has_value());

  const auto types = read_journal_event_types(journal_path);
  ASSERT_EQ(types.size(), 6u);
  EXPECT_EQ(types[0], static_cast<int>(OmsEventType::SubmitNew));
  EXPECT_EQ(types[1], static_cast<int>(OmsEventType::NewAck));
  EXPECT_EQ(types[2], static_cast<int>(OmsEventType::SubmitNew));
  EXPECT_EQ(types[3], static_cast<int>(OmsEventType::NewAck));
  EXPECT_EQ(types[4], static_cast<int>(OmsEventType::Fill));
  EXPECT_EQ(types[5], static_cast<int>(OmsEventType::Fill));

  EXPECT_EQ(std::remove(journal_path.c_str()), 0);
}

TEST(OrderCoordinator, JournalsCancelAckAndCancelRejectEvents) {
  const std::string ack_journal_path = make_temp_journal_path();
  ASSERT_EQ(std::remove(ack_journal_path.c_str()), 0);

  Oms oms1;
  Exchange exchange1;
  SyncJournalSink sink1(ack_journal_path);
  OrderCoordinator coordinator1(oms1, exchange1, nullptr, &sink1);
  ASSERT_TRUE(coordinator1.submit_new(MakeBuy(9701, 100, 4)).has_value());
  ASSERT_TRUE(coordinator1.submit_cancel(9701).has_value());

  const auto ack_types = read_journal_event_types(ack_journal_path);
  ASSERT_EQ(ack_types.size(), 4u);
  EXPECT_EQ(ack_types[0], static_cast<int>(OmsEventType::SubmitNew));
  EXPECT_EQ(ack_types[1], static_cast<int>(OmsEventType::NewAck));
  EXPECT_EQ(ack_types[2], static_cast<int>(OmsEventType::SubmitCancel));
  EXPECT_EQ(ack_types[3], static_cast<int>(OmsEventType::CancelAck));
  EXPECT_EQ(std::remove(ack_journal_path.c_str()), 0);

  const std::string rej_journal_path = make_temp_journal_path();
  ASSERT_EQ(std::remove(rej_journal_path.c_str()), 0);

  Oms oms2;
  Exchange exchange2;
  SyncJournalSink sink2(rej_journal_path);
  OrderCoordinator coordinator2(oms2, exchange2, nullptr, &sink2);
  ASSERT_TRUE(coordinator2.submit_new(MakeBuy(9702, 100, 4)).has_value());
  ASSERT_TRUE(exchange2.cancel(9702));

  auto cancel_result = coordinator2.submit_cancel(9702);
  ASSERT_FALSE(cancel_result.has_value());
  EXPECT_EQ(cancel_result.error().reason, ExecRejectReason::UnknownOrderId);

  const auto rej_types = read_journal_event_types(rej_journal_path);
  ASSERT_EQ(rej_types.size(), 4u);
  EXPECT_EQ(rej_types[0], static_cast<int>(OmsEventType::SubmitNew));
  EXPECT_EQ(rej_types[1], static_cast<int>(OmsEventType::NewAck));
  EXPECT_EQ(rej_types[2], static_cast<int>(OmsEventType::SubmitCancel));
  EXPECT_EQ(rej_types[3], static_cast<int>(OmsEventType::CancelReject));
  EXPECT_EQ(std::remove(rej_journal_path.c_str()), 0);
}

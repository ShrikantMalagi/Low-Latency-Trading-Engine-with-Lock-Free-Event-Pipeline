#include "order_coordinator.hpp"

#include <gtest/gtest.h>

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

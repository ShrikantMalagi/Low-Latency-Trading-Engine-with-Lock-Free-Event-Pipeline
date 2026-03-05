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

}  // namespace

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

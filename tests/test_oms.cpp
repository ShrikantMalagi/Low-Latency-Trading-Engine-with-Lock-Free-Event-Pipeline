#include "oms.hpp"

#include <gtest/gtest.h>

using namespace hft;

namespace {

Order MakeBuy(
    uint64_t order_id = 1,
    int64_t price = 100,
    int64_t qty = 10
) {
  return Order{
      .order_id = order_id,
      .side = Side::Buy,
      .price = price,
      .qty = qty,
  };
}

Order MakeSell(
    uint64_t order_id = 1,
    int64_t price = 100,
    int64_t qty = 10
) {
  return Order{
      .order_id = order_id,
      .side = Side::Sell,
      .price = price,
      .qty = qty,
  };
}

} 

TEST(Oms, SubmitNewCreatesPendingOrder) {
  Oms oms;

  auto result = oms.submit_new(MakeBuy(101, 100, 12));
  ASSERT_TRUE(result.has_value());

  auto rec = oms.get(101);
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->order.order_id, 101u);
  EXPECT_EQ(rec->status, OrderStatus::PendingNew);
  EXPECT_EQ(rec->original_qty, 12);
  EXPECT_EQ(rec->remaining_qty, 12);
  EXPECT_EQ(rec->filled_qty, 0);
  EXPECT_FALSE(rec->last_fill_price.has_value());
  EXPECT_FALSE(oms.is_live(101));
}

TEST(Oms, SubmitNewRejectsDuplicateOrderId) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(101)).has_value());

  auto result = oms.submit_new(MakeSell(101, 105, 5));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::DuplicateOrderId);
}

TEST(Oms, NewAckTransitionsPendingToLive) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(200, 101, 7)).has_value());

  auto result = oms.on_new_ack(200);
  ASSERT_TRUE(result.has_value());

  auto rec = oms.get(200);
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->status, OrderStatus::Live);
  EXPECT_TRUE(oms.is_live(200));
}

TEST(Oms, NewAckForUnknownOrderFails) {
  Oms oms;

  auto result = oms.on_new_ack(999);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::UnknownOrderId);
}

TEST(Oms, NewAckInWrongStateFails) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(201)).has_value());
  ASSERT_TRUE(oms.on_new_ack(201).has_value());

  auto result = oms.on_new_ack(201);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::InvalidTransition);
}

TEST(Oms, NewRejectTransitionsPendingToRejected) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(300, 100, 4)).has_value());

  auto result = oms.on_new_reject(300);
  ASSERT_TRUE(result.has_value());

  auto rec = oms.get(300);
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->status, OrderStatus::Rejected);
  EXPECT_FALSE(oms.is_live(300));
}

TEST(Oms, NewRejectInWrongStateFails) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(301, 100, 4)).has_value());
  ASSERT_TRUE(oms.on_new_ack(301).has_value());

  auto result = oms.on_new_reject(301);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::InvalidTransition);
}

TEST(Oms, FillForUnknownOrderFails) {
  Oms oms;

  auto result = oms.on_fill(999, 100, 1);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::UnknownOrderId);
}

TEST(Oms, FillBeforeAckFails) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(400, 100, 5)).has_value());

  auto result = oms.on_fill(400, 100, 1);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::InvalidTransition);
}

TEST(Oms, PartialFillUpdatesQuantitiesAndStatus) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(401, 100, 10)).has_value());
  ASSERT_TRUE(oms.on_new_ack(401).has_value());

  auto result = oms.on_fill(401, 99, 4);
  ASSERT_TRUE(result.has_value());

  auto rec = oms.get(401);
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->status, OrderStatus::PartiallyFilled);
  EXPECT_EQ(rec->original_qty, 10);
  EXPECT_EQ(rec->filled_qty, 4);
  EXPECT_EQ(rec->remaining_qty, 6);
  ASSERT_TRUE(rec->last_fill_price.has_value());
  EXPECT_EQ(*rec->last_fill_price, 99);
  EXPECT_TRUE(oms.is_live(401));
}

TEST(Oms, FullFillTransitionsToFilled) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(402, 100, 10)).has_value());
  ASSERT_TRUE(oms.on_new_ack(402).has_value());
  ASSERT_TRUE(oms.on_fill(402, 100, 4).has_value());

  auto result = oms.on_fill(402, 101, 6);
  ASSERT_TRUE(result.has_value());

  auto rec = oms.get(402);
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->status, OrderStatus::Filled);
  EXPECT_EQ(rec->filled_qty, 10);
  EXPECT_EQ(rec->remaining_qty, 0);
  ASSERT_TRUE(rec->last_fill_price.has_value());
  EXPECT_EQ(*rec->last_fill_price, 101);
  EXPECT_FALSE(oms.is_live(402));
}

TEST(Oms, OverfillFails) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(403, 100, 5)).has_value());
  ASSERT_TRUE(oms.on_new_ack(403).has_value());

  auto result = oms.on_fill(403, 100, 6);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::Overfill);
}

TEST(Oms, ZeroFillFails) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(404, 100, 5)).has_value());
  ASSERT_TRUE(oms.on_new_ack(404).has_value());

  auto result = oms.on_fill(404, 100, 0);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::Overfill);
}

TEST(Oms, SubmitCancelTransitionsLiveToPendingCancel) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(500, 100, 8)).has_value());
  ASSERT_TRUE(oms.on_new_ack(500).has_value());

  auto result = oms.submit_cancel(500);
  ASSERT_TRUE(result.has_value());

  auto rec = oms.get(500);
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->status, OrderStatus::PendingCancel);
  EXPECT_FALSE(oms.is_live(500));
}

TEST(Oms, SubmitCancelAllowsPartiallyFilledOrder) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(501, 100, 8)).has_value());
  ASSERT_TRUE(oms.on_new_ack(501).has_value());
  ASSERT_TRUE(oms.on_fill(501, 100, 3).has_value());

  auto result = oms.submit_cancel(501);
  ASSERT_TRUE(result.has_value());

  auto rec = oms.get(501);
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->status, OrderStatus::PendingCancel);
}

TEST(Oms, SubmitCancelForUnknownOrderFails) {
  Oms oms;

  auto result = oms.submit_cancel(999);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::UnknownOrderId);
}

TEST(Oms, CancelAckTransitionsPendingCancelToCanceled) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(502, 100, 8)).has_value());
  ASSERT_TRUE(oms.on_new_ack(502).has_value());
  ASSERT_TRUE(oms.submit_cancel(502).has_value());

  auto result = oms.on_cancel_ack(502);
  ASSERT_TRUE(result.has_value());

  auto rec = oms.get(502);
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->status, OrderStatus::Canceled);
  EXPECT_FALSE(oms.is_live(502));
}

TEST(Oms, CancelAckInWrongStateFails) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(505, 100, 8)).has_value());
  ASSERT_TRUE(oms.on_new_ack(505).has_value());

  auto result = oms.on_cancel_ack(505);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::InvalidTransition);
}

TEST(Oms, CancelRejectRestoresLiveOrderWithoutFills) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(503, 100, 8)).has_value());
  ASSERT_TRUE(oms.on_new_ack(503).has_value());
  ASSERT_TRUE(oms.submit_cancel(503).has_value());

  auto result = oms.on_cancel_reject(503);
  ASSERT_TRUE(result.has_value());

  auto rec = oms.get(503);
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->status, OrderStatus::Live);
  EXPECT_TRUE(oms.is_live(503));
}

TEST(Oms, CancelRejectRestoresPartiallyFilledOrder) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(504, 100, 8)).has_value());
  ASSERT_TRUE(oms.on_new_ack(504).has_value());
  ASSERT_TRUE(oms.on_fill(504, 100, 3).has_value());
  ASSERT_TRUE(oms.submit_cancel(504).has_value());

  auto result = oms.on_cancel_reject(504);
  ASSERT_TRUE(result.has_value());

  auto rec = oms.get(504);
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->status, OrderStatus::PartiallyFilled);
  EXPECT_EQ(rec->filled_qty, 3);
  EXPECT_EQ(rec->remaining_qty, 5);
  EXPECT_TRUE(oms.is_live(504));
}

TEST(Oms, CancelRejectInWrongStateFails) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(506, 100, 8)).has_value());
  ASSERT_TRUE(oms.on_new_ack(506).has_value());

  auto result = oms.on_cancel_reject(506);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::InvalidTransition);
}

TEST(Oms, SubmitCancelBeforeAckFails) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(507, 100, 8)).has_value());

  auto result = oms.submit_cancel(507);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::InvalidTransition);
}

TEST(Oms, TerminalOrdersCannotBeCanceled) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(600, 100, 4)).has_value());
  ASSERT_TRUE(oms.on_new_ack(600).has_value());
  ASSERT_TRUE(oms.on_fill(600, 100, 4).has_value());

  auto result = oms.submit_cancel(600);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::AlreadyTerminal);
}

TEST(Oms, RejectedOrdersCannotBeCanceled) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(601, 100, 4)).has_value());
  ASSERT_TRUE(oms.on_new_reject(601).has_value());

  auto result = oms.submit_cancel(601);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::AlreadyTerminal);
}

TEST(Oms, CanceledOrdersCannotBeCanceledAgain) {
  Oms oms;

  ASSERT_TRUE(oms.submit_new(MakeBuy(602, 100, 4)).has_value());
  ASSERT_TRUE(oms.on_new_ack(602).has_value());
  ASSERT_TRUE(oms.submit_cancel(602).has_value());
  ASSERT_TRUE(oms.on_cancel_ack(602).has_value());

  auto result = oms.submit_cancel(602);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, OmsErrorCode::AlreadyTerminal);
}

TEST(Oms, GetReturnsEmptyForUnknownOrder) {
  Oms oms;

  auto rec = oms.get(123456);
  EXPECT_FALSE(rec.has_value());
}

#include "exchange.hpp"
#include <gtest/gtest.h>

using namespace hft;

TEST(Exchange, SimpleCrossingFill) {
  Exchange ex;

  ex.add_order(Order{.order_id=1, .side=Side::Sell, .price=100, .qty=10});
  auto fills = ex.add_order(Order{.order_id=2, .side=Side::Buy, .price=105, .qty=7});

  ASSERT_EQ(fills.size(), 1u);
  EXPECT_EQ(fills[0].taker_order_id, 2u);
  EXPECT_EQ(fills[0].maker_order_id, 1u);
  EXPECT_EQ(fills[0].price, 100);
  EXPECT_EQ(fills[0].qty, 7);

  auto top = ex.top();
  ASSERT_TRUE(top.best_ask.has_value());
  EXPECT_EQ(*top.best_ask, 100);
}

TEST(Exchange, PartialAndRemainderGoesOnBook) {
  Exchange ex;

  ex.add_order(Order{.order_id=1, .side=Side::Sell, .price=100, .qty=5});
  auto fills = ex.add_order(Order{.order_id=2, .side=Side::Buy, .price=100, .qty=12});

  ASSERT_EQ(fills.size(), 1u);
  EXPECT_EQ(fills[0].qty, 5);

  auto top = ex.top();
  ASSERT_TRUE(top.best_bid.has_value());
  EXPECT_EQ(*top.best_bid, 100);
}

TEST(Exchange, CancelRemovesOrder) {
  Exchange ex;
  ex.add_order(Order{.order_id=10, .side=Side::Buy, .price=99, .qty=3});
  EXPECT_TRUE(ex.cancel(10));
  EXPECT_FALSE(ex.cancel(10));

  auto top = ex.top();
  EXPECT_FALSE(top.best_bid.has_value());
}
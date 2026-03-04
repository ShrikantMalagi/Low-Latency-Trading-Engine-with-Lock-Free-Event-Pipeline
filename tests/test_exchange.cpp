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

TEST(Exchange, SamePriceOrdersMatchInFifoOrder) {
  Exchange ex;

  ex.add_order(Order{.order_id=1, .side=Side::Sell, .price=100, .qty=5});
  ex.add_order(Order{.order_id=2, .side=Side::Sell, .price=100, .qty=7});

  auto fills = ex.add_order(Order{.order_id=3, .side=Side::Buy, .price=100, .qty=6});

  ASSERT_EQ(fills.size(), 2u);
  EXPECT_EQ(fills[0].maker_order_id, 1u);
  EXPECT_EQ(fills[0].qty, 5);
  EXPECT_EQ(fills[1].maker_order_id, 2u);
  EXPECT_EQ(fills[1].qty, 1);

  auto top = ex.top();
  ASSERT_TRUE(top.best_ask.has_value());
  EXPECT_EQ(*top.best_ask, 100);
}

TEST(Exchange, CrossingOrderMatchesAcrossMultiplePriceLevels) {
  Exchange ex;

  ex.add_order(Order{.order_id=10, .side=Side::Sell, .price=100, .qty=3});
  ex.add_order(Order{.order_id=11, .side=Side::Sell, .price=101, .qty=4});
  ex.add_order(Order{.order_id=12, .side=Side::Sell, .price=103, .qty=5});

  auto fills = ex.add_order(Order{.order_id=20, .side=Side::Buy, .price=101, .qty=10});

  ASSERT_EQ(fills.size(), 2u);
  EXPECT_EQ(fills[0].maker_order_id, 10u);
  EXPECT_EQ(fills[0].price, 100);
  EXPECT_EQ(fills[0].qty, 3);
  EXPECT_EQ(fills[1].maker_order_id, 11u);
  EXPECT_EQ(fills[1].price, 101);
  EXPECT_EQ(fills[1].qty, 4);

  auto top = ex.top();
  ASSERT_TRUE(top.best_bid.has_value());
  EXPECT_EQ(*top.best_bid, 101);
  ASSERT_TRUE(top.best_ask.has_value());
  EXPECT_EQ(*top.best_ask, 103);
}

TEST(Exchange, SellOrderMatchesAcrossMultipleBidLevels) {
  Exchange ex;

  ex.add_order(Order{.order_id=30, .side=Side::Buy, .price=103, .qty=2});
  ex.add_order(Order{.order_id=31, .side=Side::Buy, .price=101, .qty=4});
  ex.add_order(Order{.order_id=32, .side=Side::Buy, .price=99, .qty=6});

  auto fills = ex.add_order(Order{.order_id=40, .side=Side::Sell, .price=101, .qty=8});

  ASSERT_EQ(fills.size(), 2u);
  EXPECT_EQ(fills[0].maker_order_id, 30u);
  EXPECT_EQ(fills[0].price, 103);
  EXPECT_EQ(fills[0].qty, 2);
  EXPECT_EQ(fills[1].maker_order_id, 31u);
  EXPECT_EQ(fills[1].price, 101);
  EXPECT_EQ(fills[1].qty, 4);

  auto top = ex.top();
  ASSERT_TRUE(top.best_ask.has_value());
  EXPECT_EQ(*top.best_ask, 101);
  ASSERT_TRUE(top.best_bid.has_value());
  EXPECT_EQ(*top.best_bid, 99);
}

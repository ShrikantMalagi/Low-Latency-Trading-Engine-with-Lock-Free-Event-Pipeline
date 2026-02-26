#include "exchange.hpp"
#include <algorithm>

namespace hft {

void Exchange::insert_order(std::vector<Order>& v, Order o, bool is_bid) {

  auto it = std::find_if(v.begin(), v.end(), [&](const Order& x) {
    if (x.price == o.price) return false; 
    return is_bid ? (x.price < o.price) : (x.price > o.price);
  });

  if (it == v.end()) {
    v.push_back(o);
    return;
  }

  if (it != v.begin()) {
    auto eq = std::find_if(it, v.end(), [&](const Order& x){ return x.price != o.price; });
    if (eq != v.end() && (eq-1)->price == o.price) it = eq;
  }

  v.insert(it, o);
}

std::vector<Fill> Exchange::add_order(Order o) {
  std::vector<Fill> fills;

  auto& opp = (o.side == Side::Buy) ? asks_ : bids_;
  auto& same = (o.side == Side::Buy) ? bids_ : asks_;

  auto crosses = [&](const Order& top) {
    if (o.side == Side::Buy)  return o.price >= top.price;
    else                      return o.price <= top.price;
  };

  while (o.qty > 0 && !opp.empty() && crosses(opp.front())) {
    Order& maker = opp.front();
    int64_t traded = std::min(o.qty, maker.qty);

    fills.push_back(Fill{
      .taker_order_id = o.order_id,
      .maker_order_id = maker.order_id,
      .price = maker.price,
      .qty = traded
    });

    o.qty -= traded;
    maker.qty -= traded;

    if (maker.qty == 0) opp.erase(opp.begin());
  }

  if (o.qty > 0) {
    insert_order(same, o, o.side == Side::Buy);
  }

  return fills;
}

bool Exchange::cancel(uint64_t order_id) {
  auto kill = [&](std::vector<Order>& v) {
    auto it = std::find_if(v.begin(), v.end(), [&](const Order& x){ return x.order_id == order_id; });
    if (it == v.end()) return false;
    v.erase(it);
    return true;
  };
  return kill(bids_) || kill(asks_);
}

BookTop Exchange::top() const {
  BookTop t;
  if (!bids_.empty()) t.best_bid = bids_.front().price;
  if (!asks_.empty()) t.best_ask = asks_.front().price;
  return t;
}

}
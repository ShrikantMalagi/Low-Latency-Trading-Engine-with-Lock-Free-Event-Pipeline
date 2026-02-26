#include "exchange.hpp"
#include <iostream>

using namespace hft;

int main() {
  Exchange ex;

  ex.add_order(Order{.order_id=1, .side=Side::Sell, .price=101, .qty=10});
  ex.add_order(Order{.order_id=2, .side=Side::Buy,  .price=100, .qty=7});

  auto fills = ex.add_order(Order{.order_id=3, .side=Side::Buy, .price=105, .qty=12});
  for (auto& f : fills) {
    std::cout << "Fill taker=" << f.taker_order_id
              << " maker=" << f.maker_order_id
              << " px=" << f.price
              << " qty=" << f.qty << "\n";
  }

  auto top = ex.top();
  std::cout << "Top: bid=" << (top.best_bid ? std::to_string(*top.best_bid) : "NA")
            << " ask=" << (top.best_ask ? std::to_string(*top.best_ask) : "NA")
            << "\n";
  return 0;
}
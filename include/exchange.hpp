#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace hft {

enum class Side : uint8_t { Buy, Sell };

struct Order {
  uint64_t order_id{};
  Side side{};
  int64_t price{};
  int64_t qty{};
};

struct Fill {
  uint64_t taker_order_id{};
  uint64_t maker_order_id{};
  int64_t price{};
  int64_t qty{};
};

struct BookTop {
  std::optional<int64_t> best_bid;
  std::optional<int64_t> best_ask;
};

class Exchange {
public:
  std::vector<Fill> add_order(Order o);

  bool cancel(uint64_t order_id);

  BookTop top() const;

private:
  std::vector<Order> bids_;
  std::vector<Order> asks_;

  void insert_order(std::vector<Order>& side_vec, Order o, bool is_bid);
};

}
#pragma once

#include "exchange.hpp"

#include <cstdint>
#include <expected>
#include <optional>
#include <string_view>
#include <unordered_map>

namespace hft {

enum class OrderStatus : uint8_t {
  PendingNew,
  Live,
  PendingCancel,
  PartiallyFilled,
  Filled,
  Canceled,
  Rejected,
};

enum class OmsErrorCode : uint8_t {
  DuplicateOrderId,
  UnknownOrderId,
  InvalidTransition,
  AlreadyTerminal,
  Overfill,
};

struct OmsError {
  OmsErrorCode code;
  std::string_view message;
};

struct OrderRecord {
  Order order{};
  OrderStatus status{OrderStatus::PendingNew};
  int64_t original_qty{0};
  int64_t remaining_qty{0};
  int64_t filled_qty{0};
  std::optional<int64_t> last_fill_price;
};

class Oms {
public:
  std::expected<void, OmsError> submit_new(Order order);

  std::expected<void, OmsError> submit_cancel(uint64_t order_id);

  std::expected<void, OmsError> on_new_ack(uint64_t order_id);

  std::expected<void, OmsError> on_new_reject(uint64_t order_id);

  std::expected<void, OmsError> on_cancel_ack(uint64_t order_id);

  std::expected<void, OmsError> on_cancel_reject(uint64_t order_id);

  std::expected<void, OmsError> on_fill(uint64_t order_id, int64_t price, int64_t qty);

  std::optional<OrderRecord> get(uint64_t order_id) const;

  bool is_live(uint64_t order_id) const;

private:
  std::unordered_map<uint64_t, OrderRecord> orders;
};

}

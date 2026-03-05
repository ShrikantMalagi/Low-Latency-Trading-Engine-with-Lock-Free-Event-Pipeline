#pragma once

#include "exchange.hpp"
#include "oms.hpp"

#include <cstdint>
#include <expected>
#include <string_view>
#include <vector>

namespace hft {

enum class ExecRejectReason : uint16_t {
  InvalidOrder = 1,
  DuplicateOrderId = 2,
  UnknownOrderId = 3,
  InvalidTransition = 4,
};

struct ExecReject {
  uint64_t order_id{};
  ExecRejectReason reason{};
  std::string_view message{};
};

struct ExecResponse {
  bool ack{false};
  std::vector<Fill> fills;
  std::optional<ExecReject> reject;
};

class OrderCoordinator {
public:
  OrderCoordinator(Oms& oms_ref, Exchange& exchange_ref)
      : oms(oms_ref), exchange(exchange_ref) {}

  std::expected<ExecResponse, ExecReject> submit_new(Order order);

  std::expected<ExecResponse, ExecReject> submit_cancel(uint64_t order_id);

private:
  Oms& oms;
  Exchange& exchange;
};

}

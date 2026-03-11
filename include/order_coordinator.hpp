#pragma once

#include "exchange.hpp"
#include "oms.hpp"
#include "journal_sink.hpp"

#include <cstdint>
#include <expected>
#include <optional>
#include <string>
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
};

enum class CoordinatorEventType : uint8_t {
  NewAccepted,
  NewRejected,
  FillEmitted,
  CancelAccepted,
  CancelRejected,
  InternalError,
};

struct CoordinatorEvent {
  CoordinatorEventType type{};
  uint64_t order_id{};
  std::optional<uint64_t> contra_order_id{std::nullopt};
  std::optional<int64_t> price{std::nullopt};
  std::optional<int64_t> qty{std::nullopt};
  std::optional<ExecRejectReason> reject_reason{std::nullopt};
  std::optional<std::string_view> message{std::nullopt};
};

struct CoordinatorEventSink {
  virtual ~CoordinatorEventSink() = default;
  virtual void on_event(const CoordinatorEvent& event) = 0;
};

struct CoordinatorEventEnvelope {
  uint64_t event_id{};
  uint64_t timestamp_ns{};
  std::string_view source{"order_coordinator"};
  CoordinatorEvent event{};
};

class OrderCoordinator {
public:
  OrderCoordinator(
      Oms& oms_ref,
      Exchange& exchange_ref,
      CoordinatorEventSink* sink_ref = nullptr,
      JournalSink* journal_sink_ref = nullptr)
      : oms(oms_ref),
        exchange(exchange_ref),
        sink(sink_ref),
        journal_sink(journal_sink_ref) {}

  std::expected<ExecResponse, ExecReject> submit_new(Order order);

  std::expected<ExecResponse, ExecReject> submit_cancel(uint64_t order_id);

private:
  void emit(const CoordinatorEvent& event){
    if(sink != nullptr){
      sink->on_event(event);
    }
  }

  Oms& oms;
  Exchange& exchange;
  CoordinatorEventSink* sink;
  JournalSink* journal_sink{};
};

}

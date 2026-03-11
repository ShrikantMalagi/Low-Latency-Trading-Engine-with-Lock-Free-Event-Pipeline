#pragma once

#include "oms.hpp"
#include "exchange.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <string_view>

namespace hft {

enum class OmsEventType : uint8_t {
  SubmitNew = 1,
  NewAck = 2,
  NewReject = 3,
  SubmitCancel = 4,
  CancelAck = 5,
  CancelReject = 6,
  Fill = 7,
};

struct OmsJournalEvent {
  OmsEventType type{};
  uint64_t order_id{};
  uint8_t side{};
  int64_t price{};
  int64_t qty{};
};

enum class ReplayErrorCode : uint8_t {
  ParseError,
  InvalidTransition,
};

struct ReplayError {
  ReplayErrorCode code{};
  std::size_t line_number{};
  std::string message;
  std::optional<OmsEventType> event_type{};
  uint64_t records_replayed{};
};

struct ReplayStats {
  uint64_t records_replayed{};
};

struct RecoveryStatus {
  bool replay_attempted{};
  bool replay_succeeded{};
  uint64_t records_replayed{};
  uint64_t replay_error_code{};
  uint64_t replay_error_line{};
};

bool append_journal_event(const std::string& path, const OmsJournalEvent& e);
std::expected<ReplayStats, ReplayError> replay_journal(const std::string& path, Oms& oms);
void rebuild_exchange_from_oms(const Oms& oms, Exchange& exchange);

}

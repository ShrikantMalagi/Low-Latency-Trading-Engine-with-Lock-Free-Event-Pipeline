#pragma once

#include "oms.hpp"
#include "exchange.hpp"

#include <string>

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

bool append_journal_event(const std::string& path, const OmsJournalEvent& e);
bool replay_journal(const std::string& path, Oms& oms);
void rebuild_exchange_from_oms(const Oms& oms, Exchange& exchange);

}
#include "journal_replay.hpp"

#include "journal_format.hpp"

#include <fstream>

namespace hft {

std::expected<ReplayStats, ReplayError> replay_journal(const std::string& path, Oms& oms) {
  std::ifstream in(path);
  if (!in.is_open()) {
    return ReplayStats{};
  }

  std::string line;
  std::size_t line_number = 0;
  ReplayStats stats{};
  while (std::getline(in, line)) {
    ++line_number;
    if (line.empty()) {
      continue;
    }

    auto parsed = parse_journal_line(line, line_number);
    if (!parsed) {
      parsed.error().records_replayed = stats.records_replayed;
      return std::unexpected(parsed.error());
    }
    const OmsJournalEvent& e = *parsed;

    switch (e.type) {
      case OmsEventType::SubmitNew:
        if (!oms.submit_new(Order{
              .order_id = e.order_id,
              .side = static_cast<Side>(e.side),
              .price = e.price,
              .qty = e.qty,
            })) {
          return std::unexpected(ReplayError{
              .code = ReplayErrorCode::InvalidTransition,
              .line_number = line_number,
              .message = "submit_new failed during replay",
              .event_type = e.type,
              .records_replayed = stats.records_replayed,
          });
        }
        break;
      case OmsEventType::NewAck:
        if (!oms.on_new_ack(e.order_id)) {
          return std::unexpected(ReplayError{
              .code = ReplayErrorCode::InvalidTransition,
              .line_number = line_number,
              .message = "on_new_ack failed during replay",
              .event_type = e.type,
              .records_replayed = stats.records_replayed,
          });
        }
        break;
      case OmsEventType::NewReject:
        if (!oms.on_new_reject(e.order_id)) {
          return std::unexpected(ReplayError{
              .code = ReplayErrorCode::InvalidTransition,
              .line_number = line_number,
              .message = "on_new_reject failed during replay",
              .event_type = e.type,
              .records_replayed = stats.records_replayed,
          });
        }
        break;
      case OmsEventType::SubmitCancel:
        if (!oms.submit_cancel(e.order_id)) {
          return std::unexpected(ReplayError{
              .code = ReplayErrorCode::InvalidTransition,
              .line_number = line_number,
              .message = "submit_cancel failed during replay",
              .event_type = e.type,
              .records_replayed = stats.records_replayed,
          });
        }
        break;
      case OmsEventType::CancelAck:
        if (!oms.on_cancel_ack(e.order_id)) {
          return std::unexpected(ReplayError{
              .code = ReplayErrorCode::InvalidTransition,
              .line_number = line_number,
              .message = "on_cancel_ack failed during replay",
              .event_type = e.type,
              .records_replayed = stats.records_replayed,
          });
        }
        break;
      case OmsEventType::CancelReject:
        if (!oms.on_cancel_reject(e.order_id)) {
          return std::unexpected(ReplayError{
              .code = ReplayErrorCode::InvalidTransition,
              .line_number = line_number,
              .message = "on_cancel_reject failed during replay",
              .event_type = e.type,
              .records_replayed = stats.records_replayed,
          });
        }
        break;
      case OmsEventType::Fill:
        if (!oms.on_fill(e.order_id, e.price, e.qty)) {
          return std::unexpected(ReplayError{
              .code = ReplayErrorCode::InvalidTransition,
              .line_number = line_number,
              .message = "on_fill failed during replay",
              .event_type = e.type,
              .records_replayed = stats.records_replayed,
          });
        }
        break;
    }

    ++stats.records_replayed;
  }

  return stats;
}

}

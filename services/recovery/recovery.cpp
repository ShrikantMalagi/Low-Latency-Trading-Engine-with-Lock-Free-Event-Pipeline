#include "recovery.hpp"

#include <fstream>
#include <sstream>

namespace hft {

bool append_journal_event(const std::string& path, const OmsJournalEvent& e) {
  std::ofstream out(path, std::ios::app);
  if (!out.is_open()) return false;
  out << static_cast<int>(e.type) << ','
      << e.order_id << ','
      << static_cast<int>(e.side) << ','
      << e.price << ','
      << e.qty << '\n';
  out.flush();
  return static_cast<bool>(out);
}

static bool parse_line(const std::string& line, OmsJournalEvent& e) {
  std::istringstream ss(line);
  std::string tok;
  int type = 0, side = 0;
  if (!std::getline(ss, tok, ',')) return false; type = std::stoi(tok);
  if (!std::getline(ss, tok, ',')) return false; e.order_id = std::stoull(tok);
  if (!std::getline(ss, tok, ',')) return false; side = std::stoi(tok);
  if (!std::getline(ss, tok, ',')) return false; e.price = std::stoll(tok);
  if (!std::getline(ss, tok, ',')) return false; e.qty = std::stoll(tok);
  e.type = static_cast<OmsEventType>(type);
  e.side = static_cast<uint8_t>(side);
  return true;
}

bool replay_journal(const std::string& path, Oms& oms) {
  std::ifstream in(path);
  if (!in.is_open()) return true;

  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) continue;

    OmsJournalEvent e{};
    if (!parse_line(line, e)) return false;

    switch (e.type) {
      case OmsEventType::SubmitNew:
        if (!oms.submit_new(Order{
              .order_id = e.order_id,
              .side = static_cast<Side>(e.side),
              .price = e.price,
              .qty = e.qty,
            })) return false;
        break;
      case OmsEventType::NewAck:
        if (!oms.on_new_ack(e.order_id)) return false;
        break;
      case OmsEventType::NewReject:
        if (!oms.on_new_reject(e.order_id)) return false;
        break;
      case OmsEventType::SubmitCancel:
        if (!oms.submit_cancel(e.order_id)) return false;
        break;
      case OmsEventType::CancelAck:
        if (!oms.on_cancel_ack(e.order_id)) return false;
        break;
      case OmsEventType::CancelReject:
        if (!oms.on_cancel_reject(e.order_id)) return false;
        break;
      case OmsEventType::Fill:
        if (!oms.on_fill(e.order_id, e.price, e.qty)) return false;
        break;
    }
  }

  return true;
}

void rebuild_exchange_from_oms(const Oms& oms, Exchange& exchange) {
  for (const auto& rec : oms.all_orders()) {
    if (rec.status == OrderStatus::Live || rec.status == OrderStatus::PartiallyFilled) {
      Order o = rec.order;
      o.qty = rec.remaining_qty;
      if (o.qty > 0) {
        (void)exchange.add_order(o);
      }
    }
  }
}

}

#include "coordinator_event_sink.hpp"

#include <cstdio>
#include <string>

namespace hft {
namespace {

const char* to_string(CoordinatorEventType type) {
  switch (type) {
    case CoordinatorEventType::NewAccepted:    return "NewAccepted";
    case CoordinatorEventType::NewRejected:    return "NewRejected";
    case CoordinatorEventType::FillEmitted:    return "FillEmitted";
    case CoordinatorEventType::CancelAccepted: return "CancelAccepted";
    case CoordinatorEventType::CancelRejected: return "CancelRejected";
    case CoordinatorEventType::InternalError:  return "InternalError";
    default:                                   return "Unknown";
  }
}

const char* to_string(ExecRejectReason reason) {
  switch (reason) {
    case ExecRejectReason::InvalidOrder:      return "InvalidOrder";
    case ExecRejectReason::DuplicateOrderId:  return "DuplicateOrderId";
    case ExecRejectReason::UnknownOrderId:    return "UnknownOrderId";
    case ExecRejectReason::InvalidTransition: return "InvalidTransition";
    default:                                  return "Unknown";
  }
}

}

void LoggingEventSink::on_event(const CoordinatorEvent& event) {
  const std::string contra = event.contra_order_id ? std::to_string(*event.contra_order_id) : "-";
  const std::string price = event.price ? std::to_string(*event.price) : "-";
  const std::string qty = event.qty ? std::to_string(*event.qty) : "-";
  const char* reject = event.reject_reason ? to_string(*event.reject_reason) : "-";
  const char* message = event.message ? event.message->data() : "-";
  const int message_len = event.message ? static_cast<int>(event.message->size()) : 1;

  std::fprintf(
      stdout,
      "[coordinator_event] type=%s order_id=%llu contra=%s price=%s qty=%s reject=%s msg=%.*s\n",
      to_string(event.type),
      static_cast<unsigned long long>(event.order_id),
      contra.c_str(),
      price.c_str(),
      qty.c_str(),
      reject,
      message_len,
      message);
}

}

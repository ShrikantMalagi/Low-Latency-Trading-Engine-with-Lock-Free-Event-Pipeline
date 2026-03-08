#include "coordinator_event_sink.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>

namespace hft {
namespace {

std::atomic<uint64_t> g_next_event_id{1};

uint64_t now_ns() {
  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

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

void QueueEventSink::on_event(const CoordinatorEvent& event) {
  std::lock_guard<std::mutex> lock(mutex);
  if (queue.size() >= max_queue_size_) {
    queue.pop_front();
    ++dropped_events_;
  }
  queue.push_back(CoordinatorEventEnvelope{
      .event_id = g_next_event_id.fetch_add(1, std::memory_order_relaxed),
      .timestamp_ns = now_ns(),
      .source = "order_coordinator",
      .event = event,
  });
}

std::optional<CoordinatorEventEnvelope> QueueEventSink::try_pop() {
  std::lock_guard<std::mutex> lock(mutex);
  if (queue.empty()) return std::nullopt;
  CoordinatorEventEnvelope envelope = std::move(queue.front());
  queue.pop_front();
  return envelope;
}

std::vector<CoordinatorEventEnvelope> QueueEventSink::snapshot() const {
  std::lock_guard<std::mutex> lock(mutex);
  return std::vector<CoordinatorEventEnvelope>(queue.begin(), queue.end());
}

std::size_t QueueEventSink::size() const {
  std::lock_guard<std::mutex> lock(mutex);
  return queue.size();
}

std::size_t QueueEventSink::dropped_events() const {
  std::lock_guard<std::mutex> lock(mutex);
  return dropped_events_;
}

std::size_t QueueEventSink::max_queue_size() const {
  std::lock_guard<std::mutex> lock(mutex);
  return max_queue_size_;
}

void QueueEventSink::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  queue.clear();
}

}

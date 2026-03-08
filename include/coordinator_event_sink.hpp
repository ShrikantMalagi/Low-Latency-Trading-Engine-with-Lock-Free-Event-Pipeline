#pragma once

#include "order_coordinator.hpp"

#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>
#include <vector>

namespace hft {

class LoggingEventSink final : public CoordinatorEventSink {
public:
  void on_event(const CoordinatorEvent& event) override;
};

class QueueEventSink final : public CoordinatorEventSink {
public:
  explicit QueueEventSink(std::size_t max_queue_size = 4096)
      : max_queue_size_(max_queue_size == 0 ? 1 : max_queue_size) {}

  void on_event(const CoordinatorEvent& event) override;

  std::optional<CoordinatorEventEnvelope> try_pop();

  std::vector<CoordinatorEventEnvelope> snapshot() const;

  std::size_t size() const;

  std::size_t dropped_events() const;

  std::size_t max_queue_size() const;

  void clear();

private:
  mutable std::mutex mutex;
  std::deque<CoordinatorEventEnvelope> queue;
  std::size_t max_queue_size_;
  std::size_t dropped_events_{0};
};

}

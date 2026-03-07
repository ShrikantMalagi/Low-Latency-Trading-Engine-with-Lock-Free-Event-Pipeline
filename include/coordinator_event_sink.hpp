#pragma once

#include "order_coordinator.hpp"

namespace hft {

class LoggingEventSink final : public CoordinatorEventSink {
  public:
  void on_event(const CoordinatorEvent& event) override;

  std::optional<CoordinatorEvent> try_pop();

  std::vector<CoordinatorEvent> snapshot() const;

  std::size_t size() const;

  void clear();

  private:
  mutable std::mutex mutex;
  std::deque<CoordinatorEvent> queue;
};

}
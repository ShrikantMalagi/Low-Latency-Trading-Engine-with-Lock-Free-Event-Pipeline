#pragma once

#include "recovery.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace hft {

class JournalSink {
public:
  virtual ~JournalSink() = default;
  virtual bool write(const OmsJournalEvent& event) = 0;
  virtual void flush() {}
  virtual uint64_t enqueued() const { return 0; }
  virtual uint64_t flushed() const { return 0; }
  virtual uint64_t dropped() const { return 0; }
  virtual uint64_t queue_depth() const { return 0; }
};

class SyncJournalSink final : public JournalSink {
public:
  explicit SyncJournalSink(std::string path) : path_(std::move(path)) {}
  bool write(const OmsJournalEvent& event) override {
    return append_journal_event(path_, event);
  }
  void flush() override {}

private:
  std::string path_;
};

class AsyncJournalSink final : public JournalSink {
public:
  AsyncJournalSink(std::string path, std::size_t capacity = 8192)
      : path_(std::move(path)),
        capacity_(capacity == 0 ? 1 : capacity),
        ring_(capacity_),
        worker_(&AsyncJournalSink::run, this) {}

  ~AsyncJournalSink() override {
    flush();
    stop_.store(true, std::memory_order_release);
    cv_.notify_all();
    if (worker_.joinable()) {
      worker_.join();
    }
  }

  bool write(const OmsJournalEvent& event) override {
    const auto head = head_.load(std::memory_order_relaxed);
    const auto next = increment(head);
    const auto tail = tail_.load(std::memory_order_acquire);

    if (next == tail) {
      dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    ring_[head] = event;
    head_.store(next, std::memory_order_release);
    enqueued_.fetch_add(1, std::memory_order_relaxed);
    cv_.notify_all();
    return true;
  }

  void flush() override {
    std::unique_lock<std::mutex> lock(m_);
    cv_.wait(lock, [&] {
      return queue_depth() == 0 && in_flight_.load(std::memory_order_acquire) == 0;
    });
  }

  uint64_t enqueued() const override { return enqueued_.load(std::memory_order_relaxed); }
  uint64_t dropped() const override { return dropped_.load(std::memory_order_relaxed); }
  uint64_t flushed() const override { return flushed_.load(std::memory_order_relaxed); }
  uint64_t queue_depth() const override {
    const auto head = head_.load(std::memory_order_acquire);
    const auto tail = tail_.load(std::memory_order_acquire);
    return head >= tail ? (head - tail) : (capacity_ - tail + head);
  }

private:
  std::size_t increment(std::size_t i) const noexcept {
    ++i;
    return (i == capacity_) ? 0 : i;
  }

  bool pop_one(OmsJournalEvent& out) {
    const auto tail = tail_.load(std::memory_order_relaxed);
    const auto head = head_.load(std::memory_order_acquire);
    if (tail == head) {
      return false;
    }
    out = ring_[tail];
    tail_.store(increment(tail), std::memory_order_release);
    return true;
  }

  void flush_remaining() {
    OmsJournalEvent e{};
    while (pop_one(e)) {
      in_flight_.fetch_add(1, std::memory_order_release);
      if (append_journal_event(path_, e)) {
        flushed_.fetch_add(1, std::memory_order_relaxed);
      }
      in_flight_.fetch_sub(1, std::memory_order_release);
      cv_.notify_all();
    }
  }

  void run() {
    while (!stop_.load(std::memory_order_acquire)) {
      {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait_for(lk, std::chrono::milliseconds(2), [&] {
          return stop_.load(std::memory_order_acquire) ||
                 tail_.load(std::memory_order_relaxed) != head_.load(std::memory_order_relaxed);
        });
      }

      OmsJournalEvent e{};
      while (pop_one(e)) {
        in_flight_.fetch_add(1, std::memory_order_release);
        if (append_journal_event(path_, e)) {
          flushed_.fetch_add(1, std::memory_order_relaxed);
        }
        in_flight_.fetch_sub(1, std::memory_order_release);
        cv_.notify_all();
      }
    }
  }

  std::string path_;
  std::size_t capacity_;
  std::vector<OmsJournalEvent> ring_;

  alignas(64) std::atomic<std::size_t> head_{0};
  alignas(64) std::atomic<std::size_t> tail_{0};

  std::atomic<bool> stop_{false};
  std::thread worker_;
  std::mutex m_;
  std::condition_variable cv_;

  std::atomic<uint64_t> enqueued_{0};
  std::atomic<uint64_t> dropped_{0};
  std::atomic<uint64_t> flushed_{0};
  std::atomic<uint64_t> in_flight_{0};
};

}

#include "journal_sink.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <unistd.h>

namespace {

std::string make_temp_journal_path() {
  char tmp[] = "/tmp/hft_journal_bench_XXXXXX";
  const int fd = ::mkstemp(tmp);
  if (fd >= 0) {
    ::close(fd);
    std::remove(tmp);
  }
  return std::string(tmp);
}

uint64_t parse_u64_arg(const char* raw, uint64_t fallback) {
  if (raw == nullptr || raw[0] == '\0') {
    return fallback;
  }
  char* end = nullptr;
  const unsigned long long value = std::strtoull(raw, &end, 10);
  if (end == raw || *end != '\0') {
    return fallback;
  }
  return static_cast<uint64_t>(value);
}

template <typename Sink>
void run_bench(const char* label, Sink& sink, uint64_t total_events) {
  const auto start = std::chrono::steady_clock::now();
  uint64_t accepted = 0;
  for (uint64_t i = 0; i < total_events; ++i) {
    const hft::OmsJournalEvent event{
        .type = hft::OmsEventType::SubmitNew,
        .order_id = 1000000u + i,
        .side = static_cast<uint8_t>(hft::Side::Buy),
        .price = 100,
        .qty = 1,
    };
    if (sink.write(event) == hft::JournalWriteResult::Enqueued) {
      ++accepted;
    }
  }
  sink.flush();
  const auto end = std::chrono::steady_clock::now();
  const auto elapsed_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  const double seconds = static_cast<double>(elapsed_ns) / 1'000'000'000.0;
  const double throughput = seconds > 0.0 ? static_cast<double>(accepted) / seconds : 0.0;

  std::printf(
      "%s accepted=%llu backpressure=%llu flushed=%llu elapsed_ms=%.3f throughput_ev_s=%.0f\n",
      label,
      static_cast<unsigned long long>(accepted),
      static_cast<unsigned long long>(sink.backpressure_events()),
      static_cast<unsigned long long>(sink.flushed()),
      static_cast<double>(elapsed_ns) / 1'000'000.0,
      throughput);
}

}  // namespace

int main(int argc, char* argv[]) {
  const uint64_t total_events = argc > 1 ? parse_u64_arg(argv[1], 100000) : 100000;
  const uint64_t async_capacity = argc > 2 ? parse_u64_arg(argv[2], 16384) : 16384;

  const std::string sync_path = make_temp_journal_path();
  const std::string async_path = make_temp_journal_path();

  {
    hft::SyncJournalSink sink(sync_path);
    run_bench("sync", sink, total_events);
  }

  {
    hft::AsyncJournalSink sink(async_path, static_cast<std::size_t>(async_capacity));
    run_bench("async", sink, total_events);
  }

  std::remove(sync_path.c_str());
  std::remove(async_path.c_str());
  return 0;
}

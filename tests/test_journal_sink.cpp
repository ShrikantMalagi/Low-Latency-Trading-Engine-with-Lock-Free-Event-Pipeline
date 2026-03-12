#include "journal_sink.hpp"
#include "recovery.hpp"
#include "oms.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <string>
#include <thread>
#include <unistd.h>

namespace {

std::string make_temp_journal_path() {
  char tmp[] = "/tmp/hft_async_journal_XXXXXX";
  const int fd = ::mkstemp(tmp);
  if (fd >= 0) {
    ::close(fd);
    std::remove(tmp);
  }
  return std::string(tmp);
}

std::size_t count_lines(const std::string& path) {
  std::ifstream in(path);
  std::size_t count = 0;
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty()) {
      ++count;
    }
  }
  return count;
}

}  // namespace

TEST(AsyncJournalSink, FailFastReturnsBackpressureWhenCapacityIsExceeded) {
  constexpr std::size_t kCapacity = 8;
  constexpr std::size_t kTotalEvents = 20000;

  const std::string journal_path = make_temp_journal_path();

  std::size_t accepted = 0;
  std::size_t backpressure = 0;
  {
    hft::AsyncJournalSink sink(journal_path, kCapacity);

    for (std::size_t i = 0; i < kTotalEvents; ++i) {
      const hft::OmsJournalEvent event{
          .type = hft::OmsEventType::SubmitNew,
          .order_id = 100000u + static_cast<uint64_t>(i),
          .side = static_cast<uint8_t>(hft::Side::Buy),
          .price = 100,
          .qty = 1,
      };
      if (sink.write(event) == hft::JournalWriteResult::Enqueued) {
        ++accepted;
      } else {
        ++backpressure;
      }
    }

    EXPECT_EQ(sink.enqueued(), accepted);
    EXPECT_EQ(sink.backpressure_events(), backpressure);
    EXPECT_LT(accepted, kTotalEvents);
    EXPECT_GT(backpressure, 0u);
  }

  const std::size_t persisted = count_lines(journal_path);
  EXPECT_EQ(persisted, accepted);

  hft::Oms recovered;
  ASSERT_TRUE(hft::replay_journal(journal_path, recovered).has_value());

  for (std::size_t i = 0; i < kTotalEvents; ++i) {
    const auto rec = recovered.get(100000u + static_cast<uint64_t>(i));
    if (rec.has_value()) {
      EXPECT_EQ(rec->order.order_id, 100000u + static_cast<uint64_t>(i));
      EXPECT_EQ(rec->order.price, 100);
      EXPECT_EQ(rec->order.qty, 1);
      EXPECT_EQ(rec->status, hft::OrderStatus::PendingNew);
    }
  }

  EXPECT_EQ(std::remove(journal_path.c_str()), 0);
}

TEST(AsyncJournalSink, BlockPolicyAllowsProducerToWaitForSpace) {
  const std::string journal_path = make_temp_journal_path();

  hft::AsyncJournalSink sink(
      journal_path,
      /*capacity=*/2,
      hft::BackpressurePolicy::Block);

  const hft::OmsJournalEvent first{
      .type = hft::OmsEventType::SubmitNew,
      .order_id = 1,
      .side = static_cast<uint8_t>(hft::Side::Buy),
      .price = 100,
      .qty = 1,
  };
  const hft::OmsJournalEvent second{
      .type = hft::OmsEventType::SubmitNew,
      .order_id = 2,
      .side = static_cast<uint8_t>(hft::Side::Buy),
      .price = 100,
      .qty = 1,
  };

  ASSERT_EQ(sink.write(first), hft::JournalWriteResult::Enqueued);

  hft::JournalWriteResult second_result = hft::JournalWriteResult::IoError;
  std::thread writer([&] {
    second_result = sink.write(second);
  });

  writer.join();
  EXPECT_EQ(second_result, hft::JournalWriteResult::Enqueued);

  EXPECT_EQ(std::remove(journal_path.c_str()), 0);
}

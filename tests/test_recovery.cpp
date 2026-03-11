#include "recovery.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>

namespace {

std::string make_temp_journal_path() {
  char tmp[] = "/tmp/hft_recovery_XXXXXX";
  const int fd = ::mkstemp(tmp);
  if (fd >= 0) {
    ::close(fd);
  }
  return std::string(tmp);
}

void write_text(const std::string& path, const std::string& text) {
  std::ofstream out(path, std::ios::trunc);
  ASSERT_TRUE(out.is_open());
  out << text;
  ASSERT_TRUE(static_cast<bool>(out));
}

void append_event(const std::string& path, const hft::OmsJournalEvent& event) {
  ASSERT_TRUE(hft::append_journal_event(path, event));
}

uint64_t checksum64(std::string_view data) {
  uint64_t hash = 14695981039346656037ull;
  for (unsigned char c : data) {
    hash ^= static_cast<uint64_t>(c);
    hash *= 1099511628211ull;
  }
  return hash;
}

std::string make_record(std::string_view payload) {
  std::ostringstream out;
  out << "HFTJ|version=1|len=" << payload.size() << '|' << payload
      << "|checksum=" << checksum64(payload) << '\n';
  return out.str();
}

}

TEST(Recovery, ReplayJournalFailsOnPartialJournalLine) {
  const std::string path = make_temp_journal_path();
  write_text(path, "HFTJ|version=1|len=40|type=SubmitNew|order_id=1001");

  hft::Oms oms;
  const auto result = hft::replay_journal(path, oms);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, hft::ReplayErrorCode::ParseError);
  EXPECT_EQ(result.error().line_number, 1u);
  EXPECT_FALSE(result.error().event_type.has_value());
  EXPECT_FALSE(oms.get(1001).has_value());

  EXPECT_EQ(std::remove(path.c_str()), 0);
}

TEST(Recovery, ReplayJournalFailsOnMalformedNumericField) {
  const std::string path = make_temp_journal_path();
  write_text(
      path,
      make_record("type=SubmitNew|order_id=not-a-number|side=0|price=100|qty=5"));

  hft::Oms oms;
  const auto result = hft::replay_journal(path, oms);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, hft::ReplayErrorCode::ParseError);
  EXPECT_EQ(result.error().line_number, 1u);
  EXPECT_FALSE(result.error().event_type.has_value());

  EXPECT_EQ(std::remove(path.c_str()), 0);
}

TEST(Recovery, ReplayJournalFailsOnChecksumMismatch) {
  const std::string path = make_temp_journal_path();
  write_text(
      path,
      "HFTJ|version=1|len=50|type=SubmitNew|order_id=4001|side=0|price=100|qty=5|checksum=1\n");

  hft::Oms oms;
  const auto result = hft::replay_journal(path, oms);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, hft::ReplayErrorCode::ParseError);
  EXPECT_EQ(result.error().line_number, 1u);
  EXPECT_FALSE(oms.get(4001).has_value());

  EXPECT_EQ(std::remove(path.c_str()), 0);
}

TEST(Recovery, ReplayJournalFailsOnDuplicateReplayedEvents) {
  const std::string path = make_temp_journal_path();
  append_event(path, hft::OmsJournalEvent{
      .type = hft::OmsEventType::SubmitNew,
      .order_id = 2001,
      .side = static_cast<uint8_t>(hft::Side::Buy),
      .price = 100,
      .qty = 5,
  });
  append_event(path, hft::OmsJournalEvent{
      .type = hft::OmsEventType::NewAck,
      .order_id = 2001,
      .side = static_cast<uint8_t>(hft::Side::Buy),
      .price = 100,
      .qty = 5,
  });
  append_event(path, hft::OmsJournalEvent{
      .type = hft::OmsEventType::SubmitNew,
      .order_id = 2001,
      .side = static_cast<uint8_t>(hft::Side::Buy),
      .price = 100,
      .qty = 5,
  });

  hft::Oms oms;
  const auto result = hft::replay_journal(path, oms);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, hft::ReplayErrorCode::InvalidTransition);
  EXPECT_EQ(result.error().line_number, 3u);
  ASSERT_TRUE(result.error().event_type.has_value());
  EXPECT_EQ(*result.error().event_type, hft::OmsEventType::SubmitNew);

  auto rec = oms.get(2001);
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->status, hft::OrderStatus::Live);

  EXPECT_EQ(std::remove(path.c_str()), 0);
}

TEST(Recovery, ReplayJournalFailsWhenCancelOrFillOccursAfterTerminalState) {
  {
    const std::string path = make_temp_journal_path();
    append_event(path, hft::OmsJournalEvent{
        .type = hft::OmsEventType::SubmitNew,
        .order_id = 3001,
        .side = static_cast<uint8_t>(hft::Side::Buy),
        .price = 100,
        .qty = 5,
    });
    append_event(path, hft::OmsJournalEvent{
        .type = hft::OmsEventType::NewAck,
        .order_id = 3001,
        .side = static_cast<uint8_t>(hft::Side::Buy),
        .price = 100,
        .qty = 5,
    });
    append_event(path, hft::OmsJournalEvent{
        .type = hft::OmsEventType::SubmitCancel,
        .order_id = 3001,
        .side = static_cast<uint8_t>(hft::Side::Buy),
        .price = 0,
        .qty = 0,
    });
    append_event(path, hft::OmsJournalEvent{
        .type = hft::OmsEventType::CancelAck,
        .order_id = 3001,
        .side = static_cast<uint8_t>(hft::Side::Buy),
        .price = 0,
        .qty = 0,
    });
    append_event(path, hft::OmsJournalEvent{
        .type = hft::OmsEventType::SubmitCancel,
        .order_id = 3001,
        .side = static_cast<uint8_t>(hft::Side::Buy),
        .price = 0,
        .qty = 0,
    });

    hft::Oms oms;
    const auto result = hft::replay_journal(path, oms);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, hft::ReplayErrorCode::InvalidTransition);
    EXPECT_EQ(result.error().line_number, 5u);
    ASSERT_TRUE(result.error().event_type.has_value());
    EXPECT_EQ(*result.error().event_type, hft::OmsEventType::SubmitCancel);
    EXPECT_EQ(std::remove(path.c_str()), 0);
  }

  {
    const std::string path = make_temp_journal_path();
    append_event(path, hft::OmsJournalEvent{
        .type = hft::OmsEventType::SubmitNew,
        .order_id = 3002,
        .side = static_cast<uint8_t>(hft::Side::Buy),
        .price = 100,
        .qty = 5,
    });
    append_event(path, hft::OmsJournalEvent{
        .type = hft::OmsEventType::NewAck,
        .order_id = 3002,
        .side = static_cast<uint8_t>(hft::Side::Buy),
        .price = 100,
        .qty = 5,
    });
    append_event(path, hft::OmsJournalEvent{
        .type = hft::OmsEventType::Fill,
        .order_id = 3002,
        .side = static_cast<uint8_t>(hft::Side::Buy),
        .price = 100,
        .qty = 5,
    });
    append_event(path, hft::OmsJournalEvent{
        .type = hft::OmsEventType::Fill,
        .order_id = 3002,
        .side = static_cast<uint8_t>(hft::Side::Buy),
        .price = 100,
        .qty = 1,
    });

    hft::Oms oms;
    const auto result = hft::replay_journal(path, oms);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, hft::ReplayErrorCode::InvalidTransition);
    EXPECT_EQ(result.error().line_number, 4u);
    ASSERT_TRUE(result.error().event_type.has_value());
    EXPECT_EQ(*result.error().event_type, hft::OmsEventType::Fill);
    auto rec = oms.get(3002);
    ASSERT_TRUE(rec.has_value());
    EXPECT_EQ(rec->status, hft::OrderStatus::Filled);
    EXPECT_EQ(std::remove(path.c_str()), 0);
  }
}

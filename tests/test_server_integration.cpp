#include "wire.hpp"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <chrono>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

#ifndef EXCHANGE_SERVER_PATH
#define EXCHANGE_SERVER_PATH "./exchange_server"
#endif

namespace {

constexpr int kPort = 9000;
constexpr std::string_view kLoopback = "127.0.0.1";

bool write_all(int fd, const void* buf, size_t n) {
  const auto* in = static_cast<const char*>(buf);
  size_t total = 0;

  while (total < n) {
    const ssize_t w = ::write(fd, in + total, n - total);
    if (w < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (w == 0) {
      return false;
    }
    total += static_cast<size_t>(w);
  }

  return true;
}

bool read_exact(int fd, void* buf, size_t n) {
  auto* out = static_cast<char*>(buf);
  size_t total = 0;

  while (total < n) {
    const ssize_t r = ::read(fd, out + total, n - total);
    if (r < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (r == 0) {
      return false;
    }
    total += static_cast<size_t>(r);
  }

  return true;
}

int connect_to_server() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return -1;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(kPort));
  if (::inet_pton(AF_INET, kLoopback.data(), &addr.sin_addr) != 1) {
    ::close(fd);
    return -1;
  }

  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    return -1;
  }

  return fd;
}

class ServerFixture : public ::testing::Test {
protected:
  virtual const char* EventSinkMaxQueueArg() const { return nullptr; }

  void SetUp() override {
    server_pid_ = ::fork();
    ASSERT_GE(server_pid_, 0) << "fork failed: " << std::strerror(errno);

    if (server_pid_ == 0) {
      const char* max_queue_arg = EventSinkMaxQueueArg();
      if (max_queue_arg != nullptr) {
        ::execl(EXCHANGE_SERVER_PATH, EXCHANGE_SERVER_PATH, max_queue_arg, static_cast<char*>(nullptr));
      } else {
        ::execl(EXCHANGE_SERVER_PATH, EXCHANGE_SERVER_PATH, static_cast<char*>(nullptr));
      }
      std::perror("execl");
      _exit(127);
    }

    bool connected = false;
    for (int i = 0; i < 50; ++i) {
      const int probe = connect_to_server();
      if (probe >= 0) {
        ::close(probe);
        connected = true;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    ASSERT_TRUE(connected) << "server did not start listening on port " << kPort;
  }

  void TearDown() override {
    if (server_pid_ > 0) {
      ::kill(server_pid_, SIGTERM);
      int status = 0;
      ::waitpid(server_pid_, &status, 0);
    }
  }

  int OpenClient() {
    const int fd = connect_to_server();
    EXPECT_GE(fd, 0) << "connect failed: " << std::strerror(errno);
    return fd;
  }

  pid_t server_pid_ = -1;
};

class SmallQueueServerFixture : public ServerFixture {
protected:
  const char* EventSinkMaxQueueArg() const override { return "--event-sink-max-queue=2"; }
};

} 

TEST_F(ServerFixture, NewOrderReturnsAck) {
  const int fd = OpenClient();
  ASSERT_GE(fd, 0);

  const wire::Header hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 1,
  };
  const wire::NewOrder msg{
      .order_id = 42,
      .side = 0,
      .price = 100,
      .qty = 10,
  };

  ASSERT_TRUE(write_all(fd, &hdr, sizeof(hdr)));
  ASSERT_TRUE(write_all(fd, &msg, sizeof(msg)));

  wire::Header reply_hdr{};
  wire::Ack reply{};

  ASSERT_TRUE(read_exact(fd, &reply_hdr, sizeof(reply_hdr)));
  ASSERT_TRUE(read_exact(fd, &reply, sizeof(reply)));

  EXPECT_EQ(reply_hdr.type, static_cast<uint16_t>(wire::MsgType::Ack));
  EXPECT_EQ(reply_hdr.length, sizeof(wire::Ack));
  EXPECT_EQ(reply_hdr.seq, 1);
  EXPECT_EQ(reply.order_id, 42u);

  ::close(fd);
}

TEST_F(ServerFixture, CancelExistingOrderReturnsAck) {
  const int fd = OpenClient();
  ASSERT_GE(fd, 0);

  const wire::Header new_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 11,
  };
  const wire::NewOrder new_msg{
      .order_id = 77,
      .side = 0,
      .price = 101,
      .qty = 5,
  };

  ASSERT_TRUE(write_all(fd, &new_hdr, sizeof(new_hdr)));
  ASSERT_TRUE(write_all(fd, &new_msg, sizeof(new_msg)));

  wire::Header ack_hdr{};
  wire::Ack ack{};
  ASSERT_TRUE(read_exact(fd, &ack_hdr, sizeof(ack_hdr)));
  ASSERT_TRUE(read_exact(fd, &ack, sizeof(ack)));
  ASSERT_EQ(ack_hdr.type, static_cast<uint16_t>(wire::MsgType::Ack));
  ASSERT_EQ(ack.order_id, 77u);

  const wire::Header cancel_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::Cancel),
      .length = static_cast<uint16_t>(sizeof(wire::Cancel)),
      .seq = 12,
  };
  const wire::Cancel cancel_msg{
      .order_id = 77,
  };

  ASSERT_TRUE(write_all(fd, &cancel_hdr, sizeof(cancel_hdr)));
  ASSERT_TRUE(write_all(fd, &cancel_msg, sizeof(cancel_msg)));

  wire::Header cancel_reply_hdr{};
  wire::Ack cancel_reply{};
  ASSERT_TRUE(read_exact(fd, &cancel_reply_hdr, sizeof(cancel_reply_hdr)));
  ASSERT_TRUE(read_exact(fd, &cancel_reply, sizeof(cancel_reply)));

  EXPECT_EQ(cancel_reply_hdr.type, static_cast<uint16_t>(wire::MsgType::Ack));
  EXPECT_EQ(cancel_reply_hdr.length, sizeof(wire::Ack));
  EXPECT_EQ(cancel_reply_hdr.seq, 12);
  EXPECT_EQ(cancel_reply.order_id, 77u);

  ::close(fd);
}

TEST_F(ServerFixture, InvalidSideReturnsReject) {
  const int fd = OpenClient();
  ASSERT_GE(fd, 0);

  const wire::Header hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 21,
  };
  const wire::NewOrder msg{
      .order_id = 99,
      .side = 9,
      .price = 100,
      .qty = 1,
  };

  ASSERT_TRUE(write_all(fd, &hdr, sizeof(hdr)));
  ASSERT_TRUE(write_all(fd, &msg, sizeof(msg)));

  wire::Header reply_hdr{};
  wire::Reject reply{};

  ASSERT_TRUE(read_exact(fd, &reply_hdr, sizeof(reply_hdr)));
  ASSERT_TRUE(read_exact(fd, &reply, sizeof(reply)));

  EXPECT_EQ(reply_hdr.type, static_cast<uint16_t>(wire::MsgType::Reject));
  EXPECT_EQ(reply_hdr.length, sizeof(wire::Reject));
  EXPECT_EQ(reply_hdr.seq, 21);
  EXPECT_EQ(reply.order_id, 99u);
  EXPECT_EQ(reply.reason, 2);

  ::close(fd);
}

TEST_F(ServerFixture, CrossingOrderReturnsAckThenFill) {
  const int fd = OpenClient();
  ASSERT_GE(fd, 0);

  const wire::Header resting_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 31,
  };
  const wire::NewOrder resting_order{
      .order_id = 1001,
      .side = 1,
      .price = 101,
      .qty = 7,
  };

  ASSERT_TRUE(write_all(fd, &resting_hdr, sizeof(resting_hdr)));
  ASSERT_TRUE(write_all(fd, &resting_order, sizeof(resting_order)));

  wire::Header resting_ack_hdr{};
  wire::Ack resting_ack{};
  ASSERT_TRUE(read_exact(fd, &resting_ack_hdr, sizeof(resting_ack_hdr)));
  ASSERT_TRUE(read_exact(fd, &resting_ack, sizeof(resting_ack)));
  ASSERT_EQ(resting_ack_hdr.type, static_cast<uint16_t>(wire::MsgType::Ack));
  ASSERT_EQ(resting_ack_hdr.seq, 31);
  ASSERT_EQ(resting_ack.order_id, 1001u);

  const wire::Header taker_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 32,
  };
  const wire::NewOrder taker_order{
      .order_id = 1002,
      .side = 0,
      .price = 105,
      .qty = 5,
  };

  ASSERT_TRUE(write_all(fd, &taker_hdr, sizeof(taker_hdr)));
  ASSERT_TRUE(write_all(fd, &taker_order, sizeof(taker_order)));

  wire::Header taker_ack_hdr{};
  wire::Ack taker_ack{};
  ASSERT_TRUE(read_exact(fd, &taker_ack_hdr, sizeof(taker_ack_hdr)));
  ASSERT_TRUE(read_exact(fd, &taker_ack, sizeof(taker_ack)));

  EXPECT_EQ(taker_ack_hdr.type, static_cast<uint16_t>(wire::MsgType::Ack));
  EXPECT_EQ(taker_ack_hdr.length, sizeof(wire::Ack));
  EXPECT_EQ(taker_ack_hdr.seq, 32);
  EXPECT_EQ(taker_ack.order_id, 1002u);

  wire::Header fill_hdr{};
  wire::Fill fill{};
  ASSERT_TRUE(read_exact(fd, &fill_hdr, sizeof(fill_hdr)));
  ASSERT_TRUE(read_exact(fd, &fill, sizeof(fill)));

  EXPECT_EQ(fill_hdr.type, static_cast<uint16_t>(wire::MsgType::Fill));
  EXPECT_EQ(fill_hdr.length, sizeof(wire::Fill));
  EXPECT_EQ(fill_hdr.seq, 32);
  EXPECT_EQ(fill.order_id, 1002u);
  EXPECT_EQ(fill.price, 101);
  EXPECT_EQ(fill.qty, 5);

  ::close(fd);
}

TEST_F(ServerFixture, CancelMissingOrderReturnsReject) {
  const int fd = OpenClient();
  ASSERT_GE(fd, 0);

  const wire::Header cancel_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::Cancel),
      .length = static_cast<uint16_t>(sizeof(wire::Cancel)),
      .seq = 41,
  };
  const wire::Cancel cancel_msg{
      .order_id = 5555,
  };

  ASSERT_TRUE(write_all(fd, &cancel_hdr, sizeof(cancel_hdr)));
  ASSERT_TRUE(write_all(fd, &cancel_msg, sizeof(cancel_msg)));

  wire::Header reply_hdr{};
  wire::Reject reply{};
  ASSERT_TRUE(read_exact(fd, &reply_hdr, sizeof(reply_hdr)));
  ASSERT_TRUE(read_exact(fd, &reply, sizeof(reply)));

  EXPECT_EQ(reply_hdr.type, static_cast<uint16_t>(wire::MsgType::Reject));
  EXPECT_EQ(reply_hdr.length, sizeof(wire::Reject));
  EXPECT_EQ(reply_hdr.seq, 41);
  EXPECT_EQ(reply.order_id, 5555u);
  EXPECT_EQ(reply.reason, 3);

  ::close(fd);
}

TEST_F(ServerFixture, InvalidLengthReturnsReject) {
  const int fd = OpenClient();
  ASSERT_GE(fd, 0);

  const wire::Header bad_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder) - 1),
      .seq = 42,
  };

  ASSERT_TRUE(write_all(fd, &bad_hdr, sizeof(bad_hdr)));

  wire::Header reply_hdr{};
  wire::Reject reply{};
  ASSERT_TRUE(read_exact(fd, &reply_hdr, sizeof(reply_hdr)));
  ASSERT_TRUE(read_exact(fd, &reply, sizeof(reply)));

  EXPECT_EQ(reply_hdr.type, static_cast<uint16_t>(wire::MsgType::Reject));
  EXPECT_EQ(reply_hdr.length, sizeof(wire::Reject));
  EXPECT_EQ(reply_hdr.seq, 42);
  EXPECT_EQ(reply.order_id, 0u);
  EXPECT_EQ(reply.reason, 1);

  ::close(fd);
}

TEST_F(ServerFixture, ZeroQuantityReturnsReject) {
  const int fd = OpenClient();
  ASSERT_GE(fd, 0);

  const wire::Header hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 51,
  };
  const wire::NewOrder msg{
      .order_id = 2001,
      .side = 0,
      .price = 100,
      .qty = 0,
  };

  ASSERT_TRUE(write_all(fd, &hdr, sizeof(hdr)));
  ASSERT_TRUE(write_all(fd, &msg, sizeof(msg)));

  wire::Header reply_hdr{};
  wire::Reject reply{};
  ASSERT_TRUE(read_exact(fd, &reply_hdr, sizeof(reply_hdr)));
  ASSERT_TRUE(read_exact(fd, &reply, sizeof(reply)));

  EXPECT_EQ(reply_hdr.type, static_cast<uint16_t>(wire::MsgType::Reject));
  EXPECT_EQ(reply_hdr.length, sizeof(wire::Reject));
  EXPECT_EQ(reply_hdr.seq, 51);
  EXPECT_EQ(reply.order_id, 2001u);
  EXPECT_EQ(reply.reason, 4);

  ::close(fd);
}

TEST_F(ServerFixture, NonPositivePriceReturnsReject) {
  const int fd = OpenClient();
  ASSERT_GE(fd, 0);

  const wire::Header hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 52,
  };
  const wire::NewOrder msg{
      .order_id = 2002,
      .side = 1,
      .price = 0,
      .qty = 3,
  };

  ASSERT_TRUE(write_all(fd, &hdr, sizeof(hdr)));
  ASSERT_TRUE(write_all(fd, &msg, sizeof(msg)));

  wire::Header reply_hdr{};
  wire::Reject reply{};
  ASSERT_TRUE(read_exact(fd, &reply_hdr, sizeof(reply_hdr)));
  ASSERT_TRUE(read_exact(fd, &reply, sizeof(reply)));

  EXPECT_EQ(reply_hdr.type, static_cast<uint16_t>(wire::MsgType::Reject));
  EXPECT_EQ(reply_hdr.length, sizeof(wire::Reject));
  EXPECT_EQ(reply_hdr.seq, 52);
  EXPECT_EQ(reply.order_id, 2002u);
  EXPECT_EQ(reply.reason, 5);

  ::close(fd);
}

TEST_F(ServerFixture, DuplicateOrderIdReturnsReject) {
  const int fd = OpenClient();
  ASSERT_GE(fd, 0);

  const wire::Header first_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 61,
  };
  const wire::NewOrder first_msg{
      .order_id = 3001,
      .side = 0,
      .price = 100,
      .qty = 5,
  };

  ASSERT_TRUE(write_all(fd, &first_hdr, sizeof(first_hdr)));
  ASSERT_TRUE(write_all(fd, &first_msg, sizeof(first_msg)));

  wire::Header first_reply_hdr{};
  wire::Ack first_reply{};
  ASSERT_TRUE(read_exact(fd, &first_reply_hdr, sizeof(first_reply_hdr)));
  ASSERT_TRUE(read_exact(fd, &first_reply, sizeof(first_reply)));

  ASSERT_EQ(first_reply_hdr.type, static_cast<uint16_t>(wire::MsgType::Ack));
  ASSERT_EQ(first_reply_hdr.seq, 61);
  ASSERT_EQ(first_reply.order_id, 3001u);

  const wire::Header second_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 62,
  };
  const wire::NewOrder second_msg{
      .order_id = 3001,
      .side = 1,
      .price = 101,
      .qty = 3,
  };

  ASSERT_TRUE(write_all(fd, &second_hdr, sizeof(second_hdr)));
  ASSERT_TRUE(write_all(fd, &second_msg, sizeof(second_msg)));

  wire::Header reject_hdr{};
  wire::Reject reject{};
  ASSERT_TRUE(read_exact(fd, &reject_hdr, sizeof(reject_hdr)));
  ASSERT_TRUE(read_exact(fd, &reject, sizeof(reject)));

  EXPECT_EQ(reject_hdr.type, static_cast<uint16_t>(wire::MsgType::Reject));
  EXPECT_EQ(reject_hdr.length, sizeof(wire::Reject));
  EXPECT_EQ(reject_hdr.seq, 62);
  EXPECT_EQ(reject.order_id, 3001u);
  EXPECT_EQ(reject.reason, 6);

  ::close(fd);
}

TEST_F(ServerFixture, OrderIdCanBeReusedAfterCancel) {
  const int fd = OpenClient();
  ASSERT_GE(fd, 0);

  const wire::Header new_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 71,
  };
  const wire::NewOrder first_order{
      .order_id = 4001,
      .side = 0,
      .price = 100,
      .qty = 2,
  };

  ASSERT_TRUE(write_all(fd, &new_hdr, sizeof(new_hdr)));
  ASSERT_TRUE(write_all(fd, &first_order, sizeof(first_order)));

  wire::Header ack1_hdr{};
  wire::Ack ack1{};
  ASSERT_TRUE(read_exact(fd, &ack1_hdr, sizeof(ack1_hdr)));
  ASSERT_TRUE(read_exact(fd, &ack1, sizeof(ack1)));

  const wire::Header cancel_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::Cancel),
      .length = static_cast<uint16_t>(sizeof(wire::Cancel)),
      .seq = 72,
  };
  const wire::Cancel cancel_msg{.order_id = 4001};

  ASSERT_TRUE(write_all(fd, &cancel_hdr, sizeof(cancel_hdr)));
  ASSERT_TRUE(write_all(fd, &cancel_msg, sizeof(cancel_msg)));

  wire::Header cancel_ack_hdr{};
  wire::Ack cancel_ack{};
  ASSERT_TRUE(read_exact(fd, &cancel_ack_hdr, sizeof(cancel_ack_hdr)));
  ASSERT_TRUE(read_exact(fd, &cancel_ack, sizeof(cancel_ack)));

  const wire::Header reuse_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 73,
  };
  const wire::NewOrder reused_order{
      .order_id = 4001,
      .side = 1,
      .price = 101,
      .qty = 4,
  };

  ASSERT_TRUE(write_all(fd, &reuse_hdr, sizeof(reuse_hdr)));
  ASSERT_TRUE(write_all(fd, &reused_order, sizeof(reused_order)));

  wire::Header ack2_hdr{};
  wire::Ack ack2{};
  ASSERT_TRUE(read_exact(fd, &ack2_hdr, sizeof(ack2_hdr)));
  ASSERT_TRUE(read_exact(fd, &ack2, sizeof(ack2)));

  EXPECT_EQ(ack2_hdr.type, static_cast<uint16_t>(wire::MsgType::Ack));
  EXPECT_EQ(ack2_hdr.seq, 73);
  EXPECT_EQ(ack2.order_id, 4001u);

  ::close(fd);
}

TEST_F(ServerFixture, DuplicateOrderIdStillRejectsAfterCoordinatorIntegration) {
  const int fd = OpenClient();
  ASSERT_GE(fd, 0);

  const wire::Header first_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 81,
  };
  const wire::NewOrder first_msg{
      .order_id = 8101,
      .side = 0,
      .price = 100,
      .qty = 5,
  };

  ASSERT_TRUE(write_all(fd, &first_hdr, sizeof(first_hdr)));
  ASSERT_TRUE(write_all(fd, &first_msg, sizeof(first_msg)));

  wire::Header ack_hdr{};
  wire::Ack ack{};
  ASSERT_TRUE(read_exact(fd, &ack_hdr, sizeof(ack_hdr)));
  ASSERT_TRUE(read_exact(fd, &ack, sizeof(ack)));
  ASSERT_EQ(ack_hdr.type, static_cast<uint16_t>(wire::MsgType::Ack));
  ASSERT_EQ(ack.order_id, 8101u);

  const wire::Header second_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 82,
  };
  const wire::NewOrder second_msg{
      .order_id = 8101,
      .side = 1,
      .price = 101,
      .qty = 3,
  };

  ASSERT_TRUE(write_all(fd, &second_hdr, sizeof(second_hdr)));
  ASSERT_TRUE(write_all(fd, &second_msg, sizeof(second_msg)));

  wire::Header rej_hdr{};
  wire::Reject rej{};
  ASSERT_TRUE(read_exact(fd, &rej_hdr, sizeof(rej_hdr)));
  ASSERT_TRUE(read_exact(fd, &rej, sizeof(rej)));

  EXPECT_EQ(rej_hdr.type, static_cast<uint16_t>(wire::MsgType::Reject));
  EXPECT_EQ(rej_hdr.seq, 82);
  EXPECT_EQ(rej.order_id, 8101u);
  EXPECT_EQ(rej.reason, 6);

  ::close(fd);
}

TEST_F(ServerFixture, RejectFlowsUpdateCoordinatorMetricsTotals) {
  const int fd1 = OpenClient();
  ASSERT_GE(fd1, 0);

  const wire::Header new_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 91,
  };
  const wire::NewOrder first_new{
      .order_id = 9901,
      .side = 0,
      .price = 100,
      .qty = 5,
  };
  ASSERT_TRUE(write_all(fd1, &new_hdr, sizeof(new_hdr)));
  ASSERT_TRUE(write_all(fd1, &first_new, sizeof(first_new)));

  wire::Header ack_hdr{};
  wire::Ack ack{};
  ASSERT_TRUE(read_exact(fd1, &ack_hdr, sizeof(ack_hdr)));
  ASSERT_TRUE(read_exact(fd1, &ack, sizeof(ack)));
  ASSERT_EQ(ack_hdr.type, static_cast<uint16_t>(wire::MsgType::Ack));
  ASSERT_EQ(ack.order_id, 9901u);

  const wire::Header dup_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 92,
  };
  const wire::NewOrder dup_new{
      .order_id = 9901,
      .side = 1,
      .price = 101,
      .qty = 2,
  };
  ASSERT_TRUE(write_all(fd1, &dup_hdr, sizeof(dup_hdr)));
  ASSERT_TRUE(write_all(fd1, &dup_new, sizeof(dup_new)));

  wire::Header dup_rej_hdr{};
  wire::Reject dup_rej{};
  ASSERT_TRUE(read_exact(fd1, &dup_rej_hdr, sizeof(dup_rej_hdr)));
  ASSERT_TRUE(read_exact(fd1, &dup_rej, sizeof(dup_rej)));
  ASSERT_EQ(dup_rej_hdr.type, static_cast<uint16_t>(wire::MsgType::Reject));
  ASSERT_EQ(dup_rej.reason, 6);

  const wire::Header cancel_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::Cancel),
      .length = static_cast<uint16_t>(sizeof(wire::Cancel)),
      .seq = 93,
  };
  const wire::Cancel cancel_msg{
      .order_id = 999999,
  };
  ASSERT_TRUE(write_all(fd1, &cancel_hdr, sizeof(cancel_hdr)));
  ASSERT_TRUE(write_all(fd1, &cancel_msg, sizeof(cancel_msg)));

  wire::Header cancel_rej_hdr{};
  wire::Reject cancel_rej{};
  ASSERT_TRUE(read_exact(fd1, &cancel_rej_hdr, sizeof(cancel_rej_hdr)));
  ASSERT_TRUE(read_exact(fd1, &cancel_rej, sizeof(cancel_rej)));
  ASSERT_EQ(cancel_rej_hdr.type, static_cast<uint16_t>(wire::MsgType::Reject));
  ASSERT_EQ(cancel_rej.reason, 3);

  ::close(fd1);

  const int fd2 = OpenClient();
  ASSERT_GE(fd2, 0);

  const wire::Header get_metrics_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::GetMetrics),
      .length = 0,
      .seq = 94,
  };
  ASSERT_TRUE(write_all(fd2, &get_metrics_hdr, sizeof(get_metrics_hdr)));

  wire::Header metrics_hdr{};
  wire::MetricsSnapshot metrics{};
  ASSERT_TRUE(read_exact(fd2, &metrics_hdr, sizeof(metrics_hdr)));
  ASSERT_TRUE(read_exact(fd2, &metrics, sizeof(metrics)));

  ASSERT_EQ(metrics_hdr.type, static_cast<uint16_t>(wire::MsgType::MetricsSnapshot));
  ASSERT_EQ(metrics_hdr.length, sizeof(wire::MetricsSnapshot));
  ASSERT_EQ(metrics_hdr.seq, 94);

  EXPECT_EQ(metrics.event_type_counts[1], 1u);
  EXPECT_EQ(metrics.event_type_counts[4], 1u);
  EXPECT_EQ(metrics.reject_reason_counts[1], 1u);
  EXPECT_EQ(metrics.reject_reason_counts[2], 1u);

  ::close(fd2);
}

TEST_F(SmallQueueServerFixture, MetricsReportDroppedEventsWhenQueueOverflows) {
  const int fd1 = OpenClient();
  ASSERT_GE(fd1, 0);

  const wire::Header new_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 201,
  };
  const wire::NewOrder first_new{
      .order_id = 20001,
      .side = 0,
      .price = 100,
      .qty = 5,
  };
  ASSERT_TRUE(write_all(fd1, &new_hdr, sizeof(new_hdr)));
  ASSERT_TRUE(write_all(fd1, &first_new, sizeof(first_new)));

  wire::Header ack_hdr{};
  wire::Ack ack{};
  ASSERT_TRUE(read_exact(fd1, &ack_hdr, sizeof(ack_hdr)));
  ASSERT_TRUE(read_exact(fd1, &ack, sizeof(ack)));
  ASSERT_EQ(ack_hdr.type, static_cast<uint16_t>(wire::MsgType::Ack));
  ASSERT_EQ(ack.order_id, 20001u);

  const wire::Header dup_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = 202,
  };
  const wire::NewOrder dup_new{
      .order_id = 20001,
      .side = 1,
      .price = 101,
      .qty = 2,
  };
  ASSERT_TRUE(write_all(fd1, &dup_hdr, sizeof(dup_hdr)));
  ASSERT_TRUE(write_all(fd1, &dup_new, sizeof(dup_new)));

  wire::Header dup_rej_hdr{};
  wire::Reject dup_rej{};
  ASSERT_TRUE(read_exact(fd1, &dup_rej_hdr, sizeof(dup_rej_hdr)));
  ASSERT_TRUE(read_exact(fd1, &dup_rej, sizeof(dup_rej)));
  ASSERT_EQ(dup_rej_hdr.type, static_cast<uint16_t>(wire::MsgType::Reject));
  ASSERT_EQ(dup_rej.reason, 6);

  const wire::Header cancel_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::Cancel),
      .length = static_cast<uint16_t>(sizeof(wire::Cancel)),
      .seq = 203,
  };
  const wire::Cancel cancel_msg{
      .order_id = 888888,
  };
  ASSERT_TRUE(write_all(fd1, &cancel_hdr, sizeof(cancel_hdr)));
  ASSERT_TRUE(write_all(fd1, &cancel_msg, sizeof(cancel_msg)));

  wire::Header cancel_rej_hdr{};
  wire::Reject cancel_rej{};
  ASSERT_TRUE(read_exact(fd1, &cancel_rej_hdr, sizeof(cancel_rej_hdr)));
  ASSERT_TRUE(read_exact(fd1, &cancel_rej, sizeof(cancel_rej)));
  ASSERT_EQ(cancel_rej_hdr.type, static_cast<uint16_t>(wire::MsgType::Reject));
  ASSERT_EQ(cancel_rej.reason, 3);

  ::close(fd1);

  const int fd2 = OpenClient();
  ASSERT_GE(fd2, 0);

  const wire::Header get_metrics_hdr{
      .type = static_cast<uint16_t>(wire::MsgType::GetMetrics),
      .length = 0,
      .seq = 204,
  };
  ASSERT_TRUE(write_all(fd2, &get_metrics_hdr, sizeof(get_metrics_hdr)));

  wire::Header metrics_hdr{};
  wire::MetricsSnapshot metrics{};
  ASSERT_TRUE(read_exact(fd2, &metrics_hdr, sizeof(metrics_hdr)));
  ASSERT_TRUE(read_exact(fd2, &metrics, sizeof(metrics)));

  ASSERT_EQ(metrics_hdr.type, static_cast<uint16_t>(wire::MsgType::MetricsSnapshot));
  ASSERT_EQ(metrics_hdr.length, sizeof(wire::MetricsSnapshot));
  ASSERT_EQ(metrics_hdr.seq, 204);

  EXPECT_EQ(metrics.dropped_events, 1u);

  ::close(fd2);
}

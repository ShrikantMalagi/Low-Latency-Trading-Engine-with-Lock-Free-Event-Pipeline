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
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

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
  void SetUp() override {
    server_pid_ = ::fork();
    ASSERT_GE(server_pid_, 0) << "fork failed: " << std::strerror(errno);

    if (server_pid_ == 0) {
      ::execl(EXCHANGE_SERVER_PATH, EXCHANGE_SERVER_PATH, static_cast<char*>(nullptr));
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

private:
  pid_t server_pid_ = -1;
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

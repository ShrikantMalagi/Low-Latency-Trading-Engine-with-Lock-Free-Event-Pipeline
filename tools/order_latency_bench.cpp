#include "wire.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <algorithm>

namespace {
bool write_all(int fd, const void* buf, std::size_t n) {
  const auto* p = static_cast<const char*>(buf);
  std::size_t off = 0;
  while (off < n) {
    const ssize_t rc = ::write(fd, p + off, n - off);
    if (rc <= 0) return false;
    off += static_cast<std::size_t>(rc);
  }
  return true;
}

bool read_exact(int fd, void* buf, std::size_t n) {
  auto* p = static_cast<char*>(buf);
  std::size_t off = 0;
  while (off < n) {
    const ssize_t rc = ::read(fd, p + off, n - off);
    if (rc <= 0) return false;
    off += static_cast<std::size_t>(rc);
  }
  return true;
}

int connect_to_server(const char* ip, int port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (::inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
    ::close(fd);
    return -1;
  }
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

double percentile(std::vector<uint64_t> values, double p) {
  std::sort(values.begin(), values.end());
  const std::size_t idx = static_cast<std::size_t>(p * (values.size() - 1));
  return static_cast<double>(values[idx]);
}
}

int main() {
  const int fd = connect_to_server("127.0.0.1", 9000);
  if (fd < 0) return 1;

  constexpr int kOrders = 10000;
  std::vector<uint64_t> latencies;
  latencies.reserve(kOrders);

  for (int i = 0; i < kOrders; ++i) {
    wire::Header hdr{
      .type = static_cast<uint16_t>(wire::MsgType::NewOrder),
      .length = static_cast<uint16_t>(sizeof(wire::NewOrder)),
      .seq = static_cast<uint16_t>(i),
    };
    wire::NewOrder msg{
      .order_id = static_cast<uint64_t>(100000 + i),
      .side = 0,
      .price = 100,
      .qty = 1,
    };

    const auto t0 = std::chrono::steady_clock::now();

    if (!write_all(fd, &hdr, sizeof(hdr))) return 2;
    if (!write_all(fd, &msg, sizeof(msg))) return 3;

    wire::Header rsp_hdr{};
    wire::Ack ack{};
    if (!read_exact(fd, &rsp_hdr, sizeof(rsp_hdr))) return 4;
    if (!read_exact(fd, &ack, sizeof(ack))) return 5;

    const auto t1 = std::chrono::steady_clock::now();
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    latencies.push_back(static_cast<uint64_t>(ns));
  }

  std::printf("count=%zu\n", latencies.size());
  std::printf("p50_ns=%.0f\n", percentile(latencies, 0.50));
  std::printf("p95_ns=%.0f\n", percentile(latencies, 0.95));
  std::printf("p99_ns=%.0f\n", percentile(latencies, 0.99));
  std::printf("max_ns=%.0f\n", percentile(latencies, 1.00));

  ::close(fd);
  return 0;
}

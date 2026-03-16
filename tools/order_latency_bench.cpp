#include "wire.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cerrno>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
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

bool set_socket_timeout(int fd, int seconds) {
  timeval tv{};
  tv.tv_sec = seconds;
  tv.tv_usec = 0;
  return ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0 &&
         ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == 0;
}
}

int main(int argc, char* argv[]) {
  int order_count = 1000;
  if (argc > 1) {
    order_count = std::atoi(argv[1]);
    if (order_count <= 0) {
      std::fprintf(stderr, "invalid order count: %s\n", argv[1]);
      return 1;
    }
  }

  const int fd = connect_to_server("127.0.0.1", 9000);
  if (fd < 0) {
    std::fprintf(stderr, "connect failed: %s\n", std::strerror(errno));
    return 1;
  }

  if (!set_socket_timeout(fd, 2)) {
    std::fprintf(stderr, "failed to set socket timeout: %s\n", std::strerror(errno));
    ::close(fd);
    return 1;
  }

  std::vector<uint64_t> latencies;
  latencies.reserve(static_cast<std::size_t>(order_count));
  std::size_t ack_count = 0;
  std::size_t reject_count = 0;

  const auto bench_start = std::chrono::steady_clock::now();

  for (int i = 0; i < order_count; ++i) {
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

    if (!write_all(fd, &hdr, sizeof(hdr))) {
      std::fprintf(stderr, "write header failed at order %d: %s\n", i, std::strerror(errno));
      ::close(fd);
      return 2;
    }
    if (!write_all(fd, &msg, sizeof(msg))) {
      std::fprintf(stderr, "write order failed at order %d: %s\n", i, std::strerror(errno));
      ::close(fd);
      return 3;
    }

    wire::Header rsp_hdr{};
    if (!read_exact(fd, &rsp_hdr, sizeof(rsp_hdr))) {
      std::fprintf(stderr, "read response header failed at order %d: %s\n", i, std::strerror(errno));
      ::close(fd);
      return 4;
    }

    if (rsp_hdr.type == static_cast<uint16_t>(wire::MsgType::Ack)) {
      wire::Ack ack{};
      if (rsp_hdr.length != sizeof(wire::Ack)) {
        std::fprintf(stderr, "unexpected ack length=%u at order %d\n",
            static_cast<unsigned>(rsp_hdr.length), i);
        ::close(fd);
        return 5;
      }
      if (!read_exact(fd, &ack, sizeof(ack))) {
        std::fprintf(stderr, "read ack failed at order %d: %s\n", i, std::strerror(errno));
        ::close(fd);
        return 6;
      }
      ++ack_count;
    } else if (rsp_hdr.type == static_cast<uint16_t>(wire::MsgType::Reject)) {
      wire::Reject rej{};
      if (rsp_hdr.length != sizeof(wire::Reject)) {
        std::fprintf(stderr, "unexpected reject length=%u at order %d\n",
            static_cast<unsigned>(rsp_hdr.length), i);
        ::close(fd);
        return 7;
      }
      if (!read_exact(fd, &rej, sizeof(rej))) {
        std::fprintf(stderr, "read reject failed at order %d: %s\n", i, std::strerror(errno));
        ::close(fd);
        return 8;
      }
      ++reject_count;
      std::fprintf(stderr, "reject at order=%d order_id=%llu reason=%u\n",
          i,
          static_cast<unsigned long long>(rej.order_id),
          static_cast<unsigned>(rej.reason));
      break;
    } else {
      std::fprintf(stderr, "unexpected response type=%u len=%u at order %d\n",
          static_cast<unsigned>(rsp_hdr.type),
          static_cast<unsigned>(rsp_hdr.length),
          i);
      ::close(fd);
      return 9;
    }

    const auto t1 = std::chrono::steady_clock::now();
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    latencies.push_back(static_cast<uint64_t>(ns));
  }

  const auto bench_end = std::chrono::steady_clock::now();
  const auto total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(bench_end - bench_start).count();

  if (latencies.empty()) {
    std::fprintf(stderr, "no successful responses recorded\n");
    ::close(fd);
    return 10;
  }

  std::printf("count=%zu\n", latencies.size());
  std::printf("acks=%zu\n", ack_count);
  std::printf("rejects=%zu\n", reject_count);
  std::printf("p50_ns=%.0f\n", percentile(latencies, 0.50));
  std::printf("p95_ns=%.0f\n", percentile(latencies, 0.95));
  std::printf("p99_ns=%.0f\n", percentile(latencies, 0.99));
  std::printf("max_ns=%.0f\n", percentile(latencies, 1.00));
  std::printf("p50_us=%.3f\n", percentile(latencies, 0.50) / 1000.0);
  std::printf("p95_us=%.3f\n", percentile(latencies, 0.95) / 1000.0);
  std::printf("p99_us=%.3f\n", percentile(latencies, 0.99) / 1000.0);
  std::printf("throughput_ops_s=%.0f\n",
      (static_cast<double>(latencies.size()) * 1'000'000'000.0) / static_cast<double>(total_ns));

  ::close(fd);
  return 0;
}

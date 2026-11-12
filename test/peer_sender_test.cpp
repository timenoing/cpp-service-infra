#include "mq/raft/peer_sender.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

// 最小接收端：只验证 4B 长度帧 + 解码后的 payload + 4B 零 ACK。
// 不用 Epoll，因为 Epoll::start 是阻塞且没有公开 stop。
class MiniReceiver {
 public:
  ~MiniReceiver() { stop(); }

  bool start() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return false;

    int one = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
      return false;
    }
    if (::listen(listen_fd_, 8) < 0) return false;

    socklen_t len = sizeof(addr);
    if (::getsockname(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), &len) < 0) {
      return false;
    }
    port_ = ntohs(addr.sin_port);

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100 * 1000;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    worker_ = std::thread(&MiniReceiver::run, this);
    return true;
  }

  void stop() {
    stopping_.store(true);
    if (listen_fd_ >= 0) ::shutdown(listen_fd_, SHUT_RDWR);
    if (worker_.joinable()) worker_.join();
    if (listen_fd_ >= 0) {
      ::close(listen_fd_);
      listen_fd_ = -1;
    }
  }

  uint16_t port() const { return port_; }

  std::vector<std::string> take() {
    std::lock_guard<std::mutex> lk(mtx_);
    std::vector<std::string> out;
    out.swap(got_);
    return out;
  }

 private:
  void run() {
    while (!stopping_.load()) {
      const int conn = ::accept(listen_fd_, nullptr, nullptr);
      if (conn < 0) continue;

      struct timeval tv;
      tv.tv_sec = 0;
      tv.tv_usec = 100 * 1000;
      ::setsockopt(conn, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

      serve(conn);
      ::close(conn);
    }
  }

  void serve(int conn) {
    while (!stopping_.load()) {
      unsigned char lenbuf[4];
      if (!read_exact(conn, lenbuf, 4)) return;

      const uint32_t len = (static_cast<uint32_t>(lenbuf[0]) << 24) |
                           (static_cast<uint32_t>(lenbuf[1]) << 16) |
                           (static_cast<uint32_t>(lenbuf[2]) << 8) |
                           static_cast<uint32_t>(lenbuf[3]);

      std::string payload(len, '\0');
      if (len > 0 &&
          !read_exact(conn, reinterpret_cast<unsigned char*>(&payload[0]), len)) {
        return;
      }

      {
        std::lock_guard<std::mutex> lk(mtx_);
        got_.push_back(std::move(payload));
      }

      const unsigned char ack[4] = {0, 0, 0, 0};
      if (::send(conn, ack, sizeof(ack), MSG_NOSIGNAL) !=
          static_cast<ssize_t>(sizeof(ack))) {
        return;
      }
    }
  }

  static bool read_exact(int fd, unsigned char* buf, std::size_t n) {
    std::size_t off = 0;
    while (off < n) {
      const ssize_t r = ::recv(fd, buf + off, n - off, 0);
      if (r > 0) {
        off += static_cast<std::size_t>(r);
        continue;
      }
      if (r < 0 && errno == EINTR) continue;
      return false;
    }
    return true;
  }

  int listen_fd_ = -1;
  uint16_t port_ = 0;
  std::atomic<bool> stopping_{false};
  std::thread worker_;
  std::mutex mtx_;
  std::vector<std::string> got_;
};

void test_bad_ip() {
  PeerSender sender(1, "not-an-ip", 1234);
  assert(!sender.start());
  assert(!sender.try_send(rcp{Requestvote{}}));
}

void test_double_start() {
  MiniReceiver receiver;
  assert(receiver.start());

  PeerSender sender(2, "127.0.0.1", receiver.port());
  assert(sender.start());
  assert(!sender.start());

  sender.stop();
  sender.stop();
}

void test_send_receive() {
  MiniReceiver receiver;
  assert(receiver.start());

  PeerSender sender(7, "127.0.0.1", receiver.port());
  assert(sender.start());

  for (int i = 1; i <= 5; ++i) {
    Requestvote v;
    v.term = static_cast<uint64_t>(i);
    v.last_Logindex = static_cast<uint64_t>(100 + i);
    v.last_Logterm = static_cast<uint64_t>(i);
    v.requseter_id = 7;
    assert(sender.try_send(rcp{v}));
  }

  std::vector<std::string> all;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (all.size() < 5 && std::chrono::steady_clock::now() < deadline) {
    auto chunk = receiver.take();
    all.insert(all.end(), chunk.begin(), chunk.end());
    if (all.size() < 5) std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  assert(all.size() == 5);
  for (std::size_t i = 0; i < all.size(); ++i) {
    rcp decoded;
    uint8_t opcode = 0;
    uint32_t from = 0;
    assert(raft_code(all[i], decoded, opcode, from));
    assert(from == 7);
    assert(opcode == 1);
    assert(std::holds_alternative<Requestvote>(decoded));
    assert(std::get<Requestvote>(decoded).term == i + 1);
  }

  sender.stop();
}

uint16_t pick_closed_port() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  assert(fd >= 0);

  struct sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;

  assert(::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);

  socklen_t len = sizeof(addr);
  assert(::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &len) == 0);
  const uint16_t port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

void test_queue_limit() {
  PeerSender sender(1, "127.0.0.1", pick_closed_port());
  assert(sender.start());

  int accepted = 0;
  int rejected = 0;
  long long max_reject_us = 0;

  for (int i = 0; i < 300; ++i) {
    Requestvote v;
    v.term = static_cast<uint64_t>(i);

    const auto t0 = std::chrono::steady_clock::now();
    const bool ok = sender.try_send(rcp{v});
    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - t0)
                        .count();

    if (ok) {
      ++accepted;
    } else {
      ++rejected;
      if (us > max_reject_us) max_reject_us = us;
    }
  }

  assert(accepted >= 128);
  assert(rejected >= 100);
  assert(max_reject_us < 5000);

  const auto t0 = std::chrono::steady_clock::now();
  sender.stop();
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - t0)
                      .count();
  assert(ms < 1500);
}

void test_monostate() {
  MiniReceiver receiver;
  assert(receiver.start());

  PeerSender sender(3, "127.0.0.1", receiver.port());
  assert(sender.start());
  assert(!sender.try_send(rcp{std::monostate{}}));
  sender.stop();
}

}  // namespace

int main() {
  test_bad_ip();
  test_double_start();
  test_send_receive();
  test_queue_limit();
  test_monostate();
  std::puts("peer_sender_test: all passed");
  return 0;
}

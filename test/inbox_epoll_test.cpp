#include "mq/raft/raft_handler.h"

#include "Epoll.h"
#include "Message.h"
#include "mq/raft/raft_inbox.h"
#include "mq/raft/raft_message.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

std::size_t ms_since(Clock::time_point t0) {
  return static_cast<std::size_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0)
          .count());
}

sockaddr_in loopback(uint16_t port) {
  sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);
  return addr;
}

uint16_t pick_free_port() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  assert(fd >= 0);

  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  sockaddr_in addr = loopback(0);
  assert(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);

  socklen_t len = sizeof(addr);
  assert(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0);

  const uint16_t port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

// ---------------- 消息构造 ----------------

std::string manual_payload(uint32_t from, uint8_t opcode,
                           const std::string& body) {
  std::string payload;
  payload.push_back(static_cast<char>((from >> 24) & 0xFF));
  payload.push_back(static_cast<char>((from >> 16) & 0xFF));
  payload.push_back(static_cast<char>((from >> 8) & 0xFF));
  payload.push_back(static_cast<char>(from & 0xFF));
  payload.push_back(static_cast<char>(opcode));
  payload += body;
  return payload;
}

rcp make_vote(uint32_t from, uint64_t term) {
  Requestvote v;
  v.term = term;
  v.last_Logindex = term * 10;
  v.last_Logterm = term;
  v.requseter_id = from;
  return rcp{v};
}

std::string make_payload(uint32_t from, const rcp& msg) {
  std::string payload;
  const bool ok = raft_encode(0, from, payload, msg);
  assert(ok);
  (void)ok;
  return payload;
}

void expect_no_message(Raft_inbox& inbox, uint32_t ms) {
  inboundmsg out;
  assert(inbox.wait_pop(out, ms) == Popstatic::Timeout);
}

// ---------------- 测试用服务端 ----------------
//
// 每个用例一个实例：
//   inbox 先声明 → 最后析构，保证 handler 捕获的引用始终有效。
//   stop() 用 SIGINT 唤醒 Epoll::start，start 返回后 join。
class Server {
 public:
  Server() = default;
  ~Server() { stop(); }

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  bool start() {
    port_ = pick_free_port();

    install_raft_handler(epoll_, inbox_);

    th_ = std::thread([this] {
      rc_.store(epoll_.start(port_));
      done_.store(true);
    });

    const auto deadline = Clock::now() + std::chrono::seconds(3);
    while (Clock::now() < deadline) {
      if (probe()) return true;
      if (done_.load() && !rc_.load()) return false;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
  }

  void stop() {
    if (!th_.joinable()) return;
    ::raise(SIGINT);
    th_.join();
  }

  uint16_t port() const { return port_; }
  Raft_inbox& inbox() { return inbox_; }

 private:
  bool probe() const {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    sockaddr_in addr = loopback(port_);
    const int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::close(fd);
    return rc == 0;
  }

  Raft_inbox inbox_;
  Epoll epoll_;
  std::thread th_;
  uint16_t port_ = 0;
  std::atomic<bool> rc_{false};
  std::atomic<bool> done_{false};
};

// ---------------- 测试用客户端 ----------------

class Client {
 public:
  ~Client() { close(); }

  bool connect_to(uint16_t port) {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) return false;

    sockaddr_in addr = loopback(port);
    if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      close();
      return false;
    }

    timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 0;
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    return true;
  }

  bool send_raw(const void* data, std::size_t n) {
    const char* p = static_cast<const char*>(data);
    std::size_t off = 0;
    while (off < n) {
      const ssize_t w = ::send(fd_, p + off, n - off, MSG_NOSIGNAL);
      if (w > 0) {
        off += static_cast<std::size_t>(w);
        continue;
      }
      if (w < 0 && errno == EINTR) continue;
      return false;
    }
    return true;
  }

  bool send_frame(const std::string& payload) {
    const std::string frame = net::encode(payload);
    return send_raw(frame.data(), frame.size());
  }

  bool expect_ack() {
    unsigned char ack[4];
    if (!recv_exact(ack, sizeof(ack))) return false;
    return ack[0] == 0 && ack[1] == 0 && ack[2] == 0 && ack[3] == 0;
  }

  void close() {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

 private:
  bool recv_exact(void* data, std::size_t n) {
    char* p = static_cast<char*>(data);
    std::size_t off = 0;
    while (off < n) {
      const ssize_t r = ::recv(fd_, p + off, n - off, 0);
      if (r > 0) {
        off += static_cast<std::size_t>(r);
        continue;
      }
      if (r < 0 && errno == EINTR) continue;
      return false;
    }
    return true;
  }

  int fd_ = -1;
};

// ---------------- C1：合法帧 ----------------

void test_valid_frame() {
  Server server;
  assert(server.start());

  Client client;
  assert(client.connect_to(server.port()));

  const std::string payload = make_payload(7, make_vote(7, 42));
  // Requestvote: to(4) + from(4) + opcode(1) + body(32)
  assert(payload.size() == 41);

  assert(client.send_frame(payload));
  assert(client.expect_ack());

  inboundmsg out;
  assert(server.inbox().wait_pop(out, 500) == Popstatic::Ok);
  assert(out.from == 7);
  assert(out.opcode == 1);
  assert(out.bytes == 41);
  assert(std::holds_alternative<Requestvote>(out.msg));
  assert(std::get<Requestvote>(out.msg).term == 42);

  expect_no_message(server.inbox(), 100);

  client.close();
  server.stop();
}

// ---------------- C2：垃圾帧不断连 ----------------

void test_garbage_frame_keeps_connection() {
  Server server;
  assert(server.start());

  Client client;
  assert(client.connect_to(server.port()));

  // 未知 opcode
  assert(client.send_frame(manual_payload(7, 0x99, "")));
  assert(client.expect_ack());

  // 已知 opcode，但 body 长度不对
  assert(client.send_frame(manual_payload(7, 1, "abcd")));
  assert(client.expect_ack());

  expect_no_message(server.inbox(), 100);

  // 同一连接还能正常收发
  const std::string good = make_payload(7, make_vote(7, 99));
  assert(client.send_frame(good));
  assert(client.expect_ack());

  inboundmsg out;
  assert(server.inbox().wait_pop(out, 500) == Popstatic::Ok);
  assert(out.from == 7);
  assert(out.bytes == good.size());
  assert(std::get<Requestvote>(out.msg).term == 99);

  client.close();
  server.stop();
}

// ---------------- C3：半包 ----------------

void test_half_packet() {
  Server server;
  assert(server.start());

  Client client;
  assert(client.connect_to(server.port()));

  const std::string payload = make_payload(7, make_vote(7, 55));
  const std::string frame = net::encode(payload);
  assert(frame.size() > 10);

  assert(client.send_raw(frame.data(), 2));
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  assert(client.send_raw(frame.data() + 2, 3));
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  assert(client.send_raw(frame.data() + 5, frame.size() - 5));

  // 只有完整帧到达后才会有 ACK
  assert(client.expect_ack());

  inboundmsg out;
  assert(server.inbox().wait_pop(out, 500) == Popstatic::Ok);
  assert(out.bytes == payload.size());
  assert(std::get<Requestvote>(out.msg).term == 55);

  expect_no_message(server.inbox(), 100);

  client.close();
  server.stop();
}

// ---------------- C4：粘包 ----------------

void test_sticky_packets() {
  Server server;
  assert(server.start());

  Client client;
  assert(client.connect_to(server.port()));

  const std::string first = make_payload(1, make_vote(1, 11));
  const std::string garbage = manual_payload(2, 0x99, "");
  const std::string third = make_payload(3, make_vote(3, 33));

  const std::string burst =
      net::encode(first) + net::encode(garbage) + net::encode(third);
  assert(client.send_raw(burst.data(), burst.size()));

  for (int i = 0; i < 3; ++i) assert(client.expect_ack());

  std::vector<uint64_t> terms;
  inboundmsg out;
  for (int i = 0; i < 2; ++i) {
    assert(server.inbox().wait_pop(out, 500) == Popstatic::Ok);
    terms.push_back(std::get<Requestvote>(out.msg).term);
  }

  // 4 个 worker 并发，不能假设顺序，只比较集合
  std::sort(terms.begin(), terms.end());
  assert(terms.size() == 2);
  assert(terms[0] == 11);
  assert(terms[1] == 33);

  expect_no_message(server.inbox(), 100);

  client.close();
  server.stop();
}

// ---------------- C5：洪水 + 队列上限 ----------------

void test_flood() {
  Server server;
  assert(server.start());

  Client client;
  assert(client.connect_to(server.port()));

  constexpr int kTotal = 200;
  std::string burst;
  burst.reserve(kTotal * 48);
  for (int i = 1; i <= kTotal; ++i) {
    burst += net::encode(make_payload(1, make_vote(1, static_cast<uint64_t>(i))));
  }

  assert(client.send_raw(burst.data(), burst.size()));

  // ACK 全部回来，说明 200 次 handler 都没有被队列满卡住
  const auto t0 = Clock::now();
  for (int i = 0; i < kTotal; ++i) assert(client.expect_ack());
  assert(ms_since(t0) < 5000);

  std::vector<uint64_t> terms;
  inboundmsg out;
  for (int i = 0; i < 128; ++i) {
    assert(server.inbox().wait_pop(out, 500) == Popstatic::Ok);
    terms.push_back(std::get<Requestvote>(out.msg).term);
  }
  expect_no_message(server.inbox(), 100);

  std::sort(terms.begin(), terms.end());
  assert(std::adjacent_find(terms.begin(), terms.end()) == terms.end());
  for (uint64_t t : terms) assert(t >= 1 && t <= kTotal);

  client.close();
  server.stop();
}

// ---------------- C6：inbox 关闭后仍回 ACK ----------------

void test_closed_inbox_still_acks() {
  Server server;
  assert(server.start());

  Client client;
  assert(client.connect_to(server.port()));

  server.inbox().close();

  for (int i = 0; i < 2; ++i) {
    assert(client.send_frame(
        make_payload(7, make_vote(7, static_cast<uint64_t>(i)))));
    assert(client.expect_ack());
  }

  inboundmsg out;
  assert(server.inbox().wait_pop(out, 0) == Popstatic::Close);

  client.close();
  server.stop();
}

// ---------------- C7：大帧 ----------------

void test_large_frame() {
  Server server;
  assert(server.start());

  Client client;
  assert(client.connect_to(server.port()));

  AppendEntries ae;
  ae.leader_term = 9;
  ae.leader_id = 4;
  ae.prterm = 3;
  ae.preLogindex = 40;
  ae.leader_commit = 30;
  for (int i = 0; i < 3; ++i) {
    ae.entries.push_back(Log{static_cast<uint64_t>(i + 1),
                             std::string(256u * 1024u,
                                         static_cast<char>('a' + i))});
  }

  const std::string payload = make_payload(4, rcp{ae});
  assert(payload.size() > 768u * 1024u);
  assert(payload.size() < net::messagemaxlen);

  assert(client.send_frame(payload));
  assert(client.expect_ack());

  inboundmsg out;
  assert(server.inbox().wait_pop(out, 3000) == Popstatic::Ok);
  assert(out.from == 4);
  assert(out.opcode == 3);
  assert(out.bytes == payload.size());

  const auto& got = std::get<AppendEntries>(out.msg);
  assert(got.leader_id == 4);
  assert(got.leader_commit == 30);
  assert(got.entries.size() == 3);
  for (int i = 0; i < 3; ++i) {
    assert(got.entries[i].term == static_cast<uint64_t>(i + 1));
    assert(got.entries[i].msg.size() == 256u * 1024u);
    assert(got.entries[i].msg == ae.entries[i].msg);
  }

  client.close();
  server.stop();
}

}  // namespace

int main() {
  test_valid_frame();
  test_garbage_frame_keeps_connection();
  test_half_packet();
  test_sticky_packets();
  test_flood();
  test_closed_inbox_still_acks();
  test_large_frame();

  std::puts("inbox_epoll_test: all passed");
  return 0;
}

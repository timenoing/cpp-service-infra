#include "Epoll.h"
#include "Message.h"
#include "mq/raft/peer_sender.h"
#include "mq/raft/raft_handler.h"
#include "mq/raft/raft_inbox.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cassert>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

constexpr int kNodeCount = 3;

struct PeerConfig {
  uint32_t id = 0;
  uint16_t port = 0;
};

struct NodeConfig {
  uint32_t id = 0;
  uint16_t port = 0;
  std::vector<PeerConfig> peers;
};

struct NodeStats {
  uint64_t recv[4] = {0, 0, 0, 0};  // 下标就是节点 id
  uint64_t sent = 0;
  uint64_t send_fail = 0;
  uint64_t parse_fail = 0;
};

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

void write_summary(const std::string& path, const NodeStats& s) {
  std::ofstream out(path);
  out << "recv_1 " << s.recv[1] << "\n";
  out << "recv_2 " << s.recv[2] << "\n";
  out << "recv_3 " << s.recv[3] << "\n";
  out << "sent " << s.sent << "\n";
  out << "send_fail " << s.send_fail << "\n";
  out << "parse_fail " << s.parse_fail << "\n";
}

bool read_summary(const std::string& path, NodeStats& s) {
  std::ifstream in(path);
  if (!in) return false;

  std::string key;
  uint64_t value = 0;
  while (in >> key >> value) {
    if (key == "recv_1") {
      s.recv[1] = value;
    } else if (key == "recv_2") {
      s.recv[2] = value;
    } else if (key == "recv_3") {
      s.recv[3] = value;
    } else if (key == "sent") {
      s.sent = value;
    } else if (key == "send_fail") {
      s.send_fail = value;
    } else if (key == "parse_fail") {
      s.parse_fail = value;
    }
  }
  return true;
}

// 一个节点的完整生命周期。
// 主线程跑 Epoll 收包，tick 线程跑"Raft 线程"的那部分：取消息、每 500ms 发一轮。
int run_node(const NodeConfig& cfg, int seconds, const std::string& summary_path) {
  Raft_inbox inbox;
  Epoll epoll;

  install_raft_handler(epoll, inbox);

  std::map<uint32_t, std::unique_ptr<PeerSender>> senders;
  for (const auto& peer : cfg.peers) {
    auto sender = std::make_unique<PeerSender>(cfg.id, "127.0.0.1", peer.port);
    if (!sender->start()) return 2;
    senders.emplace(peer.id, std::move(sender));
  }

  NodeStats stats;

  std::thread tick([&] {
    const auto interval = std::chrono::milliseconds(500);
    const auto stop_at = Clock::now() + std::chrono::seconds(seconds);
    auto next_send = Clock::now();

    uint64_t term = 0;

    for (;;) {
      inboundmsg msg;
      const Popstatic result = inbox.wait_pop(msg, 100);

      if (result == Popstatic::Close) break;

      if (result == Popstatic::Ok) {
        if (!std::holds_alternative<Requestvote>(msg.msg)) {
          ++stats.parse_fail;
        } else if (msg.from >= 1 && msg.from <= kNodeCount &&
                   msg.from != cfg.id) {
          ++stats.recv[msg.from];
        }
      }

      const auto now = Clock::now();

      if (now >= next_send) {
        ++term;

        Requestvote vote;
        vote.term = term;
        vote.requseter_id = cfg.id;

        for (auto& item : senders) {
          if (item.second->try_send(item.first, rcp{vote})) {
            ++stats.sent;
          } else {
            ++stats.send_fail;
          }
        }

        next_send += interval;
        if (next_send < now) next_send = now + interval;
      }

      if (now >= stop_at) {
        // 让主线程的 epoll.start 返回
        ::raise(SIGINT);
        break;
      }
    }
  });

  const bool ok = epoll.start(cfg.port);

  inbox.close();
  tick.join();
  for (auto& item : senders) item.second->stop();

  write_summary(summary_path, stats);
  return ok ? 0 : 3;
}

}  // namespace

int main(int argc, char** argv) {
  const int seconds = argc > 1 ? std::atoi(argv[1]) : 30;
  assert(seconds >= 2);

  std::array<uint16_t, kNodeCount> ports{};
  ports[0] = pick_free_port();
  do {
    ports[1] = pick_free_port();
  } while (ports[1] == ports[0]);
  do {
    ports[2] = pick_free_port();
  } while (ports[2] == ports[0] || ports[2] == ports[1]);

  const std::string dir = "/tmp/raft_ping_" + std::to_string(::getpid());
  assert(::mkdir(dir.c_str(), 0755) == 0);

  std::array<pid_t, kNodeCount> kids{};

  for (int i = 0; i < kNodeCount; ++i) {
    const pid_t pid = ::fork();
    assert(pid >= 0);

    if (pid == 0) {
      NodeConfig cfg;
      cfg.id = static_cast<uint32_t>(i + 1);
      cfg.port = ports[i];
      for (int j = 0; j < kNodeCount; ++j) {
        if (j == i) continue;
        cfg.peers.push_back(PeerConfig{static_cast<uint32_t>(j + 1), ports[j]});
      }

      const std::string path =
          dir + "/node" + std::to_string(cfg.id) + ".txt";
      const int rc = run_node(cfg, seconds, path);
      return rc;
    }

    kids[i] = pid;
  }

  int failures = 0;

  for (int i = 0; i < kNodeCount; ++i) {
    int status = 0;
    ::waitpid(kids[i], &status, 0);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      std::printf("node %d exited abnormally (status=%d)\n", i + 1, status);
      ++failures;
    }
  }

  std::array<NodeStats, kNodeCount> stats{};
  for (int i = 0; i < kNodeCount; ++i) {
    const std::string path =
        dir + "/node" + std::to_string(i + 1) + ".txt";
    if (!read_summary(path, stats[i])) {
      std::printf("node %d: summary missing\n", i + 1);
      ++failures;
    }
  }

  // 任意两个节点之间，双向都必须有流量
  for (int i = 0; i < kNodeCount; ++i) {
    for (int j = 0; j < kNodeCount; ++j) {
      if (i == j) continue;
      const uint64_t got = stats[i].recv[j + 1];
      std::printf("node %d <- node %d : %llu\n", i + 1, j + 1,
                  static_cast<unsigned long long>(got));
      if (got == 0) ++failures;
    }
  }

  for (int i = 0; i < kNodeCount; ++i) {
    std::printf("node %d: sent=%llu send_fail=%llu parse_fail=%llu\n", i + 1,
                static_cast<unsigned long long>(stats[i].sent),
                static_cast<unsigned long long>(stats[i].send_fail),
                static_cast<unsigned long long>(stats[i].parse_fail));
    if (stats[i].sent == 0) ++failures;
    if (stats[i].parse_fail != 0) ++failures;
  }

  if (failures != 0) {
    std::printf("ping_three_process_test: %d failure(s), logs kept in %s\n",
                failures, dir.c_str());
    return 1;
  }

  for (int i = 0; i < kNodeCount; ++i) {
    const std::string path =
        dir + "/node" + std::to_string(i + 1) + ".txt";
    ::unlink(path.c_str());
  }
  ::rmdir(dir.c_str());

  std::puts("ping_three_process_test: all passed");
  return 0;
}

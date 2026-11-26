#ifndef MQ_RAFT_PEER_SENDER_H
#define MQ_RAFT_PEER_SENDER_H
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include "mq/raft/raft_message.h"
class PeerSender {
 public:
  PeerSender(uint32_t self_id, std::string peer_ip, uint16_t peer_port);
  ~PeerSender();
  PeerSender(const PeerSender&) = delete;
  PeerSender& operator=(const PeerSender&) = delete;
  bool start();
  bool try_send(uint32_t to,const rcp& msg);
  void stop();
  void send_raw(const std::string);
 private:
  void workloop();
  bool connect_peer();
  void close_fd();
  bool send_all(const std::string& frame);
  bool read_ack();
  bool wait_backoff(std::size_t ms);
  bool stopping_requested();
  static constexpr std::size_t kMaxTasks = 128;
  static constexpr std::size_t kMaxBytes = 16u * 1024u * 1024u;
  static constexpr std::size_t kBackoffStartMs = 100;
  static constexpr std::size_t kBackoffMaxMs = 500;
  static constexpr int kIOTimeoutMs = 500;
  static constexpr int kConnectSliceMs = 50;
  uint32_t self_id_;
  std::string peer_ip_;
  uint16_t peer_port_;
  unsigned char peer_addr_[4] = {0, 0, 0, 0};
  std::mutex mtx_;
  std::condition_variable cv_;
  std::deque<std::string> queue_;
  std::size_t queued_bytes_ = 0;
  bool stopping_ = false;
  bool started_ = false;
  std::mutex fd_mtx_;
  int fd_ = -1;
  std::thread worker_;
};

#endif  

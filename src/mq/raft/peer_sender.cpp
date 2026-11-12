#include "mq/raft/peer_sender.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <utility>

namespace {

bool read_be32(const unsigned char* p, uint32_t& v) {
  v = (static_cast<uint32_t>(p[0]) << 24) |
      (static_cast<uint32_t>(p[1]) << 16) |
      (static_cast<uint32_t>(p[2]) << 8) |
      static_cast<uint32_t>(p[3]);
  return true;
}
}  // namespace
PeerSender::PeerSender(uint32_t self_id, std::string peer_ip, uint16_t peer_port)
    : self_id_(self_id), peer_ip_(std::move(peer_ip)), peer_port_(peer_port) {}
PeerSender::~PeerSender() { stop(); }

bool PeerSender::start() {
  if (inet_pton(AF_INET, peer_ip_.c_str(), peer_addr_) != 1) return false;
  std::lock_guard<std::mutex> lk(mtx_);
  if (started_ || stopping_) return false;
  try {
    worker_ = std::thread(&PeerSender::workloop, this);
  } catch (...) {
    return false;
  }
  started_ = true;
  return true;
}

bool PeerSender::try_send(const rcp& msg) {
  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (!started_ || stopping_) return false;
  }
  std::string payload;
  if (!raft_encode(self_id_, payload, msg)) return false;
  std::string frame = net::encode(payload);
  bool was_empty = false;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (stopping_) return false;
    if (queue_.size() >= kMaxTasks) return false;
    if (queued_bytes_ + frame.size() > kMaxBytes) return false;

    was_empty = queue_.empty();
    queued_bytes_ += frame.size();
    queue_.push_back(std::move(frame));
  }
  if (was_empty) cv_.notify_one();
  return true;
}
void PeerSender::stop() {
  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (stopping_) return;
    stopping_ = true;
  }
  cv_.notify_all();
  {
    std::lock_guard<std::mutex> lk(fd_mtx_);
    if (fd_ >= 0) ::shutdown(fd_, SHUT_RDWR);
  }
  if (worker_.joinable()) worker_.join();
  close_fd();
  std::lock_guard<std::mutex> lk(mtx_);
  queue_.clear();
  queued_bytes_ = 0;
}

bool PeerSender::stopping_requested() {
  std::lock_guard<std::mutex> lk(mtx_);
  return stopping_;
}

void PeerSender::workloop() {
  std::size_t backoff_ms = kBackoffStartMs;

  while(1) {
    std::string frame;
    {
      std::unique_lock<std::mutex> lk(mtx_);
      cv_.wait(lk, [this] { return stopping_ || !queue_.empty(); });
      if (stopping_) return;
      frame = std::move(queue_.front());
      queue_.pop_front();
      queued_bytes_ -= frame.size();
    }

    if (fd_ < 0 && !connect_peer()) {
      if (!wait_backoff(backoff_ms)) return;
      backoff_ms = std::min(backoff_ms * 2, kBackoffMaxMs);
      continue;
    }

    if (!send_all(frame) || !read_ack()) {
      close_fd();
      if (!wait_backoff(backoff_ms)) return;
      backoff_ms = std::min(backoff_ms * 2, kBackoffMaxMs);
      continue;
    }

    backoff_ms = kBackoffStartMs;
  }
}

bool PeerSender::connect_peer() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return false;

  int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    ::close(fd);
    return false;
  }

  struct sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(peer_port_);
  std::memcpy(&addr.sin_addr, peer_addr_, sizeof(peer_addr_));

  int rc = ::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
  if (rc < 0 && errno != EINPROGRESS && errno != EINTR) {
    ::close(fd);
    return false;
  }

  if (rc < 0) {
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLOUT;
    pfd.revents = 0;

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(kIOTimeoutMs);

    for (;;) {
      if (stopping_requested()) {
        ::close(fd);
        return false;
      }

      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        ::close(fd);
        return false;
      }

      const auto left_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               deadline - now)
                               .count();
      int timeout = static_cast<int>(left_ms);
      if (timeout <= 0) timeout = 1;
      if (timeout > kConnectSliceMs) timeout = kConnectSliceMs;

      const int n = ::poll(&pfd, 1, timeout);
      if (n > 0) break;
      if (n == 0) continue;
      if (errno == EINTR) continue;

      ::close(fd);
      return false;
    }

    int so_error = 0;
    socklen_t len = sizeof(so_error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len) < 0 || so_error != 0) {
      ::close(fd);
      return false;
    }
  }
  flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) < 0) {
    ::close(fd);
    return false;
  }
  struct timeval tv;
  tv.tv_sec = kIOTimeoutMs / 1000;
  tv.tv_usec = (kIOTimeoutMs % 1000) * 1000;
  if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0 ||
      ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
    ::close(fd);
    return false;
  }
  int one = 1;
  if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) {
    ::close(fd);
    return false;
  }
  {
    std::lock_guard<std::mutex> lk(fd_mtx_);
    fd_ = fd;
  }

  if (stopping_requested()) {
    close_fd();
    return false;
  }
  return true;
}

void PeerSender::close_fd() {
  std::lock_guard<std::mutex> lk(fd_mtx_);
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

bool PeerSender::send_all(const std::string& frame) {
  std::size_t off = 0;
  while (off < frame.size()) {
    const ssize_t n =
        ::send(fd_, frame.data() + off, frame.size() - off, MSG_NOSIGNAL);
    if (n > 0) {
      off += static_cast<std::size_t>(n);
      continue;
    }
    if (n < 0 && errno == EINTR) continue;
    return false;
  }
  return true;
}

bool PeerSender::read_ack() {
  unsigned char buf[4];
  std::size_t got = 0;
  while (got < 4) {
    const ssize_t n = ::recv(fd_, buf + got, 4 - got, 0);
    if (n > 0) {
      got += static_cast<std::size_t>(n);
      continue;
    }
    if (n == 0) return false;
    if (errno == EINTR) continue;
    return false;
  }

  uint32_t ack = 0;
  read_be32(buf, ack);
  return ack == 0;
}

bool PeerSender::wait_backoff(std::size_t ms) {
  std::unique_lock<std::mutex> lk(mtx_);
  return !cv_.wait_for(lk, std::chrono::milliseconds(ms),
                       [this] { return stopping_; });
}

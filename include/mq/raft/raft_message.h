#ifndef RAFTMESSAGE_H
#define RAFTMESSAGE_H
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <deque>
#include <chrono>
#include "Message.h"
#include <condition_variable>
namespace raft_detail {

inline void put_u32(std::string& out, uint32_t v) {
  out.push_back(static_cast<char>((v >> 24) & 0xFF));
  out.push_back(static_cast<char>((v >> 16) & 0xFF));
  out.push_back(static_cast<char>((v >> 8) & 0xFF));
  out.push_back(static_cast<char>(v & 0xFF));
}

inline void put_u64(std::string& out, uint64_t v) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out.push_back(static_cast<char>((v >> shift) & 0xFF));
  }
}

inline bool get_u32(std::string_view in, std::size_t& pos, uint32_t& v) {
  if (pos + 4 > in.size()) return false;
  v = (static_cast<uint32_t>(static_cast<unsigned char>(in[pos])) << 24) |
      (static_cast<uint32_t>(static_cast<unsigned char>(in[pos + 1])) << 16) |
      (static_cast<uint32_t>(static_cast<unsigned char>(in[pos + 2])) << 8) |
      static_cast<uint32_t>(static_cast<unsigned char>(in[pos + 3]));
  pos += 4;
  return true;
}

inline bool get_u64(std::string_view in, std::size_t& pos, uint64_t& v) {
  if (pos + 8 > in.size()) return false;
  v = 0;
  for (int i = 0; i < 8; ++i) {
    v = (v << 8) | static_cast<uint64_t>(static_cast<unsigned char>(in[pos + i]));
  }
  pos += 8;
  return true;
}

constexpr uint32_t kMaxEntries = 1024 * 1024 / 4;

}  // namespace raft_detail
enum class function_name {
  nofunction = 0,
  Requestvote = 1,
  Requestvotereply = 2,
  AppendEntries = 3,
  AppendEntriesreply = 4,
  StatusQuery = 5 ,
  Propose =6
};
struct Log {
  uint64_t term = 0;   // 该条日志产生时的 leader term
  std::string msg;
};
struct Requestvote {
  uint64_t term = 0;
  uint64_t last_Logindex = 0;
  uint64_t last_Logterm = 0;
  uint64_t requseter_id = 0;

  static bool Parse(std::string_view body, Requestvote& out) {
    if (body.size() != 32) return false;

    std::size_t pos = 0;
    Requestvote tmp;
    if (!raft_detail::get_u64(body, pos, tmp.term)) return false;
    if (!raft_detail::get_u64(body, pos, tmp.last_Logindex)) return false;
    if (!raft_detail::get_u64(body, pos, tmp.last_Logterm)) return false;
    if (!raft_detail::get_u64(body, pos, tmp.requseter_id)) return false;
    if (pos != body.size()) return false;

    out = tmp;
    return true;
  }

  std::string Encode() const {
    std::string out;
    out.reserve(32);
    raft_detail::put_u64(out, term);
    raft_detail::put_u64(out, last_Logindex);
    raft_detail::put_u64(out, last_Logterm);
    raft_detail::put_u64(out, requseter_id);
    return out;
  }
};

struct Requestvotereply {
  uint64_t term = 0;
  bool vote = false;

  static bool Parse(std::string_view body, Requestvotereply& out) {
    if (body.size() != 9) return false;

    std::size_t pos = 0;
    Requestvotereply tmp;
    if (!raft_detail::get_u64(body, pos, tmp.term)) return false;

    const auto raw_vote = static_cast<unsigned char>(body[pos]);
    if (raw_vote > 1) return false;
    tmp.vote = (raw_vote != 0);
    ++pos;

    if (pos != body.size()) return false;

    out = tmp;
    return true;
  }

  std::string Encode() const {
    std::string out;
    out.reserve(9);
    raft_detail::put_u64(out, term);
    out.push_back(static_cast<char>(vote ? 1 : 0));
    return out;
  }
};

struct AppendEntries {
  uint64_t leader_term = 0;
  uint32_t leader_id = 0;
  uint64_t prterm = 0;
  uint64_t preLogindex = 0;
  uint64_t leader_commit = 0;
  std::vector<Log> entries{};

  // Wire order: leader_term(u64), leader_id(u32), prterm(u64),
  //             preLogindex(u64), leader_commit(u64),
  //             entries_count(u32), then each entry as
  //             term(u64) + len(u32) + bytes.
  static bool Parse(std::string_view body, AppendEntries& out) {
    if (body.size() < 40) return false;

    std::size_t pos = 0;
    AppendEntries tmp;
    if (!raft_detail::get_u64(body, pos, tmp.leader_term)) return false;
    if (!raft_detail::get_u32(body, pos, tmp.leader_id)) return false;
    if (!raft_detail::get_u64(body, pos, tmp.prterm)) return false;
    if (!raft_detail::get_u64(body, pos, tmp.preLogindex)) return false;
    if (!raft_detail::get_u64(body, pos, tmp.leader_commit)) return false;

    uint32_t count = 0;
    if (!raft_detail::get_u32(body, pos, count)) return false;
    if (count > raft_detail::kMaxEntries) return false;
    if (count > (body.size() - pos) / 12) return false;

    tmp.entries.clear();
    tmp.entries.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
      Log entry;
      if (!raft_detail::get_u64(body, pos, entry.term)) return false;

      uint32_t len = 0;
      if (!raft_detail::get_u32(body, pos, len)) return false;
      if (len > body.size() - pos) return false;

      entry.msg.assign(body.substr(pos, len));
      pos += len;
      tmp.entries.push_back(std::move(entry));
    }

    if (pos != body.size()) return false;

    out = std::move(tmp);
    return true;
  }

  std::string Encode() const {
    if (entries.size() > raft_detail::kMaxEntries) return {};

    std::size_t total = 40;
    for (const auto& entry : entries) {
      total += 8 + 4 + entry.msg.size();
    }

    std::string out;
    out.reserve(total);
    raft_detail::put_u64(out, leader_term);
    raft_detail::put_u32(out, leader_id);
    raft_detail::put_u64(out, prterm);
    raft_detail::put_u64(out, preLogindex);
    raft_detail::put_u64(out, leader_commit);
    raft_detail::put_u32(out, static_cast<uint32_t>(entries.size()));
    for (const auto& entry : entries) {
      raft_detail::put_u64(out, entry.term);
      raft_detail::put_u32(out, static_cast<uint32_t>(entry.msg.size()));
      out.append(entry.msg);
    }
    return out;
  }
};

struct AppendEntriesreply {
  uint64_t term = 0;
  uint64_t xindex = 0;
  uint64_t xterm = 0;
  uint64_t follower_len = 0;  // follower 最后一条日志的 index，空日志为 0
  bool success = false;

  // Wire order: term(u64), success(u8), xindex(u64), xterm(u64),
  //             follower_len(u64).
  static bool Parse(std::string_view body, AppendEntriesreply& out) {
    if (body.size() != 33) return false;

    std::size_t pos = 0;
    AppendEntriesreply tmp;
    if (!raft_detail::get_u64(body, pos, tmp.term)) return false;

    const auto raw_success = static_cast<unsigned char>(body[pos]);
    if (raw_success > 1) return false;
    tmp.success = (raw_success != 0);
    ++pos;

    if (!raft_detail::get_u64(body, pos, tmp.xindex)) return false;
    if (!raft_detail::get_u64(body, pos, tmp.xterm)) return false;
    if (!raft_detail::get_u64(body, pos, tmp.follower_len)) return false;
    if (pos != body.size()) return false;
    out = tmp;
    return true;
  }
  std::string Encode() const {
    std::string out;
    out.reserve(33);
    raft_detail::put_u64(out, term);
    out.push_back(static_cast<char>(success ? 1 : 0));
    raft_detail::put_u64(out, xindex);
    raft_detail::put_u64(out, xterm);
    raft_detail::put_u64(out, follower_len);
    return out;
  }
};
struct Status_query{

};
struct  Proposee{
      std::string data;
      static bool Parse(std::string_view body, Proposee& out) {
       out.data.assign(body.data(),body.size());
       return true;
      }
      std::string Encode() const {
        return data;
      }
};
using rcp = std::variant<std::monostate, Requestvote, Requestvotereply,
                         AppendEntries, AppendEntriesreply,Status_query,Proposee>;

inline rcp pocket(uint8_t function, const std::string& bytes) {
  if (bytes.size() < 5) return std::monostate{};

  const std::string_view body(bytes.data() + 9, bytes.size() - 9);
  switch (function) {
    case static_cast<uint8_t>(function_name::Requestvote): {
      Requestvote msg;
      if (!Requestvote::Parse(body, msg)) return std::monostate{};
      return msg;
    }
    case static_cast<uint8_t>(function_name::Requestvotereply): {
      Requestvotereply msg;
      if (!Requestvotereply::Parse(body, msg)) return std::monostate{};
      return msg;
    }
    case static_cast<uint8_t>(function_name::AppendEntries): {
      AppendEntries msg;
      if (!AppendEntries::Parse(body, msg)) return std::monostate{};
      return msg;
    }
    case static_cast<uint8_t>(function_name::AppendEntriesreply): {
      AppendEntriesreply msg;
      if (!AppendEntriesreply::Parse(body, msg)) return std::monostate{};
      return msg;
    }
    case static_cast<uint8_t>(function_name::StatusQuery):
      return Status_query{};
    case static_cast<uint8_t>(function_name::Propose): {
      Proposee msg;
      if (!Proposee::Parse(body, msg)) return std::monostate{};
      return msg;
      }
    default:
      return std::monostate{};
  }
}

inline std::string encodpocket(const rcp& m) {
  std::string payload;
  std::visit(
      [&](const auto& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, Requestvote>) {
          payload.push_back(static_cast<char>(function_name::Requestvote));
          payload += arg.Encode();
        } else if constexpr (std::is_same_v<T, Requestvotereply>) {
          payload.push_back(static_cast<char>(function_name::Requestvotereply));
          payload += arg.Encode();
        } else if constexpr (std::is_same_v<T, AppendEntries>) {
          payload.push_back(static_cast<char>(function_name::AppendEntries));
          payload += arg.Encode();
        } else if constexpr (std::is_same_v<T, AppendEntriesreply>) {
          payload.push_back(static_cast<char>(function_name::AppendEntriesreply));
          payload += arg.Encode();
        } else if constexpr (std::is_same_v<T, Status_query>) {
          payload.push_back(static_cast<char>(function_name::StatusQuery));
        } else if constexpr (std::is_same_v<T, Proposee>) {
          payload.push_back(static_cast<char>(function_name::Propose));
          payload += arg.Encode();
        }
      },
      m);
  return payload;
}
inline bool raft_encode(uint32_t to,uint32_t from, std::string& msg, const rcp& m) {
  msg.clear();
  if (std::holds_alternative<std::monostate>(m)) return false;
  const std::string payload = encodpocket(m);
  if (payload.empty()) return false;
  if (8 + payload.size() > net::messagemaxlen) return false;
  msg.reserve(8 + payload.size());
  raft_detail::put_u32(msg, to);
  raft_detail::put_u32(msg, from);
  msg += payload;
  return true;
}


inline bool raft_code(const std::string& msg,uint32_t& to, rcp& m, uint8_t& name,
                      uint32_t& from) {
  if (msg.size() < 9) return false;
  std::size_t pos = 0;
  if (!raft_detail::get_u32(msg, pos, to)) return false;
  if (!raft_detail::get_u32(msg, pos, from)) return false;
  name = static_cast<uint8_t>(msg[pos]);
  m = pocket(name, msg);
  return !std::holds_alternative<std::monostate>(m);
}

inline bool raft_code(const std::string& msg, rcp& m, uint8_t& name, uint32_t& from) {
  uint32_t to = 0;   
  return raft_code(msg, to, m, name, from);
}
#endif

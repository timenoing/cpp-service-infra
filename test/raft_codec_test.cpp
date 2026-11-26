#include "mq/raft/raft_message.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace {

void test_requestvote_roundtrip() {
  Requestvote v;
  v.term = 0x0102030405060708ull;
  v.last_Logindex = 100;
  v.last_Logterm = 7;
  v.requseter_id = 3;

  const std::string body = v.Encode();
  assert(body.size() == 32);

  Requestvote out;
  assert(Requestvote::Parse(body, out));
  assert(out.term == v.term);
  assert(out.last_Logindex == v.last_Logindex);
  assert(out.last_Logterm == v.last_Logterm);
  assert(out.requseter_id == v.requseter_id);
}

// 心跳：entries 为空，固定头正好 40 字节。
void test_append_entries_heartbeat() {
  AppendEntries a;
  a.leader_term = 7;
  a.leader_id = 42;
  a.prterm = 3;
  a.preLogindex = 100;
  a.leader_commit = 90;

  const std::string body = a.Encode();
  assert(body.size() == 40);

  AppendEntries b;
  assert(AppendEntries::Parse(body, b));
  assert(b.leader_term == 7);
  assert(b.leader_id == 42);
  assert(b.prterm == 3);
  assert(b.preLogindex == 100);
  assert(b.leader_commit == 90);
  assert(b.entries.empty());
}

// 边界值 + 二进制 entry。
void test_append_entries_entries() {
  AppendEntries a;
  a.leader_term = 1;
  a.leader_id = 0xFFFFFFFFu;
  a.prterm = 2;
  a.preLogindex = 3;
  a.leader_commit = 0xFFFFFFFFFFFFFFFFull;
  a.entries.push_back(Log{5, std::string("a\0b", 3)});
  a.entries.push_back(Log{6, std::string(4096, 'x')});
  a.entries.push_back(Log{7, std::string()});

  const std::string body = a.Encode();
  assert(body.size() == 40 + (8 + 4 + 3) + (8 + 4 + 4096) + (8 + 4 + 0));

  AppendEntries b;
  assert(AppendEntries::Parse(body, b));
  assert(b.leader_id == 0xFFFFFFFFu);
  assert(b.leader_commit == 0xFFFFFFFFFFFFFFFFull);
  assert(b.entries.size() == 3);
  assert(b.entries[0].term == 5);
  assert(b.entries[0].msg == std::string("a\0b", 3));
  assert(b.entries[1].term == 6);
  assert(b.entries[1].msg == std::string(4096, 'x'));
  assert(b.entries[2].term == 7);
  assert(b.entries[2].msg.empty());
}

void test_append_entries_truncation() {
  AppendEntries a;
  a.leader_id = 5;
  a.leader_commit = 6;
  a.entries.push_back(Log{7, "hello"});

  const std::string body = a.Encode();
  for (std::size_t n = 0; n < body.size(); ++n) {
    AppendEntries b;
    assert(!AppendEntries::Parse(std::string_view(body.data(), n), b));
  }

  AppendEntries b;
  assert(AppendEntries::Parse(body, b));

  // 多一个字节也必须拒绝
  std::string trailing = body;
  trailing.push_back('x');
  assert(!AppendEntries::Parse(trailing, b));
}

void test_append_entries_bad_count() {
  AppendEntries a;
  const std::string heartbeat = a.Encode();
  assert(heartbeat.size() == 40);

  // count 声称 1 条，但后面没有 entry
  std::string manual = heartbeat;
  manual[36] = 0;
  manual[37] = 0;
  manual[38] = 0;
  manual[39] = 1;

  AppendEntries b;
  assert(!AppendEntries::Parse(manual, b));

  // count = 1，entry 的 term 是 0、长度声称 1MiB，但实际只有 2 字节
  std::string oversized = manual;
  oversized.append(8, '\0');  // entry.term = 0
  const uint32_t big = 1024 * 1024;
  oversized.push_back(static_cast<char>((big >> 24) & 0xFF));
  oversized.push_back(static_cast<char>((big >> 16) & 0xFF));
  oversized.push_back(static_cast<char>((big >> 8) & 0xFF));
  oversized.push_back(static_cast<char>(big & 0xFF));
  oversized.append("xy");
  assert(!AppendEntries::Parse(oversized, b));
}

void test_append_entries_reply() {
  AppendEntriesreply r;
  r.term = 9;
  r.success = true;
  r.xindex = 5;
  r.xterm = 4;
  r.follower_len = 123;

  const std::string body = r.Encode();
  assert(body.size() == 33);

  AppendEntriesreply out;
  assert(AppendEntriesreply::Parse(body, out));
  assert(out.term == 9);
  assert(out.success);
  assert(out.xindex == 5);
  assert(out.xterm == 4);
  assert(out.follower_len == 123);

  // success 只允许 0/1
  std::string bad = body;
  bad[8] = 2;
  assert(!AppendEntriesreply::Parse(bad, out));

  // 截断
  for (std::size_t n = 0; n < body.size(); ++n) {
    AppendEntriesreply tmp;
    assert(!AppendEntriesreply::Parse(std::string_view(body.data(), n), tmp));
  }

  // 多一个字节
  std::string trailing = body;
  trailing.push_back('x');
  assert(!AppendEntriesreply::Parse(trailing, out));

  // success = false 往返 + follower_len 边界值
  AppendEntriesreply f;
  f.term = 1;
  f.success = false;
  f.xindex = 10;
  f.xterm = 20;
  f.follower_len = 0xFFFFFFFFFFFFFFFFull;
  const std::string fbody = f.Encode();
  AppendEntriesreply fout;
  assert(AppendEntriesreply::Parse(fbody, fout));
  assert(!fout.success);
  assert(fout.xindex == 10);
  assert(fout.xterm == 20);
  assert(fout.follower_len == 0xFFFFFFFFFFFFFFFFull);
}

// raft_encode / raft_code 全类型往返
void test_raft_envelope() {
  std::vector<rcp> messages;

  Requestvote vote;
  vote.term = 11;
  vote.requseter_id = 1;
  messages.push_back(vote);

  Requestvotereply vreply;
  vreply.term = 12;
  vreply.vote = true;
  messages.push_back(vreply);

  AppendEntries ae;
  ae.leader_term = 13;
  ae.leader_id = 2;
  ae.prterm = 4;
  ae.preLogindex = 50;
  ae.leader_commit = 45;
  ae.entries.push_back(Log{13, "entry"});
  messages.push_back(ae);

  AppendEntriesreply areply;
  areply.term = 13;
  areply.success = true;
  areply.xindex = 51;
  areply.xterm = 13;
  areply.follower_len = 52;
  messages.push_back(areply);

  for (const auto& msg : messages) {
    std::string payload;
    assert(raft_encode(0, 3, payload, msg));

    rcp decoded;
    uint8_t opcode = 0;
    uint32_t from = 0;
    assert(raft_code(payload, decoded, opcode, from));
    assert(from == 3);
    assert(decoded.index() == msg.index());

    if (std::holds_alternative<AppendEntries>(decoded)) {
      const auto& got = std::get<AppendEntries>(decoded);
      assert(got.leader_id == 2);
      assert(got.leader_commit == 45);
      assert(got.entries.size() == 1);
      assert(got.entries[0].term == 13);
      assert(got.entries[0].msg == "entry");
    } else if (std::holds_alternative<AppendEntriesreply>(decoded)) {
      const auto& got = std::get<AppendEntriesreply>(decoded);
      assert(got.success);
      assert(got.xindex == 51);
      assert(got.follower_len == 52);
    }
  }
}

// 截断的 payload 一律拒绝
void test_raft_code_truncation() {
  AppendEntries ae;
  ae.leader_id = 1;
  ae.entries.push_back(Log{9, "data"});

  std::string payload;
  assert(raft_encode(0, 9, payload, rcp{ae}));

  for (std::size_t n = 0; n < 5; ++n) {
    rcp decoded;
    uint8_t opcode = 0;
    uint32_t from = 0;
    assert(!raft_code(payload.substr(0, n), decoded, opcode, from));
  }
}

}  // namespace

int main() {
  test_requestvote_roundtrip();
  test_append_entries_heartbeat();
  test_append_entries_entries();
  test_append_entries_truncation();
  test_append_entries_bad_count();
  test_append_entries_reply();
  test_raft_envelope();
  test_raft_code_truncation();

  std::puts("raft_codec_test: all passed");
  return 0;
}

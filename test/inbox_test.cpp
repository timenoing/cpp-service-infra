#include "mq/raft/raft_inbox.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
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

std::size_t us_since(Clock::time_point t0) {
  return static_cast<std::size_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0)
          .count());
}

// bytes 只是队列计费的字段，不要求和 msg 的真实大小一致。
inboundmsg make_msg(uint32_t from, uint8_t opcode, std::size_t bytes,
                    uint64_t term) {
  inboundmsg m;
  m.from = from;
  m.opcode = opcode;
  m.bytes = bytes;

  Requestvote v;
  v.term = term;
  v.last_Logindex = term * 10;
  v.last_Logterm = 7;
  v.requseter_id = from;
  m.msg = rcp{v};
  return m;
}

// 1. 推一条必须立刻取到，取完之后第二次必须 Timeout。
//    这条同时抓“漏 pop_front”和“每次都要睡满超时”。
void test_single_push_pop() {
  Raft_inbox inbox;

  inboundmsg sent = make_msg(5, 1, 64, 42);
  assert(inbox.try_push(std::move(sent)));

  inboundmsg out;
  const auto t0 = Clock::now();
  const auto r = inbox.wait_pop(out, 2000);
  const auto elapsed = ms_since(t0);

  assert(r == Popstatic::Ok);
  assert(elapsed < 100);
  assert(out.from == 5);
  assert(out.opcode == 1);
  assert(out.bytes == 64);
  assert(std::holds_alternative<Requestvote>(out.msg));
  assert(std::get<Requestvote>(out.msg).term == 42);

  // 队列里已经空了：必须超时，而不是重复返回 Ok。
  inboundmsg again;
  assert(inbox.wait_pop(again, 0) == Popstatic::Timeout);
}

// 2. 失败路径不能碰输出参数。
void test_failed_pop_keeps_out() {
  Raft_inbox inbox;

  inboundmsg out = make_msg(77, 9, 9, 77);
  assert(inbox.wait_pop(out, 0) == Popstatic::Timeout);
  assert(out.from == 77);
  assert(std::get<Requestvote>(out.msg).term == 77);
}

// 3. FIFO：推 3 条取 3 条，顺序、from、term 全部一致，第 4 次超时。
void test_fifo_order() {
  Raft_inbox inbox;

  for (uint64_t i = 1; i <= 3; ++i) {
    inboundmsg m = make_msg(static_cast<uint32_t>(i), 1, 16, i);
    assert(inbox.try_push(std::move(m)));
  }

  for (uint64_t i = 1; i <= 3; ++i) {
    inboundmsg out;
    assert(inbox.wait_pop(out, 0) == Popstatic::Ok);
    assert(out.from == i);
    assert(std::get<Requestvote>(out.msg).term == i);
  }

  inboundmsg out;
  assert(inbox.wait_pop(out, 0) == Popstatic::Timeout);
}

// 4. 空队列 100ms 超时，耗时落在 90~400ms。
void test_timeout_duration() {
  Raft_inbox inbox;

  inboundmsg out;
  const auto t0 = Clock::now();
  const auto r = inbox.wait_pop(out, 100);
  const auto elapsed = ms_since(t0);

  assert(r == Popstatic::Timeout);
  assert(elapsed >= 90);
  assert(elapsed <= 400);
}

// 5. 消费者阻塞中，push 必须立刻唤醒它。
//    抓“notify_one 在 return 之后、永远执行不到”。
void test_push_wakes_blocked_consumer() {
  Raft_inbox inbox;

  Popstatic result = Popstatic::Timeout;
  std::size_t elapsed = 0;
  inboundmsg out;

  std::thread consumer([&] {
    const auto t0 = Clock::now();
    result = inbox.wait_pop(out, 3000);
    elapsed = ms_since(t0);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  assert(inbox.try_push(make_msg(8, 1, 4, 88)));
  consumer.join();

  assert(result == Popstatic::Ok);
  assert(elapsed < 200);
  assert(out.from == 8);
}

// 6. 消费者阻塞中，close 必须立刻唤醒它并返回 Close。
void test_close_wakes_waiter() {
  Raft_inbox inbox;

  Popstatic result = Popstatic::Ok;
  std::size_t elapsed = 0;
  inboundmsg out;

  std::thread consumer([&] {
    const auto t0 = Clock::now();
    result = inbox.wait_pop(out, 5000);
    elapsed = ms_since(t0);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  inbox.close();
  consumer.join();

  assert(result == Popstatic::Close);
  assert(elapsed < 300);
}

// 7. close 之后：剩余消息丢弃、wait_pop 立即 Close、try_push 一律 false、
//    重复 close 安全。
void test_close_semantics() {
  Raft_inbox inbox;

  inboundmsg m = make_msg(1, 1, 8, 1);
  assert(inbox.try_push(std::move(m)));
  inbox.close();

  inboundmsg out;
  assert(inbox.wait_pop(out, 0) == Popstatic::Close);

  inboundmsg after = make_msg(2, 2, 8, 2);
  assert(!inbox.try_push(std::move(after)));

  inbox.close();
}

// 8. 条数上限：128 条，第 129 条起 false，且拒绝路径不阻塞。
//    取走一条之后必须又能 push 进来。
void test_count_limit() {
  Raft_inbox inbox;

  int accepted = 0;
  for (int i = 0; i < 200; ++i) {
    inboundmsg m = make_msg(1, 1, 8, static_cast<uint64_t>(i));

    const auto t0 = Clock::now();
    const bool ok = inbox.try_push(std::move(m));
    const auto us = us_since(t0);

    if (ok) {
      ++accepted;
    } else {
      assert(us < 5000);
    }
  }

  assert(accepted == 128);

  inboundmsg out;
  assert(inbox.wait_pop(out, 0) == Popstatic::Ok);

  inboundmsg again = make_msg(1, 1, 8, 999);
  assert(inbox.try_push(std::move(again)));
}

// 9. 字节上限：刚好 16MiB 可以进，再加 1 字节必须拒绝；
//    取空之后必须又能塞满一轮（字节计数归零）。
void test_byte_limit() {
  Raft_inbox inbox;

  const std::size_t mib = 1024u * 1024u;

  for (int i = 0; i < 16; ++i) {
    inboundmsg m = make_msg(1, 1, mib, static_cast<uint64_t>(i));
    assert(inbox.try_push(std::move(m)));
  }

  inboundmsg over = make_msg(1, 1, 1, 100);
  assert(!inbox.try_push(std::move(over)));

  for (int i = 0; i < 16; ++i) {
    inboundmsg out;
    assert(inbox.wait_pop(out, 0) == Popstatic::Ok);
    assert(out.bytes == mib);
  }

  inboundmsg out;
  assert(inbox.wait_pop(out, 0) == Popstatic::Timeout);

  inboundmsg refill = make_msg(1, 1, mib, 300);
  assert(inbox.try_push(std::move(refill)));
}

// 10. 4 个生产者各推 100 条，队列满时允许丢弃，
//     但“入队成功的条数”必须和“最终取出的条数”完全一致。
void test_multi_producer() {
  Raft_inbox inbox;

  constexpr int kThreads = 4;
  constexpr int kPerThread = 100;

  std::atomic<int> accepted{0};
  std::atomic<int> popped{0};
  std::atomic<bool> done{false};

  std::vector<std::thread> producers;
  for (int t = 0; t < kThreads; ++t) {
    producers.emplace_back([&inbox, &accepted, t] {
      for (int i = 0; i < kPerThread; ++i) {
        inboundmsg m = make_msg(static_cast<uint32_t>(t), 1, 8,
                                static_cast<uint64_t>(t * kPerThread + i));
        if (inbox.try_push(std::move(m))) accepted.fetch_add(1);
      }
    });
  }

  std::thread consumer([&] {
    inboundmsg out;
    for (;;) {
      const auto r = inbox.wait_pop(out, 10);
      if (r == Popstatic::Ok) {
        popped.fetch_add(1);
        continue;
      }
      if (r == Popstatic::Close) return;
      if (done.load()) {
        while (inbox.wait_pop(out, 0) == Popstatic::Ok) popped.fetch_add(1);
        return;
      }
    }
  });

  for (auto& p : producers) p.join();
  done.store(true);
  consumer.join();

  assert(popped.load() == accepted.load());
  std::printf("  multi producer: accepted=%d popped=%d\n", accepted.load(),
              popped.load());

  inbox.close();
}

}  // namespace

int main() {
  test_single_push_pop();
  test_failed_pop_keeps_out();
  test_fifo_order();
  test_timeout_duration();
  test_push_wakes_blocked_consumer();
  test_close_wakes_waiter();
  test_close_semantics();
  test_count_limit();
  test_byte_limit();
  test_multi_producer();

  std::puts("inbox_test: all passed");
  return 0;
}

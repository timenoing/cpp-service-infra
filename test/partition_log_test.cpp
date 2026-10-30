#include "mq/log/Partition_log.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#define CHECK(cond) do { \
  if (!(cond)) { std::printf("FAIL line %d: %s\n", __LINE__, #cond); return 1; } \
} while (0)

static mlog::AppendBatch make_batch(uint64_t id, std::size_t n) {
  mlog::AppendBatch b;
  b.request_id = id;
  b.items.assign(n, std::string(1, 'x'));
  return b;
}

static mlog::AppendBatch make_bytes(uint64_t id, std::size_t bytes) {
  mlog::AppendBatch b;
  b.request_id = id;
  b.items.push_back(std::string(bytes, 'x'));
  return b;
}

struct TempDir {
  std::filesystem::path path;

  TempDir() {
    static std::atomic<uint64_t> counter{0};
    path = std::filesystem::temp_directory_path() /
           ("mlog_plog_" + std::to_string(static_cast<long long>(::getpid())) +
            "_" + std::to_string(counter.fetch_add(1)));
    std::filesystem::create_directories(path);
  }

  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
  }

  std::string str() const { return path.string(); }
};

struct CommitRecord {
  uint64_t request_id = 0;
  uint64_t offset = 0;
  std::size_t count = 0;
  mlog::Status status = mlog::Status::ok;
  uint64_t hw_at_callback = 0;
};

struct Recorder {
  std::mutex mu;
  std::condition_variable cv;
  std::vector<CommitRecord> records;
  std::atomic<mlog::PartitionLog*> log{nullptr};

  std::function<void(uint64_t, uint64_t, std::size_t, mlog::Status)> callback() {
    return [this](uint64_t id, uint64_t off, std::size_t count,
                  mlog::Status status) {
      uint64_t hw = 0;
      auto* p = log.load(std::memory_order_acquire);
      if (p != nullptr) hw = p->published_hw();
      std::lock_guard<std::mutex> lk(mu);
      records.push_back(CommitRecord{id, off, count, status, hw});
      cv.notify_all();
    };
  }

  bool wait_for(std::size_t n,
                std::chrono::milliseconds timeout = std::chrono::milliseconds(2000)) {
    std::unique_lock<std::mutex> lk(mu);
    return cv.wait_for(lk, timeout, [&] { return records.size() >= n; });
  }

  std::vector<CommitRecord> snapshot() {
    std::lock_guard<std::mutex> lk(mu);
    return records;
  }

  std::size_t size() {
    std::lock_guard<std::mutex> lk(mu);
    return records.size();
  }
};

// ============================================================================
// 盘面读取（片 C 判据：段帧与 commit.bin 都要能读回来对照）
// ============================================================================
static std::vector<uint8_t> slurp(const std::string& p) {
  std::ifstream in(p, std::ios::binary);
  if (!in) return {};
  return std::vector<uint8_t>(std::istreambuf_iterator<char>(in),
                              std::istreambuf_iterator<char>());
}

static uint64_t be64(const uint8_t* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
  return v;
}

static uint32_t be32(const uint8_t* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v = (v << 8) | p[i];
  return v;
}

struct SFrame {
  uint64_t off = 0;
  uint32_t len = 0;
  uint32_t epoch = 0;
};

static std::vector<SFrame> parse_seg(const std::vector<uint8_t>& b) {
  std::vector<SFrame> out;
  std::size_t p = 32;  // 32B 段头
  while (p + 20 <= b.size()) {
    SFrame f;
    f.off = be64(&b[p]);
    f.len = be32(&b[p + 8]);
    f.epoch = be32(&b[p + 12]);
    out.push_back(f);
    p += 20 + f.len;
  }
  return out;
}

struct CEntry {
  uint64_t batch_no = 0;
  uint64_t hw = 0;
  uint64_t epoch = 0;
  uint8_t state = 0;
};

static std::vector<CEntry> parse_commit(const std::vector<uint8_t>& b) {
  std::vector<CEntry> out;
  for (std::size_t p = 0; p + 36 <= b.size(); p += 36) {
    CEntry e;
    e.batch_no = be64(&b[p]);
    e.hw = be64(&b[p + 8]);
    e.epoch = be64(&b[p + 16]);
    e.state = b[p + 24];
    out.push_back(e);
  }
  return out;
}

// S6 回包早于 S7 发布条落盘（发布条不单独 sync，随下一组 S5 才落），
// 所以等到回包后不能立刻读账本；按目标条数轮询，超时返回 false 让 CHECK 报错。
static bool wait_commit_entries(const std::string& p, std::size_t n,
                                std::chrono::milliseconds timeout =
                                    std::chrono::milliseconds(2000)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (true) {
    if (slurp(p).size() >= n * 36) return true;
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

int main() {
  {
    mlog::Mpsc_bounded q(16u << 20, 128);
    for (int i = 0; i < 128; ++i) CHECK(q.try_push(make_batch((uint64_t)i, 1)));
    CHECK(q.queued_task() == 128);
    CHECK(q.queued_bytes() == 128);
    CHECK(!q.try_push(make_batch(999, 1)));
    CHECK(q.queued_task() == 128);
    CHECK(q.queued_bytes() == 128);
    mlog::AppendBatch out;
    for (int i = 0; i < 128; ++i) CHECK(q.try_pop(out));
    CHECK(!q.try_pop(out));
    CHECK(q.queued_task() == 0);
    CHECK(q.queued_bytes() == 0);
    CHECK(q.try_push(make_batch(1000, 1)));
  }

  {
    mlog::Mpsc_bounded q(16u << 20, 128);
    for (int i = 0; i < 16; ++i) CHECK(q.try_push(make_bytes((uint64_t)i, 1u << 20)));
    CHECK(q.queued_bytes() == (16ull << 20));
    CHECK(q.queued_task() == 16);
    CHECK(!q.try_push(make_bytes(16, 1u << 20)));
    CHECK(q.queued_bytes() == (16ull << 20));
    CHECK(q.queued_task() == 16);
  }

  {
    mlog::Mpsc_bounded q(16u << 20, 128);
    for (int i = 0; i < 128; ++i) CHECK(q.try_push(make_batch((uint64_t)i, 1)));
    const auto t0 = std::chrono::steady_clock::now();
    CHECK(!q.try_push(make_batch(2000, 1)));
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - t0).count();
    CHECK(ms < 100);
  }

  {
    mlog::Mpsc_bounded q(1u << 20, 16);
    CHECK(q.try_push(make_batch(11, 2)));
    CHECK(q.try_push(make_batch(12, 2)));
    CHECK(q.try_push(make_batch(13, 2)));
    CHECK(q.queued_bytes() == 6);
    mlog::AppendBatch out;
    CHECK(q.try_pop(out));
    CHECK(out.request_id == 11);
    CHECK(out.total_bytes == 2);
    CHECK(q.queued_bytes() == 4);
    CHECK(q.try_pop(out));
    CHECK(out.request_id == 12);
    CHECK(q.try_pop(out));
    CHECK(out.request_id == 13);
    CHECK(q.queued_bytes() == 0);
    CHECK(!q.try_pop(out));
  }

  {
    mlog::Mpsc_bounded q(1u << 20, 16);
    mlog::AppendBatch b;
    b.request_id = 7;
    b.items.push_back("z");
    b.total_bytes = 999;
    CHECK(q.try_push(b));
    CHECK(q.queued_bytes() == 1);
    mlog::AppendBatch out;
    CHECK(q.try_pop(out));
    CHECK(out.request_id == 7);
    CHECK(out.total_bytes == 1);
    CHECK(q.queued_bytes() == 0);
  }

  {
    mlog::Mpsc_bounded q(16, 2);
    mlog::AppendBatch b;
    b.request_id = 5;
    CHECK(q.try_push(b));
    CHECK(q.queued_task() == 1);
    CHECK(q.queued_bytes() == 0);
    CHECK(q.try_push(b));
    CHECK(!q.try_push(b));
    CHECK(q.queued_task() == 2);
    mlog::AppendBatch out;
    CHECK(q.try_pop(out));
    CHECK(out.request_id == 5);
    CHECK(out.items.empty());
    CHECK(q.queued_bytes() == 0);
  }

  {
    mlog::Mpsc_bounded q(16, 8);
    std::atomic<bool> waiting{false};
    std::atomic<bool> popped{true};
    std::thread t([&] {
      waiting.store(true);
      mlog::AppendBatch out;
      popped.store(q.wait_pop(out));
    });
    while (!waiting.load()) std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q.close();
    t.join();
    CHECK(!popped.load());
  }

  {
    mlog::Mpsc_bounded q(1u << 20, 8);
    CHECK(q.try_push(make_batch(1, 1)));
    CHECK(q.try_push(make_batch(2, 1)));
    CHECK(q.try_push(make_batch(3, 1)));
    q.close();
    mlog::AppendBatch out;
    CHECK(q.wait_pop(out));
    CHECK(out.request_id == 1);
    CHECK(q.wait_pop(out));
    CHECK(out.request_id == 2);
    CHECK(q.wait_pop(out));
    CHECK(out.request_id == 3);
    CHECK(!q.wait_pop(out));
    CHECK(q.queued_task() == 0);
    CHECK(q.queued_bytes() == 0);
    CHECK(!q.try_push(make_batch(4, 1)));
  }

  {
    mlog::Mpsc_bounded q(16u << 20, 128);
    constexpr int kProducers = 4;
    constexpr int kPerProducer = 100;
    std::atomic<int> pushed{0};
    std::atomic<bool> ok{true};
    std::set<uint64_t> ids;

    std::thread consumer([&] {
      mlog::AppendBatch out;
      while (q.wait_pop(out)) {
        if (!ids.insert(out.request_id).second) ok.store(false);
      }
    });

    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
      producers.emplace_back([&, p] {
        for (int i = 0; i < kPerProducer; ++i) {
          mlog::AppendBatch b;
          b.request_id = (uint64_t)p * 1000 + (uint64_t)i;
          b.items.push_back("x");
          const auto deadline = std::chrono::steady_clock::now() +
                                std::chrono::seconds(2);
          bool done = false;
          while (!done) {
            if (q.try_push(b)) {
              pushed.fetch_add(1);
              done = true;
            } else if (std::chrono::steady_clock::now() > deadline) {
              ok.store(false);
              break;
            } else {
              std::this_thread::yield();
            }
          }
          if (!done) break;
        }
      });
    }

    for (auto& t : producers) t.join();
    q.close();
    consumer.join();

    CHECK(ok.load());
    CHECK(pushed.load() == kProducers * kPerProducer);
    CHECK(ids.size() == (std::size_t)(kProducers * kPerProducer));
    CHECK(q.queued_task() == 0);
    CHECK(q.queued_bytes() == 0);
  }

  // ==========================================================================
  // 片 C · S1~S7 主链：A(2) → B(3) → 空批 → 失租 → G(1) → 干净重开
  // ==========================================================================
  {
    TempDir dir;
    mlog::Logconfig cfg;
    const std::string seg = dir.str() + "/log-00000000000000000000.seg";
    const std::string cmt = dir.str() + "/commit.bin";

    {
      Recorder rec;
      mlog::PartitionLog log(dir.str(), cfg, rec.callback());
      rec.log.store(&log, std::memory_order_release);

      // 空目录建新日志：32B 段头 + 空账本
      CHECK(log.recover());
      CHECK(log.published_hw() == 0);
      CHECK(slurp(seg).size() == 32);
      CHECK(slurp(cmt).empty());

      // 第一组：A(2) → (0,2,ok)，HW 2，batch 1
      log.set_lease(7, true);
      CHECK(log.try_enqueue(make_batch(100, 2)));
      CHECK(rec.wait_for(1));
      auto r = rec.snapshot();
      CHECK(r[0].request_id == 100);
      CHECK(r[0].offset == 0);
      CHECK(r[0].count == 2);
      CHECK(r[0].status == mlog::Status::ok);
      CHECK(log.published_hw() == 2);

      // 第二组：B(3) → (2,3,ok)，HW 5
      CHECK(log.try_enqueue(make_batch(101, 3)));
      CHECK(rec.wait_for(2));
      r = rec.snapshot();
      CHECK(r[1].request_id == 101);
      CHECK(r[1].offset == 2);
      CHECK(r[1].count == 3);
      CHECK(r[1].status == mlog::Status::ok);
      CHECK(log.published_hw() == 5);

      // 段：5 帧，offset 0~4，epoch 7，每条 1 字节
      const auto frames = parse_seg(slurp(seg));
      CHECK(frames.size() == 5);
      for (int i = 0; i < 5; ++i) {
        CHECK(frames[(std::size_t)i].off == (uint64_t)i);
        CHECK(frames[(std::size_t)i].len == 1);
        CHECK(frames[(std::size_t)i].epoch == 7);
      }

      // 账本：两组各 candidate/credited/published 三条
      CHECK(wait_commit_entries(cmt, 6));
      auto e = parse_commit(slurp(cmt));
      CHECK(e.size() == 6);
      CHECK(e[0].batch_no == 1 && e[0].state == 0 && e[0].hw == 2);
      CHECK(e[1].batch_no == 1 && e[1].state == 1 && e[1].hw == 2);
      CHECK(e[2].batch_no == 1 && e[2].state == 2 && e[2].hw == 2);
      CHECK(e[3].batch_no == 2 && e[3].state == 0 && e[3].hw == 5);
      CHECK(e[4].batch_no == 2 && e[4].state == 1 && e[4].hw == 5);
      CHECK(e[5].batch_no == 2 && e[5].state == 2 && e[5].hw == 5);

      // 空批：不写盘、不占 offset、HW 不动
      CHECK(log.try_enqueue(make_batch(102, 0)));
      CHECK(rec.wait_for(3));
      r = rec.snapshot();
      CHECK(r[2].request_id == 102);
      CHECK(r[2].offset == 5);
      CHECK(r[2].count == 0);
      CHECK(r[2].status == mlog::Status::ok);
      CHECK(log.published_hw() == 5);
      CHECK(slurp(cmt).size() == 6 * 36);
      CHECK(parse_seg(slurp(seg)).size() == 5);

      // 批头失租：not_leader，段与账本不增长
      log.set_lease(7, false);
      CHECK(log.try_enqueue(make_batch(103, 4)));
      CHECK(rec.wait_for(4));
      r = rec.snapshot();
      CHECK(r[3].request_id == 103);
      CHECK(r[3].offset == 5);
      CHECK(r[3].count == 4);
      CHECK(r[3].status == mlog::Status::not_leader);
      CHECK(log.published_hw() == 5);
      CHECK(slurp(cmt).size() == 6 * 36);
      CHECK(parse_seg(slurp(seg)).size() == 5);

      // 重新拿回租约：继续从 5 写
      log.set_lease(7, true);
      CHECK(log.try_enqueue(make_batch(104, 1)));
      CHECK(rec.wait_for(5));
      r = rec.snapshot();
      CHECK(r[4].request_id == 104);
      CHECK(r[4].offset == 5);
      CHECK(r[4].count == 1);
      CHECK(r[4].status == mlog::Status::ok);
      CHECK(log.published_hw() == 6);

      CHECK(wait_commit_entries(cmt, 9));
      e = parse_commit(slurp(cmt));
      CHECK(e.size() == 9);
      CHECK(e[6].batch_no == 3 && e[6].state == 0 && e[6].hw == 6);
      CHECK(e[7].batch_no == 3 && e[7].state == 1 && e[7].hw == 6);
      CHECK(e[8].batch_no == 3 && e[8].state == 2 && e[8].hw == 6);
    }

    // 干净重开：读回 credit_hw / last_batch_no，继续编号与 offset
    {
      Recorder rec;
      mlog::PartitionLog log(dir.str(), cfg, rec.callback());
      rec.log.store(&log, std::memory_order_release);
      CHECK(log.recover());
      CHECK(log.published_hw() == 6);
      CHECK(log.current_epoch() == 7);

      log.set_lease(7, true);
      CHECK(log.try_enqueue(make_batch(105, 1)));
      CHECK(rec.wait_for(1));
      auto r = rec.snapshot();
      CHECK(r[0].request_id == 105);
      CHECK(r[0].offset == 6);
      CHECK(r[0].count == 1);
      CHECK(r[0].status == mlog::Status::ok);
      CHECK(log.published_hw() == 7);

      CHECK(wait_commit_entries(cmt, 12));
      const auto e = parse_commit(slurp(cmt));
      CHECK(e.size() == 12);
      CHECK(e[9].batch_no == 4 && e[9].state == 0 && e[9].hw == 7);
      CHECK(e[11].batch_no == 4 && e[11].state == 2 && e[11].hw == 7);
    }
  }

  // 全空组：不写段、不写账本、不吃 batch_no
  {
    TempDir dir;
    mlog::Logconfig cfg;
    Recorder rec;
    mlog::PartitionLog log(dir.str(), cfg, rec.callback());
    rec.log.store(&log, std::memory_order_release);
    CHECK(log.recover());
    log.set_lease(3, true);
    CHECK(log.try_enqueue(make_batch(1, 0)));
    CHECK(rec.wait_for(1));
    const auto r = rec.snapshot();
    CHECK(r[0].offset == 0);
    CHECK(r[0].count == 0);
    CHECK(r[0].status == mlog::Status::ok);
    CHECK(log.published_hw() == 0);
    CHECK(slurp(dir.str() + "/commit.bin").empty());
    CHECK(slurp(dir.str() + "/log-00000000000000000000.seg").size() == 32);
  }

  // 段 sync 失败：fail-stop，当前组 io_error，HW 不动，后续入队被拒
  {
    TempDir dir;
    mlog::io::SimFileIo io;
    mlog::Logconfig cfg;
    Recorder rec;
    mlog::PartitionLog log(dir.str(), cfg, rec.callback(), io);
    rec.log.store(&log, std::memory_order_release);
    CHECK(log.recover());
    log.set_lease(5, true);
    io.inject_sync_fail(1);  // 下一次 fdatasync 就是 S2
    CHECK(log.try_enqueue(make_batch(200, 2)));
    CHECK(rec.wait_for(1));
    const auto r = rec.snapshot();
    CHECK(r[0].request_id == 200);
    CHECK(r[0].offset == 0);
    CHECK(r[0].count == 2);
    CHECK(r[0].status == mlog::Status::io_error);
    CHECK(log.published_hw() == 0);
    CHECK(!log.try_enqueue(make_batch(201, 1)));
  }

  // fetch 仍是片 B 的桩
  {
    TempDir dir;
    mlog::Logconfig cfg;
    Recorder rec;
    mlog::PartitionLog log(dir.str(), cfg, rec.callback());
    rec.log.store(&log, std::memory_order_release);
    CHECK(log.recover());
    std::vector<std::string> out{"stale"};
    uint64_t next = 42;
    uint64_t hw = 7;
    CHECK(log.fetch(5, 1024, out, next, hw) == mlog::Status::out_of_range);
    CHECK(out.empty());
    CHECK(next == 5);
    CHECK(hw == 0);
  }

  std::printf("partition_log_test: all passed\n");
  return 0;
}

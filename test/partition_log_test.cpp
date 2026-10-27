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

  void add_seed() const {
    std::ofstream out((path / "seed").string());
    out << 'x';
  }
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

  {
    TempDir dir;
    mlog::io::SimFileIo io;
    Recorder rec;
    mlog::Logconfig cfg;
    mlog::PartitionLog log(dir.str(), cfg, rec.callback(), io);
    rec.log.store(&log, std::memory_order_release);

    CHECK(!log.recover());
    CHECK(!log.try_enqueue(make_batch(1, 1)));
    CHECK(log.published_hw() == 0);

    dir.add_seed();
    CHECK(log.recover());
    CHECK(log.recover());
    CHECK(log.current_epoch() == 0);

    log.set_lease(7, true);
    CHECK(log.current_epoch() == 7);
    CHECK(log.try_enqueue(make_batch(10, 2)));
    CHECK(log.try_enqueue(make_batch(11, 3)));
    CHECK(rec.wait_for(2));

    auto r = rec.snapshot();
    CHECK(r.size() == 2);
    CHECK(r[0].request_id == 10);
    CHECK(r[0].offset == 0);
    CHECK(r[0].count == 2);
    CHECK(r[0].status == mlog::Status::ok);
    CHECK(r[0].hw_at_callback == 2);
    CHECK(r[1].request_id == 11);
    CHECK(r[1].offset == 2);
    CHECK(r[1].count == 3);
    CHECK(r[1].status == mlog::Status::ok);
    CHECK(r[1].hw_at_callback == 5);
    CHECK(log.published_hw() == 5);

    CHECK(log.try_enqueue(make_batch(12, 0)));
    CHECK(rec.wait_for(3));
    r = rec.snapshot();
    CHECK(r.size() == 3);
    CHECK(r[2].request_id == 12);
    CHECK(r[2].offset == 5);
    CHECK(r[2].count == 0);
    CHECK(r[2].status == mlog::Status::ok);
    CHECK(r[2].hw_at_callback == 5);
    CHECK(log.published_hw() == 5);

    std::vector<std::string> out{"stale"};
    uint64_t next = 123;
    uint64_t hw = 0;
    CHECK(log.fetch(123, 4096, out, next, hw) == mlog::Status::out_of_range);
    CHECK(out.empty());
    CHECK(next == 123);
    CHECK(hw == 5);

    log.set_lease(7, false);
    CHECK(log.current_epoch() == 7);
    CHECK(log.try_enqueue(make_batch(13, 4)));
    CHECK(rec.wait_for(4));
    r = rec.snapshot();
    CHECK(r.size() == 4);
    CHECK(r[3].request_id == 13);
    CHECK(r[3].offset == 5);
    CHECK(r[3].count == 4);
    CHECK(r[3].status == mlog::Status::not_leader);
    CHECK(r[3].hw_at_callback == 5);
    CHECK(log.published_hw() == 5);
  }

  {
    TempDir dir;
    dir.add_seed();
    mlog::io::SimFileIo io;
    Recorder rec;
    mlog::Logconfig cfg;
    mlog::PartitionLog log(dir.str(), cfg, rec.callback(), io);
    rec.log.store(&log, std::memory_order_release);

    CHECK(log.recover());
    CHECK(log.try_enqueue(make_batch(20, 2)));
    CHECK(rec.wait_for(1));

    auto r = rec.snapshot();
    CHECK(r.size() == 1);
    CHECK(r[0].request_id == 20);
    CHECK(r[0].offset == 0);
    CHECK(r[0].count == 2);
    CHECK(r[0].status == mlog::Status::not_leader);
    CHECK(r[0].hw_at_callback == 0);
    CHECK(log.published_hw() == 0);
  }

  {
    TempDir dir;
    dir.add_seed();
    mlog::io::SimFileIo io;
    Recorder rec;
    mlog::Logconfig cfg;

    {
      mlog::PartitionLog log(dir.str(), cfg, rec.callback(), io);
      rec.log.store(&log, std::memory_order_release);
      CHECK(log.recover());
      log.set_lease(1, true);
      for (int i = 0; i < 64; ++i) {
        CHECK(log.try_enqueue(make_batch(1000 + (uint64_t)i, 1)));
      }
    }

    CHECK(rec.size() == 64);
    auto r = rec.snapshot();
    for (int i = 0; i < 64; ++i) {
      CHECK(r[i].request_id == 1000 + (uint64_t)i);
      CHECK(r[i].offset == (uint64_t)i);
      CHECK(r[i].count == 1);
      CHECK(r[i].status == mlog::Status::ok);
      CHECK(r[i].hw_at_callback == (uint64_t)(i + 1));
    }
  }

  std::printf("partition_log_test: all passed\n");
  return 0;
}

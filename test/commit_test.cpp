#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include "Commit_log.h"

using mlog::Commitlog;
using mlog::commitEntry;

#define CHECK(cond) do { \
  if(!(cond)) { std::printf("FAIL line %d: %s\n", __LINE__, #cond); return 1; } \
} while(0)

static const char* P1 = "/tmp/mlog_ct_1.bin";
static const char* P2 = "/tmp/mlog_ct_2.bin";
static const char* P3 = "/tmp/mlog_ct_3.bin";
static const char* P4 = "/tmp/mlog_ct_4.bin";
static const char* P5 = "/tmp/mlog_ct_5.bin";
static const char* P6 = "/tmp/mlog_ct_6.bin";

static void remove_file(const char* p) { ::unlink(p); }

static int write_batch(Commitlog& c, uint64_t batch_no, uint64_t hw, uint64_t epoch) {
  c.log.batch_no = batch_no;
  c.log.candidate_hw = hw;
  c.log.epoch = epoch;
  c.log.state = 0; if (!c.append()) return 1;
  c.log.state = 1; if (!c.append()) return 1;
  c.log.state = 2; if (!c.append()) return 1;
  return 0;
}

int main() {
  { // 判据1：追�?批�?�?+ sync + 重放逐批正确（首�?batch_no �?起）
    remove_file(P1);
    Commitlog c(P1);
    Commitlog::Reply r;
    CHECK(c.open_or_create(r));
    CHECK(r.last_batch_no == 0 && r.credit_hw == 0 && r.last_epoch == 0 && !r.corrupt);
    for (uint64_t b = 1; b <= 5; ++b) {
      CHECK(write_batch(c, b, b * 10, 7) == 0);    }
    CHECK(c.sync());
  }
  {
    Commitlog c(P1);
    Commitlog::Reply r;
    CHECK(c.open_or_create(r));
    CHECK(!r.corrupt);
    CHECK(r.last_batch_no == 5);
    CHECK(r.credit_hw == 50);
    CHECK(r.last_epoch == 7);
  }
  { // 判据2：torn 重开再追加（错位坑回归）
    remove_file(P2);
    Commitlog c(P2);
    Commitlog::Reply r;
    CHECK(c.open_or_create(r));
    CHECK(write_batch(c, 1, 10, 7) == 0);
    c.log.batch_no = 2; c.log.candidate_hw = 20; c.log.epoch = 7;
    c.log.state = 0; CHECK(c.append());
    c.log.state = 1; CHECK(c.append());
    CHECK(c.sync());
  }
  {
    int fd = ::open(P2, O_RDWR); CHECK(fd >= 0);
    CHECK(::ftruncate(fd, 180 - 20) == 0);
    ::close(fd);
    Commitlog c(P2);
    Commitlog::Reply r;
    CHECK(c.open_or_create(r));
    CHECK(!r.corrupt);
    CHECK(r.last_batch_no == 2);
    CHECK(r.credit_hw == 10);
    c.log.batch_no = 2; c.log.candidate_hw = 20; c.log.epoch = 7; c.log.state = 2;
    CHECK(c.append());
    CHECK(c.sync());
  }
  {
    Commitlog c(P2);
    Commitlog::Reply r;
    CHECK(c.open_or_create(r));
    CHECK(!r.corrupt);
    CHECK(r.last_batch_no == 2);
    CHECK(r.credit_hw == 20);
  }
  { // 判据3：中部翻转一字节 �?corrupt
    remove_file(P3);
    Commitlog c(P3);
    Commitlog::Reply r;
    CHECK(c.open_or_create(r));
    CHECK(write_batch(c, 1, 10, 7) == 0);
    CHECK(write_batch(c, 2, 20, 7) == 0);
  }
  {
    int fd = ::open(P3, O_RDWR); CHECK(fd >= 0);
    uint8_t b; CHECK(::pread(fd, &b, 1, 36 + 0) == 1);
    b ^= 0x01; CHECK(::pwrite(fd, &b, 1, 36 + 0) == 1);
    ::close(fd);
    Commitlog c(P3);
    Commitlog::Reply r;
    c.open_or_create(r);
    CHECK(r.corrupt);
  }
  { // 判据4：batch_no 回退 �?corrupt
    remove_file(P4);
    Commitlog c(P4);
    Commitlog::Reply r;
    CHECK(c.open_or_create(r));
    CHECK(write_batch(c, 1, 10, 7) == 0);
    c.log.batch_no = 2; c.log.candidate_hw = 20; c.log.epoch = 7;
    c.log.state = 0; CHECK(c.append());
    c.log.batch_no = 1; c.log.candidate_hw = 10; c.log.epoch = 7;
    c.log.state = 1; CHECK(c.append());
  }
  {
    Commitlog c(P4);
    Commitlog::Reply r;
    c.open_or_create(r);
    CHECK(r.corrupt);
  }
  { // 判据5：同�?state 倒退 �?corrupt
    remove_file(P5);
    Commitlog c(P5);
    Commitlog::Reply r;
    CHECK(c.open_or_create(r));
    CHECK(write_batch(c, 1, 10, 7) == 0);
    c.log.batch_no = 2; c.log.candidate_hw = 20; c.log.epoch = 7;
    c.log.state = 0; CHECK(c.append());
    c.log.state = 1; CHECK(c.append());
    c.log.state = 0; CHECK(c.append());
  }
  {
    Commitlog c(P5);
    Commitlog::Reply r;
    c.open_or_create(r);
    CHECK(r.corrupt);
  }
  { // 判据6：文件不存在 �?正常打开；空文件追加一条后重放正确
    remove_file(P6);
    Commitlog c(P6);
    Commitlog::Reply r;
    CHECK(c.open_or_create(r));
    CHECK(!r.corrupt && r.last_batch_no == 0 && r.credit_hw == 0 && r.last_epoch == 0);
    c.log.batch_no = 1; c.log.candidate_hw = 5; c.log.epoch = 1; c.log.state = 0;
    CHECK(c.append());
  }
  {
    Commitlog c(P6);
    Commitlog::Reply r;
    CHECK(c.open_or_create(r));
    CHECK(!r.corrupt && r.last_batch_no == 1 && r.credit_hw == 0 && r.last_epoch == 1);
  }
  std::printf("commit_test: all passed\n");
  return 0;
}

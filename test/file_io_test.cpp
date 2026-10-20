// file_io_test · 块 4 片 A 第 3 步（SimFileIo 故障实现，十判据）
// ① POSIX 往返 ② 真目录 fsync_dir ③ 丢未 sync 写 ④ 半条写->torn
// ⑤ dirent 丢失 ⑥ dirent 保护 ⑦ sync 失败注入 ⑧ 盘满注入 ⑨ 句柄纪律
// ⑩ 三套回归（commit_test/segment_test/segment_recovery_test）由构建脚本另跑

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cerrno>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <zlib.h>
#include "Commit_log.h"
#include "Segment_recovery.h"
#include "mq/io/File_io.h"

#define CHECK(cond) do { \
  if(!(cond)) { std::printf("FAIL line %d: %s\n", __LINE__, #cond); return 1; } \
} while(0)

static std::string build_seghead(uint64_t base_offset) {
  std::string h(32, 0);
  memcpy(&h[0], "PLOGSEGV", 8);
  h[8] = 0; h[9] = 0; h[10] = 0; h[11] = 1;
  uint64_t b = base_offset;
  for (int i = 7; i >= 0; --i) { h[12 + i] = uint8_t(b & 0xFF); b >>= 8; }
  uint64_t wt = 20261012000000ULL;
  for (int i = 7; i >= 0; --i) { h[20 + i] = uint8_t(wt & 0xFF); wt >>= 8; }
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const uint8_t*)h.data(), 28);
  h[28] = (crc >> 24) & 0xFF; h[29] = (crc >> 16) & 0xFF;
  h[30] = (crc >> 8) & 0xFF;  h[31] = crc & 0xFF;
  return h;
}

static std::string build_frame(uint64_t offset, uint32_t epoch, const std::string& payload) {
  std::string f(20 + payload.size(), 0);
  uint64_t o = offset;
  for (int i = 7; i >= 0; --i) { f[i] = uint8_t(o & 0xFF); o >>= 8; }
  uint32_t len = (uint32_t)payload.size();
  f[8]  = (len >> 24) & 0xFF; f[9]  = (len >> 16) & 0xFF;
  f[10] = (len >> 8)  & 0xFF; f[11] =  len        & 0xFF;
  f[12] = (epoch >> 24) & 0xFF; f[13] = (epoch >> 16) & 0xFF;
  f[14] = (epoch >> 8)  & 0xFF; f[15] =  epoch       & 0xFF;
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const uint8_t*)f.data() + 8, 8);
  if (len) crc = crc32(crc, (const uint8_t*)payload.data(), len);
  f[16] = (crc >> 24) & 0xFF; f[17] = (crc >> 16) & 0xFF;
  f[18] = (crc >> 8)  & 0xFF; f[19] =  crc        & 0xFF;
  f.replace(20, payload.size(), payload);
  return f;
}

static int write_batch(mlog::Commitlog& c, uint64_t no, uint64_t hw, uint32_t epoch) {
  c.log.batch_no = no; c.log.candidate_hw = hw; c.log.epoch = epoch;
  c.log.state = 0; if (!c.append()) return 1;
  c.log.state = 1; if (!c.append()) return 1;
  c.log.state = 2; if (!c.append()) return 1;
  return 0;
}

int main() {
  // ① POSIX 实现经真盘往返：write/read/truncate/fstat/close 全对
  {
    mlog::io::File_io& io = mlog::io::posix_file_io();
    const char* P = "/tmp/mlog_fio_posix.bin";
    ::unlink(P);
    int fd = io.open(P, O_CREAT | O_RDWR | O_TRUNC, 0644);
    CHECK(fd >= 0);
    const char* msg = "hello-fileio";
    ssize_t mlen = (ssize_t)strlen(msg);
    CHECK(io.pwrite(fd, msg, mlen, 0) == mlen);
    CHECK(io.fdatasync(fd) == 0);
    char buf[64] = {0};
    CHECK(io.pread(fd, buf, sizeof(buf), 0) == mlen);
    CHECK(memcmp(buf, msg, (size_t)mlen) == 0);
    struct stat st;
    CHECK(io.fstat(fd, &st) == 0 && st.st_size == (off_t)mlen);
    CHECK(io.ftruncate(fd, 2) == 0);
    CHECK(io.fstat(fd, &st) == 0 && st.st_size == 2);
    CHECK(io.close(fd) == 0);
    ::unlink(P);
  }

  // ② fsync_dir 打真目录成功；不存在目录返回 -1
  {
    CHECK(mlog::io::posix_file_io().fsync_dir("/tmp") == 0);
    CHECK(mlog::io::posix_file_io().fsync_dir("/tmp/mlog_fio_no_such_dir") == -1);
  }

  // ③ append 后不 sync，crash 应丢弃全部未 sync 写
  {
    mlog::io::SimFileIo sim;
    const char* P = "/tmp/mlog_fio_ct.bin";
    {
      mlog::Commitlog c(P, sim);
      mlog::Commitlog::Reply r;
      CHECK(c.open_or_create(r));
      for (uint64_t b = 1; b <= 3; ++b) CHECK(write_batch(c, b, b * 10, 1) == 0);
    }
    sim.crash();
    {
      mlog::Commitlog c(P, sim);
      mlog::Commitlog::Reply r;
      CHECK(c.open_or_create(r));
      CHECK(!r.corrupt);
      CHECK(r.last_batch_no == 0);
      CHECK(r.credit_hw == 0);
    }
  }

  // ④ 半条写：content 写满、durable 只落前半；crash 后 scanner 判 torn_tail
  {
    mlog::io::SimFileIo sim;
    const char* P = "/tmp/mlog_fio_seg.torn.seg";
    uint64_t base = 1000;
    std::string head = build_seghead(base);
    std::string f1 = build_frame(base, 1, "abc");
    std::string f2p = "torn-payload";
    std::string f2 = build_frame(base + f1.size(), 2, f2p);

    int fd = sim.open(P, O_CREAT | O_WRONLY, 0644);
    CHECK(fd >= 0);
    CHECK(sim.pwrite(fd, head.data(), (ssize_t)head.size(), 0) == (ssize_t)head.size());
    CHECK(sim.fdatasync(fd) == 0);
    CHECK(sim.fsync_dir("/tmp") == 0);
    CHECK(sim.pwrite(fd, f1.data(), (ssize_t)f1.size(), (off_t)head.size()) == (ssize_t)f1.size());
    CHECK(sim.fdatasync(fd) == 0);

    sim.inject_half_write(2);
    off_t f2off = (off_t)(head.size() + f1.size());
    CHECK(sim.pwrite(fd, f2.data(), 20, f2off) == 20);
    CHECK(sim.pwrite(fd, f2p.data(), (ssize_t)f2p.size(), f2off + 20) == (ssize_t)f2p.size());
    CHECK(sim.fdatasync(fd) == 0);
    CHECK(sim.close(fd) == 0);

    sim.crash();
    int fd2 = sim.open(P, O_RDWR, 0);
    CHECK(fd2 >= 0);
    {
      Segment_recovery rec(1 << 20, fd2, sim);
      Scan_end e = rec.segment_scanner();
      CHECK(e.kind == Scan_end::torn_tail);
      CHECK(e.vaild_end == (uint64_t)(head.size() + f1.size()));
    }
    sim.close(fd2);
  }

  // ⑤ 新建后未 fsync_dir，crash 后目录项丢失（文件不存在）
  {
    mlog::io::SimFileIo sim;
    const char* P = "/tmp/mlog_fio_dirent_lost.seg";
    int fd = sim.open(P, O_CREAT | O_RDWR, 0644);
    CHECK(fd >= 0);
    CHECK(sim.pwrite(fd, "data", 4, 0) == 4);
    CHECK(sim.fdatasync(fd) == 0);
    CHECK(sim.close(fd) == 0);

    sim.crash();
    errno = 0;
    CHECK(sim.open(P, O_RDWR, 0) == -1);
    CHECK(errno == ENOENT);
  }

  // ⑥ 新建后 fsync_dir，crash 后目录项保住且内容可读
  {
    mlog::io::SimFileIo sim;
    const char* P = "/tmp/mlog_fio_dirent_keep.seg";
    int fd = sim.open(P, O_CREAT | O_RDWR, 0644);
    CHECK(fd >= 0);
    CHECK(sim.pwrite(fd, "keepme", 6, 0) == 6);
    CHECK(sim.fdatasync(fd) == 0);
    CHECK(sim.fsync_dir("/tmp") == 0);
    CHECK(sim.close(fd) == 0);

    sim.crash();
    int fd2 = sim.open(P, O_RDWR, 0);
    CHECK(fd2 >= 0);
    char buf[16] = {0};
    CHECK(sim.pread(fd2, buf, sizeof(buf), 0) == 6);
    CHECK(memcmp(buf, "keepme", 6) == 0);
    sim.close(fd2);
  }

  // ⑦ arm 后第 1 次 fdatasync 注入失败：sync 返 false，durable 不前进
  {
    mlog::io::SimFileIo sim;
    const char* P = "/tmp/mlog_fio_syncfail.bin";
    {
      mlog::Commitlog c(P, sim);
      mlog::Commitlog::Reply r;
      CHECK(c.open_or_create(r));
      CHECK(write_batch(c, 1, 10, 1) == 0);
      CHECK(c.sync());
    }
    {
      mlog::Commitlog c(P, sim);
      mlog::Commitlog::Reply r;
      CHECK(c.open_or_create(r));
      CHECK(r.last_batch_no == 1);
      sim.inject_sync_fail(1);
      CHECK(write_batch(c, 2, 20, 1) == 0);
      CHECK(!c.sync());
    }
    sim.crash();
    {
      mlog::Commitlog c(P, sim);
      mlog::Commitlog::Reply r;
      CHECK(c.open_or_create(r));
      CHECK(!r.corrupt);
      CHECK(r.last_batch_no == 1);
      CHECK(r.credit_hw == 10);
    }
  }

  // ⑧ 盘满：pwrite 与 Commitlog::append 均返 ENOSPC/失败
  {
    mlog::io::SimFileIo sim;
    const char* P = "/tmp/mlog_fio_enospc.bin";
    int fd = sim.open(P, O_CREAT | O_RDWR, 0644);
    CHECK(fd >= 0);
    sim.inject_enospc(10);
    char buf[64] = {0};
    errno = 0;
    CHECK(sim.pwrite(fd, buf, 20, 0) == -1);
    CHECK(errno == ENOSPC);
    CHECK(sim.close(fd) == 0);
  }
  {
    mlog::io::SimFileIo sim;
    const char* P = "/tmp/mlog_fio_enospc2.bin";
    mlog::Commitlog c(P, sim);
    mlog::Commitlog::Reply r;
    CHECK(c.open_or_create(r));
    sim.inject_enospc(10);
    c.log.batch_no = 1; c.log.candidate_hw = 10; c.log.epoch = 1; c.log.state = 0;
    CHECK(!c.append());
  }

  // ⑨ 句柄纪律：fd 单调不复用 / EEXIST / 关后操作 EBADF / EOF 短读
  {
    mlog::io::SimFileIo sim;
    const char* A = "/tmp/mlog_fio_h_a.bin";
    const char* B = "/tmp/mlog_fio_h_b.bin";
    const char* C = "/tmp/mlog_fio_h_c.bin";
    int fa = sim.open(A, O_CREAT | O_RDWR, 0644);
    CHECK(fa >= 0);
    int fb = sim.open(B, O_CREAT | O_RDWR, 0644);
    CHECK(fb > fa);

    errno = 0;
    CHECK(sim.open(A, O_CREAT | O_EXCL, 0644) == -1);
    CHECK(errno == EEXIST);

    CHECK(sim.close(fa) == 0);
    errno = 0;
    CHECK(sim.close(fa) == -1 && errno == EBADF);
    errno = 0;
    CHECK(sim.pwrite(fa, "x", 1, 0) == -1 && errno == EBADF);
    char ch = 0;
    errno = 0;
    CHECK(sim.pread(fa, &ch, 1, 0) == -1 && errno == EBADF);
    struct stat st;
    errno = 0;
    CHECK(sim.fstat(fa, &st) == -1 && errno == EBADF);

    int fc = sim.open(C, O_CREAT | O_RDWR, 0644);
    CHECK(fc > fb);

    int fd = sim.open(A, O_RDWR, 0);
    CHECK(fd >= 0);
    CHECK(sim.pwrite(fd, "abcde", 5, 0) == 5);
    char rb[32] = {0};
    CHECK(sim.pread(fd, rb, sizeof(rb), 0) == 5);
    CHECK(sim.pread(fd, rb, sizeof(rb), 5) == 0);
    CHECK(sim.pread(fd, rb, sizeof(rb), 10) == 0);
    CHECK(sim.fstat(fd, &st) == 0 && st.st_size == 5);
    CHECK(sim.close(fd) == 0);
  }

  std::printf("file_io_test: all passed\n");
  return 0;
}

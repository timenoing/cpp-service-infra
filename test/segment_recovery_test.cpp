// segment_recovery_test · 块 3 单测（scanner 四终局 + corrupt_at 四刀定位 + turncate + find_idx）
// 约定：Segment_recovery(fd 借用不关闭)；扫描对象每次新建（offset/prev_epoch 是成员，
//       同对象复扫会带着上轮游标，本测试一律"一场景一对象"）
// 每条 CHECK 前的注释写"为什么"——核对时审判对 contract 的理解，偏了你改。
//
// 契约引用（design 02-partition-log-design.md v1.2 + arch_contract.md）：
//   §2.4 四判据出刀序: len≤1MiB → epoch≥prev → avail(撕裂) → CRC → offset==期望
//   §2.3 尾段: 超限 len 归损坏（§9 判据 8：corrupt+at_len）
//   §3 锚点判据: 累计 64KiB 或 1024 帧，先到者（2026-10-12 拍板：留双条件）
//   §3 锚点内容: 锚帧绝对 offset（推进前取 base_offset+(p-32)）
//   恢复终局四态: clean / torn_tail（安全截断） / corrupt（拒启） / head_bad（段头三刀不过）
//
// 注：判据 8 按契约断言 corrupt+at_len——scanner 现行 n>20+len 前闸会把超限 len
//     先按撕裂处置，与本契约相悖；此用例红即是依据，改法在 scanner 而非测试。

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <zlib.h>
#include "Segment_recovery.h"

#define CHECK(cond) do { \
  if(!(cond)) { std::printf("FAIL line %d: %s\n", __LINE__, #cond); return 1; } \
} while(0)

// 大 inteval 关掉"1024 帧"那半个条件，让锚点节奏只用字节判据，测试才可确定
static const int INEVAL_OFF = 1 << 20;

static const char* P1 = "/tmp/mlog_sgr_1.seg";
static const char* P2 = "/tmp/mlog_sgr_2.seg";
static const char* P3 = "/tmp/mlog_sgr_3.seg";
static const char* P4 = "/tmp/mlog_sgr_4.seg";
static const char* P5 = "/tmp/mlog_sgr_5.seg";
static const char* P6 = "/tmp/mlog_sgr_6.seg";
static const char* P7 = "/tmp/mlog_sgr_7.seg";
static const char* P8 = "/tmp/mlog_sgr_8.seg";
static const char* P9 = "/tmp/mlog_sgr_9.seg";

static void remove_file(const char* p) { ::unlink(p); }

// ================================================================================
// 脚手架（同 segment_test 风格：独立 oracle，不引被测代码的 static 助手）
// ================================================================================

static bool read_file(const char* p, std::string& out) {
  int fd = ::open(p, O_RDONLY);
  if (fd < 0) return false;
  out.clear();
  char buf[4096];
  ssize_t n;
  while ((n = ::read(fd, buf, sizeof(buf))) > 0) out.append(buf, (size_t)n);
  ::close(fd);
  return n == 0;
}
static bool write_file(const char* p, const std::string& d) {
  int fd = ::open(p, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return false;
  bool ok = true;
  ssize_t off = 0;
  while (off < (ssize_t)d.size()) {
    ssize_t n = ::write(fd, d.data() + off, d.size() - off);
    if (n < 0) { ok = false; break; }
    off += n;
  }
  ::close(fd);
  return ok;
}
static bool file_len(const char* p, uint64_t& len) {
  struct stat st;
  if (::stat(p, &st) == -1) return false;
  len = (uint64_t)st.st_size;
  return true;
}
static int open_ro(const char* p) {
  return ::open(p, O_RDONLY);
}
static int open_rw(const char* p) {          // turncate 需要 fd 可写（ftruncate 需 O_RDWR）
  return ::open(p, O_RDWR);
}

// ---------- 帧构造器（独立 oracle：CRC 链=len 4B+epoch 4B+payload，不含 offset） ----------
static std::string build_frame(uint64_t offset, uint32_t epoch, const std::string& payload) {
  std::string f(20 + payload.size(), 0);
  uint64_t o = offset;
  for (int i = 7; i >= 0; i--) { f[i] = uint8_t(o & 0xFF); o >>= 8; }
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

// ---------- 段头构造器（32B：magic + version u32 + base@12 + walltime@20 + CRC@28 覆前 28B） ----------
static std::string build_seghead(uint64_t base_offset) {
  std::string h(32, 0);
  memcpy(&h[0], "PLOGSEGV", 8);
  uint32_t version = 1;
  h[8] = (version >> 24) & 0xFF; h[9] = (version >> 16) & 0xFF;
  h[10] = (version >> 8) & 0xFF; h[11] = version & 0xFF;
  uint64_t b = base_offset;
  for (int i = 7; i >= 0; i--) { h[12 + i] = uint8_t(b & 0xFF); b >>= 8; }
  uint64_t walltime = 20261012000000ULL;
  for (int i = 7; i >= 0; i--) { h[20 + i] = uint8_t(walltime & 0xFF); walltime >>= 8; }
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const uint8_t*)h.data(), 28);
  h[28] = (crc >> 24) & 0xFF; h[29] = (crc >> 16) & 0xFF;
  h[30] = (crc >> 8) & 0xFF;  h[31] = crc & 0xFF;
  return h;
}

// ---------- 损坏注入器（同 segment_test：截尾 / 翻字节 / 改长度） ----------
static bool corrupt_truncate(const char* p, uint64_t size) {
  int fd = ::open(p, O_WRONLY);
  if (fd < 0) return false;
  bool ok = ::ftruncate(fd, (off_t)size) == 0;
  ::close(fd);
  return ok;
}
static bool corrupt_flip(const char* p, uint64_t pos) {
  std::string d;
  if (!read_file(p, d) || pos >= d.size()) return false;
  d[pos] ^= 0x01;
  return write_file(p, d);
}
// 只改 len 字段（CRC 必坏，但四判据 len 第一刀先出，正好压着 at_len）
static bool corrupt_setlen(const char* p, uint64_t frame_pos, uint32_t new_len) {
  std::string d;
  if (!read_file(p, d) || frame_pos + 20 > d.size()) return false;
  d[frame_pos + 8]  = uint8_t(new_len >> 24);
  d[frame_pos + 9]  = uint8_t(new_len >> 16);
  d[frame_pos + 10] = uint8_t(new_len >> 8);
  d[frame_pos + 11] = uint8_t(new_len);
  return write_file(p, d);
}
// 改帧头某 4B 字段（epoch 测试要 CRC 同步重算，用整帧重建更稳——见判据 7）
static bool corrupt_setu32(const char* p, uint64_t pos, uint32_t v) {
  std::string d;
  if (!read_file(p, d) || pos + 4 > d.size()) return false;
  d[pos]   = uint8_t(v >> 24);
  d[pos+1] = uint8_t(v >> 16);
  d[pos+2] = uint8_t(v >> 8);
  d[pos+3] = uint8_t(v);
  return write_file(p, d);
}
static bool corrupt_setu64(const char* p, uint64_t pos, uint64_t v) {
  std::string d;
  if (!read_file(p, d) || pos + 8 > d.size()) return false;
  for (int i = 7; i >= 0; i--) { d[pos + i] = uint8_t(v & 0xFF); v >>= 8; }
  return write_file(p, d);
}

static std::string fill(size_t n, char c) { return std::string(n, c); }

int main() {
  // 判据1：工整段干净扫完（kind/vaild_end/last_epoch/corrupt_at 全绿 + 内存锚空表契约）
  {
    remove_file(P1);
    std::string d = build_seghead(1000)
                  + build_frame(1000, 1, fill(100, 'A'))
                  + build_frame(1120, 1, fill(50, 'B'))
                  + build_frame(1190, 2, fill(200, 'C'));
    CHECK(write_file(P1, d));
    int fd = open_ro(P1);
    CHECK(fd >= 0);
    Segment_recovery sc(INEVAL_OFF, fd);
    Scan_end r = sc.segment_scanner();
    CHECK(r.kind == Scan_end::clean);            // 三帧全合法 → 干净终局
    CHECK(r.vaild_end == 442);                   // 扫描终点=文件尾(32+120+70+220)
    CHECK(r.torn_end == 0);                      // clean 无撕裂点
    CHECK(r.corrupt_at == 0);                    // 无损坏定位
    CHECK(r.last_epoch == 2);                    // 段内末帧 epoch（prev_epoch 链产物）
    CHECK(sc.find_idx(1) == 0);                  // 工整段字节量小(<64KiB 起点双计) → 锚点空 → 空表回 0
    ::close(fd);
  }

  // 判据2+11：锚点节奏（先到者，此处关帧数半边只测字节判据）+ find_idx 三态
  {
    remove_file(P2);
    std::string f2 = build_seghead(1000);
    for (int k = 0; k < 6; k++) {                // 6 × 30020B 帧
      f2 += build_frame(1000 + (uint64_t)k * 30020, 3, fill(30000, 'x'));
    }
    CHECK(write_file(P2, f2));
    int fd = open_ro(P2);
    CHECK(fd >= 0);
    Segment_recovery sc(INEVAL_OFF, fd);
    Scan_end r = sc.segment_scanner();
    CHECK(r.kind == Scan_end::clean);
    CHECK(r.vaild_end == 180152);                // 32+6×30020
    CHECK(r.last_epoch == 3);
    // 字节计数：F1=30020 F2=60040 F3=90060≥64KiB → F3 落锚清零；F4..F6 同理 F6 再落
    // 锚值=锚帧绝对 offset（推进前 base_offset+(p-32)）= 1000+2×30020 / 1000+5×30020
    CHECK(sc.find_idx(61040) == 61040);          // 精确命中：回锚自己的绝对 offset
    CHECK(sc.find_idx(61041) == 61040);          // 取最大锚 ≤ 目标
    CHECK(sc.find_idx(110000) == 61040);         // 两锚之间：左锚
    CHECK(sc.find_idx(151100) == 151100);
    CHECK(sc.find_idx(200000) == 151100);        // 目标超尾：仍回尾前最大锚
    CHECK(sc.find_idx(5) == 0);                  // 首锚之前：无锚语义，回 0（段头扫起）
    ::close(fd);
  }

  // 判据3：torn@帧头——EOF 残 <20B 是安全截断不是 head_bad
  {
    remove_file(P3);
    std::string d = build_seghead(1000)
                  + build_frame(1000, 1, fill(100, 'A'))
                  + build_frame(1120, 1, fill(50, 'B'))
                  + std::string("\x01\x02\x03\x04\x05\x06\x07", 7);   // 7B 残尸
    CHECK(write_file(P3, d));
    int fd = open_ro(P3);
    CHECK(fd >= 0);
    Segment_recovery sc(INEVAL_OFF, fd);
    Scan_end r = sc.segment_scanner();
    CHECK(r.kind == Scan_end::torn_tail);        // 撕在帧头里 → 撕裂尾
    CHECK(r.torn_end == 222);                    // 起撕点=最后合法帧末(32+120+70)
    CHECK(r.vaild_end == 222);                   // 截断点同值（torn 双填契约）
    uint64_t len3 = 0;
    CHECK(file_len(P3, len3));
    CHECK(len3 == 229);                          // scanner 只读不写——盘面原样留给判据10
    ::close(fd);
  }

  // 判据10a：torn 后 turncate 到 vaild_end → 升为 clean；再截一次幂等
  {
    std::string d;
    CHECK(read_file(P3, d));
    CHECK((uint64_t)d.size() == 229);            // scanner 只读不写——T 检验它没动盘
    int fd = open_rw(P3);
    CHECK(fd >= 0);
    Segment_recovery sc(INEVAL_OFF, fd);
    sc.turncate_segment(222);
    ::close(fd);
    uint64_t len_after = 0;
    CHECK(file_len(P3, len_after));
    CHECK(len_after == 222);                     // 首截生效：229 → 222
    int fd2 = open_ro(P3);
    CHECK(fd2 >= 0);
    Segment_recovery sc2(INEVAL_OFF, fd2);
    Scan_end r = sc2.segment_scanner();
    CHECK(r.kind == Scan_end::clean);            // 截掉残尾后整段合法（安全截断语义闭环）
    CHECK(r.vaild_end == 222);
    ::close(fd2);
  }

  // 判据4：torn@payload——帧头满、payload 撕，走 else 分支
  {
    remove_file(P4);
    std::string d = build_seghead(1000)
                  + build_frame(1000, 1, fill(100, 'A'))
                  + build_frame(1120, 1, fill(100, 'C'));
    CHECK(write_file(P4, d));
    CHECK(corrupt_truncate(P4, 232));            // F2 只剩 60/100 payload → n=100 ≤ 20+len
    int fd = open_ro(P4);
    CHECK(fd >= 0);
    Segment_recovery sc(INEVAL_OFF, fd);
    Scan_end r = sc.segment_scanner();
    CHECK(r.kind == Scan_end::torn_tail);
    CHECK(r.torn_end == 152);
    CHECK(r.vaild_end == 152);
    ::close(fd);
  }

  // 判据5：payload 翻字节 → CRC 失配 → corrupt + at_crc（四刀接力最后一跳）
  {
    remove_file(P5);
    std::string d = build_seghead(1000)
                  + build_frame(1000, 1, fill(100, 'D'))
                  + build_frame(1120, 1, fill(50, 'E'));
    CHECK(write_file(P5, d));
    CHECK(corrupt_flip(P5, 32 + 20 + 37));       // 动 F1 payload 内一字节，len/epoch/offset 全数完好
    int fd = open_ro(P5);
    CHECK(fd >= 0);
    Segment_recovery sc(INEVAL_OFF, fd);
    Scan_end r = sc.segment_scanner();
    CHECK(r.kind == Scan_end::corrupt);
    CHECK(r.corrupt_at == at_crc);               // 死因=位翻转，不是长度/倒退/断档
    ::close(fd);
  }

  // 判据6：offset 断档 → corrupt + at_offset（offset 不入 CRC，改它不惊动前三刀）
  {
    remove_file(P6);
    std::string d = build_seghead(1000)
                  + build_frame(1000, 1, fill(100, 'A'))
                  + build_frame(1120, 1, fill(50, 'E'));
    CHECK(write_file(P6, d));
    CHECK(corrupt_setu64(P6, 152, 99999));       // F2 自报 offset 错——期望 1120
    int fd = open_ro(P6);
    CHECK(fd >= 0);
    Segment_recovery sc(INEVAL_OFF, fd);
    Scan_end r = sc.segment_scanner();
    CHECK(r.kind == Scan_end::corrupt);
    CHECK(r.corrupt_at == at_offset);            // 同 offset 分叉类污染的信号
    ::close(fd);
  }

  // 判据7：epoch 倒退 → corrupt + at_epoch（CRC 按 epoch5 重算，保证唯一死因是倒退刀）
  {
    remove_file(P7);
    std::string d = build_seghead(1000)
                  + build_frame(1000, 10, fill(100, 'A'))
                  + build_frame(1120, 5, fill(50, 'E'));   // 第二帧 epoch 回退但合法 CRC
    CHECK(write_file(P7, d));
    int fd = open_ro(P7);
    CHECK(fd >= 0);
    Segment_recovery sc(INEVAL_OFF, fd);
    Scan_end r = sc.segment_scanner();
    CHECK(r.kind == Scan_end::corrupt);
    CHECK(r.corrupt_at == at_epoch);             // prev_epoch 链 10>5 拦截（双写者信号）
    ::close(fd);
  }

  // 判据8：超限 len → corrupt + at_len（契约 §2.3；若红，则现行 n>20+len 前闸抢先按撕裂处置）
  {
    remove_file(P8);
    std::string d = build_seghead(1000)
                  + build_frame(1000, 1, fill(100, 'A'))
                  + build_frame(1120, 1, fill(50, 'E'));
    CHECK(write_file(P8, d));
    CHECK(corrupt_setlen(P8, 152, 2u * 1024 * 1024));
    int fd = open_ro(P8);
    CHECK(fd >= 0);
    Segment_recovery sc(INEVAL_OFF, fd);
    Scan_end r = sc.segment_scanner();
    CHECK(r.kind == Scan_end::corrupt);          // 契约：荒唐长度=损坏，不是撕裂
    CHECK(r.corrupt_at == at_len);
    ::close(fd);
  }

  // 判据9：段头 CRC 三刀不过 → head_bad（返回 0 → 调用方拒启，非撕裂）
  {
    remove_file(P9);
    std::string d = build_seghead(1000)
                  + build_frame(1000, 1, fill(100, 'A'));
    CHECK(write_file(P9, d));
    CHECK(corrupt_flip(P9, 5));                  // 打在段头 28B 覆盖域内（偏 magic 尾部）
    int fd = open_ro(P9);
    CHECK(fd >= 0);
    Segment_recovery sc(INEVAL_OFF, fd);
    Scan_end r = sc.segment_scanner();
    CHECK(r.kind == Scan_end::head_bad);         // 身份证撕了 → 拒进
    ::close(fd);
  }

  // 判据10b：turncate 幂等——同点再截再扫仍 clean
  {
    std::string d;
    CHECK(read_file(P3, d));
    CHECK((uint64_t)d.size() == 222);            // 判据10a 已把文件归到 222
    int fd = open_rw(P3);
    CHECK(fd >= 0);
    Segment_recovery sc(INEVAL_OFF, fd);
    sc.turncate_segment(222);                    // 同点再截 → no-op
    ::close(fd);
    int fd2 = open_ro(P3);
    Segment_recovery sc2(INEVAL_OFF, fd2);
    Scan_end r = sc2.segment_scanner();
    CHECK(r.kind == Scan_end::clean);
    CHECK(r.vaild_end == 222);
    ::close(fd2);
  }

  // 六场景临时文件清尾
  remove_file(P1); remove_file(P2); remove_file(P3); remove_file(P4);
  remove_file(P5); remove_file(P6); remove_file(P7); remove_file(P8);
  remove_file(P9);

  std::printf("segment_recovery_test: all green\n");
  return 0;
}

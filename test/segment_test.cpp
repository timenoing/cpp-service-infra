// segment_test · 块 2 单测（判据场景由 AI 按"验收你全写"授权代写，用户终审）
// 约定：Segment 各动作成功返回 true；check_frame 三分类 enum kind{valid,torn,corrupt}
// 每条 CHECK 前的注释写"为什么"——你回头读时审判 AI 对 design 的理解，偏了你改。
//
// 契约引用（design 02-partition-log-design.md）：
//   §2.2 段头 32B        §2.3 记录帧 16+len      §2.4 四判据三分类
//   §2.3 尾段：超限 len 归损坏           §3 读路径 CRC 失败不静默跳过
//   ：176 帧内 offset=全局绝对值，帧 n offset = base_offset + n

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <zlib.h>
#include "Segment.h"

#define CHECK(cond) do { \
  if(!(cond)) { std::printf("FAIL line %d: %s\n", __LINE__, #cond); return 1; } \
} while(0)

// ================================================================================
// 脚手架（AI 维护的工具箱；判据只召唤，不重造）
// ================================================================================
static const char* P0 = "/tmp/mlog_sg_0.seg";
static const char* P1 = "/tmp/mlog_sg_1.seg";
static const char* P2 = "/tmp/mlog_sg_2.seg";
static const char* P3 = "/tmp/mlog_sg_3.seg";
static const char* P4 = "/tmp/mlog_sg_4.seg";
static const char* P5 = "/tmp/mlog_sg_5.seg";
static const char* P6 = "/tmp/mlog_sg_6.seg";
static const char* P7 = "/tmp/mlog_sg_7.seg";
static const char* P8 = "/tmp/mlog_sg_8.seg";
static const char* P9 = "/tmp/mlog_sg_9.seg";

static void remove_file(const char* p) { ::unlink(p); }

// ---------- 文件读写原语 ----------
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

// ---------- BE 解码（读回器 oracle 用，与被测代码无关） ----------
static uint32_t be32(const uint8_t* p) {
  return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) |
         (uint32_t(p[2]) << 8)  |  uint32_t(p[3]);
}
static uint64_t be64(const uint8_t* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; i++) v = (v << 8) | p[i];
  return v;
}

// ---------- 帧构造器（独立 oracle：自带打包与同链 CRC，不引 Segment 的 static 助手） ----------
// [v1.2 ADR-003] 标准 20+len 帧：offset@0 u64 + len@8 u32 + epoch@12 u32 + crc@16
// （CRC 覆盖 len 4B + epoch 4B + payload，不含 offset）
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

// ---------- 损坏注入器（截尾 / 翻字节 / 错 len） ----------
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
// frame_pos = 帧起始字节位（含 20B 头 [v1.2]）；len 字段在 frame_pos+8 起 4B，epoch 在 +12
static bool corrupt_setlen(const char* p, uint64_t frame_pos, uint32_t new_len) {
  std::string d;
  if (!read_file(p, d) || frame_pos + 20 > d.size()) return false;
  d[frame_pos + 8]  = uint8_t(new_len >> 24);
  d[frame_pos + 9]  = uint8_t(new_len >> 16);
  d[frame_pos + 10] = uint8_t(new_len >> 8);
  d[frame_pos + 11] = uint8_t(new_len);
  return write_file(p, d);
}
// [v1.2] 错 epoch 注入：把 frame_pos+12 起 4B 的 epoch 改成 new_epoch
static bool corrupt_setepoch(const char* p, uint64_t frame_pos, uint32_t new_epoch) {
  std::string d;
  if (!read_file(p, d) || frame_pos + 20 > d.size()) return false;
  d[frame_pos + 12] = uint8_t(new_epoch >> 24);
  d[frame_pos + 13] = uint8_t(new_epoch >> 16);
  d[frame_pos + 14] = uint8_t(new_epoch >> 8);
  d[frame_pos + 15] = uint8_t(new_epoch);
  return write_file(p, d);
}

// ---------- 读回器（独立 oracle：扫帧 + 停靠判定） ----------
struct RFrame { uint64_t offset; uint32_t len; uint32_t epoch; std::string payload; };
enum class RStop { eof, torn, corrupt };
// start_pos 通常 32（跳段头）；扫到首个非合法点即停，out 只收停靠前的合法帧。
// 连续性 oracle：第 2 帧起 offset 必须 == 前帧+1、epoch 必须 ≥ 前帧 [v1.2 §2.4]，违则 corrupt。
static RStop readback(const char* p, uint64_t start_pos, std::vector<RFrame>& out) {
  std::string d;
  if (!read_file(p, d)) return RStop::torn;
  out.clear();
  uint64_t pos = start_pos;
  while (true) {
    if (pos + 20 > d.size())
      return pos == d.size() ? RStop::eof : RStop::torn;
    uint32_t len = be32((const uint8_t*)d.data() + pos + 8);
    if (len > 1024u * 1024) return RStop::corrupt;
    if (pos + 20 + len > d.size()) return RStop::torn;
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, (const uint8_t*)d.data() + pos + 8, 8);
    if (len) crc = crc32(crc, (const uint8_t*)d.data() + pos + 20, len);
    if ((uint32_t)crc != be32((const uint8_t*)d.data() + pos + 16))
      return RStop::corrupt;
    if (!out.empty() && be64((const uint8_t*)d.data() + pos) != out.back().offset + 1)
      return RStop::corrupt;
    if (!out.empty() && be32((const uint8_t*)d.data() + pos + 12) < out.back().epoch)
      return RStop::corrupt;                       // [v1.2] epoch 倒退
    RFrame f;
    f.offset = be64((const uint8_t*)d.data() + pos);
    f.len = len;
    f.epoch = be32((const uint8_t*)d.data() + pos + 12);
    f.payload = d.substr(pos + 20, len);
    out.push_back(f);
    pos += 20 + len;
  }
}

// ================================================================================
// 判据
// ================================================================================
int main() {
  // ---------------------------------------------------------------------------
  // case 0：脚手架自检——工具链自身可靠，之后判据的红绿灯才有公信力
  // ---------------------------------------------------------------------------
  {
    remove_file(P0);
    std::string f = build_frame(7, 3, "abc");
    CHECK(f.size() == 23);                                   // [v1.2] 20 头 + 3 payload
    CHECK(write_file(P0, f));
    std::vector<RFrame> out;
    CHECK(readback(P0, 0, out) == RStop::eof);               // 整帧完好 → eof
    CHECK(out.size() == 1 && out[0].offset == 7 && out[0].len == 3 &&
          out[0].epoch == 3 && out[0].payload == "abc");
    CHECK(corrupt_flip(P0, 16));                             // 翻 CRC 首字节
    CHECK(readback(P0, 0, out) == RStop::corrupt);           // 位翻转必破 CRC
    CHECK(corrupt_setlen(P0, 0, 5));                         // len→5，23B < 20+5 → torn
    CHECK(corrupt_truncate(P0, 10));                         // 截到 10B
    CHECK(readback(P0, 0, out) == RStop::torn);              // 16B 头都凑不齐
  }
  remove_file(P0);

  // ---------------------------------------------------------------------------
  // 判据① 段头五字段（§2.2）：creat → 重开 → 盘上字节用独立 oracle 逐字段核
  // 为什么不信成员自报：open_append 成功只证"验过"，盘上真实字节才是?";
  //   验证对象。段头任何字段错，判据⑥ 之后的恢复会埋雷。
  // 注意：creat() 现无 base_offset 入参（成员恒 0），非零 base 场景留给块 4
  //   partition_log 接线后补（段名/续段校验用到）。
  // ---------------------------------------------------------------------------
  {
    remove_file(P1);
    Segment* c = new Segment(P1);
    CHECK(c->creat());                      // 开新段：O_EXCL 防覆盖，写 32B 段头
    delete c;                               // 显式收 fd（析构即 close）
    Segment* r = new Segment(P1);
    CHECK(r->open_append());                // ← open 成功 = magic/version==1/CRC 三刀全过
    delete r;
    std::string hdr;
    CHECK(read_file(P1, hdr));
    CHECK(hdr.size() == 32);                                  // 段头恰 32B
    CHECK(::memcmp(hdr.data(), "PLOGSEGV", 8) == 0);          // magic
    CHECK(be32((const uint8_t*)hdr.data() + 8) == 1);         // version 固定 1
    CHECK(be64((const uint8_t*)hdr.data() + 12) == 0);        // base_offset == 开段值（现恒 0）
    CHECK(be64((const uint8_t*)hdr.data() + 20) > 0);         // walltime 非零（仅观测，FM-5）
    uLong hcrc = crc32(0L, Z_NULL, 0);
    hcrc = crc32(hcrc, (const uint8_t*)hdr.data(), 28);
    CHECK((uint32_t)hcrc == be32((const uint8_t*)hdr.data() + 28)); // CRC 覆盖前 28B
    remove_file(P1);
  }

  // ---------------------------------------------------------------------------
  // 判据② 边界尺寸 5 条（§2.3 帧上限=INV-8 的对称验证）
  // 尺寸取值：0B（帧=恰 16B）/ 1B / 64KiB / 1MiB（=messagemaxlen 满帧）/ 47B 常规。
  // 为什么袖长都要：len u32 编码位级不因尺寸变化出错才算过关，
  //   1MiB 恰好贴上限（合法），读回等值=上限帧不丢内容。
  // ---------------------------------------------------------------------------
  {
    remove_file(P2);
    Segment* s = new Segment(P2);
    CHECK(s->creat());
    const std::string p0;                                    // 0B
    const std::string p1 = "x";                              // 1B
    const std::string p64k(64 * 1024, 'a');                  // 64KiB
    const std::string p1m(1024 * 1024, 'b');                 // 1MiB 满帧（合法上界）
    const std::string p47(47, 'c');                          // 常规值
    CHECK(s->append(p0,   0, 1));                            // 帧 offset 连号 base+0..4；epoch 恒 1（v0.8 单机）
    CHECK(s->append(p1,   1, 1));
    CHECK(s->append(p64k, 2, 1));
    CHECK(s->append(p1m,  3, 1));
    CHECK(s->append(p47,  4, 1));
    CHECK(s->sysnc());                                       // fdatasync 收尾（EINTR 重试语义）
    delete s;
    std::vector<RFrame> out;
    CHECK(readback(P2, 32, out) == RStop::eof);
    CHECK(out.size() == 5);
    CHECK(out[0].offset == 0 && out[1].offset == 1 && out[2].offset == 2 &&
          out[3].offset == 3 && out[4].offset == 4);         // 序号连号
    CHECK(out[0].len == 0 && out[1].len == 1 && out[2].len == 65536 &&
          out[3].len == 1048576 && out[4].len == 47);        // 长度逐条对上
    CHECK(out[1].payload == p1 && out[2].payload == p64k &&
          out[3].payload == p1m && out[4].payload == p47);   // 内容逐条等值
    remove_file(P2);
  }

  // ---------------------------------------------------------------------------
  // 判据③ 截半条 → torn（§2.4：剩余不足以构成一帧=可修复的未写入尾，不属损坏）
  // 关键的语义检查：torn 前的帧全部合法可读回——torn 截断方案的"可不损历史"前提。
  // 白盒直调 check_frame：判定函数与文件读路径各裁一次，两边一致才闭环。
  // ---------------------------------------------------------------------------
  {
    remove_file(P3);
    Segment* s = new Segment(P3);
    CHECK(s->creat());
    CHECK(s->append(std::string(40, 'a'), 0, 1));            // [v1.2] 帧0 = 32..(20+40)
    CHECK(s->append(std::string(30, 'b'), 1, 1));            // [v1.2] 帧1 起点 = 32+60=92
    CHECK(s->append(std::string(20, 'c'), 2, 1));
    delete s;
    uint64_t f0_end = 32 + FRAME_HEAD + 40;                  // 帧0 末 = 帧1 头
    CHECK(corrupt_truncate(P3, f0_end + FRAME_HEAD + 15));   // 帧1 payload 只剩 15B（半条）
    std::vector<RFrame> out;
    CHECK(readback(P3, 32, out) == RStop::torn);
    CHECK(out.size() == 1 && out[0].offset == 0 && out[0].len == 40 &&
          out[0].payload.size() == 40);                      // 停靠前帧无损
    std::string d;
    CHECK(read_file(P3, d));
    Framecheck cf = check_frame((const uint8_t*)d.data() + f0_end,
                                d.size() - f0_end, 1, 1);  // [v1.2] 帧1 半条，期望 offset=1，prev epoch=1
    CHECK(cf.kind == Framecheck::torn);                    // 内存侧同判 torn
    CHECK(cf.torn_len == (off_t)(d.size() - f0_end));      // torn_len == avail 口径
    remove_file(P3);
  }

  // ---------------------------------------------------------------------------
  // 判据④ len 超 1MiB → corrupt（§2.3 尾段：超限 len 一律损坏类别）
  // 为什么独立于 CRC：writer 从不写超限帧（INV-8 入队前拦 + append 组帧前拦），
  //   len 超 1MiB 只可能是盘面损坏或错写 → 直接裁定，不必看 CRC。
  // 注：setlen 也会弄破 CRC，两类原因同下 corrupt——终态一致，顺序本身不可观测。
  // ---------------------------------------------------------------------------
  {
    remove_file(P4);
    Segment* s = new Segment(P4);
    CHECK(s->creat());
    CHECK(s->append(std::string(50, 'x'), 0, 1));
    delete s;
    CHECK(corrupt_setlen(P4, 32, 1024 * 1024 + 1));          // len = 1MiB+1
    std::vector<RFrame> out;
    CHECK(readback(P4, 32, out) == RStop::corrupt);          // 文件侧：一眼 corrupt
    CHECK(out.empty());                                      // 坏即停，不收帧
    std::string d;
    CHECK(read_file(P4, d));
    Framecheck cf = check_frame((const uint8_t*)d.data() + 32,
                                d.size() - 32, 0, 1);
    CHECK(cf.kind == Framecheck::corrupt);                   // 判定函数同裁
    remove_file(P4);
  }

  // ---------------------------------------------------------------------------
  // 判据⑤ offset 不连续 → corrupt（§2.4 四判据之二，判据⑤的文件侧）
  // CHECK 形态特意选"payload 与 CRC 都好、单 offset 断号"：
  //   若拿坏 CRC 帧测，corrupt 证据会被 CRC 吞掉，测不出"offset 独立判据"存在。
  // 白盒直调：帧自身完好，只是期望 offset 对不上 → corrupt。
  // ---------------------------------------------------------------------------
  {
    remove_file(P5);
    Segment* s = new Segment(P5);
    CHECK(s->creat());
    CHECK(s->append(std::string(10, 'a'), 0, 1));            // 帧0：offset 0（base=0）
    delete s;
    std::string all;
    CHECK(read_file(P5, all));
    all += build_frame(0, 1, std::string(10, 'x'));          // 手拼帧：offset 又是 0（应为 1）
    CHECK(write_file(P5, all));                              // 文件重构（段头+帧0+冒名帧0）
    std::vector<RFrame> out;
    CHECK(readback(P5, 32, out) == RStop::corrupt);
    CHECK(out.size() == 1);                                  // 帧0 合法，停靠在冒名帧
    Framecheck cf = check_frame((const uint8_t*)build_frame(0, 1, std::string(10, 'x')).data(),
                                20 + 10, 1, 1);              // [v1.2] 期望 1，实际 0
    CHECK(cf.kind == Framecheck::corrupt);
    // 注：len 通道契约=仅 valid 分支填写；corrupt 分支不填（见块 8 遗留清单：
    //   Framecheck 建议改为值初始化，防出参字段带脏值随机翻车）。
    remove_file(P5);
  }

  // ---------------------------------------------------------------------------
  // 判据⑥ 段头 magic / CRC 翻 1 字节 → open 拒（§2.2 CRC 是"段可信"的锚）
  // 为什么翻两处分开测：magic（文件身份）与 CRC（完整性）失败必须都拒开，
  //   谁都不能静默重建——否则损坏段会被"福气续写"写污染。
  // 翻回用同一 corrupt_flip（xor 幂等）：验证"灭了之后修好还能开"，而非永久残废。
  // 每次尝试用新对象：open_append 失败时不回收 fd 会复用不到，以对象粒度隔离。
  // ---------------------------------------------------------------------------
  {
    remove_file(P6);
    Segment* s = new Segment(P6);
    CHECK(s->creat());
    delete s;
    Segment* r = new Segment(P6);
    CHECK(r->open_append());                                 // 正路对照：未损应开
    delete r;
    CHECK(corrupt_flip(P6, 3));                              // 翻 magic 区字节（pos 0..7）
    Segment* m = new Segment(P6);
    CHECK(!m->open_append());                                // magic 错 → 拒
    delete m;
    CHECK(corrupt_flip(P6, 3));                              // 翻回（xor 幂等恢复）
    Segment* m2 = new Segment(P6);
    CHECK(m2->open_append());                                // 修复后应能重开
    delete m2;
    CHECK(corrupt_flip(P6, 29));                             // 翻 CRC 区字节（pos 28..31）
    Segment* h = new Segment(P6);
    CHECK(!h->open_append());                                // CRC 坏 → 拒
    delete h;
    remove_file(P6);
  }

  // ---------------------------------------------------------------------------
  // 判据⑦ 空段重开可续写（§2.5 与块 3 §4.3.7：空段恢复时清理由块 3 负责，
  //   但"空段不是损坏"是本块契约——重开能续写是它的直接证据。）
  // 续写首帧 offset 必须 == base_offset+0：空段上"期望 offset"的锚就是段头 base。
  // ---------------------------------------------------------------------------
  {
    remove_file(P7);
    Segment* s = new Segment(P7);
    CHECK(s->creat());
    delete s;
    std::vector<RFrame> out;
    CHECK(readback(P7, 32, out) == RStop::eof);              // 空段：跳过段头即 eof
    CHECK(out.empty());                                      // 0 帧
    Segment* r = new Segment(P7);
    CHECK(r->open_append());                                 // 空段合法可开
    CHECK(r->append(std::string(33, 'z'), 0, 1));            // 首帧 offset = base_offset+0
    delete r;
    CHECK(readback(P7, 32, out) == RStop::eof);              // 续写后帧可读回
    CHECK(out.size() == 1 && out[0].offset == 0 && out[0].len == 33);
    remove_file(P7);
  }

  // ---------------------------------------------------------------------------
  // 判据⑧ [v1.2 ADR-003] epoch 单调不减合法：1,1,2 → 全 valid，readback 逐帧记 epoch
  // 为什么允许 ==：同一 leader epoch 窗口内连续写多帧是常态（不是每帧换 leader）
  // ---------------------------------------------------------------------------
  {
    remove_file(P8);
    Segment* s = new Segment(P8);
    CHECK(s->creat());
    CHECK(s->append(std::string(6, 'a'), 0, 1));
    CHECK(s->append(std::string(6, 'b'), 1, 1));             // epoch 同值（合法）
    CHECK(s->append(std::string(6, 'c'), 2, 2));             // epoch 升（合法）
    delete s;
    std::vector<RFrame> out;
    CHECK(readback(P8, 32, out) == RStop::eof);
    CHECK(out.size() == 3);
    CHECK(out[0].epoch == 1 && out[1].epoch == 1 && out[2].epoch == 2); // epoch 逐帧对上
    remove_file(P8);
  }

  // ---------------------------------------------------------------------------
  // 判据⑨ [v1.2 ADR-003] epoch 倒退 → corrupt
  // 注入器只改 epoch 字段 → CRC 已破 → 这测不出"epoch 独立判据"；
  //   所以白盒直接调 check_frame（epoch=0 < prev=1，其余全好）：
  //   与判据⑤同款形态——单独证一条判据在场，防被 CRC 掩盖。
  // 文件侧用 corrupt_setepoch 后 readback 也会 corrupt（但 CRC 同坏，两因合一）
  // ---------------------------------------------------------------------------
  {
    std::string f1 = build_frame(0, 2, std::string(5, 'a'));
    std::string f2 = build_frame(1, 1, std::string(5, 'b')); // epoch 1 < 前帧 2 → 应 corruption
    std::string two = f1 + f2;
    Framecheck cf = check_frame((const uint8_t*)f2.data(), f2.size(), 1, 2);
    CHECK(cf.kind == Framecheck::corrupt);                   // 内存侧：epoch 倒退直接裁 corrupt
    (void)two;
  }
  {
    remove_file(P9);
    Segment* s = new Segment(P9);
    CHECK(s->creat());
    CHECK(s->append(std::string(5, 'a'), 0, 2));             // 帧0 epoch=2
    delete s;
    CHECK(corrupt_setepoch(P9, 32, 1));                      // 帧0 epoch 改 1（倒退）
    std::vector<RFrame> out;
    CHECK(readback(P9, 32, out) == RStop::corrupt);          // 文件侧 corrupt（CRC/epoch 同坏）
    remove_file(P9);
  }

  std::printf("segment_test: all green\n");
  return 0;
}

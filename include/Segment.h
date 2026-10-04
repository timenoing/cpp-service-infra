#ifndef SEGMENT_H
#define SEGMENT_H
#include <unistd.h>
#include <cstdint>
#include<string>
#include <fcntl.h>
#include <sys/stat.h>
#include <chrono>
#include "Message.h"
// [v1.2 ADR-003] 帧头 20B：offset@0 u64 + len@8 u32 + epoch@12 u32 + CRC@16
//   CRC 覆盖 len(4B)+epoch(4B)+payload；段内 epoch 单调不减（≥ 前帧）
#define FRAME_HEAD 20
 struct Framecheck{
  enum kind{valid,torn,corrupt} kind;
  uint32_t len=0;
  off_t torn_len=0;
};
// avail 契约：header16 起可用总字节数（含 20B 帧头）；torn ⇔ avail < 20+len，torn_len 记 avail
// prev_epoch 待改：合法帧须 epoch ≥ prev_epoch（单调不减）；首帧 prev_epoch 传 0
Framecheck check_frame(const uint8_t* header16,
                       size_t avail,
                       uint64_t expected_offset,
                       uint32_t prev_epoch);
class Segment{
  public:
  Segment(std::string path);
  ~Segment();
  Segment(const Segment&)=delete;
  Segment& operator=(const Segment& ) =delete;
  bool creat();
  bool open_append();
  bool append(const std::string& resp,uint64_t byte_off,uint32_t epoch);
  bool sysnc();
  private:
  int fd;//正在写的文件
  std::string path;
  ssize_t offset=0;
  uint64_t base_offset=0;
  uint64_t walltime=0;
  uint32_t last_epoch=0;// 段内最后写入的 epoch（单调不减判定用）
 bool  write_len(const uint8_t* buf,ssize_t len);
 bool write_len(const char* buf,ssize_t len);
};
#endif
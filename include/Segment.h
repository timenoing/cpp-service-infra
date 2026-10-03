#ifndef SEGMENT_H
#define SEGMENT_H
#include <unistd.h>
#include <cstdint>
#include<string>
#include <fcntl.h>
#include <sys/stat.h>
#include <chrono>
#include "Message.h"
 struct Framecheck{
  enum kind{valid,torn,corrupt} kind;
  uint32_t len=0;
  off_t torn_len=0;
};
Framecheck check_frame(const uint8_t* header16,
                       size_t avail,
                       uint64_t expected_offset);
class Segment{
  public:
  Segment(std::string path);
  ~Segment();
  Segment(const Segment&)=delete;
  Segment& operator=(const Segment& ) =delete;
  bool creat();
  bool open_append();
  bool append(const std::string& resp,uint64_t byte_off);
  bool sysnc();
  private:
  int fd;//正在写的文件
  std::string path;
  ssize_t offset=0;
  uint64_t base_offset=0;
  uint64_t walltime=0;
 bool  write_len(const uint8_t* buf,ssize_t len);
 bool write_len(const char* buf,ssize_t len);
};
#endif
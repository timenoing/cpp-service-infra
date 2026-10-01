#ifndef SEGMENT_H
#define SEGMENT_H
#include <unistd.h>
#include <cstdint>
#include<string>
#include <fcntl.h>
class Segment{
  public:
  Segment(std::string path);
  ~Segment();
  Segment(const Segment&)=delete;
  Segment& operator=(const Segment& ) =delete;
  bool creat();
  bool open_append();
  bool append();
  bool sysnc();
  private:
  int fd;//正在写的文件
  std::string path;
  ssize_t offset;
};
#endif
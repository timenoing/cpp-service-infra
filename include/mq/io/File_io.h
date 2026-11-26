#ifndef FILE_IO_H
#define FILE_IO_H
#include <cstdint>
#include <zlib.h>
#include<cstring>
#include<fcntl.h>
#include <unistd.h>
#include<sys/stat.h>
#include <cerrno>
#include <vector>
#include<map>
#include <string>
#include <algorithm>
static uint32_t calculate_crc(const uint8_t* data, size_t length) {
    static uint32_t crc_table[256]{};
    static bool table_empyt=true;
    if(table_empyt){
        for(uint32_t i=0;i<256;i++){
        uint32_t c = i;
            for (int j = 0; j < 8; ++j) {
                c = (c & 1) ? (0xEDB88320 ^ (c >> 1)) : (c >> 1);
            }
            crc_table[i] = c;
        }
        table_empyt=false;
    }
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; ++i) {
        crc = crc_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

static bool verify_crc(const uint8_t*data, size_t length, uint32_t received_crc) {
    uint32_t calculated_crc = calculate_crc(data, length);
    return calculated_crc == received_crc;
}
static void write_u64_be(uint8_t* p, uint64_t v) {
    p[0] = (v >> 56) & 0xFF;
    p[1] = (v >> 48) & 0xFF;
    p[2] = (v >> 40) & 0xFF;
    p[3] = (v >> 32) & 0xFF;
    p[4] = (v >> 24) & 0xFF;
    p[5] = (v >> 16) & 0xFF;
    p[6] = (v >> 8)  & 0xFF;
    p[7] =  v        & 0xFF;
}
static uint64_t read_u64_be(uint8_t* p) {
    return (uint64_t(p[0]) << 56) |
           (uint64_t(p[1]) << 48) |
           (uint64_t(p[2]) << 40) |
           (uint64_t(p[3]) << 32) |
           (uint64_t(p[4]) << 24) |
           (uint64_t(p[5]) << 16) |
           (uint64_t(p[6]) << 8)  |
            uint64_t(p[7]);
}
static void write_u32_be(uint8_t* p, uint32_t v) {
  p[0] = (v >> 24) & 0xFF;
  p[1] = (v >> 16) & 0xFF;
  p[2] = (v >> 8)  & 0xFF;
  p[3] =  v        & 0xFF;
}
static uint32_t read_u32_be(const uint8_t* p) {
  return (uint32_t(p[0]) << 24) |
         (uint32_t(p[1]) << 16) |
         (uint32_t(p[2]) << 8)  |
         (uint32_t(p[3]) << 0);
}
namespace mlog {namespace io {
 class File_io{
 public:
 virtual ~File_io()=default;
 virtual int  open(const char* path,int flags,int mod )=0;
 virtual ssize_t pwrite(int fd,const void* buf,ssize_t n,off_t offset)=0;
 virtual ssize_t pread(int fd, void* buf,ssize_t n,off_t offset)=0;
 virtual int fdatasync(int fd)=0;
 virtual int fsync_dir(const char* dirpath)=0;
 virtual int ftruncate(int fd,off_t len)=0;
 virtual int fstat(int fd, struct stat* st) const =0;
 virtual int unlink(const char* path)=0;
 virtual int close(int fd)=0;
 };
 class PosixFileIo : public File_io {
  public:
  int  open(const char* path,int flags,int mod ) override;
  ssize_t pwrite(int fd,const void* buf,ssize_t n,off_t offset) override;
  ssize_t pread(int fd, void* buf,ssize_t n,off_t offset) override;
  int fdatasync(int fd) override;
  int fsync_dir(const char* dirpath) override;
  int ftruncate(int fd,off_t len) override;
  int fstat(int fd, struct stat* st) const  override;
  int unlink(const char* path) override;
  int close(int fd) override;                           
 };
 class SimFileIo :public File_io{
    public:
 int  open(const char* path,int flags,int mod ) override;
  ssize_t pwrite(int fd,const void* buf,ssize_t n,off_t offset) override;
  ssize_t pread(int fd, void* buf,ssize_t n,off_t offset) override;
  int fdatasync(int fd) override;
  int fsync_dir(const char* dirpath) override;
  int ftruncate(int fd,off_t len) override;
  int fstat(int fd, struct stat* st) const  override;
  int unlink(const char* path) override;
  int close(int fd) override;
  void crash();
  void inject_half_write(size_t n);
  void inject_sync_fail(size_t n);
  void inject_enospc(ssize_t limit);
  private:
  struct Filestatus{
  std::vector<uint8_t> content;
  std::vector<uint8_t> durable;
  bool dirsync_durable=false;
  bool alive=true;
  };
  std::map<std::string, Filestatus>  files;
  std::map<int , std::string> handles;
  int next_fd=3;
  size_t half_write_after=SIZE_MAX;
  size_t sync_fail_after=SIZE_MAX;
  ssize_t byte_limit=-1;
  size_t sync_count=0;
  size_t write_count=0;
 };
 File_io& posix_file_io();
} 
}
#endif
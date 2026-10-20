#ifndef FILE_IO_H
#define FILE_IO_H
#include <cstdint>
#include<cstring>
#include<fcntl.h>
#include <unistd.h>
#include<sys/stat.h>
#include <cerrno>
#include <vector>
#include<map>
#include <string>
#include <algorithm>
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
#ifndef FILE_IO_H
#define FILE_IO_H
#include <cstdint>
#include<fcntl.h>
#include <unistd.h>
#include<sys/stat.h>
#include <cerrno>
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
 File_io& posix_file_io();
} 
}
#endif
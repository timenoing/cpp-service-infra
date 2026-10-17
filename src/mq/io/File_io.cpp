#include "mq/io/File_io.h"
mlog::io::File_io&  mlog::io::posix_file_io(){   
  static PosixFileIo inst;                       
  return inst;                                    
}
int mlog::io::PosixFileIo::open(const char* path,int flags,int mod){
  return ::open(path, flags, mod);
}
ssize_t mlog::io::PosixFileIo::pwrite(int fd,const void* buf,ssize_t n,off_t offset){
    return ::pwrite(fd, buf, n, offset);
}
ssize_t mlog::io::PosixFileIo::pread(int fd,void* buf,ssize_t n,off_t offset){
    return ::pread(fd, buf, n, offset);
}
int mlog::io::PosixFileIo::fdatasync(int fd){
  while (1) {
  int result=::fdatasync( fd);
  if(result==0){
   return result;
  }else {
    if(errno==EINTR)
    {continue;}
  else { return result; }
  }
  }
}
int mlog::io::PosixFileIo::fsync_dir(const char* dirpath){
  int dir_fd=::open(dirpath,  O_RDONLY | O_DIRECTORY);
  if(dir_fd==-1){return -1;}
  if(::fsync(dir_fd)==-1){::close(dir_fd); return -1; }
  if(::close(dir_fd)==-1){return -1; }
  return 0;
}
int mlog::io::PosixFileIo::ftruncate(int fd,off_t len){
     return ::ftruncate(fd, len);
}
int mlog::io::PosixFileIo::fstat(int fd, struct stat* st) const{
    return ::fstat(fd, st);
}
int mlog::io::PosixFileIo::unlink(const char* path){
  return 0;
}
int mlog::io::PosixFileIo::close(int fd){
  return ::close(fd);
}
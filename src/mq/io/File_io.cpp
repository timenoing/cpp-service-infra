#include "mq/io/File_io.h"
static std::string dir_name(const std::string& p) {
  auto pos = p.rfind('/');
  return (pos == std::string::npos) ? std::string(".") : p.substr(0, pos);
}
mlog::io::File_io&  mlog::io::posix_file_io(){     static PosixFileIo inst;                       
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

int mlog::io::SimFileIo::open(const char* path,int flags,int mod){
 if((flags & (O_CREAT|O_TRUNC)) && !files.count(path)){
  files[path]=Filestatus{};
 }else if(!files.count(path)){
  errno=ENOENT; return -1;
 }else if(flags & O_CREAT){
  errno=EEXIST; return -1;
 }else if ((flags & O_TRUNC) && files.count(path)) {
      files[path].content.clear();
      files[path].durable.clear();
  }
 handles[next_fd]=path;
 return next_fd++;
}
ssize_t mlog::io::SimFileIo::pwrite(int fd,const void* buf,ssize_t n,off_t offset){
  if(!handles.count(fd)){errno=EBADF ; return -1; }
  auto& f=files[handles[fd]];
  int writelen=n;int len=0;
  for(auto& file :files){
   len+=file.second.content.size();
  }
  if(byte_limit>=0 &&byte_limit<len+n){errno=ENOSPC; return -1;}
  write_count++;
  if(offset+n>f.content.size()){
    f.content.resize(offset+n, 0);
  }
    std::copy(static_cast<const uint8_t*>(buf), 
              static_cast<const uint8_t*>(buf) + writelen, 
              f.content.begin() + offset);  
              return writelen;
}
ssize_t mlog::io::SimFileIo::pread(int fd,void* buf,ssize_t n,off_t offset){
  if(!handles.count(fd)){errno=EBADF ; return -1; }
    auto& f=files[handles[fd]];
   if(offset >= (off_t)f.content.size()) return 0;
  size_t avail = f.content.size() - (size_t)offset;   
  size_t k = ((size_t)n < avail) ? (size_t)n : avail; 
  memcpy(buf, f.content.data() + offset, k);
  return (ssize_t)k;                                    
}
int mlog::io::SimFileIo::fdatasync(int fd){
  if(!handles.count(fd)){errno=EBADF ; return -1; }
  auto& f=files[handles[fd]];
  if(sync_fail_after==sync_count+1){
    errno=EIO; return -1;
  }
  sync_count++;
  if(half_write_after==write_count){
    auto old=f.durable.size();
    int delet; 
    delet =f.content.size()-old;
    f.durable.assign(f.content.begin(), f.content.end()-delet/2);
    write_count=0;
    return 0;
  }
  f.durable=f.content;
  return 0;
}
int mlog::io::SimFileIo::fsync_dir(const char* dirpath){
  bool exist=false;
 for(auto& file :files ){
  if( dir_name(file.first)==dirpath){
    file.second.dirsync_durable=true;
    exist=true;
  }
 }
 if(!exist){errno=ENOENT; return -1; }
 return 0;
}
int mlog::io::SimFileIo::ftruncate(int fd,off_t len){
  if(!handles.count(fd)){errno=EBADF ; return -1; }
    auto& f=files[handles[fd]];
    if(len<f.durable.size()){f.durable.resize(len); }
  f.content.resize(len);
  return 0;
}
int mlog::io::SimFileIo::fstat(int fd, struct stat* st) const{
  if(!handles.count(fd)){errno=EBADF ; return -1; }
    if(!st){errno=EFAULT; return -1; }
    auto it=handles.find(fd);
    if(it==handles.end()){
      errno=EBADF ; return -1;
    }
     auto it_file = files.find(it->second);
    if (it_file == files.end()) {
        errno = ENOENT; return -1;
    }
    auto& f = it_file->second;
    *st = {};
    st->st_size=f.content.size();
    return 0;
}
int mlog::io::SimFileIo::unlink(const char* path){
  return 0;
}
int mlog::io::SimFileIo::close(int fd){
  auto it=handles.find(fd);
  if(it==handles.end()){
   errno=EBADF ; return -1;
  }
  
  handles.erase(fd); return 0;
}
void mlog::io::SimFileIo::crash(){
  std::vector<std::string> delet;
  for(auto& it : files){
      if(it.second.dirsync_durable==false){
       delet.push_back(it.first);
      }
      it.second.content=it.second.durable;
  }
  for(auto& it : delet){
    files.erase(it);
  }
  handles.clear();
   half_write_after=SIZE_MAX;
   sync_fail_after=SIZE_MAX;
   byte_limit=-1;
   sync_count=0;
   write_count=0;
   next_fd=3;
}
void mlog::io::SimFileIo::inject_enospc(ssize_t limit){
  byte_limit=limit;
}
void mlog::io::SimFileIo::inject_half_write(size_t n){
  half_write_after=n;
  write_count=0;
}
void mlog::io::SimFileIo::inject_sync_fail(size_t n){
  sync_fail_after=n;
  sync_count=0;
}
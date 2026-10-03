#include "Segment.h"
#include <zlib.h>
#include <cstring>
#include <cerrno>

Segment::Segment(std::string pat){
  fd=-1;
  path=pat;
}
Segment::~Segment(){
    if(fd>=0) {close(fd);}
}
bool Segment:: write_len(const uint8_t* buf,ssize_t len){
  ssize_t n= pwrite(fd, buf, len,  offset);
  if(n==-1){
    return false;
  }else if(n<len){
   ftruncate(fd, offset);
   return false;
  }
  else {
    offset+=len;
    return true;
  }
}
bool Segment:: write_len(const char* buf,ssize_t len){
  ssize_t n= pwrite(fd, buf, len,  offset);
  if(n==-1){
    return false;
  }else if(n<len){
   ftruncate(fd, offset);
   return false;
  }
  else {
    offset+=len;
    return true;
  }
}
static uint64_t now_millis() {
    using namespace std::chrono;
    return static_cast<uint64_t>(
        duration_cast<milliseconds>(
            system_clock::now().time_since_epoch()
        ).count()
    );
}
static uint32_t read_crc_big(const uint8_t* p) {
  return (uint32_t(p[0]) << 24) |
         (uint32_t(p[1]) << 16) |
         (uint32_t(p[2]) << 8)  |
         (uint32_t(p[3]) << 0);
}
static uint64_t read_u64_be(const uint8_t* p) {
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
// 段头 32B：magic(8) + version@8 u32 + base_offset@12 u64 + walltime@20 u64 + CRC@28（覆盖前 28B）
static void write_seghead_bg(uint8_t* buf, uint32_t version,
                             uint64_t base_offset, uint64_t walltime) {
  memcpy(buf, "PLOGSEGV", 8);
  write_u32_be(buf + 8, version);
  write_u64_be(buf + 12, base_offset);
  write_u64_be(buf + 20, walltime);
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, buf, 28);
  write_u32_be(buf + 28, (uint32_t)crc);
}
// 帧头 16B：offset@0 u64 + len@8 u32 + CRC@12（覆盖 len 的 4B + payload，不含 offset）
static void write_frame_bg(uint8_t* buf, uint64_t offset,
                           const std::string& payload) {
  uint32_t len = (uint32_t)payload.size();
  write_u64_be(buf, offset);
  write_u32_be(buf + 8, len);
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, buf + 8, 4);
  if (len) crc = crc32(crc, (const uint8_t*)payload.data(), len);
  write_u32_be(buf + 12, (uint32_t)crc);
}
// 验段头：magic + version==1 + CRC 三刀；过刀吐 base_offset/walltime 给 open_append 入库
static bool compare_magic_crc(const uint8_t* buf,
                              uint64_t* base_offset, uint64_t* walltime) {
  if (memcmp(buf, "PLOGSEGV", 8) != 0) return false;
  if (read_crc_big(buf + 8) != 1) return false;
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, buf, 28);
  if ((uint32_t)crc != read_crc_big(buf + 28)) return false;
  if (base_offset) *base_offset = read_u64_be(buf + 12);
  if (walltime)    *walltime   = read_u64_be(buf + 20);
  return true;
}
bool Segment::creat(){
 fd=open(path.c_str(), O_CREAT|O_EXCL|O_WRONLY, 0644);
 if(fd==-1){
  return false;
 }
 uint8_t buf[32];
 write_seghead_bg(buf, 1, base_offset, now_millis());
 if(write_len(buf, 32)){ return true; }
 else{ftruncate(fd, offset); return false;}
 }
bool Segment::open_append(){
 fd=open(path.c_str(), O_RDWR, 0644);
 if(fd==-1){
  return false;
 }
 uint8_t buf[32];
 if(pread(fd,buf,32,0)!=32){ return false;}
 if(!compare_magic_crc(buf,&base_offset,&walltime)){return false; }
 struct stat st;
 if(fstat(fd,&st)==-1){
  return false;
 }
 offset=st.st_size;
 return true;
}
bool Segment::append(const std::string& resp,uint64_t byte_off){
  uint8_t buf[16];
  std::string payload =std::move(resp);
  ssize_t len=payload.size();
  if(len>net::messagemaxlen) return false;
  write_frame_bg(buf, byte_off, payload);
 if(write_len(buf, 16)) {
  if(write_len(payload.c_str(), len)){
  return true;
  } else{ offset-=16;  ftruncate(fd, offset); return false;}
  }else{return false; }
}
bool Segment::sysnc(){
    while(1){
    if(::fdatasync(fd)==-1){
      if(errno==EINTR){ continue;}
      else return false;
    }else{ return true;}
   }
}
Framecheck  check_frame(const uint8_t* header16,size_t avail,uint64_t expected_offset){
  Framecheck check;
  if(read_crc_big(header16+8)>net::messagemaxlen){check.kind=Framecheck::corrupt; return check; }
  uint32_t len= read_crc_big(header16+8);
   if(avail<len+16) {check.kind=Framecheck::torn;check.torn_len=avail; return check;}
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc,  header16+8, 4);
  crc = crc32(crc, header16+16, len);
  if(crc!=read_crc_big(header16+12)){check.kind=Framecheck::corrupt; return check;}
  if(read_u64_be(header16)!=expected_offset){check.kind=Framecheck::corrupt; return check;}
  else {check.kind=Framecheck::valid; check.len=len; return check;}
  
}
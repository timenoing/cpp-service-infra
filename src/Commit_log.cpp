#include"Commit_log.h"
mlog::Commitlog::Commitlog(std::string pat){
  path=pat;
  fd=-1;
}
mlog::Commitlog::~Commitlog(){
 if(fd>0) ::close(fd);
}
static uint32_t read_crc_big(uint8_t* p){
  return (uint32_t(p[0])<<24) |
          (uint32_t(p[1])<<16) |
          (uint32_t(p[2])<<8) |
          (uint32_t(p[3])<<0);
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
static void write_u32_be(uint8_t* p,uint32_t v){
    p[0]=(v >>24)& 0xFF;
    p[1]=(v >>16)& 0xFF;
    p[2]=(v >>8)& 0xFF;
    p[3]= (v)& 0xFF;  
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
bool mlog::Commitlog::open_or_create(Commitlog::Reply& out){
  fd=::open(path.c_str(),O_RDWR| O_CREAT,0644);
  if(fd==-1){
    return false;
  }
  uint8_t buf[36]={0};                                         
  struct stat st;
  int len=::fstat(fd, &st);
  if(len==0){ len=st.st_size;} else{return false; }
  uint64_t prev_batch_no=0;
  uint64_t prev_state=0;
  while(1){
    if(len-offset<36){
      int i=::ftruncate(fd, offset);
      break;
    }
    if(::pread(fd,buf,36,offset)<0)
    {out.corrupt=true; return false;}
    uint32_t crc_cmp=read_crc_big(buf+32);
  uLong crc=crc32(0,Z_NULL,0);
  crc=crc32(crc,buf,32);
  uint32_t c=static_cast<uint32_t>(crc);
  if(c!=crc_cmp){
      out.corrupt=true;
      return false;
  }else{
    offset+=36;
    if(prev_batch_no==read_u64_be(buf)){
      if(prev_state>buf[24]||buf[24]>2) { out.corrupt=true; return false;}
      prev_state=buf[24];
    out.last_batch_no=read_u64_be(buf);
     if(buf[24]>=1&&out.credit_hw<read_u64_be(buf+8))
    out.credit_hw=read_u64_be(buf+8);
    out.last_epoch=read_u64_be(buf+16);
    }else if(prev_batch_no+1==read_u64_be(buf)){
      prev_state=buf[24];
      prev_batch_no=read_u64_be(buf);
      if(buf[24]>2) { out.corrupt=true; return false;}
      out.last_batch_no=read_u64_be(buf);
      if(buf[24]>=1&&out.credit_hw<read_u64_be(buf+8))
    out.credit_hw=read_u64_be(buf+8);
    out.last_epoch=read_u64_be(buf+16);
    }else{
    out.corrupt=true;
    return false;
    }
  }
  }
 return true;
}
bool mlog::Commitlog::append(){
  if(fd<0) return false;
 uint8_t buf[36]={0};
 write_u64_be(buf, log.batch_no);
 write_u64_be(buf+8, log.candidate_hw);
 write_u64_be(buf+16, log.epoch);
 buf[24]=log.state;
 uLong crc=crc32(0,Z_NULL,0);
crc=crc32(crc,buf,32);
uint32_t c=static_cast<uint32_t>(crc);
 write_u32_be(buf+32,c );
 ssize_t n=::pwrite(fd,buf,36,offset);
 if(n==36){offset+=n;  return true;}
 else if(n==-1)return false;
 else{int i=::ftruncate(fd, offset); return false;}
 
}
bool mlog::Commitlog::sync(){
   while(1){
    if(::fdatasync(fd)==-1){
      if(errno==EINTR){ continue;}
      else return false;
    }else{ return true;}
   }
}
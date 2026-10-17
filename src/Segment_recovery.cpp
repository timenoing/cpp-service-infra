#include "Segment_recovery.h"
static uint64_t read_segment32(mlog::io::File_io& io, int fd ){
   uint8_t buf[32];
 if(io.pread(fd, buf, 32, 0)!=32){ return 0; }
 uint64_t base_offset=read_u64_be(buf+12);
 uint64_t walltiem =read_u64_be(buf+20);
 if ( !compare_magic_crc(buf, &base_offset,&walltiem )){return 0; }
 else{ return base_offset; }
}
Segment_recovery::Segment_recovery(int inteval_,int fd_,mlog::io::File_io& io): io_(io){
 inteval=inteval_;
 fd=fd_;
  buf= new uint8_t [20+net::messagemaxlen] ;
}
Segment_recovery::~Segment_recovery(){
 if(fd>=0) { fd=-1;}
 delete [] buf;
}
void Segment_recovery::turncate_segment(uint64_t offset){
    io_.ftruncate(fd, offset);
}
void Segment_recovery::server_idx(uint64_t offset){
  idx.push_back(offset);
}
uint64_t Segment_recovery::find_idx(const uint64_t idx_){
  if(idx.empty()){return 0; }
  int left=0; int right=idx.size()-1;
  int result_idx=-1;
  while(left<=right){
    int medium=left + (right-left)/2;
    if(idx_==idx[medium]){
      return idx[medium];
    }else if(idx_>idx[medium]){
     left=medium+1;
     result_idx=idx[medium];
    }else{
      right=medium-1;
    }
  }
  if(result_idx!=-1){
    return result_idx;
  }else{
    return 0;
  }
}
Scan_end Segment_recovery::segment_scanner(){
    uint64_t base_offset=read_segment32(io_, fd);
    uint64_t times=0;
    uint64_t byte_since=0;
    struct stat st;
    Scan_end ans;
    if(io_.fstat(fd, &st)==-1){ ans.kind=Scan_end::corrupt; return ans; }
    auto filelen=st.st_size;
    if(base_offset==0){ans.kind=Scan_end::head_bad; return ans;}
    while(1){
     auto  want=20+std::min(static_cast<uint64_t>(net::messagemaxlen), 
                          static_cast<uint64_t>(filelen - offset));
    int n=io_.pread(fd,buf,want,offset);
    int len=read_crc_big(buf+8);
    if(len>net::messagemaxlen){
    ans.kind = Scan_end::corrupt;
  ans.corrupt_at = at_len;    
  return ans;
    }
    if(n<20){ans.torn_end=offset;ans.vaild_end=offset; ans.kind=Scan_end::torn_tail; return ans; }
    if(n>=20+len){
    Framecheck result= check_frame(buf,  n, base_offset+offset-32, prev_epoch);
    ans.corrupt_at=result.corrupt_at;
    if(result.kind==Framecheck::corrupt){ans.kind=Scan_end::corrupt;   return ans; }
    if(result.kind==Framecheck::torn){ans.torn_end=offset;ans.vaild_end=offset; ans.kind=Scan_end::torn_tail; return ans;}
    if(result.kind==Framecheck::valid){
      times++;byte_since+=20+len;  ans.last_epoch=result.epoch;
        if(times%inteval==0||byte_since>=1024*64) {times=0;byte_since=0;   server_idx(base_offset + (offset - 32)); }
        offset+=result.len+20;
        prev_epoch=result.epoch; ans.vaild_end=offset;
        if( ans.vaild_end==filelen) {ans.kind=Scan_end::clean; return ans;}

  }
    }else{
      ans.torn_end=offset;ans.vaild_end=offset; ans.kind=Scan_end::torn_tail; return ans;
    }
    }
 
}
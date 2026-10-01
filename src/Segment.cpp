#include "Segment.h"
Segment::Segment(std::string pat){
  fd=-1;
  path=pat;
}
Segment::~Segment(){
    if(fd>0) {close(fd);}
}
static void write_seghead_bg(uint8_t* buf,uint32_t* version,uint64_t* base_offset,uint64_t* creat_walltime ){
  
}
static void write_frame_bg(uint8_t* buf,uint64_t* offset,uint32_t* len){

}
bool Segment::creat(){
 fd=open(path.c_str(), O_CREAT|O_EXCL, 0644);
 if(fd==-1){
  return false;
 }
 uint8_t buf[36];
 write_seghead_bg(buf, uint32_t *0, uint64_t* offset, uint64_t *creat_walltime);
 while(1){
 int n=write(fd, buf, 36);
 if(n==36){
    return true;
 }else if(n==-1){
  if(errno==EINTR){continue;}
  else { return false;}
 }else {return false;}
 }
}
bool Segment::open_append(){

}
bool Segment::append(){

}
bool Segment::sysnc(){
    
}
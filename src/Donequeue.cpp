 #include "Donequeue.h"
 void Donequeue::push(int fd,uint64_t id,std::string resp){
   {
   std::lock_guard<std::mutex> lk(mut);
   
   queue.push_back({fd,id,std::move(resp)});
   }
   uint64_t len=1;
   int n=write(wake_fd, &len, sizeof(len));
   if(n==-1&&errno!=EAGAIN){
    
   }
 }
 std::deque<Donequeue::doit_item>  Donequeue::drain(){
    std::lock_guard<std::mutex> lk(mut);
   return std::move(queue);
 }
 int Donequeue::fd() const
 {
  return wake_fd;
 }
 Donequeue::Donequeue()
 {
  wake_fd=eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
 }
 Donequeue::~Donequeue(){
  if(wake_fd>0)
  close(wake_fd);
 }
#include "mq/raft/raft_inbox.h"
Raft_inbox::Raft_inbox(){

}
Raft_inbox::~Raft_inbox(){
   close();
}
bool Raft_inbox::try_push(inboundmsg m){
   {
    std::unique_lock<std::mutex> lock(mu_);
     if(stopping) {return false;}
    if(queued_bytes+m.bytes>max_bytes ||tasks.size() >=max_size) return false;
    queued_bytes+=m.bytes;
    tasks.push_back(std::move(m));
   }
   cv_.notify_one();
   return true;
}
Popstatic Raft_inbox::wait_pop(inboundmsg &m, uint32_t time_out){
   {
    std::unique_lock<std::mutex> lock(mu_);
    if(stopping) {return Popstatic::Close;}
    if(!tasks.empty()){ 
        m=std::move(tasks.front());
        tasks.pop_front();
        queued_bytes-=m.bytes;
        return Popstatic::Ok;
    }
     cv_.wait_until( lock,std::chrono::steady_clock::now() + std::chrono::milliseconds(time_out)
     ,[this]{
        return !tasks.empty()||stopping;
    });
    if(stopping) {return Popstatic::Close; }
    if(!tasks.empty()){ 
        m=std::move(tasks.front());
        tasks.pop_front();
        queued_bytes-=m.bytes;
        return Popstatic::Ok;
    }
}
    return Popstatic::Timeout;
}
void Raft_inbox::close(){
   {
    std::unique_lock<std::mutex> lock(mu_);
    queued_bytes=0;
    stopping=true;
    tasks.clear();
    cv_.notify_all();
   }
}
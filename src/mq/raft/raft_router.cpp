#include "mq/raft/raft_router.h"
static bool read_be32(const unsigned char* p, uint32_t& v) {
  v = (static_cast<uint32_t>(p[0]) << 24) |
      (static_cast<uint32_t>(p[1]) << 16) |
      (static_cast<uint32_t>(p[2]) << 8) |
      static_cast<uint32_t>(p[3]);
  return true;
}
Raft_router::Raft_router(std::unordered_map<uint32_t, std::pair<std::string , uint32_t >> map_,int port)
 :map(map_){

}
bool Raft_router::start(int port){
    epoll_.set_handler([this](const std::string payload)->std::string {
        if(payload.size()<9) return {};
        uint32_t to=0;
        if(read_be32(reinterpret_cast<const unsigned char*>(payload.c_str()), to)){
          auto it=out.find(to);
          if(it!=out.end()){
            it->second->send_raw(std::move(payload));
          }
        }
        return {};
    });
    for(auto& p :map){
      std::unique_ptr<PeerSender> m=std::make_unique<PeerSender>(0,p.second.first,p.second.second);
       if(!m->start()) return false;
      out.emplace(p.first,std::move(m));
    }
     epoll_.start(port);
     return true;
     }

    

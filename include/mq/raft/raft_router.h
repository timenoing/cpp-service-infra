#ifndef RAFT_ROUTER_H
#define RAFT_ROUTER_H
#include "Epoll.h"
#include "mq/raft/peer_sender.h"
#include <unordered_map>
class Raft_router{
    public:
    Raft_router(std::unordered_map<uint32_t, std::pair<std::string , uint32_t >>  map,int port);
    Epoll epoll_;
    bool start(int port);
    private:
    int port=0;
     bool stopping=false;
     std::unordered_map<uint32_t, std::pair<std::string , uint32_t >> map;
     std::unordered_map<uint32_t, std::unique_ptr<PeerSender>> out;
     std::condition_variable cv_;
     std::mutex mu_;
};
#endif
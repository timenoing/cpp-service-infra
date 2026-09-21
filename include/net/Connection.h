#ifndef CONNECTION_H
#define CONNECTION_H
#include <cstddef>
#include<sys/socket.h>
#include<sys/epoll.h>
#include<errno.h>
#include<stdio.h>
#include<sys/types.h>
#include<unistd.h>
#include<netinet/in.h>
#include<fcntl.h>
#include<string.h>
#include<arpa/inet.h>
#include"ThreadPool.h"
#include <Message.h>
#include <csignal>
#include"Logger.h"
#include <unordered_set>
#include <functional>
#include "Donequeue.h"
namespace net {
class Connection{
    public:
     Connection(int epfd,int fd,uint64_t conn_id,ThreadPool* poll,Donequeue* done, std::function<std::string (const std::string&)> handler);
     ~Connection();
 struct fd_status{
      bool live;
      std::vector<char> Outbuffer;
      int idx=0;
      net::messagedecode decoder;
      bool peer_close=false;
    };
    fd_status status;
    uint64_t conn_id;
    bool handreadable();
    bool handwrite();
    void set_event(uint32_t event);
    bool senddata(const std::string& data);
    int inflight=0;
    private:
    int fd;
    int epfd;
    bool closed=false;
    std::function<std::string (const std::string&)> handler;
    ThreadPool *pool;
    Donequeue* done;
};
}
#endif
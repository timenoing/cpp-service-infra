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
namespace net {
class Connection{
    public:
     Connection(int epfd,int fd);
     ~Connection();
 struct fd_status{
      bool live;
      std::vector<char> Outbuffer;
      int idx=0;
      net::messagedecode decoder;
      bool peer_close=false;
    };
    fd_status status;
    bool handreadable();
    bool handwrite();
    void set_event(uint32_t event);
    bool senddata(const std::string& data);
    private:
    int fd;
    int epfd;
    bool closed=false;
    socklen_t client_len;
    struct epoll_event ev;//兴趣列表
};

}


#endif
#ifndef EPOLL_H
#define EPOLL_H
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
#include "net/Connection.h"
class Epoll{
    public:
    Epoll(); 
    ~Epoll();
    bool start(int port);//创建变量函数
    private:
    ThreadPool epoll_poll;//线程池
    int  set_nonblocking(int fd);//阻塞函数
    int  create_fd(int a); //套接字函数，a为端口号
    void addevent(int fd, int event,int cancelevent,net::Connection& it);
    void onaccept(int fd);
    void onclose(int fd);
    int epfd;
    int m_listen;
    struct epoll_event ev64[64];
    struct sockaddr_in client_addr;
    std::unordered_map<int, net::Connection> status;
    socklen_t client_len;
    struct epoll_event ev;//兴趣列表
};
#endif
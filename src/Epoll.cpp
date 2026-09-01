#include "ThreadPool.h"
#include<Epoll.h>
#include <unistd.h>
#include <vector>
int Epoll::set_nonblocking(int fd) { //设置阻塞
          int flags = fcntl(fd, F_GETFL, 0);  // 1. 先拿到当前的文件状态标志
          if (flags == -1) {
          perror("fcntl F_GETFL");
          return -1;
          }
         if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {  // 2. 在原有标志上加上非阻塞
         perror("fcntl F_SETFL O_NONBLOCK");
         return -1;
          }
         return 0;
         }
int Epoll::create_fd(int a){ //放在那个端口上
    struct sockaddr_in addr;
    int m_listen=socket(AF_INET,SOCK_STREAM,0);
    if(m_listen<0)
    {
        close(m_listen);
        return -1;
    }
    addr.sin_family=AF_INET;
    addr.sin_port=htons(a);
    addr.sin_addr.s_addr=INADDR_ANY;
    if(bind(m_listen, (struct sockaddr*)&addr,sizeof(addr))<0)
    {
      close(m_listen);
    return -1;
    }
    if(listen(m_listen, 128)<0)
    {
        close(m_listen);
        return -1;
    }
    return m_listen;
}
bool Epoll::start(int port){
   epfd=epoll_create1(0); //一个收发室
   m_listen= this-> create_fd(port); //创建一个fd端口号为8888
    if(m_listen<0)
   {
   return false;
   }
   if(epfd<0)
   {
    perror("epoll_create falt");
    close(epfd);
    close(m_listen);
    return false;
   
   }
   ev.events = EPOLLIN;//监听新连接
   ev.data.fd = m_listen;
   if(epoll_ctl(epfd,EPOLL_CTL_ADD , m_listen, &ev)<0)
   {
    close(m_listen);
    close(epfd);
    return false;
   }
   printf("成功启动\n");
   client_len=(sizeof(client_addr));
   while(1)
   {
     int nfds =epoll_wait(epfd, ev64, 64, -1);
     if(nfds<0) //失效了
     {
        break;
     }else if(nfds>0) //收到了连接
    {
         for(int i=0;i<nfds;++i)
    {
         printf("事件 %d: fd = %d\n", i, ev64[i].data.fd);
        if(ev64[i].data.fd==m_listen)
        {
          int client=accept(m_listen,(struct sockaddr*)&client_addr, (socklen_t *)&client_len);//取出第一个连接
           if(set_nonblocking(client)==-1)
           {
            close(client);
            continue;
           }
           ev.events = EPOLLIN;//监听新连接
           ev.data.fd = client;
           char ip[INET_ADDRSTRLEN];
           inet_ntop(AF_INET,&client_addr.sin_addr,ip,sizeof(ip));
           int port=ntohs(client_addr.sin_port);
           printf("连接到了 ip为%s 端口为%d\n",ip,port);
           epoll_ctl(epfd, EPOLL_CTL_ADD, client, &ev);
        }else if(ev64[i].data.fd!=m_listen)
        {
           int fd=ev64[i].data.fd;
           char buf[1024];
           int n=recv(fd,buf,sizeof(buf)-1,0);
           if(n>0)
           {
            buf[n]='\0';
            std::vector<char> data(buf,buf+n);
            printf("收到 %d 字节",n);
            epoll_poll.submit([fd,data=std::move(data)](){
                send(fd,data.data(),data.size(),0);
            });
           }else if(n==0)
           {
            epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
            close(fd);
            printf("删除连接");
           }else{
            perror("连接出错");
            if(errno!=EAGAIN)
            {
            epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
            close(fd);
            perror("recv错误");
            }
           }
        }
    }
    }
   }
    close(m_listen);
    close(epfd);
    return true;

}
Epoll::Epoll() :epoll_poll(4) 
{
  m_listen=-1;
  epfd=-1;
 client_len=(sizeof(client_addr));
}
Epoll::~Epoll(){
  if(m_listen>0)
  {
    close(m_listen);
  }
  if(epfd>0)
  {
    close(epfd);
  }
}

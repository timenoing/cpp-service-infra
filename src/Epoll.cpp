#include<Epoll.h>
#include "ThreadPool.h"
#include <unistd.h>
#include <vector>
bool Epoll::senddata(const std::string& data,int fd,fd_status& it)
{
int achieve=send(fd,data.data(),data.size(),0);
       if(achieve<data.size())
    {
      if(achieve==-1 )
      {
      if (errno == EAGAIN||errno == EWOULDBLOCK ||errno == EINTR ){
        it.Outbuffer.insert(it.Outbuffer.end(),data.begin(),data.end());
      set_event(fd, EPOLLIN |EPOLLOUT);
      return true;
      }
      onclose(fd);
      return false;
      }else{
        it.Outbuffer.insert(it.Outbuffer.end(),data.begin()+achieve,data.end());
        set_event(fd, EPOLLIN |EPOLLOUT);
      }
    }
    return true;
}
void Epoll::onreadable(int fd,int event)
{
    char buf[1024*64];
    while(1){
      int n=recv(fd, buf, sizeof(buf), 0);
      auto& it=status.find(fd)->second;
      if(n>0)
    {
    it.decoder.feed(buf, n);
    bool pass=it.decoder.brokenmessage();
    if(pass!=true){
    std::vector<std::string> msg=it.decoder.take();
    auto its=msg.size();
    std::string data;
    for(int i=0;i<its;++i)
    {
     data=net::encode(msg[i]);
     if(senddata(data, fd, it))
     {
      
     }else{
      return;
     }
    }
    }
    else{ onclose(fd); return;}
    continue;
    }else if(n==0){
    it.peer_close = true;
    bool pass=it.decoder.brokenmessage();
    if(pass!=true){
    std::vector<std::string> msg=it.decoder.take();
    auto its=msg.size();
    std::string data;
    for(int i=0;i<its;++i)
    {
     data=net::encode(msg[i]);
     if(senddata(data, fd, it))
     {
      
     }else{
      return;
     }
    }
    }
    if(it.Outbuffer.empty()){onclose(fd); return;}
    else{ set_event(fd, EPOLLOUT);  return;}
    }else{
    if(errno!=EAGAIN&&errno!=EINTR)
    {
    onclose(fd);
    }
    return;
  }
  }
  
}
void Epoll::onwrite(int fd,int &event)
{
  ssize_t n=0;
  auto& it=status.find(fd)->second;
  while(it.idx<it.Outbuffer.size())
  {
     n=send(fd,it.Outbuffer.data()+it.idx,it.Outbuffer.size()-it.idx,0);
    if(n==(it.Outbuffer.size()-it.idx))
    {
      it.idx=0;
      it.Outbuffer.erase(it.Outbuffer.begin(),it.Outbuffer.end());
      if(it.peer_close==true){onclose(fd); return;}
      set_event(fd, EPOLLIN);
      return ;
    }else if(n==-1){
      if (errno ==EAGAIN|| errno == EWOULDBLOCK ||errno == EINTR){
      return;
    }
      onclose(fd);
      return;
  }else if(n<it.Outbuffer.size()-it.idx){
      it.idx+=n;
      continue;
    }
  
  }

}
void Epoll::onaccept(int fd)
{
   client_len=(sizeof(client_addr));
    int client=accept(m_listen,(struct sockaddr*)&client_addr, (socklen_t *)&client_len);//取出第一个连接
    if(client<0)
    {
      if(errno==EAGAIN||errno==EWOULDBLOCK)
      {
        return ;
      }else if (errno == EMFILE || errno == ENFILE){
          return ;
      }else{ //以后分配到线程池的任务
       return;
      }
    }
      if(set_nonblocking(client)==-1)
      {
      close(client);
      return ;
      }
      ev.events = EPOLLIN;//监听新连接
      ev.data.fd = client;
      char ip[INET_ADDRSTRLEN];
      inet_ntop(AF_INET,&client_addr.sin_addr,ip,sizeof(ip));
      int port=ntohs(client_addr.sin_port);
      printf("连接到了 ip为%s 端口为%d\n",ip,port);
      epoll_ctl(epfd, EPOLL_CTL_ADD, client, &ev);
      struct fd_status fd_status;
      fd_status.live=true;
      status.emplace(client,fd_status);
}
void Epoll::onclose(int fd)
{
   epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
    status.erase(fd);
}
void Epoll::set_event(int fd,uint32_t event)
{
    struct epoll_event ev;
    ev.events=event;
    ev.data.fd=fd;
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
}
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
    int mlisten=socket(AF_INET,SOCK_STREAM,0);
    if(mlisten<0)
    {
        return -1;
    }
    addr.sin_family=AF_INET;
    addr.sin_port=htons(a);
    addr.sin_addr.s_addr=INADDR_ANY;
    int opt = 1;
    if (setsockopt(mlisten, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt SO_REUSEADDR"); // 打印但不要返回，不影响主要功能
    }
    if(bind(mlisten, (struct sockaddr*)&addr,sizeof(addr))<0)
    {
      close(mlisten);
    return -1;
    }
    if(listen(mlisten, 128)<0)
    {
        close(mlisten);
        return -1;
    }
    return mlisten;
}
void Epoll::addevent(int fd,int event,int cancelEvent)
{
 if(fd==m_listen)
 {
      onaccept(fd);
      return;     
 }else if(event & (EPOLLERR | EPOLLHUP))
 {
    onclose(fd);
    return;
 }else if(event & EPOLLRDHUP)
 {
    onclose(fd);
    return;
 }else if(event & EPOLLIN)
 {
  
  onreadable( fd, event);
  return;
   
}else if(event & (EPOLLIN |EPOLLOUT))
{
   onwrite( fd, event);
  return;
}
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
   if(set_nonblocking(m_listen)==-1)
           {
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
         if(errno==EINTR)
         {
            continue;
         }
         break;
     }else if(nfds>0) //收到了连接
     {
     
         for(int i=0;i<nfds;++i)
    {
      int fd=ev64[i].data.fd;
      int event_flags=ev64[i].events;
      if(fd==m_listen||status.find( fd)!=status.end())
      {
        int fd=ev64[i].data.fd;
        addevent(fd, event_flags, event_flags);
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
  signal(SIGPIPE, SIG_IGN);
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

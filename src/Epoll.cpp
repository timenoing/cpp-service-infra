#include<Epoll.h>
#include "ThreadPool.h"
#include <unistd.h>
#include <vector>
static std::vector<Epoll*>g_instance;
static std::mutex g_mutex;
void Epoll::set_handler(std::function<std::string (const std::string &)> messge_handler)
{
  message_handler=messge_handler;
}
void Epoll::onaccept(int fd)
{
   while(1){
     if(fd<0) return;
   client_len=(sizeof(client_addr));
    int client=accept(m_listen,(struct sockaddr*)&client_addr, (socklen_t *)&client_len);//取出第一个连接
    if(client<0)
    {
      if(errno==EAGAIN||errno==EWOULDBLOCK)
      {
        return ;
      }else if (errno == EMFILE || errno == ENFILE){
        std::lock_guard<std::mutex> lock(g_mutex);
        close(idle_fd);
        int client=accept(m_listen,(struct sockaddr*)&client_addr, (socklen_t *)&client_len);
        close(client);
        idle_fd=open("/dev/null", O_RDONLY);
        return;
      }else{ //以后分配到线程池的任务
       return;
      }
    }
    int opt = 1;
    setsockopt(client, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
      if(set_nonblocking(client)==-1)
      {
      close(client);
      return ;
      }
      ev.events = EPOLLIN| EPOLLRDHUP;//监听新连接
      ev.data.fd = client;
      epoll_ctl(epfd, EPOLL_CTL_ADD, client, &ev);
      conn_id++;
      status.emplace(client,net::Connection(epfd,client,conn_id,&epoll_poll,&done,message_handler));
   }
}
void Epoll::onclose(int fd)
{
   epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
    status.erase(fd);
    close(fd);
}
void Epoll::ondone(){
  uint64_t cut; 
  read(done.fd(),&cut,sizeof(cut));
  for( auto& item : done.drain()){
    auto it=status.find(item.fd);
    if(it==status.end()||it->second.conn_id!=item.id) { continue; }
    net::Connection& con=it->second;
    --con.inflight;
    if(!con.senddata(item.resp))
    { 
      onclose(item.fd);
      continue;
    }
    if(con.status.peer_close==true&&con.status.Outbuffer.empty()&& con.inflight==0)
    {
      onclose(item.fd);
      continue;
    }
  }
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
    setsockopt(mlisten, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    if(bind(mlisten, (struct sockaddr*)&addr,sizeof(addr))<0)
    {
      close(mlisten);
    return -1;
    }
    if(listen(mlisten, 8192)<0)
    {
        close(mlisten);
        return -1;
    }
    return mlisten;
}
void Epoll::addevent(int fd,int event,int cancelEvent)
{
 if(fd==done.fd())
 {
  ondone();
  return;
 }
 if(fd==m_listen)
 {
      onaccept(fd);
      return;     
 }
 auto it=status.find((fd));
 if(it==status.end()) return;
 net::Connection& con=it->second;
 if(stopping){
    if(event & EPOLLOUT){ if(!con.handwrite()) onclose(fd); }
    return;
}
 if(event & (EPOLLERR | EPOLLHUP))
 {
    onclose(fd);
    return;
 }else if(event & EPOLLIN)
 {
  
  if(con.handreadable()!=true) {onclose(fd);}
  return;
   
}else if(event & (EPOLLIN |EPOLLOUT))
{
   if(con.handwrite()!=true) {onclose(fd); }
  return;
}
}
bool Epoll::start(int port){
   epfd=epoll_create1(0); //一个收发室
   m_listen= this-> create_fd(port); //创建一个fd端口号为8888
    if(m_listen<0||done.fd()<0)
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
   if(set_nonblocking(m_listen)==-1||set_nonblocking(done.fd())==-1)
           {
            close(m_listen);
            return false;
           }
   ev.events = EPOLLIN;//监听新连接
   ev.data.fd = m_listen;
   ev_wake.events=EPOLLIN;
   ev_wake.data.fd=done.fd();
   if(epoll_ctl(epfd,EPOLL_CTL_ADD , m_listen, &ev)<0||epoll_ctl(epfd,EPOLL_CTL_ADD , done.fd(), &ev_wake)<0)
   {
    close(m_listen);
    close(epfd);
    return false;
   }
  Logger::Instance().Log(INFO, "成功启动", "Epoll");
   client_len=(sizeof(client_addr));
   while(1)
   {
     if(stopping){
      close(m_listen);
      m_listen=-1;
    auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(3);
     while(1){
       bool all_pass=true;
      for(auto& kv : status)
      {
       if(kv.second.inflight>0) { all_pass=false; break; }
      }
      if(all_pass||deadline<std::chrono::steady_clock::now()) {break;}
      int npfd=epoll_wait(epfd, ev64, 512, 100);
      for(int i=0;i<npfd;++i){
      int fd=ev64[i].data.fd;
      int event_flags=ev64[i].events;
      if(fd==m_listen||status.find( fd)!=status.end()||fd==done.fd())
      {
        
        addevent(fd, event_flags, event_flags);
      }  
      }
     }
     break;
     }
     int nfds =epoll_wait(epfd, ev64, 512, -1);
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
      if(fd==m_listen||status.find( fd)!=status.end()||fd==done.fd())
      {
        
        addevent(fd, event_flags, event_flags);
      }  
    }
    }
   }
   while(!status.empty()) onclose(status.begin()->first);
    close(m_listen);
    close(epfd);
    epfd=-1;
    m_listen=-1;
    Logger::Instance().Log(INFO, "优雅退出", "Epoll");
    return true;

}
Epoll::Epoll() :epoll_poll(4)
{
  g_instance.push_back(this);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT,  on_signal);
  signal(SIGTERM, on_signal);
  m_listen=-1;
  epfd=-1;
  idle_fd= open("/dev/null", O_RDONLY);
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
 auto it = std::find(g_instance.begin(), g_instance.end(), this);
if (it != g_instance.end()) {
    g_instance.erase(it); 
}
}
void Epoll::on_signal(int signo){
   uint64_t one=1;
 for(auto* g: g_instance){
  g->stopping=true;
 write(g->done.fd(),&one,8);
 }
}

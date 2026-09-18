#include "net/Connection.h"
net::Connection::Connection(int epfd,int fd,std::function<void(net::Connection&,const std::string&)> handler)
  :epfd(epfd),fd(fd),handler(handler)
{
   
}
net::Connection::~Connection()
{

}
bool  net::Connection::handreadable(){
     char buf[1024*64];
    while(1){
      int n=recv(fd, buf, sizeof(buf), 0);
      auto& it=status;
      if(n>0)
    {
    it.decoder.feed(buf, n);
    bool pass=it.decoder.brokenmessage();
    if(pass!=true){
    std::vector<std::string> msg=it.decoder.take();
    for(auto& str :msg)
    {
     handler(*this,str);
     if(closed) return false;
    }
    }
    else{  return false;}
    continue;
    }else if(n==0){
    it.peer_close = true;
    bool pass=it.decoder.brokenmessage();
    if(pass!=true){
    std::vector<std::string> msg=it.decoder.take();
   for(auto& str :msg)
    {
     handler(*this,str);
     if(closed) return false;
    }
    }
    if(it.Outbuffer.empty()){  return false;}
    else{ set_event( EPOLLOUT);  return true;}
    }else{
    if(errno!=EAGAIN&&errno!=EINTR)
    {
     return false;
    }
    return true;
  }
  }

}
bool  net::Connection::handwrite(){
ssize_t n=0;
  auto& it=status;
  while(it.idx<it.Outbuffer.size())
  {
     n=send(fd,it.Outbuffer.data()+it.idx,it.Outbuffer.size()-it.idx,0);
    if(n==(it.Outbuffer.size()-it.idx))
    {
      it.idx=0;
      it.Outbuffer.erase(it.Outbuffer.begin(),it.Outbuffer.end());
      if(it.peer_close==true){ return false;}
      set_event(EPOLLIN);
      return true;
    }else if(n==-1){
      if (errno ==EAGAIN|| errno == EWOULDBLOCK ||errno == EINTR){
      return true;
    }
      
      return false;
  }else if(n<it.Outbuffer.size()-it.idx){
      it.idx+=n;
      continue;
    }
  
  }
  
}
bool net::Connection::senddata(const std::string& accept)
{
 std::string data= net::encode(accept);
int achieve=send(fd,data.data(),data.size(),0);
       if(achieve<data.size())
    {
      if(achieve==-1 )
      {
      if (errno == EAGAIN||errno == EWOULDBLOCK ||errno == EINTR ){
        status.Outbuffer.insert(status.Outbuffer.end(),data.begin(),data.end());
      set_event(EPOLLIN |EPOLLOUT);
      return true;
      }
      closed=true;
      return false;
      }else{
        status.Outbuffer.insert(status.Outbuffer.end(),data.begin()+achieve,data.end());
        set_event( EPOLLIN |EPOLLOUT);
      }
    }
    return true;
}
void net::Connection::set_event(uint32_t event)
{
    struct epoll_event ev;
    ev.events=event;
    ev.data.fd=fd;
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
}
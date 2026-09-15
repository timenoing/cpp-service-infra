#include <asm-generic/errno-base.h>
#include <cstddef>
#include <cstdio>
#include<sys/socket.h>
#include<sys/epoll.h>
#include<fcntl.h>
#include<errno.h>
#include<stdio.h>
#include<sys/types.h>
#include<unistd.h>
#include<netinet/in.h>
#include<fcntl.h>
#include<string>
#include<arpa/inet.h>
#include<iostream>
#include<random>
#include<cstdlib>
#include<Message.h>
int create_client(int a){ //放在那个端口上
    struct sockaddr_in addr;
    int m_listen=socket(AF_INET,SOCK_STREAM,0);
    if(m_listen<0)
    {
        close(m_listen);
        return -1;
    }
    addr.sin_family=AF_INET;
    addr.sin_port=htons(a);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if(connect(m_listen, (struct sockaddr *)&addr, sizeof(addr))<0)
    {
      close(m_listen);
      return -1;
    }
    return m_listen;
}
void send_all(int fd,const char *data,size_t len)
{
   size_t sent=0;
   while(sent<len)
   {
      ssize_t n =send(fd,data+sent,len-sent,MSG_NOSIGNAL);
      if(n<0)
      {
         if(errno==EINTR)
         {
            continue;
         }
         perror("发送失败");
         exit(1);
      }
      sent+=n;
   }
}
std::string generate_randombytes(ssize_t min_len,ssize_t max_len)
{
   static std::random_device rd;
   static std::mt19937 gen(rd());
   std::uniform_int_distribution<size_t> len_dist(min_len,max_len);
   std::uniform_int_distribution<int> byte_dist(0,255);
   ssize_t len=len_dist(gen);
   std::string str;
   str.reserve(len);
   str.resize(len);
   for(int i=0;i<len;++i)
   {
      str[i]=static_cast<char>(byte_dist(gen));
   }
   return str;
}
 std::vector<std::string>reservemessage(int fd,int except)
{
    char buf[1024*64];
    net::messagedecode decoder;
    std::vector<std::string> msg;
    while(1){
   if(msg.size()>=except)
   break;
    int n=recv(fd, buf, sizeof(buf), 0);
    if(n>0)
    {
    decoder.feed(buf, n);
    bool pass=decoder.brokenmessage();
    if(pass!=true){
    std::vector<std::string> got=decoder.take();
    for(size_t i=0;i<got.size();++i)
    msg.push_back(got[i]);
    }
    else{  return msg;}
    continue;
    }else if(n==0)
    {
      return msg;
    }else{
      if(errno==EINTR)
      continue;
      else{ perror("接收失败"); return msg;}
    }
   } 
   return msg;
}
int interactive(int fd)
{
  std::string line;
  while(std::getline(std::cin,line))
  {
    std::string packet=net::encode(line);
    send_all(fd,packet.data(),packet.size());
    std::vector<std::string> got=reservemessage(fd,1);
    if(got.empty())
    {
      std::cout<<"server closed"<<std::endl;
      return 1;
    }
    std::cout<<"echo len="<<got[0].size()<<": "<<got[0]<<std::endl;
  }
  return 0;
}
int regression(int fd,int total)
{
  const int batch=50;
  size_t total_bytes=0;
  int bad=0;
  int done=0;
  while(done<total)
  {
    int n=batch;
    if(total-done<n)
      n=total-done;
    std::vector<std::string> sent;
    for(int i=0;i<n;++i)
    {
      if(done==0&&i==0)
        sent.push_back(std::string());
      else if(done==0&&i==1)
        sent.push_back(generate_randombytes(1,1));
      else if(done==0&&i==2)
        sent.push_back(generate_randombytes(65535,65535));
      else if(done==0&&i==3)
        sent.push_back(generate_randombytes(65536,65536));
      else if(done==0&&i==4)
        sent.push_back(generate_randombytes(65537,65537));
      else
        sent.push_back(generate_randombytes(0,64*1024));
    }
    for(size_t i=0;i<sent.size();++i)
    {
      std::string packet=net::encode(sent[i]);
      send_all(fd,packet.data(),packet.size());
      total_bytes+=sent[i].size();
    }
    std::vector<std::string> got=reservemessage(fd,n);
    if((int)got.size()<n)
    {
      std::cout<<"FAIL batch@"<<done<<" lost "<<(n-(int)got.size())<<" messages"<<std::endl;
      bad+=n-(int)got.size();
    }
    for(size_t i=0;i<got.size()&&i<sent.size();++i)
    {
      if(got[i]!=sent[i])
      {
        std::cout<<"FAIL batch@"<<done<<" idx="<<i<<" sent_len="<<sent[i].size()<<" recv_len="<<got[i].size()<<std::endl;
        ++bad;
      }
    }
    done+=n;
  }
  if(bad==0)
    std::cout<<"PASS N="<<total<<" bytes="<<total_bytes<<std::endl;
  else
    std::cout<<"FAIL N="<<total<<" bytes="<<total_bytes<<" bad="<<bad<<std::endl;
  return bad==0?0:1;
}
int main(int argc,char **argv)
{
  const int port=8888;
  if(argc==3&&std::string(argv[1])=="-n")
  {
    int n=std::atoi(argv[2]);
    if(n<=0)
    {
      std::cerr<<"usage: test_client [-n N]"<<std::endl;
      return 2;
    }
    int fd=create_client(port);
    if(fd<0)
    {
      perror("connect");
      return 1;
    }
    int r=regression(fd,n);
    close(fd);
    return r;
  }
  if(argc==1)
  {
    int fd=create_client(port);
    if(fd<0)
    {
      perror("connect");
      return 1;
    }
    int r=interactive(fd);
    close(fd);
    return r;
  }
  std::cerr<<"usage: test_client [-n N]"<<std::endl;
  return 2;
}
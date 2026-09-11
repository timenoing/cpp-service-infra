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
std::string sendmessage(std::string s)
{
   std::string ans;
   uint32_t len=htonl(s.size());
   ans.append((char *)&len,4);
   ans.append(s);
   return ans;
}
void send_all(int fd,const char *data,size_t len)
{
   size_t sent=0;
   while(sent<len)
   {
      ssize_t n =send(fd,data+sent,len-sent,0);
      if(n<0)
      {
         perror("发送失败");
         exit(1);
      }
      sent+=n;
   }
}
int main()
{
  int client=create_client(8888);
  if(client<0)
  {
   perror("error");
   return 1;
  }
  while(1)
  {
   std::string temp;
     std::getline(std::cin,temp);
     size_t len;
     temp=sendmessage(temp);
     len=temp.size();
     send_all(client, temp.c_str(), len);
  }
  close(client);
}
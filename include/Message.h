#ifndef MESSAGE_H
#define MESSAGE_H
#include<vector>
#include <string>
#include<cstring>
#include<sys/types.h>
#include <arpa/inet.h>
namespace net {
constexpr uint32_t messagemaxlen=1024*1024;
std::string encode(const std::string &msg);
class messagedecode{
   public:
   void feed(const char* data,ssize_t n);
   std::vector<std::string> take();
   bool brokenmessage();
   private:
   std::vector<char> Inputbuf;
   std::vector<std::string> msg;
   bool error=false;
   int idx=0;
};
}
#endif
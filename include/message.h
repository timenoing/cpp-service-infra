#ifndef MESSAFE_H
#define MESSAFE_H
#include<vector>
#include <string>
#include<cstring>
#include<sys/types.h>
namespace net {
constexpr uint32_t messagemaxlen=1024*1024;

std::string encode(const std::string &accept);

class messagedecode{
   public:
   void feed(const char* data,ssize_t n);
   std::vector<std::string> take();
   bool brokenmessage();


   private:
};
}


#endif
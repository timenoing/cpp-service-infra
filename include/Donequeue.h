#ifndef  DONEQUEUE_H
#define DONEQUEUE_H
#include<queue>
#include<string>
#include<mutex>
#include<sys/eventfd.h>
#include <unistd.h>
#include <cstdint>
#include<deque>
class Donequeue{
    public:
    struct doit_item{
      int fd;
      uint64_t id;
      std::string  resp;
    };
    Donequeue();
    ~Donequeue();  
    int fd() const;
    void push(int fd,uint64_t id,std::string resp);
    std::deque<doit_item> drain();
    private:
    std::deque<doit_item> queue;
    std::mutex mut;
    int wake_fd;
};

#endif
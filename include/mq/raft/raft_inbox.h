#ifndef RAFT_INBOX
#define RAFT_INBOX
#include "mq/raft/raft_message.h"
struct inboundmsg{
   uint32_t from=0;
   uint8_t  opcode=0;
   size_t bytes=0;
   rcp msg;
};
 enum class Popstatic { Ok=0,
   Timeout=1,
   Close=2
};
class Raft_inbox{
   public:
   Raft_inbox();
   ~Raft_inbox();
   bool try_push(inboundmsg  m);
   Popstatic wait_pop(inboundmsg& m,uint32_t time_out);
   void close();
   private:
   std::deque<inboundmsg> tasks;
   std::condition_variable cv_;
   std::mutex mu_;
   bool stopping=false;
   std::size_t queued_bytes=0;
   std::size_t max_bytes=16u*1024*1024 ;
   std::size_t max_size=128;
};
#endif  
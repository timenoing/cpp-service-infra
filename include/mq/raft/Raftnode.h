#ifndef  RAFTNODE
#define RAFTNODE
#include <set>
#include <random>
#include "mq/raft/raft_message.h"
#include "Epoll.h"
#include "mq/raft/raft_inbox.h"
#include "mq/raft//peer_sender.h"
#include "mq/io/File_io.h"
struct peerSenderconfig{
      uint32_t id=0;
      uint16_t port=0;
      std::string ip;
};
struct raftconfig{
    uint32_t me=0;
    uint16_t listen_port=0;
     std::vector<peerSenderconfig> peers;
};
class raftnode{
      public:
      raftnode(uint64_t peers_,mlog::io::PosixFileIo io_,
            std::string path,std::string log_path,const raftconfig config
            );
       ~raftnode();
      bool start(const raftconfig& config);
      bool recover();
      void ticker();
      void select();
      void heartbeating();
      void worker_acqire();
      void becomeleader();
      void becomecandidate();
      void becomefloower(uint32_t term);
      void peisist();
      void readpeisist();
      void on_message(const inboundmsg& m);
      std::string status_json();
      void raft_killed();
      void worker_apply();
      bool raft_kiii();
      bool rpc(uint32_t to, const rcp& msg);
      void stop();
      private:
      std::thread worker_;
      std::thread raft;
      mlog::io::PosixFileIo io_;
      int fd_=-1;
      int log_fd_=-1;
      std::string path_;
      std::string log_path_;
      bool kill=false;
      uint64_t peers=0;
      uint32_t term=0;
      uint64_t me=0;
      uint64_t commit_idx=0;
      std::vector<uint64_t> commit_idx_team;
      uint64_t apply_idx=0;
      std::string status="floower";
      std::mutex mu_;
      std::condition_variable cv_;
      std::vector<Log> entries{};
      uint64_t votefor=-1;
      int votecount=0;
      std::vector<uint64_t> preLogindex;
      std::vector<uint64_t> prterm;
      std::vector<Log> Logs;
      inboundmsg box;
      uint64_t cut(std::vector<Log>& logs,uint32_t term);
      Raft_inbox inbox;
      Epoll epoll_;
      std::vector<std::unique_ptr<PeerSender>> owned;
      std::vector<PeerSender*>by_id;
      std::random_device rd; 
};
#endif
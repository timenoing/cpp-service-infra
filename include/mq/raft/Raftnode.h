#ifndef  RAFTNODE
#define RAFTNODE
#include <set>
#include <random>
#include "mq/raft/raft_message.h"
#include "Epoll.h"
#include "mq/raft/raft_inbox.h"
#include "mq/raft//peer_sender.h"
#include "mq/io/File_io.h"
constexpr uint32_t RAFT_HEAD_MAGIC =0x52485354;
constexpr uint32_t RAFT_LOG_MAGIC =0x524C4F47;
struct peerSenderconfig{
      uint32_t id=0;
      uint16_t port=0;
      std::string ip;
};
struct raftconfig{
    uint32_t me=0;
    uint16_t listen_port=0;
    uint16_t router_port=0;
    std::string router_ip;
     std::vector<peerSenderconfig> peers;
};
class raftnode{
      public:
      raftnode(uint64_t peers_,mlog::io::File_io& io_,
            std::string path,std::string log_path
       );
       ~raftnode();
      bool start(const raftconfig& config);
      bool recover();
      bool recover_headstate();
      bool recover_log();
      bool save_haedstate();
      void ticker();
      void select();
      void heartbeating();
      void worker_acqire();
      void becomeleader();
      void becomecandidate();
      void becomefloower(uint32_t term);
      void on_message(const inboundmsg& m);
      std::string status_json();
      void raft_killed();
      void worker_apply();
      bool raft_kiii();
      bool rpc(uint32_t to, const rcp& msg);
      void stop();
      [[noreturn]] void fatal_stop(const char* what);
      std::string make_frame(uint64_t i,const Log& l);
      bool flush_logs();
      bool cut_log(uint64_t end);
      private:
      std::vector<uint64_t> log_offset;
      uint64_t log_end=16;
      uint64_t fsync_idx=0;
      std::thread worker_;
      std::thread raft;
      mlog::io::File_io& io_;
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
      std::unique_ptr<PeerSender> router_;
      std::random_device rd;
      uint64_t hs_seq_ = 0;     // 上次写入的 seq
       int hs_slot_ = 0;        //上次写入成功的槽
};
#endif
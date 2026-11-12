#include "mq/raft/Raftnode.h"
static void write_u64_be(uint8_t* p, uint64_t v) {
    p[0] = (v >> 56) & 0xFF;
    p[1] = (v >> 48) & 0xFF;
    p[2] = (v >> 40) & 0xFF;
    p[3] = (v >> 32) & 0xFF;
    p[4] = (v >> 24) & 0xFF;
    p[5] = (v >> 16) & 0xFF;
    p[6] = (v >> 8)  & 0xFF;
    p[7] =  v        & 0xFF;
}
static uint64_t read_u64_be(uint8_t* p) {
    return (uint64_t(p[0]) << 56) |
           (uint64_t(p[1]) << 48) |
           (uint64_t(p[2]) << 40) |
           (uint64_t(p[3]) << 32) |
           (uint64_t(p[4]) << 24) |
           (uint64_t(p[5]) << 16) |
           (uint64_t(p[6]) << 8)  |
            uint64_t(p[7]);
}
raftnode::raftnode(uint64_t peers_,mlog::io::PosixFileIo io
,std::string path,std::string Log_path, raftconfig config
) :peers(peers_),
io_(io),path_(path),log_path_(Log_path)
{
  
}
raftnode::~raftnode(){
    stop();
}
void raftnode::stop(){
    kill=true;
    if(worker_.joinable()) worker_.join();
    if(raft.joinable())   raft.join();
    inbox.close();
    for(auto& s : owned ) s->stop();
}
bool raftnode::rpc(uint32_t to, const rcp &msg){
    if(to == 0 || to >= by_id.size() || by_id[to] == nullptr) return false;
     return by_id[to]->try_send(std::move(msg));
}
bool raftnode::recover(){
     return true;
}
uint64_t  raftnode::cut(std::vector<Log>& Logs,uint32_t term){
  auto it = std::lower_bound(Logs.begin(), Logs.end(), term, 
        [](const Log& l, uint32_t t) {
            return l.term < t; 
        });
        Logs.erase(it,Logs.end());
        size_t index = std::distance(Logs.begin(), it);
        return Logs[index-1].term;
}
void raftnode::becomefloower(uint32_t term_){
    status="floower";
    term=term_;
    votecount=0;
    votefor=-1;
}
void raftnode::becomecandidate(){
    status="candidate";
    term++;
    votecount=1;
    votefor=me;                                                                           
}
void raftnode::becomeleader(){
    std::fprintf(stderr, "node %llu leader term %llu\n",
             (unsigned long long)me, (unsigned long long)term);
    status="leader";
    for(int i=0;i<=peers;i++){
        if(i==me||by_id[i]==nullptr) {
            continue;
        }
        preLogindex[i]=Logs.size()-1;
        prterm[i]=term;
    }
}
std::string raftnode::status_json(){
    const uint64_t last_index = Logs.empty() ? 0 : Logs.size() - 1;
    const uint64_t last_term  = Logs.empty() ? 0 : Logs.back().term;
    std::string out;
    out.reserve(192);
    out += "{\"id\":";             out += std::to_string(me);
    out += ",\"role\":\"";         out += status; out += '"';
    out += ",\"term\":";           out += std::to_string(term);
    out += ",\"voted_for\":";
    if (votefor == static_cast<uint64_t>(-1)) out += "null";
    else                                      out += std::to_string(votefor);
    out += ",\"votecount\":";       out += std::to_string(votecount);
    out += ",\"commit_index\":";    out += std::to_string(commit_idx);
    out += ",\"apply_index\":";     out += std::to_string(apply_idx);
    out += ",\"last_log_index\":";  out += std::to_string(last_index);
    out += ",\"last_log_term\":";   out += std::to_string(last_term);
    out += ",\"log_entries\":";     out += std::to_string(last_index);
    out += ",\"peers\":";           out += std::to_string(peers);
    out += '}';
    return out;
}
void raftnode::on_message(const inboundmsg& n){
   if(const auto* v=std::get_if<Requestvote>(&n.msg)){
    Requestvotereply m{};
    if(v->term>term){becomefloower(v->term); m.vote=true;
    votefor=n.from;
    m.term=term;
    }
    else if(v->term==term) {
        if(votefor==v->requseter_id){
            m.vote=true;
            votefor=n.from;
         } 
    if(v->last_Logterm>=Logs.back().term||votefor==-1){
        if(v->last_Logindex>=Logs.size()-1){
            m.vote=true; 
            votefor=n.from;
        }
    }
    m.term=term;
    }
    else {
        m.vote=false; 
        m.term=term; }
    rpc(n.from,m);
   } if(const auto* v=std::get_if<Requestvotereply>(&n.msg)){
    if(v->term>term){
        becomefloower(v->term);
         return;}
    if(v->vote){
        votecount++; 
    }
   } if(const auto* v=std::get_if<AppendEntries>(&n.msg)){
    AppendEntriesreply m{};
    if(v->leader_term==term&&status=="leader"){
        becomefloower(v->leader_term);
        return;
    }
    if(v->leader_term<term){
      m.term=term;
      rpc(n.from, m);  
      return;
     }
    if(v->leader_term>term){becomefloower(v->leader_term); }
    if(v->preLogindex<Logs.size()-1&&v->leader_term==term){
        Logs.resize(v->preLogindex); 
        m.follower_len=Logs.size(); 
        m.xterm=Logs.back().term; 
        m.xindex=Logs.size();
        rpc(n.from, m);
        return; }
    if(v->preLogindex==Logs.size()-1&&v->leader_term==term){
        if(v->prterm==Logs.back().term){
            Logs.insert(Logs.end(), std::make_move_iterator(v->entries.begin()), 
            std::make_move_iterator(v->entries.end()));//唯一增加日志路径
            m.success=true; 
            m.follower_len=Logs.size();
            m.xindex=Logs.size()-1;           
            m.term=Logs.back().term;
            rpc(n.from, m); 
            return;
         }
        else{m.term= cut(Logs,Logs.back().term);
         m.xindex=Logs.size()-1; m.follower_len=Logs.size();
         rpc(n.from, m);
         return;
        }
     }
     if(v->preLogindex>Logs.size()-1&&v->leader_term==term){m.xindex=-1; 
        m.xterm=Logs.back().term;
         m.follower_len=Logs.size();
         rpc(n.from, m); 
         return; }
   } if(const auto* v=std::get_if<AppendEntriesreply>(&n.msg)){
    if(v->term>term){ 
        becomefloower(v->term);
    }
     if(v->success){
     commit_idx_team[me]=Logs.size()-1;
     commit_idx_team[n.from]=v->xindex;
     std::multiset<uint64_t> sort;
     for(int i=0;i<=peers;i++){
        sort.insert(commit_idx_team[i]);
     }
     auto it=sort.begin();
     std::advance(it, peers/2);
     auto mid=*it;
     if(commit_idx<mid&&term==Logs[mid].term){
        commit_idx=mid;
        cv_.notify_one();
     }
     preLogindex[n.from]=Logs.size()-1;
     prterm[n.from]=Logs.back().term;
     }else {
        if(v->xindex==-1){
            preLogindex[n.from]=v->follower_len-1;
            prterm[n.from]=Logs[v->follower_len-1].term;
        }else{
            preLogindex[n.from]=v->follower_len-1;
            prterm[n.from]=Logs[v->follower_len-1].term;
        }
     }
   }
   
}
void raftnode::select(){
    thread_local std::mt19937 gen(rd()); 
    std::uniform_int_distribution<> distrib(150, 300);
    int timeout_ms=distrib(gen);
    auto deadline=std::chrono::steady_clock::now()+std::chrono::milliseconds(150+timeout_ms);
    becomecandidate();
    Requestvote m{};
        if(status!="candidate"){
            return; 
        } 
        m.requseter_id=me;
        m.term=term;
        m.last_Logindex=Logs.size()-1;
        m.last_Logterm=Logs.back().term;
    for(int i=0;i<=peers;i++){
        if(i==me||by_id[i]==nullptr){continue; }
        rpc(i, m);
    }
   while(std::chrono::steady_clock::now()<deadline){
    auto r = inbox.wait_pop(box, 50);
  if (r == Popstatic::Ok) {
   on_message(box);
    if(status!="candidate"){
            return; 
     }else if(votecount>=peers/2+1){
    becomeleader();
    return;
     } 
   }
   }
   
}
void raftnode::heartbeating(){
    while(!raft_kiii()){
         auto deadline=std::chrono::steady_clock::now()+std::chrono::milliseconds(100);
        if(status!="leader"){ std::printf("notleader"); return; }
        AppendEntries m;
         m.leader_commit=commit_idx;
         m.leader_id=me;
         m.leader_term=term;
         m.entries={};
         for(int i=0;i<=peers;i++){
            if(i==me||by_id[i]==nullptr) continue;
            m.prterm=prterm[i];
            m.preLogindex=preLogindex[i];

           rpc(i, m);
         }
         while(std::chrono::steady_clock::now()<deadline){
        auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
             deadline - std::chrono::steady_clock::now()
             ).count();
         auto r = inbox.wait_pop(box, remaining_ms);
        on_message(box);
        if(status!="leader"){
         return;
        }
        AppendEntries m{};
        m.leader_commit=commit_idx;
        m.leader_id=me;
        m.leader_term=term;
        for(int i=0;i<=peers;i++){
            if(i==me||by_id[i]==nullptr){
                continue;
            }
            m.preLogindex=preLogindex[i];
            m.prterm=prterm[i];
            m.entries.insert(m.entries.end(), Logs.begin()+m.preLogindex, Logs.end());
            rpc(i, m);
        }
         }
    }
}
void raftnode::peisist(){
    uint8_t buf[32]={};
    write_u64_be(buf, term);
    write_u64_be(buf+8, votefor);
    write_u64_be(buf+6, commit_idx);
    io_.pwrite(fd_, buf, 8, 0);                               
    io_.pwrite(fd_, buf+8, 8, 8);
    io_.pwrite(fd_, buf+16, 8, 16);                           
} 
void raftnode::readpeisist(){
    uint8_t buf[32]={};
    io_.pread(fd_, buf, 24, 0);
    term=read_u64_be(buf);
    votefor=read_u64_be(buf+8);
    commit_idx=read_u64_be(buf+16);
}
bool raftnode::raft_kiii(){
    return kill;
}
void raftnode::raft_killed(){
    kill=true;
    peers=-1;
}
void raftnode::worker_acqire(){
    
}
void raftnode::worker_apply(){
    
    while(1){
    {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait_for(lock, std::chrono::microseconds(50),[this](){
      return kill;
    });
    }
    if(kill){ return;}
    if(apply_idx<commit_idx){
    
    }
    }
}
bool raftnode::start(const raftconfig& config){
    Logs.push_back( Log{});
    me=config.me;
    commit_idx_team.resize(peers+1, 0);
    preLogindex.resize(peers+1, 0);
    prterm.resize(peers+1, 0);
    by_id.resize(peers+1,nullptr);
    epoll_.set_handler([this](const std::string& payload) -> std::string {
        inboundmsg m;
        if (!raft_code(payload, m.msg, m.opcode, m.from)) return {};
        if (std::holds_alternative<Status_query>(m.msg)) return status_json();
        m.bytes = payload.size();
        inbox.try_push(std::move(m));
        return {};
    });
    if(!recover()){return false; }
    for(auto& p:config.peers){
        auto s=std::make_unique<PeerSender>(static_cast<uint32_t>(me),p.ip,p.port);
        if(!s->start()){return false; }
        by_id[p.id]=s.get();
        owned.push_back(std::move(s));
    }
    if(!recover()){return false;}
   /* if(io_.open(path_.c_str(), O_CREAT||O_WRONLY, 0644)<0||
    io_.open(log_path_.c_str(), O_CREAT||O_WRONLY||O_APPEND, 0644)<0){
        return false;
    }*/
   try {
   worker_=std::thread(&raftnode::worker_apply,this);
   } catch (...) {
   return false;
   }
    try {
    raft=std::thread ([&]{
    this->ticker();
   });
   } catch (...) {
   return false;
   }
    epoll_.start(config.listen_port);
    stop();
    return true;
}
void raftnode::ticker(){
    thread_local std::mt19937 gen(rd()); 
    while(!raft_kiii()){
        if(status=="leader"){
            heartbeating();
        }
        std::uniform_int_distribution<> distrib(150, 300);
        int timeout_ms=distrib(gen);
        auto r=inbox.wait_pop(box, timeout_ms+150);
        if(r==Popstatic::Ok){
            on_message(box);
        }else if(r==Popstatic::Timeout){
            select();
        }
    }
}

#include "mq/raft/Raftnode.h"
raftnode::raftnode(uint64_t peers_,mlog::io::File_io& io
,std::string path,std::string Log_path
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
}
bool raftnode::rpc(uint32_t to, const rcp &msg){
    if(to == 0 ) return false;
     return router_->try_send(to,std::move(msg));
}
bool raftnode::recover(){
    if(path_.empty() ||log_path_.empty()) return true;
    fd_ = io_.open(path_.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd_ < 0) return false;
    log_fd_ = io_.open(log_path_.c_str(), O_CREAT | O_RDWR, 0644);
    if (log_fd_ < 0) { io_.close(fd_); fd_ = -1; return false; }
    if(!recover_headstate()){return false;}
    if(!recover_log()){return false;}
    if(!Logs.empty()&&Logs.back().term>term){
        term=Logs.back().term;
        votefor=-1; 
        if(!save_haedstate()){return false; }
    }
     return true;
}
bool raftnode::recover_headstate(){
    struct stat st;
     uint8_t buf[128]{};
     if(io_.fstat(fd_, &st)!=0) {
        return false;
     }
     if(st.st_size==0){
        term=0;
        commit_idx=0;
        votefor=-1;
        hs_seq_=0;
        hs_slot_=0;
        return save_haedstate();
     }
      if(io_.pread(fd_, buf, 128, 0)<40){
        return false;
      }
     if(st.st_size>=40&&st.st_size<104){ //一条有效的头
      if(read_u32_be(buf)!=RAFT_HEAD_MAGIC){return false; }
      if(io_.pread(fd_, buf, 40, 0)<40){return false; }
      if(read_u32_be(buf)!=RAFT_HEAD_MAGIC){
        return false;
      }
      if(!verify_crc(buf+8, 32, read_u32_be(buf+4))){return false; }
      hs_slot_=0;
      hs_seq_=read_u64_be(buf+8);
      term=read_u64_be(buf+16);
      votefor=read_u64_be(buf+24);
      commit_idx=read_u64_be(buf+32);
     }else if(st.st_size>=104){//两条有效的头,如果有多写检查过crc后截断
      bool crc1=verify_crc(buf+8, 32, read_u32_be(buf+4));
      bool crc2=verify_crc(buf+72, 32, read_u32_be(buf+68));
      if(read_u32_be(buf)!=RAFT_HEAD_MAGIC){crc1=false; }
      if(read_u32_be(buf+64)!=RAFT_HEAD_MAGIC){crc2=false; }
      uint64_t seq1=read_u64_be(buf+8);
      uint64_t seq2=read_u64_be(buf+72);
      if(crc1&&crc2){
       if(seq1>seq2){
        term=read_u64_be(buf+16);
      votefor=read_u64_be(buf+24);
      commit_idx=read_u64_be(buf+32);
      hs_seq_=read_u64_be(buf+8); 
      hs_slot_=0;
       }else{
        term=read_u64_be(buf+80);
      votefor=read_u64_be(buf+88);
      commit_idx=read_u64_be(buf+96);
      hs_seq_=read_u64_be(buf+72); 
      hs_slot_=1;
       }
      }else if(crc1){
       term=read_u64_be(buf+16);
      votefor=read_u64_be(buf+24);
      commit_idx=read_u64_be(buf+32);
      hs_seq_=read_u64_be(buf+8); 
      hs_slot_=0;
      }else if(crc2){
      term=read_u64_be(buf+80);
      votefor=read_u64_be(buf+88);
      commit_idx=read_u64_be(buf+96);
      hs_seq_=read_u64_be(buf+72); 
      hs_slot_=1;
      }else {return false;  }
     }
     return true;
}
bool raftnode::recover_log(){
    log_offset.clear();
    log_offset.push_back(16);
     struct stat st;
    if(io_.fstat(log_fd_, &st)!=0) {
        return false;
     }
     if(st.st_size==0){
        uint8_t hdr[16]{};
        write_u32_be(hdr, RAFT_LOG_MAGIC);
        if(io_.pwrite(log_fd_, hdr, 16, 0)!=16) return false;
        if(io_.fdatasync(log_fd_)!=0) return false;
        fsync_idx=0;
        log_end=16;
        return true;
     }
     uint8_t data[128];
     uint32_t offset=16;
     if(st.st_size<16){return false; }
     if(io_.pread(log_fd_, data, 16, 0)<16){return false; }
     if(read_u32_be(data)!=RAFT_LOG_MAGIC){return false; }
     while(st.st_size>offset){
        if(st.st_size-offset<4){
            io_.ftruncate(log_fd_, offset); //不完整消息截断
            break;
        }
        io_.pread(log_fd_, data, 4, offset);
          uint32_t len=read_u32_be(data);
        if(len<20||st.st_size-offset-4<len){
            io_.ftruncate(log_fd_, offset); //不完整消息截断
            break;
        }
        std::string body(len,'\0');
        if(io_.pread(log_fd_, &body[0], len, offset+4)!=(ssize_t)len){
            io_.ftruncate(log_fd_, offset);
            break;
        }   //data空间不够改用string缓冲
        std::string joined;
        joined.append(body.data(), 16);
        joined.append(body.data()+20, len-20);//去掉没用的crc
        if(!verify_crc(reinterpret_cast<const uint8_t*>(joined.data()),
                       joined.size(),
                       read_u32_be(reinterpret_cast<const uint8_t*>(body.data())+16))){
            io_.ftruncate(log_fd_, offset);
            break;
        }
        fsync_idx=read_u64_be(reinterpret_cast<uint8_t*>(&body[0]));
        Logs.push_back(Log{
            read_u64_be(reinterpret_cast<uint8_t*>(&body[0]) + 8),
            body.substr(20)});
        offset+=len+4;
        log_end=offset;
        log_offset.push_back(offset);
    }
    return true;
}
bool raftnode::save_haedstate(){
    if(fd_<0){ return true;}
    uint8_t buf[64]{};
    write_u32_be(buf, RAFT_HEAD_MAGIC);
    write_u64_be(buf+8, hs_seq_+1);
    write_u64_be(buf+16, term);
    write_u64_be(buf+24, votefor);
    write_u64_be(buf+32, commit_idx);
    write_u32_be(buf+4, calculate_crc(buf+8, 32));
    int offset=0;
    if(hs_slot_==0){ offset=64;}
    if(io_.pwrite(fd_, buf, 64, offset)<64){
        return false;
    }
    if(io_.fdatasync(fd_)<0) {fatal_stop("fdatasync fail"); return false; }
    hs_seq_++;
    if(hs_slot_==0){hs_slot_=1;}
    else{hs_slot_=0; }
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
    if (term_ < term) return;
    status="floower";
    if(term_>term){
        term=term_;
        votecount=0;
        votefor=-1;
        save_haedstate();
    }
}
void raftnode::becomecandidate(){
    status="candidate";
    term++;
    votecount=1;
    votefor=me;
    save_haedstate();                                                                           
}
void raftnode::becomeleader(){
    std::fprintf(stderr, "node %llu leader term %llu\n",
             (unsigned long long)me, (unsigned long long)term);
    status="leader";
    votecount=0;
    for(int i=0;i<=peers;i++){
        if(i==me) {
            continue;
        }
        preLogindex[i]=Logs.size()-1;
        prterm[i]=Logs.back().term;
    }
    save_haedstate();
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
    if(v->term>term){
    if(v->last_Logindex>=Logs.size()-1){
    becomefloower(v->term); m.vote=true;
    votefor=n.from;
    m.term=term;
    save_haedstate();
    }else{
        m.vote=false; 
        m.term=term;
    }
    }
    else if(v->term==term) {
        if(votefor==v->requseter_id){
            m.vote=true;
            votefor=n.from;
            save_haedstate();
         } 
    if(v->last_Logterm>=Logs.back().term&&votefor==-1){
        if(v->last_Logindex>=Logs.size()-1){
            m.vote=true; 
            votefor=n.from;
            save_haedstate();
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
    if(v->leader_term>=term){becomefloower(v->leader_term); }
    if(v->preLogindex<Logs.size()-1&&v->leader_term==term){
        Logs.resize(v->preLogindex+1);
        log_offset.resize(v->preLogindex+1);
        cut_log(log_offset[v->preLogindex]); //这是唯一截断路径，冲突日志和过长都在这截断，其他情况都不需要截断
        if(fsync_idx>v->preLogindex){fsync_idx=v->preLogindex; }
        m.follower_len=Logs.size(); 
        m.xterm=Logs.back().term; 
        m.xindex=Logs.size()-1;
        rpc(n.from, m);
        return; }
    if(v->preLogindex==Logs.size()-1&&v->leader_term==term){
        if(v->prterm==Logs.back().term){
            if(!v->entries.empty()){
            Logs.insert(Logs.end(), std::make_move_iterator(v->entries.begin()), 
                std::make_move_iterator(v->entries.end()));//唯一增加日志路径
            flush_logs();
            }
            m.success=true; 
            m.follower_len=Logs.size();
            m.xindex=Logs.size()-1;           
            m.term=Logs.back().term;
            rpc(n.from, m); 
            return;
         }
        else{
        m.term = cut(Logs, Logs.back().term);
        log_offset.resize(Logs.size());
        cut_log(log_offset.back());  
        if (fsync_idx > Logs.size() - 1) fsync_idx = Logs.size() - 1;
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
     if(v->follower_len>=1){
        preLogindex[n.from]=v->follower_len-1;
        prterm[n.from]=Logs[v->follower_len-1].term;
     }
     commit_idx_team[me]=Logs.size()-1;
     commit_idx_team[n.from]=v->xindex;
     std::multiset<uint64_t> sort;
     for(int i=1;i<=peers;i++){
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
   }if(const auto* v=std::get_if<Proposee>(&n.msg)){
    if(status!="leader"){
        return;
    }
    Logs.push_back(Log{term,v->data});
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
        if(i==me){continue; }
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
         while(std::chrono::steady_clock::now()<deadline){
        auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
             deadline - std::chrono::steady_clock::now()
             ).count();
         auto r = inbox.wait_pop(box, remaining_ms);
         if(r==Popstatic::Ok){
         on_message(box);  //处理完所有消息
         }else if(r==Popstatic::Timeout){
         }else if(r==Popstatic::Close){
            return;
         }
        if(status!="leader"){
         return;
        }
         }
         flush_logs();
         AppendEntries m{};
        m.leader_commit=commit_idx;
        m.leader_id=me;
        m.leader_term=term;
        for(int i=0;i<=peers;i++){
            if(i==me){
                continue;
            }
            m.preLogindex=preLogindex[i];
            m.prterm=prterm[i];
           if(m.preLogindex+1<Logs.size()){
            m.entries.assign(Logs.begin()+m.preLogindex+1,Logs.end());
           }else{m.entries.clear(); }
            rpc(i, m);
        }
    }
}

bool raftnode::raft_kiii(){
    return kill;
}
void raftnode::raft_killed(){
    kill=true;
    peers=-1;
}
void raftnode::worker_apply(){
    
    while(1){
    {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait_for(lock, std::chrono::microseconds(1000),[this](){
      return kill;
    });
    }
    if(kill){ return;}
    if(apply_idx<commit_idx){
    while(apply_idx<commit_idx){
        
    }
    }
    }
}
bool raftnode::start(const raftconfig& config){
    Logs.push_back( Log{});
    me=config.me;
    commit_idx_team.resize(peers+1, 0);
    preLogindex.resize(peers+1, 0);
    prterm.resize(peers+1, 0);
    router_=std::make_unique<PeerSender>(me,config.router_ip,config.router_port);
    if(!router_->start()){return false; }
    epoll_.set_handler([this](const std::string& payload) -> std::string {
        inboundmsg m;
        uint32_t to=0;
        if (!raft_code(payload,to, m.msg, m.opcode, m.from)) return {};
        if(to!=0 && to!=me){
            return {};
        }
        if (std::holds_alternative<Status_query>(m.msg)) return status_json();
        m.bytes = payload.size();
        inbox.try_push(std::move(m));
        return {};
    });
    if(!recover()){return false;}
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
std::string raftnode::make_frame(uint64_t i, const Log& l) {
    std::string frame;
    frame.resize(4 + 20 + l.msg.size());
    uint8_t* p = reinterpret_cast<uint8_t*>(&frame[0]);
    write_u32_be(p,      static_cast<uint32_t>(20 + l.msg.size()));  
    write_u64_be(p + 4,  i);                                         
    write_u64_be(p + 12, l.term);                                   
    if (!l.msg.empty()) {
        std::memcpy(p + 24, l.msg.data(), l.msg.size());
    }
    std::string covered;
    covered.reserve(16 + l.msg.size());
    covered.append(frame, 4, 16);          // index + term
    covered.append(frame, 24, l.msg.size());
    write_u32_be(p + 20, calculate_crc(
        reinterpret_cast<const uint8_t*>(covered.data()), covered.size()));

    return std::move(frame);
}
bool raftnode::flush_logs(){
    if (log_fd_ < 0) {
    for (uint64_t i = fsync_idx + 1; i < Logs.size(); ++i) {
        log_end += 4 + 20 + Logs[i].msg.size();
        log_offset.push_back(log_end);
    }
    fsync_idx = Logs.size() - 1;
    return true;
}
    if(fsync_idx+1>=Logs.size())  {return true; }
    std::string buf;
    std::vector<uint64_t> recode;
    uint64_t offset=log_end;
    for(uint64_t i=fsync_idx+1;i<Logs.size();i++){
       std:: string f=make_frame(i,Logs[i]);
       offset+=f.size();
       recode.push_back(offset);
       buf+=f;
    }
    uint64_t off=0;
    while(off<buf.size()){
        ssize_t n=io_.pwrite(log_fd_, buf.data()+off, buf.size()-off, log_end+off);
        if (n <= 0) fatal_stop("pwrite raftlog");
        off+=n;
    }
    if (io_.fdatasync(log_fd_) != 0) fatal_stop("fdatasync raftlog");
    for(auto& o:recode){log_offset.push_back(std::move(o)); }
    log_end+=buf.size();
    fsync_idx=Logs.size()-1;
    return true;
}
bool raftnode::cut_log(uint64_t end){
   log_end = end;
    if (log_fd_ < 0) return true;
    if (io_.ftruncate(log_fd_, end) < 0) { fatal_stop("ftruncate raftlog"); return false; }
    return true;
}
[[noreturn]] void raftnode::fatal_stop(const char* what) {
    std::fprintf(stderr, "node %llu fatal: %s errno=%d\n",
                 (unsigned long long)me, what, errno);
    kill = true;
    std::_Exit(2);
}

#include "mq/log/Partition_log.h"
mlog::Mpsc_bounded::Mpsc_bounded(uint32_t max_bytes,uint32_t max_tasks):max_bytes(max_bytes),max_tasks(max_tasks)
{

}
bool mlog::Mpsc_bounded::try_pop(AppendBatch& ans){
  std::lock_guard<std::mutex> lock(mu_);{
    if(mpsc.empty())
   return false;
     ans = std::move(mpsc.front());
    mpsc.pop();
    current_bytes-=ans.total_bytes;
    return true;
  }
}
bool mlog::Mpsc_bounded::try_push(AppendBatch item){
  item.total_bytes=0;
  for(auto& msg : item.items ){
  item.total_bytes+=msg.size();
  }
  std::lock_guard<std::mutex> lock(mu_);{
     if(is_closed) return false;
   if (mpsc.size() >= max_tasks || current_bytes+ item.total_bytes > max_bytes) {
        return false; 
    }
    current_bytes+=item.total_bytes;
    mpsc.push(std::move(item));
    cv_.notify_one();
    return true;
  }
}
bool mlog::Mpsc_bounded::wait_pop(AppendBatch& ans){
  std::unique_lock<std::mutex> lock(mu_);
  if(!mpsc.empty()){  ans = std::move(mpsc.front());
    mpsc.pop();
    current_bytes-=ans.total_bytes;
    return true; }
  cv_.wait(lock,[this](){
    return !mpsc.empty()||is_closed;
  });
  if(mpsc.empty() &&is_closed){
    return false;
  }
    ans = std::move(mpsc.front());
    mpsc.pop();
     current_bytes-=ans.total_bytes;
    return true;
}
void mlog::Mpsc_bounded::close(){
   std::unique_lock<std::mutex> lock(mu_);{
    is_closed=true;
   }
   cv_.notify_all();
}
uint64_t  mlog::Mpsc_bounded::queued_bytes(){
  std::unique_lock<std::mutex> lock(mu_);
 return current_bytes;
}
uint64_t mlog::Mpsc_bounded::queued_task(){
  std::unique_lock<std::mutex> lock(mu_);
  return mpsc.size();
}
mlog::PartitionLog::PartitionLog(const std::string path_,Logconfig config_,
    std::function<void(uint64_t request_id, uint64_t offset,size_t count,Status status)> on_commit_,mlog::io::File_io& io_):path(path_)
    ,config(config_),on_commit(on_commit_),io(io_), mpsc(16ULL  *  1024 * 1024, 128) {
    writer=std::thread([this](){
    this->workloop();
  });
    }
mlog::PartitionLog::PartitionLog(const std::string path_,Logconfig config_,
    std::function<void(uint64_t request_id, uint64_t offset,size_t count,Status status)> on_commit_):path(path_)
,config(config_),on_commit(on_commit_),io(io::posix_file_io()), mpsc(16ULL  * 1024 * 1024, 128) {
  writer=std::thread([this](){
    this->workloop();
  });
}
mlog::PartitionLog::~PartitionLog(){
  mpsc.close();
  writer.join();
}
bool mlog::PartitionLog::recover(){
   if(is_recovered)  { return true;}
   commit_log_=std::make_unique<Commitlog>(path+ "/commit.bin",io);
   segment_ =std::make_unique<Segment>(path+"/log-00000000000000000000.seg",io);
   struct stat st;
   if(::stat( path.c_str(), &st)!=0){
    return false;
   }
   DIR* dir=::opendir(path.c_str()); 
   if(dir==nullptr){
    return false;
   }
   struct dirent* entry;
   bool have_file=false;
   bool losecommit=true;
   bool loseseg=true;
   entry = ::readdir(dir);
   while(entry != nullptr ){
    if(std::strcmp(entry->d_name,".")!=0 &&std::strcmp(entry->d_name,"..")!=0 ){
      have_file=true;
      break;
    }
    if(std::strcmp(entry->d_name,"commit.bin")!=0 ){
    losecommit=false;
    }
    if(std::strcmp(entry->d_name,"log-00000000000000000000.seg")!=0){
      loseseg=false;
    }
    entry = ::readdir(dir);
   }
   ::closedir(dir);
   Commitlog::Reply ans {};
   if(!have_file&&losecommit&&loseseg) {commit_log_->open_or_create(ans);
    segment_->creat();
   current_hw=0;
   epoch=0;
   next_batch_no=0; }
   else if(losecommit&&loseseg){commit_log_->open_or_create(ans);
   current_hw=ans.credit_hw;
   epoch=ans.last_epoch;
   next_batch_no=ans.last_batch_no+1;
  }else{ return false;}
   is_recovered=true; return true;
}
void mlog::PartitionLog::set_lease(uint64_t new_epoch,bool vaild){
  if(epoch<=new_epoch)
   epoch=new_epoch;
  else {return;}
  is_leader.store(vaild,std::memory_order_release);
}
bool mlog::PartitionLog::try_enqueue(AppendBatch item){
 if(!is_recovered) {return false;}
 bool ans=mpsc.try_push(std::move(item)); 
 if(ans){return true; } else {return false; }
}
mlog::Status mlog::PartitionLog::fetch(uint64_t offset_, uint32_t max_bytes, 
             std::vector<std::string>& out_messages, 
             uint64_t& next_offset, 
             uint64_t& hw) const {
            out_messages.clear();
            next_offset=offset_;
            hw=current_hw;
             return mlog::Status::out_of_range;
             }
uint64_t  mlog::PartitionLog::published_hw() const{
  return  current_hw.load(std::memory_order_acquire);
}
void mlog::PartitionLog::workloop(){
  AppendBatch batch;
  std::deque<AppendBatch> tasks;
  while(true){
    if(mpsc.wait_pop(batch)){
      auto time=std::chrono::steady_clock::now();
      tasks.push_back(batch);
      while(time + std::chrono::milliseconds(config.batch_window) > std::chrono::steady_clock::now()){
       AppendBatch next_batch;
       if(mpsc.try_pop(next_batch)){
       tasks.push_back(next_batch);
       }else{break; }
      }
    }else{
      return;
    }
   
    if(tasks.size()!=0){
      AppendBatch ans{};
      uint64_t group_base=0;
      if(is_leader){
        group_base=current_hw.load();
       while(!tasks.empty()){
        ans=std::move(tasks.front());
        tasks.pop_front();
        uint64_t base=current_hw.load();
      if(is_leader){ 
        for(auto& msg :ans.items){
         if(!segment_->append(msg, base, epoch)){
           
         }
         current_hw.fetch_add(1);
        }
        on_commit(ans.request_id,base,ans.items.size(),mlog::Status::ok);
      }
      else{ on_commit(ans.request_id,base,ans.items.size(),mlog::Status::not_leader); }
       }
      }else{
        while(!tasks.empty()){
      ans=std::move(tasks.front());
        tasks.pop_front();
      on_commit(ans.request_id,current_hw,ans.items.size(),mlog::Status::not_leader);
       }
      }
    }
  }
   
}
uint64_t  mlog::PartitionLog::current_epoch() const{
  return epoch.load(std::memory_order_acquire);
}

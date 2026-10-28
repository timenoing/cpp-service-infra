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
,config(config_),on_commit(on_commit_),io(io::posix_file_io()), mpsc(16ULL  *  1024 * 1024, 128) {
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
   bool hascommit=false;
   bool hasseg=false;
   entry = ::readdir(dir);
   while(entry != nullptr ){
    if(std::strcmp(entry->d_name,"commit.bin")==0 ){
     hascommit=true;
    }
    if(std::strcmp(entry->d_name,"log-00000000000000000000.seg")==0){
     hasseg=true;
    }
    entry = ::readdir(dir);
   }
   ::closedir(dir);
   Commitlog::Reply ans {};
   if(hasseg&&hascommit){
     if(!commit_log_->open_or_create(ans)){ return false; }
     if(!segment_->open_append()){ return false; }
     current_hw.store(ans.credit_hw, std::memory_order_release);
     epoch.store(ans.last_epoch, std::memory_order_release);
     next_batch_no.store(ans.last_batch_no+1, std::memory_order_release);
     is_recovered.store(true, std::memory_order_release);
     return true;
   }
   if(!hasseg&&!hascommit){
     if(!commit_log_->open_or_create(ans)){ return false; }
     if(!segment_->creat()){ return false; }
     current_hw.store(0, std::memory_order_release);
     epoch.store(0, std::memory_order_release);
     next_batch_no.store(1, std::memory_order_release);
     is_recovered.store(true, std::memory_order_release);
     return true;
   }
   return false;
}
void mlog::PartitionLog::set_lease(uint64_t new_epoch,bool vaild){
  if(epoch<=new_epoch)
   epoch=new_epoch;
  else {return;}
  is_leader.store(vaild,std::memory_order_release);
}
bool mlog::PartitionLog::try_enqueue(AppendBatch item){
 if(!is_recovered || halt) {return false;}
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
  std::vector<uint64_t> starts;

  // 唯一失败出口：回包 -> 关通道 -> 抽干队列 -> 停写。
  // reply_group=false 用于 S6 失租（当前组不回包，留作已凭据未发布）
  // 与 S7 失败（ok 已发出，不撤回）。
  auto fail_stop = [&](Status status, bool reply_group) {
    halt.store(true, std::memory_order_release);
    if (reply_group) {
      for (std::size_t i = 0; i < tasks.size(); ++i) {
        on_commit(tasks[i].request_id, starts[i], tasks[i].items.size(), status);
      }
    }
    tasks.clear();
    starts.clear();
    mpsc.close();
    uint64_t cursor = current_hw.load(std::memory_order_acquire);
    AppendBatch rest;
    while (mpsc.try_pop(rest)) {
      on_commit(rest.request_id, cursor, rest.items.size(), status);
      cursor += rest.items.size();
    }
  };

  while(true){
    if(!mpsc.wait_pop(batch)){
      return;
    }

    tasks.clear();
    tasks.push_back(std::move(batch));
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(config.batch_window);
    while(std::chrono::steady_clock::now() < deadline){
      AppendBatch next_batch;
      if(!mpsc.try_pop(next_batch)){ break; }
      tasks.push_back(std::move(next_batch));
    }

    const uint64_t group_epoch = current_epoch();
    uint64_t cursor = current_hw.load(std::memory_order_acquire);
    uint64_t total = 0;
    starts.resize(tasks.size());
    for(std::size_t i = 0; i < tasks.size(); ++i){
      starts[i] = cursor;
      cursor += tasks[i].items.size();
      total += tasks[i].items.size();
    }
    const uint64_t group_end = cursor;

    // 批头租约闸门
    if(!is_leader.load(std::memory_order_acquire)){
      for(std::size_t i = 0; i < tasks.size(); ++i){
        on_commit(tasks[i].request_id, starts[i], tasks[i].items.size(),
                  Status::not_leader);
      }
      continue;
    }

    // 空组：不写段、不写账本、不推进 HW
    if(total == 0){
      for(std::size_t i = 0; i < tasks.size(); ++i){
        on_commit(tasks[i].request_id, starts[i], 0, Status::ok);
      }
      continue;
    }

    if(group_epoch > 0xffffffffull){
      fail_stop(Status::io_error, true);
      return;
    }
    const uint32_t write_epoch = static_cast<uint32_t>(group_epoch);

    // S1 追加记录：只写段，不推进 HW、不回包
    for(std::size_t i = 0; i < tasks.size(); ++i){
      uint64_t off = starts[i];
      for(auto& msg : tasks[i].items){
        if(!segment_->append(msg, off, write_epoch)){
          fail_stop(Status::io_error, true);
          return;
        }
        ++off;
      }
    }

    // S2 段同步
    if(!segment_->sysnc()){
      fail_stop(Status::io_error, true);
      return;
    }

    // S3/S4 候选 + 凭据
    commit_log_->log.batch_no = next_batch_no.load(std::memory_order_acquire);
    commit_log_->log.candidate_hw = group_end;
    commit_log_->log.epoch = group_epoch;
    commit_log_->log.state = 0;
    if(!commit_log_->append()){
      fail_stop(Status::io_error, true);
      return;
    }
    commit_log_->log.state = 1;
    if(!commit_log_->append()){
      fail_stop(Status::io_error, true);
      return;
    }

    // S5 账本同步：到这里之前不允许回包
    if(!commit_log_->sync()){
      fail_stop(Status::io_error, true);
      return;
    }

    // S6 发布前复验租约
    if(!is_leader.load(std::memory_order_acquire) ||
       group_epoch != current_epoch()){
      fail_stop(Status::not_leader, false);
      return;
    }
    current_hw.store(group_end, std::memory_order_release);
    for(std::size_t i = 0; i < tasks.size(); ++i){
      on_commit(tasks[i].request_id, starts[i], tasks[i].items.size(), Status::ok);
    }

    // S7 发布条：不单独 sync，随下一组 S5 落盘
    commit_log_->log.state = 2;
    if(!commit_log_->append()){
      fail_stop(Status::io_error, false);
      return;
    }
    next_batch_no.fetch_add(1, std::memory_order_release);
  }
}
uint64_t  mlog::PartitionLog::current_epoch() const{
  return epoch.load(std::memory_order_acquire);
}

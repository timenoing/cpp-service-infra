#ifndef PARTITION_LOG_H
#define PARTITION_LOG_H
#include "mq/io/File_io.h"
#include "Commit_log.h"
#include "Segment.h"
#include <queue>
#include <vector>
#include <cstdint>
#include <mutex>
#include <string>
#include <condition_variable>
#include<functional>
#include <thread>
#include <atomic>
#include <dirent.h>
#include <cstring>
#include <chrono>
#include <deque>
#include <memory>
namespace mlog {
    enum class Status: uint8_t {ok,full,io_error,not_leader,out_of_range };
       struct Logconfig{
        uint64_t segment_size=256ULL*1024*1024;
        uint64_t segment_age=3600ULL*1000;
        uint64_t retetion=1ULL*3600*1000;
        uint64_t batch_window=5;
        uint32_t anchor_bytes=64000;
        uint32_t anchor_idx=1024;
        size_t capacity_limit=2ULL*1024*1024*1024;
        std::string dir;
        };
          struct AppendBatch{
    uint64_t request_id=0;
    std::vector<std::string> items;
    uint64_t total_bytes=0;
    };
        class Mpsc_bounded{
        public:
        Mpsc_bounded(uint32_t max_bytes,uint32_t max_tasks);
        bool try_push(AppendBatch item);
        bool try_pop(AppendBatch& ans);
        bool wait_pop(AppendBatch& ans);
        void close();
        uint64_t queued_task();
        uint64_t queued_bytes();
        private:
        bool is_closed=false;
        std::queue<AppendBatch> mpsc;
        uint32_t max_bytes;
        uint32_t max_tasks;
        std::mutex mu_;
        std::condition_variable cv_;
        uint64_t current_bytes=0;
        };
    class PartitionLog{
   public:
   PartitionLog(const PartitionLog&)=delete;
   PartitionLog& operator =(const PartitionLog)=delete;
   PartitionLog(const std::string path_,Logconfig config_,
    std::function<void(uint64_t request_id, uint64_t offset,size_t count,Status status)> on_commit_,mlog::io::File_io& io_);
    PartitionLog(const std::string path_,Logconfig config_,
    std::function<void(uint64_t request_id, uint64_t offset,size_t count,Status status)> on_commit_);
   ~PartitionLog();
   Status fetch(uint64_t offset, uint32_t max_bytes, 
             std::vector<std::string>& out_messages, 
             uint64_t& next_offset, 
             uint64_t& hw) const ;
   bool recover();
   void set_lease(uint64_t new_epoch,bool vaild);
   bool try_enqueue(AppendBatch item);
  uint64_t  published_hw()const ;
  uint64_t  current_epoch()const ;
   void workloop();
   private:
   std::string path;
   std::atomic_bool halt=false;
   mlog::io::File_io& io;
   std::function<void(uint64_t request_id, uint64_t offset,size_t count,Status status)> on_commit;
   Logconfig config;
   std::thread writer;
   Mpsc_bounded mpsc;
   std::atomic_bool is_recovered=false;
   std::atomic_bool is_leader=false;
   std::atomic_uint64_t current_hw=0;
   std::atomic_uint64_t next_batch_no=0;
    std::atomic_uint64_t epoch=0;
    std::unique_ptr<Commitlog> commit_log_=nullptr;
    std::unique_ptr<Segment> segment_=nullptr;
};
}
#endif
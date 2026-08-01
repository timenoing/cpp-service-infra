#ifndef CONNECTIONPOOL_H
#define CONNECTIONPOOL_H
#include<unordered_map>
#include<chrono>
#include<condition_variable>
#include<mutex>
#include<functional>
#include<thread>
#include<future>
#include<list>
#include<memory>
#include"Connection.h"
#include"ConnectionGuard.h"
#include"ThreadPool.h"


class ConnectionPool
{
  public:
  ConnectionPool(const std::string& filename,size_t min_conn,size_t max_conn);
  ~ConnectionPool();

  ConnectionPool(const ConnectionPool&)=delete;
  ConnectionPool& operator=(const ConnectionPool&)=delete;
  ConnectionPool(ConnectionPool&&)=delete;
  ConnectionPool& operator=(ConnectionPool&&)=delete;

  std::future<ConnectionGuard> acquire();
  void release(Connection* conn);



  private:
  bool expand();
  void scanLoop();
  bool stopping_=false;
  std::string filename_;
  size_t min_conn_;
  size_t max_conn_;
  std::list<Connection*> idle_;
  std::list<Connection*> in_use_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::thread scanner_thread_;
  std::unique_ptr<ThreadPool>  internal_pool_;
  std::unordered_map<Connection*,std::chrono::steady_clock::time_point> borrow_time_;
  std::unordered_map<Connection*,std::chrono::steady_clock::time_point> return_time_;
  size_t total_conn_ =0;


};

#endif
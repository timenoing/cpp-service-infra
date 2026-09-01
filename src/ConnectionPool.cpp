#include "ConnectionPool.h"
#include "Logger.h"
#include <algorithm>

ConnectionPool::ConnectionPool(const std::string& filename,size_t min_conn,size_t max_conn)
: filename_(filename)
    , min_conn_(min_conn)
    , max_conn_(max_conn)
    , total_conn_(min_conn)
    , stopping_(false)
    , internal_pool_(std::make_unique<ThreadPool>(2))
{
    for(size_t i=0;i<min_conn_;++i)
    {
        try
        {
         idle_.push_back(new Connection(filename_));
        }
        catch(...)
        {
        Logger::Instance().Log(WARN,"new Connection异常","请检查");
        }
    }
    scanner_thread_=std::thread(&ConnectionPool::scanLoop,this);
}
ConnectionPool::~ConnectionPool()
{
    {
    std::unique_lock<std::mutex> lock(mutex_);
    stopping_=true;
    lock.unlock();
    cv_.notify_all();
    if(scanner_thread_.joinable())
    scanner_thread_.join();
    internal_pool_.reset();
    lock.lock();
    auto wait_time=std::chrono::seconds(30);
    cv_.wait_for(lock,wait_time,[this]{return in_use_.empty();});
    if(!in_use_.empty())
    {
    Logger::Instance().Log(WARN,"还有连接","等待连接析构");
    }

    for(auto it=idle_.begin();it!=idle_.end();)
    {
      Connection* conn=*it;
      it=idle_.erase(it);
      delete conn;
    }
    }

}
void ConnectionPool::release(Connection* conn)
{
    {
    std::unique_lock<std::mutex> lock(mutex_);
    if(borrow_time_.count(conn))
    {
    in_use_.remove(conn);
    borrow_time_.erase(conn);

    if(stopping_)
    {
    delete conn;
    return;
    }
    else
    {
        idle_.push_back(conn);
    }
    return_time_.emplace(conn,std::chrono::steady_clock::now());

    }
    else  return ;
    }
    cv_.notify_one();
}
bool ConnectionPool::expand()
{
  size_t need=0;
  {

  std::unique_lock<std::mutex> lock(mutex_);
  need=std::min<size_t>(2,max_conn_-total_conn_);
  if(need==0)
  return false;
  total_conn_+=need;
  }
  std::vector<Connection*> tem_; size_t retries=need; size_t count=0;
  for(size_t i=0;i<retries;++i)
  {
    Connection* conn=new Connection(filename_);
  if(conn->isvalid())
  {
  tem_.push_back(conn);
  ++count;
  }
  else {delete conn;++retries; if(retries>5) break;}
  }
  {
    std::unique_lock<std::mutex> lock(mutex_);
    total_conn_=total_conn_-need+count;
    for(auto*conn:tem_)
    {
    idle_.push_back(conn);
    }
  }
cv_.notify_one();
return true;
}
std::future<ConnectionGuard> ConnectionPool::acquire()
{
    auto task = std::make_shared<std::packaged_task<ConnectionGuard()>>([this]()->ConnectionGuard{
        Connection* conn=nullptr;
        {
        std::unique_lock<std::mutex> lock(mutex_);
        if(stopping_)
        return ConnectionGuard(nullptr, nullptr);
        }
        {
        std::unique_lock<std::mutex> lock(mutex_);
       if(idle_.empty()&&total_conn_<max_conn_)
       {
        lock.unlock();
        expand();
        lock.lock();
       }
        }
       auto start = std::chrono::steady_clock::now();
       {
        std::unique_lock<std::mutex> lock(mutex_);
       while (idle_.empty()&&!stopping_)
       {
        auto used =std::chrono::steady_clock::now() - start;
        auto remain=std::chrono::seconds(5)-used;
        if(remain<= std::chrono::seconds(0))
        return ConnectionGuard(nullptr, nullptr);

        bool judge=cv_.wait_for(lock,remain,[this]{return !idle_.empty()||stopping_;});
        if(stopping_)
        return ConnectionGuard(nullptr, nullptr);
        if(judge)
        break;

       }
       if (stopping_) return ConnectionGuard(nullptr, nullptr);
        conn=idle_.back();
        return_time_.erase(conn);
        idle_. pop_back();
        in_use_.push_back(conn);
        borrow_time_.emplace(conn,std::chrono::steady_clock::now());
        warn_.erase(conn);

        }
         if(!conn->ping())
       {
         {
         std::unique_lock<std::mutex> lock(mutex_);
         total_conn_--;
         in_use_.remove(conn);
         warn_.erase(conn);
         borrow_time_.erase(conn);
         }
         delete conn;
         cv_.notify_one();
         return ConnectionGuard(nullptr,nullptr);
       }
        return ConnectionGuard(conn,[this](Connection* conn){this->release(conn);});
       });
        std::future<ConnectionGuard> f=task->get_future();
        internal_pool_->submit([task]() {
        (*task)(); });
        return f;
}
void  ConnectionPool::scanLoop()
{
    while (!stopping_)
    {   auto  time=std::chrono::seconds(30);
        {
        std::unique_lock<std::mutex>lock(mutex_);
        cv_.wait_for(lock,time,[this]{return stopping_;});
        if(stopping_)
        return ;
        for(const auto& pair:borrow_time_)
        {   auto now=std::chrono::steady_clock::now();
            if(now-pair.second>std::chrono::seconds(5))
            {
            if(warn_[pair.first]<=0)
            {
            warn_[pair.first]++;
            Logger::Instance().Log(WARN,"连接超时未还","警告");
            }
            }
            
        }
        std::vector<Connection*> return_tem_;
        if(total_conn_>min_conn_)
        {
        for(const auto& pair:return_time_)
        {
            auto elapsed=std::chrono::steady_clock::now();
        if(elapsed-pair.second>std::chrono::seconds(30))
            return_tem_.push_back(pair.first);
        }
        for(const auto& conn:return_tem_)
        {
            if(total_conn_<=min_conn_)
            break;
            idle_.remove(conn);
            return_time_.erase(conn);
            warn_.erase(conn);
            total_conn_--;
            delete conn;
        }
        }

        }
    }

}
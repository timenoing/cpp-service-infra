#ifndef TRACE_H
#define TRACE_H
#include<atomic>
#include<thread>
#include<string>
#include<cstdint>
class TraceID
{
    public:
    TraceID();
    TraceID(std::uint64_t timestamp,std::uint64_t counter);
    std::string toString() const;
    bool operator<(const TraceID& other) const;

    private:
    std::uint64_t timestamp_;
    std::uint64_t counter_;
};

class Trace
{
public:
 static TraceID generateID();//建立
 static TraceID getCurrentID();//获取
 static void clearCurrentID();//清除
 static void setCurrentID(const TraceID& id);//存放
  private:
  static std::atomic<uint64_t> counter_;
  static thread_local TraceID current_id_;


};

#endif
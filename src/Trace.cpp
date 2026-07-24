#include"Trace.h"
#include<chrono>
std::atomic<uint64_t> Trace::counter_{0};
thread_local TraceID Trace::current_id_;
TraceID::TraceID()
 : timestamp_{0},counter_{0}
{
}
bool  TraceID::isempty() const
{
    return timestamp_==0&&counter_==0;
}
bool  TraceID::operator<(const TraceID& other) const {
        if (timestamp_ != other.timestamp_)
            return timestamp_ < other.timestamp_;
        return counter_ < other.counter_;
    }

TraceID::TraceID(std::uint64_t ts,std::uint64_t ct)
 : timestamp_(ts),counter_(ct)
{
}
std::string  TraceID::toString() const
{
   std::string ts=std::to_string(timestamp_);
   std::string ctr=std::to_string(counter_);
   return std::string ("[")+"Trace"+"]["+ts+"]["+ctr+"]";
}

TraceID Trace::generateID()
{
    auto now=std::chrono::system_clock::now();
    auto ms=std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    uint64_t timestamp=static_cast<uint64_t>(ms);
    uint64_t counter = counter_.fetch_add(1, std::memory_order_relaxed);
    return TraceID(timestamp,counter);
}
TraceID Trace::getCurrentID()
{
     return current_id_;
}
void Trace::clearCurrentID()
{
     current_id_= TraceID();
}
void Trace::setCurrentID(const TraceID& id)
{
     current_id_=id;
}
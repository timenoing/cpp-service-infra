#include"ThreadPool.h"


ThreadPool::ThreadPool(size_t threads_count)
{
    for(size_t i=1;i<=threads_count;++i)
    threads.emplace_back(&ThreadPool::workerLoop,this);//&ThreadPool::workerLoop 创建后之间执行这个函数
}
void ThreadPool::workerLoop()
{
    while(true)
  {  
    std::function<void()> task;
    {
    std::unique_lock<std::mutex> lock(mut);
    cv.wait(lock,[this] {return !tasks.empty()||stopping;});
    if (stopping && tasks.empty())
    return;
    task = std::move(tasks.front());
    tasks.pop();
    }
    try{
   task();
    }
    catch(...)
    {
        std::cout <<"异常程序"<<std::endl;
    }
  } 
}
ThreadPool::~ThreadPool()
{   {
    std::unique_lock<std::mutex> lock(mut);
    cv.wait(lock,[this]{return tasks.empty();});
    stopping=true;
    }
     cv.notify_all();
    for(auto&t :threads) t.join();
}
void ThreadPool::submit(std::function<void()> task)
{ 
      {
   std::unique_lock<std::mutex> lock(mut);
   if(stopping)  return;
    tasks.push(std::move(task));
    }
    cv.notify_one();
    
}

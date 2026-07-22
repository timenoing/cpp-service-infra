#ifndef THREAD_POOL_H
#define THREAD_POOL_H
#include<iostream>
#include<mutex>
#include<queue>
#include<thread>
#include<condition_variable>
#include<functional>
#include<vector>

class ThreadPool
{
    public:
      void submit(std::function<void()> task);//提交任务函数
      ThreadPool(size_t thread_count);//构造
      ~ThreadPool();//析构
      ThreadPool(const ThreadPool&) = delete;
      ThreadPool& operator=(const ThreadPool&) = delete;

    private:
     void workerLoop(); //循环函数
     std::vector <std::thread> threads;
     std::queue<std::function<void()>> tasks;
     std::mutex  mut;
     std::condition_variable cv;
     bool stopping=false;
     

};
#endif
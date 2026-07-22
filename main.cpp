#include "ThreadPool.h"
#include <thread>
#include <chrono>
#include<atomic>
#include<iostream>
#include<stdexcept>
void test1();
void test2();
void test3();/*
void test4();*/
void test1()
{
    std::atomic<int> counter{0};
    {

    ThreadPool pool_(4);
    for(size_t i=0;i<4;++i)
    {
        pool_.submit([i,&counter] {std::cout << i <<std::endl;counter++;});
    }
   
    }  
     std::cout << counter << std::endl;
}
void test2()
{
    auto start =std::chrono::steady_clock::now();
    std::atomic<int> counter{0};
    {
    ThreadPool pool_(4);
    for(size_t i=0;i<8;++i)
    {
        pool_.submit([i,&counter] {std::cout<<i<<std::endl;counter++;std::this_thread::sleep_for(std::chrono::seconds(1));});
    }
    }
    std::cout << counter << std::endl;

    auto end =std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "耗时: " << ms << " ms\n";
}
void test3()
{
   std::atomic<int> counter{0};
    {

    ThreadPool pool_(4);
     pool_.submit([&counter] {throw std::runtime_error("...");counter++;});
    for(size_t i=0;i<3;++i)
    {
        pool_.submit([&counter] {counter++;});
    }
   
    }  
     std::cout << counter << std::endl;
}
int main()
{
 
    test2();
    return 0;
}

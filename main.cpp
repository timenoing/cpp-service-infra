#include"logger.h"
#include"ThreadPool.h"
#include"Trace.h"
#include<iostream>
#include<set>
void test1();
void test1()
{
    Logger::Instance().Log(WARN,"abc","def");
}
int main()
{  

    test1();
    return 0;
    
}
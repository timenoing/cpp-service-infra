#include"logger.h"
#include"ThreadPool.h"
void test1();

void test1()
{
    ThreadPool pool(5);
    for(size_t i=0;i<50;++i)
    pool.submit([]{ Logger::Instance().Log(INFO, "test", "");});
    
}
int main()
{
    test1();
    return 0;
}
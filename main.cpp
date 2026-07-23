#include"logger.h"
#include"ThreadPool.h"
#include"Trace.h"
#include<iostream>
#include<set>
void test1();
void test2();
void test3();
void test4();
void test1()
{
    TraceID id;
    id=Trace::generateID();
    std::cout<<id.toString()<<std::endl;
    
}

void test2()
{ TraceID id;
  std::set<TraceID> s;
  for(size_t i=0;i<100;++i)
  {
    id=Trace::generateID();
    s.insert(id);
    
  }
 std::cout<<s.size()<<std::endl;
}
void test3() {
    TraceID main_id = Trace::generateID();
    Trace::setCurrentID(main_id);
    {
        ThreadPool pool(1);
        pool.submit([] {
            TraceID sub_id = Trace::generateID();
            Trace::setCurrentID(sub_id);
            std::cout << "子线程 ID: " << Trace::getCurrentID().toString() << std::endl;
        });
    
    }   // ← pool 在这里析构
  
    std::cout << "主线程 ID: " << Trace::getCurrentID().toString() << std::endl;
}
void test4()
{
    TraceID id=Trace::generateID();
    Trace::setCurrentID(id);
    ThreadPool pool(1);
    pool.submit([id]{Trace::setCurrentID(id);TraceID id2=Trace::getCurrentID();std::cout<<Trace::getCurrentID().toString()<<std::endl;});
    std::cout<<Trace::getCurrentID().toString()<<std::endl;
}

int main()
{  
    test1();
    test2();
    test3();
    test4();
    return 0;
    
}
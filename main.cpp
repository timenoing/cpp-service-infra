#include "ConnectionPool.h"
#include "ConnectionGuard.h"
#include"Cursor.h"
#include "Logger.h"
#include <iostream>
#include <thread>
#include <vector>
#include <string>

#define CHECK(expr) do {                                                    \
    if(!(expr)) {                                                           \
        std::cerr << "CHECK failed: " << #expr << " @line " << __LINE__     \
                  << std::endl;                                             \
        Logger::Instance().Log(WARN, "CHECK failed", #expr);                \
        std::terminate();                                                   \
    }                                                                       \
} while(0)

static const std::string DB = "test_pool.db";

static void createTable(ConnectionPool& pool)
{
    auto g = pool.acquire().get();
    CHECK(!g.empty());
    g->execute("DROP TABLE IF EXISTS t");
    g->execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)");
}

int main()
{
    // TODO: 照你现有 main 的写法初始化 Logger(单例注入单线程池)
    // Logger::Instance().Init(...);

    // ---- Test 1: 主路径 ----
    {
        ConnectionPool pool(DB, 2, 8);
        auto g = pool.acquire().get();
        CHECK(!g.empty());
        g->execute("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, name TEXT)");
        g->execute("DELETE FROM t");
        g->execute("INSERT INTO t(id,name) VALUES(1,'hello')");

        auto cur = g->query("SELECT name FROM t WHERE id=1");
        CHECK(cur->next());
        CHECK(cur->getString(0) == "hello");
    }   // g 析构归还 → pool 析构,无崩无泄漏

    // ---- Test 4: Guard 移动 ----
    {
        ConnectionPool pool(DB, 2, 8);
        auto g1 = pool.acquire().get();
        CHECK(!g1.empty());
        ConnectionGuard g2 = std::move(g1);
        CHECK(g1.empty());
        CHECK(!g2.empty());
        g2->execute("INSERT INTO t(id,name) VALUES(2,'world')");
    }

    // ---- Test 2: 并发 + 扩容 ----
    {
        ConnectionPool pool(DB, 2, 8);
        createTable(pool);
        const int T = 8, N = 50;
        std::vector<std::thread> ths;
        for(int t = 0; t < T; ++t)
        {
            ths.emplace_back([&pool, t]{
                for(int i = 0; i < N; ++i)
                {
                    auto g = pool.acquire().get();
                    CHECK(!g.empty());
                    g->execute("INSERT INTO t(name) VALUES('x')");
                }
            });
        }
        for(auto& th : ths) th.join();

        auto g = pool.acquire().get();
        auto cur = g->query("SELECT COUNT(*) FROM t");
        CHECK(cur->next());
        CHECK(cur->getInt(0) == T * N);
    }

    // ---- Test 3: 超 max 阻塞 5s 后返回空 Guard ----
    {
        ConnectionPool pool(DB, 2, 8);
        createTable(pool);
        std::vector<ConnectionGuard> hold;
        for(int i = 0; i < 8; ++i)
        {
            auto g = pool.acquire().get();
            CHECK(!g.empty());
            hold.push_back(std::move(g));
        }
        auto g = pool.acquire().get();   // 第 9 个:等 5s
        CHECK(g.empty());                 // 超时返回空 Guard
    }   // hold 析构归还 → pool 析构

    std::cout << "ALL TESTS PASSED" << std::endl;
    return 0;
}
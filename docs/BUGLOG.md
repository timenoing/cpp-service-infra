# BUGLOG

记录项目演进过程中踩到、定位、修复（或挂账）的每一个 BUG。
每条按六要素写：是什么 / 在哪 / 谁碰 / 为什么 / 怎么修 / 怎么验。
格式约定：状态为 已修（附提交号）或 欠账（待修）。

---

## 1. stopping 未初始化——启动即可能静默退出 【已修】

- **是什么**：`std::atomic<bool> stopping;` 只声明不置值，内存内容是随机比特。
- **在哪**：`include/Epoll.h` 成员声明处。
- **谁碰**：`start()` 主循环第一圈 `if(stopping)`（src/Epoll.cpp）。若随机值非零，服务器一起来就进退出流程：关 listen → 排干（空载立即通过）→ 静默退出，日志表现为"成功启动"紧跟"优雅退出"。
- **为什么**：atomic 与普通 bool 一样，声明不等于初始化，读到的是上一进程残留的内存。
- **怎么修**：`std::atomic<bool> stopping{false};`
- **怎么验**：启动后主循环正常运行不触发退出分支。

## 2. Nagle + 延迟 ACK 打架——流水线延迟尾部 50ms 【已修 8a7ab51】

- **是什么**：服务端 accepted socket 默认开 Nagle，客户端默认开延迟 ACK（Linux 底线 40ms），流水线模式下互相等待。
- **在哪**：`onaccept` 未对 client socket 设 `TCP_NODELAY`。
- **谁碰**：所有 in-flight>1 的连接（bench_client -d 流水线）。
- **为什么**：一问一答时 ACK 搭请求包的车，永不相撞；流水线时客户端灌完请求无数据可发，延迟 ACK 干等 40ms 才回，服务端 Nagle 攥住后续小包——一批响应只有第 1 个快，其余全卡 ACK 定时器。
- **怎么修**：onaccept 中 `setsockopt(client, IPPROTO_TCP, TCP_NODELAY, ...)`。
- **怎么验**：c=2 n=200 s=128 d=10：P99 51.2ms → 0.56ms（↓91 倍），QPS 6.7K → 70K。
- **教训**：延迟敏感框架，服务端小包攒包语义是错的；P50/P99 分布形状（批次头部快、尾部整齐卡在 40ms 倍数）是 Nagle 的指纹。

## 3. accept 慢排空——连接风暴下 fd 积压、秒级尾延迟 【已修，见 fix 提交】

- **是什么**：`onaccept` 每轮 epoll 唤醒只 accept 一个连接。
- **在哪**：src/Epoll.cpp `onaccept`。
- **谁碰**：并发建连 > accept 消化速率的任何场景（500 并发建连即现形）。
- **为什么**：水平触发下 listen 虽会持续可读，但每轮 epoll_wait 循环还夹着海量业务事件，accept 速率跟不上建连速率；连接滞留内核队列，排队时间被灌入其上消息的延迟。
- **怎么修**：onaccept 改 `while(1)` 排干循环，`EAGAIN/EWOULDBLOCK` 退出。
- **怎么验**：负载中 fd 快照 290/505 → 508/505 齐；d=10 流水线 P99 3104ms → 58.5ms。

## 4. listen backlog=128 溢出——SYN-ACK 重传固定 1s 尾巴 【已修，见 fix 提交】

- **是什么**：并发建连超过 backlog 时，溢出连接卡 SYN 队列，触发 SYN-ACK 重传（Linux 定时器 1s），建连侧以为已建立、发出的消息延迟被抬高约 1s。
- **在哪**：src/Epoll.cpp `create_fd` 中 `listen(mlisten, 128)`。
- **谁碰**：连接风暴（>128 并发 connect）。
- **为什么**：accept 队列满后新 SYN 无法入队完成三次握手，客户端阻塞 connect 返回后立即发数据，服务端其实尚未握手完成。
- **怎么修**：backlog 128 → 1024。
- **怎么验**：判别实验——c=120（backlog 内）P999=9.1ms 无尾巴；c=500 P999=1078ms，数字与 SYN 重传定时器精确吻合；改后 c=500 P999 16.3ms。
- **教训**：秒级尾巴先查 TCP 重传定时器（1s/2s/4s 指数退避），别急着怀疑业务代码。

## 5. epoll 事件数组容量 256 截断风险 【已修，见 fix 提交】

- **是什么**：`ev64[256]` 配 500 连接时单轮事件批量可达数百，数组截断。
- **在哪**：include/Epoll.h `ev64[256]` + src/Epoll.cpp 两处 `epoll_wait` 的 maxevents。
- **怎么修**：256 → 512 同步三处。
- **怎么验**：500 连接下无事件丢失（正确性长跑 PASS）。水平触发下截断不丢事件（下轮继续报），但会增加唤醒轮次。

## 6. EMFILE 忙等——fd 打满后空转烧核 【欠账，已实锤待修】

- **是什么**：accept 返回 EMFILE/ENFILE 时直接 return，但 listen 水平触发永远可读 → epoll_wait 立即返回 → accept 再失败 → **纯忙等死循环**，CPU 单核 100%，零产出。
- **在哪**：src/Epoll.cpp `onaccept` 的 `errno == EMFILE || errno == ENFILE` 分支。
- **谁碰**：fd 配额用尽 + backlog 里还有存货的任何时刻（生产环境 fd 泄漏、突发建连高峰都会撞）。
- **实锤方法**：`ulimit -n 60` 起服务器，200 个只连不发空连接挂着（fd=60 打满），CPU 74.5%→83.1%，TIME 持续增长，STAT=R。
- **修法候选**（未拍板）：EMFILE 时 accept 一个并立即 close（泄洪排队）；或退避定时器限频重试。
- **教训**：EMFILE 分支"处理失败"不等于"处理事件"——水平触发的可读不会因为你没能力 accept 而消失。

## 7. g_epoll 头文件 static——每个编译单元一份副本 【欠账】

- **是什么**：`static Epoll* g_epoll` 写在头文件，每个 include 它的 .cpp 各有一份 nullptr 副本。
- **在哪**：include/Epoll.h 文件尾部。
- **为什么能跑**：on_signal 定义在 Epoll.cpp，读的是本 TU 那份被构造函数赋值的副本。单实例 + 单 TU 现状下安全。
- **风险**：任何其他 TU 读 g_epoll 都是 nullptr；多 Epoll 实例时信号路由错误。
- **修法候选**：改为非 static 定义于 Epoll.cpp + 头文件 extern 声明；多实例则需信号→实例注册表。

## 8. 优雅退出 deadline 只管 epoll 层，进程退出可超 deadline 【欠账，观察项】

- **是什么**：排干期 deadline(3s) 准点停止收发，但 `~Epoll` 中线程池析构要**排干整个 worker 队列才 join**，剩余任务继续执行完，进程退出时间超出 deadline。
- **在哪**：src/ThreadPool.cpp `~ThreadPool`（`stopping && tasks.empty()` 才退出，队列非空继续消费）。
- **实测**：200 条 SLOW(100ms) 在途 + SIGTERM：epoll 层 3s 准点，进程 4.5s 退出（剩余 80 任务 × 100ms / 4 worker）。
- **影响**：echo 无感；MQ 慢任务场景退出时间不可控。
- **修法候选**：池析构改为丢弃队列立即 join（`if(stopping) return`），或提供 discard 语义开关。

---

## 附录：验收守则（环境坑，非代码 BUG）

- **PowerShell→ssh 引号坑**：远端命令含 `()`、`|`、`"` 必炸；一律走"本地写脚本文件 → scp → bash 执行"。
- **起没起来靠日志判定**：server bind 失败只静默退出，验收脚本必须 grep app.log 的"成功启动"，不能只看进程存在。
- **残留 server 占端口**：实验脚本开头 `pkill -f "bin/server"` + bind 失败退出码 1 的表象要学会识别。
- **快照定时器要跟上性能**：负载内 fd 快照必须赶在压测结束前采样，跑得快的服务器会让"1s 后快照"拍到空场（本次 EMFILE 首测误判的根源）。
- **服务器侧 connect 语义**：backlog 未溢出时，客户端 blocking connect 在 SYN 队列即可返回，"connect 成功"≠"server 已 accept"。

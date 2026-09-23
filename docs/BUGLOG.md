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

## 6. EMFILE 忙等——fd 打满后空转烧核 【已修，见 fix 提交】

- **是什么**：accept 返回 EMFILE/ENFILE 时直接 return，但 listen 水平触发永远可读 → epoll_wait 立即返回 → accept 再失败 → **纯忙等死循环**，CPU 单核 100%，零产出。
- **在哪**：src/Epoll.cpp `onaccept` 的 `errno == EMFILE || errno == ENFILE` 分支。
- **谁碰**：fd 配额用尽 + backlog 里还有存货的任何时刻（生产环境 fd 泄漏、突发建连高峰都会撞）。
- **实锤方法**：`ulimit -n 60` 起服务器，200 个只连不发空连接挂着（fd=60 打满），CPU 74.5%→83.1%，TIME 持续增长，STAT=R。
- **怎么修**：预留 fd 泄洪——构造时 `idle_fd=open("/dev/null")` 占位；accept 撞 EMFILE/ENFILE 时：`close(idle_fd)` 腾槽 → `accept()` 拿一个真连接 → **立即 `close(client)` 拒绝**（泄洪本体）→ 重开 `/dev/null` 占位归还槽。单线程保证 close 与 open 之间无第三方抢槽，重开必成。
- **怎么验**：ulimit 60 + 200 空连接：CPU 83% 空转 → **0.1%、TIME 零增长、STAT=Sl**。
- **教训**：先退款再申请，永远别问自己没有的槽；EMFILE 分支"处理失败"不等于"处理事件"，水平触发的可读不会消失。

## 7. g_epoll 头文件 static——每个编译单元一份副本 【欠账】

- **是什么**：`static Epoll* g_epoll` 写在头文件，每个 include 它的 .cpp 各有一份 nullptr 副本。
- **在哪**：include/Epoll.h 文件尾部。
- **为什么能跑**：on_signal 定义在 Epoll.cpp，读的是本 TU 那份被构造函数赋值的副本。单实例 + 单 TU 现状下安全。
- **风险**：任何其他 TU 读 g_epoll 都是 nullptr；多 Epoll 实例时信号路由错误。
- **修法候选**：改为非 static 定义于 Epoll.cpp + 头文件 extern 声明；多实例则需信号→实例注册表。

## 8. 优雅退出 deadline 只管 epoll 层，进程退出可超 deadline 【已修，见 fix 提交】

- **是什么**：排干期 deadline(3s) 准点停止收发，但 `~Epoll` 中线程池析构要**排干整个 worker 队列才 join**，剩余任务继续执行完，进程退出时间超出 deadline。
- **在哪**：src/ThreadPool.cpp `~ThreadPool`（`stopping && tasks.empty()` 才退出，队列非空继续消费）。
- **实测**：200 条 SLOW(100ms) 在途 + SIGTERM：epoll 层 3s 准点，进程 4.5s 退出（剩余 80 任务 × 100ms / 4 worker）。
- **影响**：echo 无感；MQ 慢任务场景退出时间不可控。
- **怎么修**：workerLoop 退出条件 `stopping && tasks.empty()` → `stopping`——见 stopping 即退，未开工任务即弃；析构无条件 join（worker 即退故 join 近瞬时）。曾试图用 detach/空分支代替 join，两条都是雷：joinable 线程析构触发 std::terminate（每次优雅退出 abort）；detached 线程在 ~Epoll 成员析构后执行引用已死成员的任务（UAF）。**join 的速度由 worker 何时返回决定，不需要 detach**。
- **怎么验**：deadline 实验 4536ms → **3125ms**；全程 exit=0 无 abort；回归 smoke 7/7。
- **教训**：关停语义是池子自己的设计决定，不许调用侧 hack；"排干"和"丢弃"是两种合法契约，MQ 期需要"排干再关"时再加带真语义的 shutdown 开关。

## 9. senddata 直发绕队列——响应流帧错位 【已修，见 fix 提交】

- **是什么**：`senddata` 无条件先试直发，`Outbuffer` 非空时不检查就 send()——新响应插队进内核，排到队列里待发的旧尾巴**前面**。
- **在哪**：src/net/Connection.cpp `senddata` / `handwrite` 双写入口。
- **谁碰**：内核发送缓冲被灌满的时刻——test_client 50 条×~32KB≈1.6MB 批次流水线正好压到；bench_client 单飞小包永远灌不满（30 万条 --check 全过的盲区）。
- **为什么**：TCP 只保证按 send() 调用序进内核；`Outbuffer` 里的字节晚一步才进内核 → 线上字节序 `R1头|R2|R1尾`，客户端解码器帧边界错位并级联污染后续帧。
- **指纹**：FAIL 成对出现于相邻序号、长度互换（如 sent 63439/recv 36531 + sent 36531/recv 63439）。
- **怎么修**：单写者不变量——队列非空禁止直发，一律 append；直发只许一次 send，发不完/没发出全部进队列，`handwrite` 独家排空。
- **怎么验**：修复前 test_client -n 1000 连续 bad=13/5；修复后 **30 万+ 消息零失败**（68×tc1000 + 10×tc5000 + 并发 5×3×10000 + 大包 --check 4000 条）；排查链：messagedecode 七种切法（65536/4096/1024/7/4/1）50 帧零错、Donequeue push/drain 双侧加锁核实、ondone fd+conn_id 双验——逐项排除后锁定。
- **教训**：一条 socket 只能有一个写入口；"失败才进队列"是残料暂存，不是队列化；B 阶段执行权转移时埋下，大包流水线引爆，潜伏两代版本。

---

## 附录：验收守则（环境坑，非代码 BUG）

- **PowerShell→ssh 引号坑**：远端命令含 `()`、`|`、`"` 必炸；一律走"本地写脚本文件 → scp → bash 执行"。
- **起没起来靠日志判定**：server bind 失败只静默退出，验收脚本必须 grep app.log 的"成功启动"，不能只看进程存在。
- **残留 server 占端口**：实验脚本开头 `pkill -f "bin/server"` + bind 失败退出码 1 的表象要学会识别。
- **快照定时器要跟上性能**：负载内 fd 快照必须赶在压测结束前采样，跑得快的服务器会让"1s 后快照"拍到空场（本次 EMFILE 首测误判的根源）。
- **服务器侧 connect 语义**：backlog 未溢出时，客户端 blocking connect 在 SYN 队列即可返回，"connect 成功"≠"server 已 accept"。
- **pkill 自杀案**：`pkill -f` 的模式串若出现在 ssh 命令行里（如 `pkill -f test_client`），会匹配到自己的 bash -c 进程把会话杀掉，表现为命令"无输出"。pkill 只准写在脚本文件内部。
- **海森 bug 与观察成本**：竞态窗口窄到任何观察手段（strace、逐块 memcpy 落盘）都会挪动时序躲开它。取证仪器必须零成本、只在失败分支动手（正常路径一条指令不多加），否则永远抓不到现行。

---

## 性能台账（v0.6-graceful-exit-done 基准，回环相对值）

以上所有数字均为回环测量，作**相对值**用于版本对比，不宣称绝对性能。
基准文件：`logs/bench_v0.6-graceful-exit-done.txt`；工具：`src/bench_client.cpp` + `test/bench_run.py`。

| 科目 | 数据 |
|---|---|
| 单连接 ping-pong（1B） | 75K QPS，p50=11µs——纯 RTT/协议开销上限 |
| 并发甜点（10 连接 ping-pong） | ~119K QPS，p99≈0.3ms |
| 流水线吞吐（10 连接 d=10，128B） | 162K QPS，p99≈1.2ms |
| 流水线吞吐峰值（500 连接 d=10，128B） | **199K QPS**，p99 58.5ms（epoll_poll(4) 排队主导） |
| 500 连接 ping-pong（64B） | 129K QPS，p99 8.7ms，P999 16.3ms（backlog 修复前 1078ms） |
| 大包 65535B | ~1.2-2.3K QPS（≈143MB/s 拷贝路径主导），p50 随并发爬升 |
| 正确性长跑 | 10 万条（bench_run 长跑）+ 30 万+ 条（修复后全套 hammer）零丢零重零篡改 |
| 优雅退出 | 空载 2ms / 负载排干 <10ms / deadline 兜底实测 3125ms（修复前 4536ms） |
| EMFILE | fd 打满后 CPU 83% 空转 → 修复后 0.1% |

观察项（转 MQ 期背景观察）：d=10 下 P99 随连接数爬升（epoll_poll(4) 调度）、submit 队列无上限背压、大包 65535 拷贝路径。

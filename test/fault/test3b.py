#!/usr/bin/env python3
"""3B：日志复制。

纯内存模式下（main.cpp 传空路径）只验内存状态；一旦盘上有 raftlog，
所有"日志一致"的断言都改成逐条比文件。两种模式都跑同一批用例。
"""

import time
import unittest

from cluster import Cluster, wait_until
from raftlog import read_log, wait_log


class TestReplication(unittest.TestCase):
    def setUp(self):
        self.c = Cluster(3).start()
        self.addCleanup(self.c.stop)

    # ---------- 工具 ----------

    def alive(self):
        return {i: s for i, s in self.c.sample().items() if s}

    def persistent(self):
        """main.cpp 接了 RAFT_DATA_DIR 才会有这个文件。"""
        return self.c.log_path(1).exists()

    def converge(self, n, timeout=8.0):
        def ok():
            st = self.alive()
            return bool(st) and all(s["last_log_index"] >= n for s in st.values())
        self.assertTrue(wait_until(ok, timeout),
                        "没收敛到 %d: %s" % (n, self.alive()))

    def disk_equal(self, n, timeout=6.0):
        """有落盘就等三份文件逐条一致且至少到 n，返回日志；纯内存返回 None。

        不能只等每个文件都出现第 n 条：旧 leader 重启后盘上本来就有
        index 1..n 的旧尾巴，会立刻满足「到 n」但内容还是旧的。必须等
        三份内容真正一致，截断 / 重写落盘完成才能返回。
        """
        if not self.persistent():
            return None

        last = None

        def ok():
            nonlocal last
            try:
                logs = [read_log(self.c.log_path(i)) for i in (1, 2, 3)]
            except (AssertionError, OSError) as e:
                last = e
                return False
            last = logs
            return (all(len(l) >= n for l in logs)
                    and logs[0] == logs[1] == logs[2])

        self.assertTrue(wait_until(ok, timeout),
                        "三节点 raftlog 没收敛到一致(>=%d): %s" % (n, last))
        return last[0]

    # ---------- 用例 ----------

    def test_basic(self):
        lead = self.c.check_one_leader(3.0)
        base = self.c.status(lead)["last_log_index"]
        for k in range(20):
            self.c.propose(lead, "m%d" % k)
        self.converge(base + 20)

        entries = self.disk_equal(base + 20)
        if entries is not None:
            self.assertGreaterEqual(entries[-1][0], base + 20)

        # follower 的 commit 恒为 0，只能看 leader
        self.assertTrue(wait_until(
            lambda: (self.c.status(lead) or {}).get("commit_index", -1) >= base + 20, 6.0),
            "leader commit 没推到头: %s" % self.c.status(lead))

    def test_lagging_follower_catches_up(self):
        lead = self.c.check_one_leader(3.0)
        victim = next(i for i in (1, 2, 3) if i != lead)
        base = self.c.status(lead)["last_log_index"]

        self.c.kill9(victim)
        for k in range(10):
            # 提案没有回执，Epoll 线程池会让并发连接乱序；这条用例要
            # 逐条比顺序，所以确认上一条进日志后再发下一条。
            self.c.propose_ordered(lead, "x%d" % k)
        self.converge(base + 10)              # 2/3 仍能提交

        self.c.restart(victim)
        self.assertTrue(wait_until(
            lambda: (self.c.status(victim) or {}).get("last_log_index", -1)
                    >= base + 10, 12.0),
            "node %d 没追上: %s" % (victim, self.c.status(victim)))

        entries = self.disk_equal(base + 10)
        if entries is not None:
            self.assertEqual([m for _, _, m in entries][-10:],
                             [b"x%d" % k for k in range(10)])

    def test_old_leader_tail_truncated(self):
        """旧 leader 带着未提交尾巴回来，必须被截掉。

        落盘模式下必须用 kill9 而不是 pause：SIGSTOP 不丢包，攒在
        socket 里的 AppendEntries 会在 SIGCONT 后照样被 follower 处理，
        制造不出「两个 follower 漏掉尾巴」的局面。
        """
        if not self.persistent():
            self.skipTest("需要落盘镜像：SIGSTOP 不丢包，纯内存模式造不出旧尾巴")

        lead = self.c.check_one_leader(3.0)
        others = [i for i in (1, 2, 3) if i != lead]
        base = self.c.status(lead)["last_log_index"]

        for i in others:
            self.c.kill9(i)
        for k in range(3):
            self.c.propose(lead, "old%d" % k)
        self.assertTrue(wait_until(
            lambda: (self.c.status(lead) or {}).get("last_log_index", -1) >= base + 3,
            3.0), "leader 没收下 old*: %s" % self.c.status(lead))

        self.assertEqual(self.c.status(lead)["commit_index"], base,
                         "少数派状态下不该提交")

        self.c.kill9(lead)                    # 带着 3 条未提交条目死掉
        for i in others:
            self.c.restart(i)

        new = self.c.check_one_leader(8.0)
        self.assertNotEqual(new, lead)
        new_base = self.c.status(new)["last_log_index"]
        self.assertEqual(new_base, base, "新 leader 不该看到旧尾巴")

        for k in range(3):
            self.c.propose(new, "new%d" % k)
        self.converge(new_base + 3)

        self.c.restart(lead)
        self.assertTrue(wait_until(
            lambda: (self.c.status(lead) or {}).get("last_log_index", -1)
                    == new_base + 3, 12.0),
            "旧 leader 没被覆盖: %s" % self.c.status(lead))

        entries = self.disk_equal(new_base + 3)
        if entries is not None:
            self.assertNotIn(b"old0", [m for _, _, m in entries])
        else:
            # 没有文件时退一步：三个节点的最后一条 term 必须一致
            terms = {i: (self.c.status(i) or {}).get("last_log_term")
                     for i in (1, 2, 3)}
            self.assertEqual(len(set(terms.values())), 1, "term 不一致: %s" % terms)

    def test_duplicate_propose_appends_twice(self):
        """没有去重：同一份数据提两次就是两条日志。"""
        lead = self.c.check_one_leader(3.0)
        base = self.c.status(lead)["last_log_index"]
        self.c.propose(lead, "dup")
        self.c.propose(lead, "dup")
        self.converge(base + 2)

        if self.persistent():
            entries = wait_log(self.c.log_path(lead), base + 2)
            self.assertEqual([m for _, _, m in entries].count(b"dup"), 2)
        else:
            self.assertEqual(self.c.status(lead)["last_log_index"], base + 2)


if __name__ == "__main__":
    unittest.main()

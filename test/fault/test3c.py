#!/usr/bin/env python3
"""3C：持久化。

这三个用例都要求节点真的把日志写到盘上。main.cpp 目前还传空路径，
所以整类会 skip，并打印一行为什么。接上 RAFT_DATA_DIR 之后自动开始跑。
"""

import unittest

from cluster import Cluster, wait_until
from raftlog import read_log, wait_log


class TestPersistence(unittest.TestCase):
    def setUp(self):
        self.c = Cluster(3).start()
        self.addCleanup(self.c.stop)
        if not self.c.log_path(1).exists():
            self.skipTest("节点没有落盘（main.cpp 传的是空路径），3C 需要 "
                          "raft_node 读 RAFT_DATA_DIR 并把 raftlog 写进去")

    # ---------- 工具 ----------

    def propose_n(self, n, prefix):
        lead = self.c.check_one_leader(3.0)
        base = self.c.status(lead)["last_log_index"]
        for k in range(n):
            self.c.propose(lead, "%s%d" % (prefix, k))
        self.assertTrue(wait_until(
            lambda: all((s or {}).get("last_log_index", -1) >= base + n
                        for s in self.c.sample().values()), 10.0),
            "内存没收敛: %s" % self.c.sample())
        # 落盘比内存慢一个心跳，必须等磁盘
        for i in (1, 2, 3):
            wait_log(self.c.log_path(i), base + n)
        return lead, base + n

    # ---------- 用例 ----------

    def test_restart_preserves_log(self):
        lead, last = self.propose_n(10, "p")
        before = {i: read_log(self.c.log_path(i)) for i in (1, 2, 3)}
        old_term = self.c.status(lead)["term"]

        for i in (1, 2, 3):
            self.c.kill9(i)
        for i in (1, 2, 3):
            self.c.restart(i)

        new = self.c.check_one_leader(10.0)
        after = {i: read_log(self.c.log_path(i)) for i in (1, 2, 3)}
        for i in (1, 2, 3):
            self.assertEqual(before[i], after[i], "node%d 重启后日志变了" % i)
        self.assertGreaterEqual(self.c.status(new)["term"], old_term)
        self.assertEqual(self.c.status(new)["last_log_index"], last)

    def test_torn_tail_is_truncated(self):
        self.propose_n(5, "t")
        for i in (1, 2, 3):
            self.c.kill9(i)

        good = read_log(self.c.log_path(1))
        size = self.c.log_path(1).stat().st_size
        with open(self.c.log_path(1), "ab") as f:      # 半帧 + 垃圾
            f.write(b"\x00\x00\x00\x40" + b"\xaa" * 8)

        for i in (1, 2, 3):
            self.c.restart(i)
        self.c.check_one_leader(10.0)

        self.assertEqual(self.c.log_path(1).stat().st_size, size,
                         "半帧没被截掉")
        self.assertEqual(read_log(self.c.log_path(1)), good)

    def test_leader_kill9_keeps_committed(self):
        lead, last = self.propose_n(10, "c")
        self.assertTrue(wait_until(
            lambda: (self.c.status(lead) or {}).get("commit_index", -1) >= last, 6.0),
            "commit 没推到头: %s" % self.c.status(lead))
        committed = read_log(self.c.log_path(lead))[:last]

        self.c.kill9(lead)
        new = self.c.check_one_leader(10.0)
        self.assertTrue(wait_until(
            lambda: (self.c.status(new) or {}).get("last_log_index", -1) >= last, 8.0))
        self.assertEqual(read_log(self.c.log_path(new))[:last], committed,
                         "已提交条目在换主后丢了")


if __name__ == "__main__":
    unittest.main()

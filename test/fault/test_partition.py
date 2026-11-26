#!/usr/bin/env python3
"""分区故障注入：把节点从网络上摘掉再放回来，看集群还能不能收敛。

和 test_election.py 的区别：这里统一用 5 节点，故障动作偏向
反复隔离 / 恢复，重点在「每次动作之后仍然只有一个 leader」。
"""

import random
import time
import unittest

from cluster import Cluster


class Partition(unittest.TestCase):
    def setUp(self):
        self.cfg = None
        random.seed(20261118)

    def tearDown(self):
        if self.cfg:
            self.cfg.stop()
            self.cfg = None

    def cluster(self, n=5):
        self.cfg = Cluster(n).start()
        return self.cfg

    def track_terms(self, state, seen):
        """同一进程的 term 只许涨，不许跌。"""
        for i, s in state.items():
            if not s:
                continue
            prev = seen.get(i)
            if prev is not None and s["term"] < prev:
                raise AssertionError(
                    "node %d 的 term 从 %d 掉到 %d" % (i, prev, s["term"]))
            seen[i] = s["term"]

    # ---------------------------------------------------------------

    def test_minority_isolation(self):
        """5 节点隔离一个 follower，leader 和 term 都不该动。"""
        cfg = self.cluster()

        leader = cfg.check_one_leader(3.0)
        follower = next(i for i in range(1, 6) if i != leader)

        cfg.pause(follower)
        time.sleep(0.5)
        self.assertEqual(leader, cfg.check_one_leader(3.0),
                         "隔离一个 follower 不该导致重新选举")
        cfg.check_terms_stable(1.0)

        cfg.resume(follower)
        time.sleep(1.0)
        cfg.check_one_leader(3.0)

    def test_leader_isolation_and_rejoin(self):
        """隔离 leader 后选出新的，旧的回来也不能把集群搞乱。"""
        cfg = self.cluster()

        old = cfg.check_one_leader(3.0)
        cfg.pause(old)

        new = cfg.check_one_leader(3.0)
        self.assertNotEqual(new, old)

        cfg.resume(old)
        time.sleep(1.0)
        cfg.check_one_leader(3.0)
        cfg.check_terms_stable(1.0)

    def test_alternating_isolations(self):
        """每轮停一个、放回来，每次动作后都必须唯一 leader。"""
        cfg = self.cluster()
        seen = {}

        for _ in range(5):
            leader = cfg.check_one_leader(3.0)
            others = [i for i in range(1, 6)
                      if i != leader and i not in cfg.paused]
            victim = random.choice(others)

            cfg.pause(victim)
            time.sleep(0.2)
            self.track_terms(cfg.sample(), seen)
            cfg.check_one_leader(3.0)

            cfg.resume(victim)
            time.sleep(0.6)
            self.track_terms(cfg.sample(), seen)
            cfg.check_one_leader(3.0)

        cfg.check_terms_stable(0.5)

    def test_term_never_decreases(self):
        """连续抖动 10 次，任何节点的 term 都不许回退。"""
        cfg = self.cluster()
        seen = {}

        for _ in range(10):
            leader = cfg.check_one_leader(3.0)
            victim = random.choice([i for i in range(1, 6) if i != leader])
            if victim in cfg.paused:
                cfg.resume(victim)
            else:
                cfg.pause(victim)

            time.sleep(0.2)
            self.track_terms(cfg.sample(), seen)

        for i in range(1, 6):
            cfg.resume(i)
        time.sleep(1.0)
        self.track_terms(cfg.sample(), seen)
        cfg.check_one_leader(3.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)

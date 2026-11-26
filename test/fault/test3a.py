#!/usr/bin/env python3
"""5 节点脑裂：旧 leader 落在少数派，合并后必须退位。"""

import time
import unittest

from cluster import Cluster


class TestPartition(unittest.TestCase):

    def test_HEAD_follower(self):
        # 5节点脑裂
        # 原先选出的leader被分到少数区
        # 多数区选出leader稳定后两个分区合并
        # 少数区leader退位算ok
        #
        # 说明：少数派用 SIGSTOP 冻结，表现为「收不到也发不出」。
        # 这不是严格意义的双向分区（少数区不会自己继续选举），
        # 真正的两组同时跑需要 router 按 from/to 丢帧，需要时再升级。
        cfg = Cluster(5).start()
        self.addCleanup(cfg.stop)

        old = cfg.check_one_leader(3.0)
        mate = next(i for i in range(1, 6) if i != old)
        minority = [old, mate]
        for i in minority:
            cfg.pause(i)

        # 多数区（3/5）必须自己选出新 leader，且不能是少数区的人
        new = cfg.check_one_leader(3.0)
        self.assertNotIn(new, minority, "新 leader 不该来自少数区")
        cfg.check_terms_stable(1.0)

        # 合并：少数区两个节点回到网络
        for i in minority:
            cfg.resume(i)

        # 合并后仍然只有一个 leader（这一步同时会等到旧 leader 让位）
        cfg.check_one_leader(3.0)

        # 旧 leader 必须在 2s 内退位，不再自称 leader
        deadline = time.time() + 2.0
        role = None
        while time.time() < deadline:
            status = cfg.status(old)
            if status:
                role = status["role"]
                if role != "leader":
                    break
            time.sleep(0.05)
        self.assertNotEqual(
            role, "leader",
            "旧 leader 合并后仍然自称 leader, logs=%s" % cfg.logs())

        # 收敛之后不许再抖
        cfg.check_terms_stable(1.0)
        cfg.check_one_leader(2.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)

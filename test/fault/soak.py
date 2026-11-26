#!/usr/bin/env python3
"""并行 soak：每个 worker 一套独立端口 + 独立临时数据目录。

用法：
    python3 soak.py test3b test3c --count 10 --workers 4

每个任务都是一个独立的 `python3 -m unittest <test>` 子进程，
RAFT_PORT_BASE 按 worker 槽位分配（默认 7100 起，间隔 20），
所以多套 3 节点集群可以同时跑而不抢端口。日志落在临时目录里。
"""

import argparse
import os
import pathlib
import queue
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

HERE = pathlib.Path(__file__).resolve().parent


def run_one(test, seq, base, outdir):
    env = dict(os.environ, RAFT_PORT_BASE=str(base))
    proc = subprocess.run(
        [sys.executable, "-m", "unittest", test],
        cwd=str(HERE), env=env, capture_output=True, text=True)
    text = proc.stdout + proc.stderr
    log = outdir / ("%s_%03d_base%d.log" % (test, seq, base))
    log.write_text(text, encoding="utf-8")
    return {"test": test, "seq": seq, "base": base,
            "rc": proc.returncode, "log": log, "text": text}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tests", nargs="+")
    ap.add_argument("--count", type=int, default=10,
                    help="每个测试模块跑多少次")
    ap.add_argument("--workers", type=int, default=4,
                    help="同时在跑的集群数（建议不超过核数/4）")
    ap.add_argument("--base", type=int, default=7100)
    ap.add_argument("--spacing", type=int, default=20)
    args = ap.parse_args()

    outdir = pathlib.Path(tempfile.mkdtemp(prefix="raft-soak-"))
    print("logs: %s" % outdir, flush=True)

    free = queue.Queue()
    for k in range(args.workers):
        free.put(args.base + k * args.spacing)

    def task(item):
        test, seq = item
        base = free.get()
        try:
            return run_one(test, seq, base, outdir)
        finally:
            free.put(base)

    tasks = [(t, i) for i in range(1, args.count + 1) for t in args.tests]
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for res in pool.map(task, tasks):
            results.append(res)
            print("%-8s #%-3d base=%-5d rc=%d"
                  % (res["test"], res["seq"], res["base"], res["rc"]),
                  flush=True)

    bad = [r for r in results if r["rc"] != 0]
    print("\n%d/%d passed, %d failed"
          % (len(results) - len(bad), len(results), len(bad)))
    for r in bad:
        print("\n===== %s #%d failed (%s) ====="
              % (r["test"], r["seq"], r["log"]))
        print("\n".join(r["text"].splitlines()[-25:]))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
import subprocess
import sys
import time
import os
import re
import signal

BIN = "./bin/bench_client"
SERVER = "./bin/server"
LOG = "logs/app.log"

LONG_RUNS = [
    (1, 50000, 1024, 1),
    (10, 5000, 4096, 1),
]
MATRIX_CONNS = [1, 10, 100]
MATRIX_SIZES = [1, 64, 1024, 65535]
MATRIX_N = 500
DEPTH_SWEEP_CONNS = [1, 10, 100]
DEPTH_SWEEP_SIZE = 128
DEPTH_SWEEP_N = 200
DEPTH = 10


def line_count(path):
    if not os.path.exists(path):
        return 0
    with open(path, errors="ignore") as f:
        return sum(1 for _ in f)


def tail_log(path, n0):
    with open(path, errors="ignore") as f:
        return "".join(f.readlines()[n0:])


def start_server():
    n0 = line_count(LOG)
    p = subprocess.Popen([SERVER], stdout=open("/tmp/server_out.txt", "w"),
                         stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    time.sleep(0.5)
    if p.poll() is not None:
        raise RuntimeError("server died on start, rc=%s" % p.returncode)
    if "成功启动" not in tail_log(LOG, n0):
        p.kill()
        raise RuntimeError("no startup log")
    return p


def stop_server(p, sig=signal.SIGINT):
    t0 = time.time()
    p.send_signal(sig)
    rc = p.wait(timeout=15)
    return rc, time.time() - t0


def bench(conns, per_conn, size, depth, check=True):
    args = [BIN, "-c", str(conns), "-n", str(per_conn),
            "-s", str(size), "-d", str(depth), "-t", "300"]
    if check:
        args.append("--check")
    r = subprocess.run(args, capture_output=True, text=True, timeout=330)
    kv = {}
    for line in r.stdout.splitlines():
        for k, v in re.findall(r"(\w+)=([^\s]+)", line):
            kv[k] = v
    ok = "PASS" in r.stdout
    return kv, ok, r.stdout


def row(conns, size, depth, kv, ok):
    return "%5d %8d %5d %10s %8s %8s %8s %8s %8s %6s" % (
        conns, size, depth,
        kv.get("qps", "?"), kv.get("p50", "?"), kv.get("p90", "?"),
        kv.get("p99", "?"), kv.get("p999", "?"), kv.get("max", "?"),
        "PASS" if ok else "FAIL")


def main():
    results = []
    failures = 0

    print("== starting server ==")
    srv = start_server()
    try:
        print("== correctness long runs ==")
        for conns, per_conn, size, depth in LONG_RUNS:
            kv, ok, out = bench(conns, per_conn, size, depth)
            print(out.rstrip())
            if not ok:
                failures += 1
            results.append((conns, size, depth, kv, ok))

        print()
        print("== matrix: ping-pong depth=1 ==")
        print("%5s %8s %5s %10s %8s %8s %8s %8s %8s %6s" % (
            "conns", "size", "depth", "qps", "p50", "p90", "p99", "p999", "max", "ok"))
        for conns in MATRIX_CONNS:
            for size in MATRIX_SIZES:
                kv, ok, _ = bench(conns, MATRIX_N, size, 1)
                print(row(conns, size, 1, kv, ok), flush=True)
                if not ok:
                    failures += 1
                results.append((conns, size, 1, kv, ok))

        print()
        print("== pipeline sweep: depth=%d (watch p99 vs conns) ==" % DEPTH)
        for conns in DEPTH_SWEEP_CONNS:
            kv, ok, _ = bench(conns, DEPTH_SWEEP_N, DEPTH_SWEEP_SIZE, DEPTH)
            print(row(conns, DEPTH_SWEEP_SIZE, DEPTH, kv, ok), flush=True)
            if not ok:
                failures += 1
            results.append((conns, DEPTH_SWEEP_SIZE, DEPTH, kv, ok))
    finally:
        rc, dt = stop_server(srv)
        print()
        print("server exit=%d stop=%.3fs" % (rc, dt))
        if rc != 0:
            failures += 1

    with open("/tmp/bench_result.txt", "w") as f:
        for conns, size, depth, kv, ok in results:
            f.write(row(conns, size, depth, kv, ok) + "\n")
    print("results saved to /tmp/bench_result.txt")
    print("TOTAL failures=%d" % failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

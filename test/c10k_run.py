#!/usr/bin/env python3
import subprocess
import sys
import os
import re
import time
import signal

BIN = "./bin/bench_client"
SERVER = "./bin/server"
LOG = "logs/app.log"
GRADIENT = [500, 1000, 5000, 10000, 50000]
TOTAL_MSGS = 200000
FDLIMIT = 64000
OUTFILE = "logs/bench_c10k.txt"


def line_count(path):
    if not os.path.exists(path):
        return 0
    with open(path, errors="ignore") as f:
        return sum(1 for _ in f)


def tail_log(path, n0):
    with open(path, errors="ignore") as f:
        return "".join(f.readlines()[n0:])


def srv_rss_mb(pid):
    try:
        with open("/proc/%d/status" % pid) as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        pass
    return -1


def cli_rss_mb():
    return srv_rss_mb(os.getpid())


def start_server():
    n0 = line_count(LOG)
    p = subprocess.Popen(
        ["bash", "-c", "ulimit -n %d; exec %s" % (FDLIMIT, SERVER)],
        stdout=open("/tmp/server_out.txt", "w"),
        stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    time.sleep(0.8)
    if p.poll() is not None:
        raise RuntimeError("server died rc=%s" % p.returncode)
    if "成功启动" not in tail_log(LOG, n0):
        p.kill()
        raise RuntimeError("no startup log")
    return p


def stop_server(p):
    t0 = time.time()
    p.send_signal(signal.SIGINT)
    rc = p.wait(timeout=20)
    return rc, time.time() - t0


def bench(conns, per_conn, size, depth):
    args = [BIN, "-c", str(conns), "-n", str(per_conn),
            "-s", str(size), "-d", str(depth), "-t", "300"]
    r = subprocess.run(
        ["bash", "-c", "ulimit -n %d; exec %s" % (FDLIMIT, " ".join(args))],
        capture_output=True, text=True, timeout=400)
    kv = {}
    for line in r.stdout.splitlines():
        for k, v in re.findall(r"(\w+)=([^\s]+)", line):
            kv[k] = v
    ok = "PASS" in r.stdout
    return kv, ok, r.stdout


def main():
    n_reactors = 2
    results = []
    failures = 0
    n0 = line_count(LOG)
    p = subprocess.Popen(
        ["bash", "-c", "ulimit -n %d; exec %s" % (FDLIMIT, SERVER)],
        stdout=open("/tmp/server_out.txt", "w"),
        stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    time.sleep(1.0)
    if p.poll() is not None:
        print("server died rc=%s" % p.returncode)
        return 1
    if "成功启动" not in tail_log(LOG, n0):
        p.kill()
        print("no startup log")
        return 1
    print("server up (pid=%d, ulimit=%d)" % (p.pid, FDLIMIT), flush=True)
    try:
        print("%7s %6s %9s %9s %7s %8s %8s %9s %6s" % (
            "conns", "n/conn", "connect", "qps", "p99", "rss_srv",
            "rss_cli", "recv", "ok"))
        for conns in GRADIENT:
            per_conn = max(1, TOTAL_MSGS // conns)
            kv, ok, out = bench(conns, per_conn, 64, 1)
            print(out.rstrip(), flush=True)
            rss_srv = srv_rss_mb(p.pid)
            rss_cli = cli_rss_mb()
            if not ok:
                failures += 1
            results.append((conns, per_conn, kv, ok, rss_srv, rss_cli))
            print("%7d %6d %9s %9s %8s %6sMB %7sMB %9s %6s" % (
                conns, per_conn, kv.get("connect", "?"), kv.get("qps", "?"),
                kv.get("p99", "?"), rss_srv, rss_cli,
                kv.get("recv", "?"), "PASS" if ok else "FAIL"), flush=True)
    finally:
        rc, dt = stop_server(p)
        print("server exit=%d stop=%.2fs" % (rc, dt), flush=True)
        if rc != 0:
            failures += 1
    with open(OUTFILE, "w") as f:
        f.write("C10K gradient (n_reactors=%d, total=%d msgs/cell, s=64B, d=1)\n" % (
            n_reactors, TOTAL_MSGS))
        f.write("%7s %6s %9s %9s %8s %9s %9s\n" % (
            "conns", "n/conn", "connect", "qps", "p99", "rss_srv", "rss_cli"))
        for conns, per_conn, kv, ok, rss_srv, rss_cli in results:
            f.write("%7d %6d %9s %9s %8s %6dMB %7dMB  %s\n" % (
                conns, per_conn, kv.get("connect", "?"), kv.get("qps", "?"),
                kv.get("p99", "?"), rss_srv, rss_cli,
                "PASS" if ok else "FAIL"))
    print("saved to", OUTFILE)
    print("failures=%d" % failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

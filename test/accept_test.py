#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# epoll echo server 验收测试
# 用例1: 完整性+无重复 — SO_RCVBUF=4KB, 发送1MB已知随机数据, 慢读收回, 逐字节比对, 监测server CPU
# 用例2: 中途断连 — 发数据收到一半直接close(RST), 验证server存活且不空转
# 用例3: 空闲10s — 连上不发数据, 验证server CPU≈0 且连接仍可用
import socket, sys, time, os, subprocess

HOST = "127.0.0.1"
PORT = 8888
CLK_TCK = os.sysconf("SC_CLK_TCK")
TIMEOUT = 120


def find_server_pid():
    out = subprocess.run(["pgrep", "-x", "server"], capture_output=True, text=True)
    pids = [int(p) for p in out.stdout.split()]
    if not pids:
        sys.exit("server 未运行。先启动: cd ~/cpp-service-infra && ./bin/server &")
    return pids[0]


def cpu_ticks(pid):
    with open("/proc/%d/stat" % pid) as f:
        text = f.read()
    fields = text[text.rfind(")") + 2:].split()
    return int(fields[11]) + int(fields[12])  # utime + stime


def connect(rcvbuf=None):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(TIMEOUT)
    if rcvbuf:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    s.connect((HOST, PORT))
    return s


def ping(tag="ping"):
    s = connect()
    s.sendall(tag.encode())
    data = b""
    while len(data) < len(tag):
        chunk = s.recv(len(tag) - len(data))
        if not chunk:
            raise RuntimeError("ping: 连接被关闭, server 可能已死")
        data += chunk
    s.close()
    if data != tag.encode():
        raise RuntimeError("ping: 回声内容错误: %r" % data)


def cpu_pct_between(pid, t0, c0):
    return (cpu_ticks(pid) - c0) / CLK_TCK / max(time.time() - t0, 1e-6) * 100


def test1_integrity(pid):
    print("=== 用例1: 完整性+无重复 (1MB, SO_RCVBUF=4KB, 慢读) ===")
    payload = os.urandom(1024 * 1024)
    s = connect(rcvbuf=4096)
    c0, t0 = cpu_ticks(pid), time.time()
    s.sendall(payload)
    got = bytearray()
    while len(got) < len(payload):
        chunk = s.recv(2048)
        if not chunk:
            break
        got += chunk
        time.sleep(0.02)
    elapsed = time.time() - t0
    cpu = cpu_pct_between(pid, t0, c0)
    s.close()
    ok_len = len(got) == len(payload)
    ok_data = bytes(got) == payload
    ok_cpu = cpu < 50.0
    print("  收到/预期: %d/%d 字节  %s" % (len(got), len(payload), "OK" if ok_len else "FAIL"))
    print("  内容比对:   %s" % ("OK (无丢失/无重复/无错位)" if ok_data else "FAIL"))
    print("  server CPU: %.1f%% (耗时 %.1fs)  %s" % (cpu, elapsed, "OK" if ok_cpu else "FAIL(疑似忙转)"))
    return ok_len and ok_data and ok_cpu


def test2_disconnect(pid):
    print("=== 用例2: 中途直接close断连 (RST) ===")
    s = connect()
    s.sendall(os.urandom(256 * 1024))
    got = 0
    while got < 32 * 1024:
        chunk = s.recv(4096)
        if not chunk:
            break
        got += len(chunk)
        time.sleep(0.01)
    s.close()  # 还有未读回声数据 -> 内核发RST
    time.sleep(1.5)
    c0, t0 = cpu_ticks(pid), time.time()
    time.sleep(2.0)
    cpu = cpu_pct_between(pid, t0, c0)
    ok_cpu = cpu < 20.0
    print("  断连后2s server CPU: %.1f%%  %s" % (cpu, "OK" if ok_cpu else "FAIL(疑似空转)"))
    try:
        ping()
        ok_alive = True
        print("  断连后新连接回声: OK (server 存活, 清理路径正常)")
    except Exception as e:
        ok_alive = False
        print("  断连后新连接回声: FAIL (%s)" % e)
    return ok_cpu and ok_alive


def test3_idle(pid):
    print("=== 用例3: 空闲连接 10s ===")
    s = connect()
    c0, t0 = cpu_ticks(pid), time.time()
    time.sleep(10)
    cpu = cpu_pct_between(pid, t0, c0)
    ok_cpu = cpu < 5.0
    print("  空闲期间 server CPU: %.1f%%  %s" % (cpu, "OK" if ok_cpu else "FAIL(空转)"))
    ok_echo = True
    try:
        s.sendall(b"idle-alive")
        data = b""
        while len(data) < 10:
            chunk = s.recv(10 - len(data))
            if not chunk:
                raise RuntimeError("空闲连接被关闭")
            data += chunk
        ok_echo = data == b"idle-alive"
    except Exception as e:
        ok_echo = False
        print("  空闲连接回声: FAIL (%s)" % e)
    if ok_echo:
        print("  空闲连接回声: OK")
    s.close()
    return ok_cpu and ok_echo


def main():
    pid = find_server_pid()
    print("server pid=%d  端口=%d\n" % (pid, PORT))
    results = []
    results.append(("完整性+无重复", test1_integrity(pid)))
    print()
    results.append(("中途断连", test2_disconnect(pid)))
    print()
    results.append(("空闲10s", test3_idle(pid)))
    print("\n========== 汇总 ==========")
    all_ok = True
    for name, ok in results:
        print("  %-12s %s" % (name, "PASS" if ok else "FAIL"))
        all_ok = all_ok and ok
    print("==========================")
    print("验收结果: %s" % ("全部通过" if all_ok else "存在失败项"))
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import os
import socket
import struct
import sys
import time

HOST, PORT = "127.0.0.1", 8888
TIMEOUT = 10
TOTAL = 7
failures = []


def frame(msg):
    return struct.pack("!I", len(msg)) + msg


def connect():
    return socket.create_connection((HOST, PORT), timeout=TIMEOUT)


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("EOF while expecting %d more bytes" % (n - len(buf)))
        buf += chunk
    return buf


def recv_frame(sock):
    ln = struct.unpack("!I", recv_exact(sock, 4))[0]
    return recv_exact(sock, ln) if ln else b""


def run(name, fn):
    try:
        fn()
        print("PASS  " + name)
    except Exception as e:
        print("FAIL  %s: %s" % (name, e))
        failures.append(name)


def t_echo():
    s = connect()
    s.sendall(frame(b"hello"))
    assert recv_frame(s) == b"hello"
    s.close()


def t_fragment():
    s = connect()
    data = frame(b"fragment me please")
    for i in range(0, len(data), 3):
        s.sendall(data[i:i + 3])
        time.sleep(0.015)
    assert recv_frame(s) == b"fragment me please"
    s.close()


def t_sticky():
    s = connect()
    s.sendall(frame(b"one") + frame(b"two") + frame(b"three"))
    got = [recv_frame(s) for _ in range(3)]
    assert sorted(got) == [b"one", b"three", b"two"], "unexpected set %r" % got
    s.close()


def t_empty():
    s = connect()
    s.sendall(frame(b""))
    assert recv_frame(s) == b""
    s.close()


def t_badframe():
    s = connect()
    s.sendall(struct.pack("!I", 2 * 1024 * 1024))
    try:
        got = s.recv(1)
    except ConnectionResetError:
        return
    assert got == b"", "expected close, got %r" % got


def t_good_then_bad():
    s = connect()
    s.sendall(frame(b"ping") + struct.pack("!I", 0xFFFFFFFF))
    seen = []
    while True:
        try:
            seen.append(recv_frame(s))
        except (ConnectionError, ConnectionResetError):
            break
    assert all(m == b"ping" for m in seen), "unexpected echo %r" % seen


def t_halfclose():
    s = connect()
    msgs = [os.urandom(256 * 1024) for _ in range(16)]
    for m in msgs:
        s.sendall(frame(m))
    s.shutdown(socket.SHUT_WR)
    got = [recv_frame(s) for _ in msgs]
    assert sorted(got) == sorted(msgs), "drain set mismatch"
    assert s.recv(1) == b"", "expected EOF after drain"


def cpu_seconds(pid):
    with open("/proc/%d/stat" % pid) as f:
        parts = f.read().split()
    return (int(parts[13]) + int(parts[14])) / os.sysconf("SC_CLK_TCK")


def main():
    pid = int(sys.argv[1]) if len(sys.argv) > 1 else None
    cpu0 = cpu_seconds(pid) if pid else None
    run("echo", t_echo)
    run("fragment", t_fragment)
    run("sticky", t_sticky)
    run("empty", t_empty)
    run("bad-frame close", t_badframe)
    run("good-then-bad", t_good_then_bad)
    run("half-close drain", t_halfclose)
    if pid:
        used = cpu_seconds(pid) - cpu0
        print("server cpu: %.2fs" % used)
        if used > 5.0:
            print("FAIL  server cpu too high (busy loop?)")
            failures.append("cpu")
    print("RESULT %d/%d PASS" % (TOTAL - len(failures), TOTAL))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()

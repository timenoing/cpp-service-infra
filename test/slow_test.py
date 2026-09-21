#!/usr/bin/env python3
import socket
import struct
import sys
import time
import threading

HOST, PORT = "127.0.0.1", 8888
TIMEOUT = 10

SLOW_N = 3
SLEEP_MS = 100
FAST_CONNS = 5
FAST_N = 200


def frame(msg):
    return struct.pack("!I", len(msg)) + msg


def connect():
    return socket.create_connection((HOST, PORT), timeout=TIMEOUT)


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("EOF while expecting %d bytes" % (n - len(buf)))
        buf += chunk
    return buf


def recv_frame(sock):
    ln = struct.unpack("!I", recv_exact(sock, 4))[0]
    return recv_exact(sock, ln) if ln else b""


def slow_conn(result):
    s = connect()
    t0 = time.perf_counter()
    for _ in range(SLOW_N):
        s.sendall(frame(b"SLOW-" + b"p" * 32))
    got = [recv_frame(s) for _ in range(SLOW_N)]
    dt = time.perf_counter() - t0
    result["slow"] = (len(got) == SLOW_N, dt)
    s.close()


def fast_conn(idx, result):
    s = connect()
    lat = []
    payload = b"fast-%d" % idx
    for _ in range(FAST_N):
        t0 = time.perf_counter()
        s.sendall(frame(payload))
        got = recv_frame(s)
        lat.append(time.perf_counter() - t0)
        if got != payload:
            lat[-1] = -1.0
            break
    result[idx] = lat
    s.close()


def main():
    result = {}
    ts = threading.Thread(target=slow_conn, args=(result,))
    fasts = [threading.Thread(target=fast_conn, args=(i, result)) for i in range(FAST_CONNS)]
    ts.start()
    for t in fasts:
        t.start()
    ts.join()
    for t in fasts:
        t.join()

    ok, slow_dt = result["slow"]
    if ok and slow_dt * 1000 < 90:
        print("FAIL slow business not sleeping (total=%.0fms) - server is plain echo?" % (slow_dt * 1000))
        print("RESULT FAIL")
        sys.exit(1)
    all_lat = []
    corrupted = False
    for i in range(FAST_CONNS):
        for v in result[i]:
            if v < 0:
                corrupted = True
                break
            all_lat.append(v)
    all_lat.sort()
    bad = 0
    if not ok:
        print("FAIL slow conn did not drain")
        bad += 1
    if corrupted:
        print("FAIL fast conn echo corrupted")
        bad += 1
    if not all_lat:
        print("FAIL no fast samples")
        bad += 1
    else:
        avg = sum(all_lat) / len(all_lat)
        p99 = all_lat[min(int(len(all_lat) * 0.99), len(all_lat) - 1)]
        mx = all_lat[-1]
        print("slow conn: %d msg x %dms, total=%.0fms" % (SLOW_N, SLEEP_MS, slow_dt * 1000))
        print("fast conns: n=%d avg=%.1fms p99=%.1fms max=%.1fms"
              % (len(all_lat), avg * 1000, p99 * 1000, mx * 1000))
        if avg * 1000 > 50:
            print("FAIL avg too high (>50ms)")
            bad += 1
        if mx * 1000 > 200:
            print("FAIL max too high (>200ms)")
            bad += 1
    print("RESULT " + ("PASS" if bad == 0 else "FAIL"))
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()

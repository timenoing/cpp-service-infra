#!/usr/bin/env python3
"""Raft 集群测试支架，接口对齐 6.824 的 config.go。

一个 Cluster 就是一个 router 加 n 个 raft_node 进程。断开用 SIGSTOP，
恢复用 SIGCONT，等价于「消息全丢但进程还在」。
"""

import atexit
import json
import os
import pathlib
import signal
import socket
import struct
import subprocess
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]  # test/fault/cluster.py -> 仓库根
BIN = ROOT / "bin"
HOST = "127.0.0.1"
# 并行 soak 时每个 worker 用不同的基址；单跑默认仍是 7000。
ROUTER_PORT = int(os.environ.get("RAFT_PORT_BASE", "7000"))
OPCODE_STATUS = 5
OPCODE_PROPOSE = 6


class ClusterError(RuntimeError):
    pass


def wait_until(pred, timeout, step=0.05):
    """轮询 pred，超时返回 False。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(step)
    return False


def propose(port, data, timeout=1.0):
    """发一条 Propose。leader 不回包，发完就关。"""
    body = struct.pack(">II", 0, 0) + bytes([OPCODE_PROPOSE]) + data.encode()
    with socket.create_connection((HOST, port), timeout=timeout) as s:
        s.sendall(struct.pack(">I", len(body)) + body)


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed")
        buf += chunk
    return buf


def query(port, timeout=0.15):
    """查一个节点的 status_json，查不到返回 None。"""
    try:
        with socket.create_connection((HOST, port), timeout=timeout) as s:
            s.settimeout(timeout)
            body = struct.pack(">II", 0, 0) + bytes([OPCODE_STATUS])
            s.sendall(struct.pack(">I", len(body)) + body)
            size = struct.unpack(">I", _recv_exact(s, 4))[0]
            if size == 0 or size > (1 << 20):
                return None
            return json.loads(_recv_exact(s, size).decode())
    except (OSError, EOFError, ValueError):
        return None


class Cluster:
    def __init__(self, n=3):
        self.n = n
        self.procs = {}
        self.files = {}
        self.paused = set()
        self.router = None
        self.router_file = None
        self.logdir = pathlib.Path(tempfile.mkdtemp(prefix="raftlab-"))
        self.env = dict(os.environ, RAFT_DATA_DIR=str(self.logdir / "data"))
        self._closed = False
        atexit.register(self.stop)

    # ---------- 生命周期 ----------

    def _port_busy(self, port):
        with socket.socket() as s:
            s.settimeout(0.1)
            return s.connect_ex((HOST, port)) == 0

    def start(self, timeout=10.0):
        busy = [p for p in range(ROUTER_PORT, ROUTER_PORT + self.n + 1)
                if self._port_busy(p)]
        if busy:
            raise ClusterError(
                "ports already in use: %s，先清掉旧进程" % busy)

        self.router_file = open(self.logdir / "router.log", "ab", buffering=0)
        self.router = subprocess.Popen(
            [str(BIN / "raft_router"), str(self.n)],
            stdout=self.router_file, stderr=subprocess.STDOUT)
        for i in range(1, self.n + 1):
            self._spawn(i)

        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.router.poll() is not None:
                raise ClusterError(
                    "router exited early, logs in %s" % self.logdir)
            if all(self.status(i) for i in range(1, self.n + 1)):
                return self
            time.sleep(0.05)
        raise ClusterError("cluster not ready in %.1fs, logs in %s"
                           % (timeout, self.logdir))

    def _spawn(self, i):
        f = open(self.logdir / ("node%d.log" % i), "ab", buffering=0)
        self.files[i] = f
        self.procs[i] = subprocess.Popen(
            [str(BIN / "raft_node"), str(i), str(self.n)],
            stdout=f, stderr=subprocess.STDOUT, env=self.env)

    def stop(self):
        if self._closed:
            return
        self._closed = True

        # 先 CONT：被 STOP 的进程收到 TERM 只会挂起，永远不退
        for i in list(self.procs):
            self._send(i, signal.SIGCONT)
        for p in self.procs.values():
            try:
                p.terminate()
            except OSError:
                pass
        if self.router:
            try:
                self.router.terminate()
            except OSError:
                pass

        deadline = time.time() + 5.0
        waitable = list(self.procs.values())
        if self.router:
            waitable.append(self.router)
        for p in waitable:
            try:
                p.wait(timeout=max(0.0, deadline - time.time()))
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

        for f in list(self.files.values()) + ([self.router_file] if self.router_file else []):
            f.close()
        self.procs.clear()
        self.files.clear()
        self.paused.clear()
        self.router = None
        self.router_file = None

    def logs(self):
        return str(self.logdir)

    # ---------- 断开 / 恢复 ----------

    def _send(self, i, sig):
        p = self.procs.get(i)
        if not p:
            return
        try:
            os.kill(p.pid, sig)
        except ProcessLookupError:
            pass

    def pause(self, i):
        if i in self.paused:
            return
        self._send(i, signal.SIGSTOP)
        self.paused.add(i)

    def resume(self, i):
        if i not in self.paused:
            return
        self._send(i, signal.SIGCONT)
        self.paused.discard(i)

    # ---------- 提案 / 进程级故障 ----------

    def propose(self, i, data):
        propose(ROUTER_PORT + i, data)

    def propose_ordered(self, i, data, timeout=2.0):
        """发一条并等它真的进了日志，再返回。

        propose 是发完就关、没有回执的；Epoll 用线程池处理连接，
        多条提案的追加顺序本身就不保证。需要在日志里断言顺序的用例
        用这个：上一条确认落进 Logs 之后再发下一条。
        """
        before = (self.status(i) or {}).get("last_log_index")
        if before is None:
            raise ClusterError("node %d 不可达，无法提案" % i)
        self.propose(i, data)
        if not wait_until(
                lambda: (self.status(i) or {}).get("last_log_index", -1) > before,
                timeout):
            raise ClusterError("node %d 没接收提案 %r" % (i, data))

    def log_path(self, i):
        return self.logdir / "data" / ("node%d" % i) / "raftlog"

    def kill9(self, i):
        """SIGKILL，不给收尾机会。"""
        p = self.procs.pop(i)
        f = self.files.pop(i, None)
        p.kill()
        p.wait(timeout=5)
        if f:
            f.close()
        self.paused.discard(i)

    def restart(self, i, timeout=10.0):
        self._spawn(i)
        if not wait_until(lambda: self.status(i) is not None, timeout):
            raise ClusterError("node %d 重启后不可用, logs=%s"
                               % (i, self.logdir))

    # ---------- 观察 ----------

    def status(self, i, timeout=0.15):
        return query(ROUTER_PORT + i, timeout)

    def sample(self):
        out = {}
        for i in range(1, self.n + 1):
            out[i] = query(ROUTER_PORT + i,
                           0.05 if i in self.paused else 0.15)
        return out

    def leader(self, timeout=2.0):
        """等到恰好一个 leader，返回 (id, term)；等不到返回 None。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            state = self.sample()
            leaders = {i: s for i, s in state.items()
                       if s and s.get("role") == "leader"}
            by_term = {}
            for i, s in leaders.items():
                by_term.setdefault(s["term"], []).append(i)
            for term, ids in by_term.items():
                if len(ids) > 1:
                    raise AssertionError(
                        "term %d 出现多个 leader %s, state=%s, logs=%s"
                        % (term, ids, state, self.logdir))
            if len(leaders) == 1:
                i, s = next(iter(leaders.items()))
                return i, s["term"]
            time.sleep(0.05)
        return None

    def check_one_leader(self, timeout=2.0):
        res = self.leader(timeout)
        if res is None:
            raise AssertionError(
                "%.1fs 内没有唯一的 leader, state=%s, logs=%s"
                % (timeout, self.sample(), self.logdir))
        return res[0]

    def check_no_leader(self, duration=1.2):
        deadline = time.time() + duration
        while time.time() < deadline:
            state = self.sample()
            leaders = [i for i, s in state.items()
                       if s and s.get("role") == "leader"]
            if leaders:
                raise AssertionError(
                    "可达节点里不该有 leader，却有 %s, state=%s, logs=%s"
                    % (leaders, state, self.logdir))
            time.sleep(0.05)

    def check_terms_stable(self, seconds=1.0):
        """所有可达节点的 term 在 seconds 内不许变，返回起始快照。"""
        start = self.sample()
        reference = {i: s["term"] for i, s in start.items() if s}
        deadline = time.time() + seconds
        while time.time() < deadline:
            time.sleep(0.05)
            state = self.sample()
            for i, s in state.items():
                if not s:
                    continue
                if i in reference and s["term"] != reference[i]:
                    raise AssertionError(
                        "node %d 的 term 无故从 %d 变到 %d, state=%s, logs=%s"
                        % (i, reference[i], s["term"], state, self.logdir))
                reference.setdefault(i, s["term"])
        return reference

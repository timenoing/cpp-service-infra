#!/usr/bin/env python3
"""Query every raft node at once.

Speaks the same 4-byte length framed envelope as net::encode():

    request   [u32 len][u32 from][u8 opcode]
    response  [u32 len][json]

Usage:
    python3 test/status_all.py
    python3 test/status_all.py --watch 1
    python3 test/status_all.py --json
"""

import argparse
import json
import socket
import struct
import sys

OPCODE_STATUS_QUERY = 5


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed after %d/%d bytes" % (len(buf), n))
        buf += chunk
    return buf


def query(host, port, timeout):
    sock = socket.create_connection((host, port), timeout=timeout)
    try:
        payload = struct.pack(">I", 0) + bytes([OPCODE_STATUS_QUERY])
        sock.sendall(struct.pack(">I", len(payload)) + payload)

        body_len = struct.unpack(">I", recv_exact(sock, 4))[0]
        if body_len == 0:
            raise ValueError("empty response, node probably predates opcode 5")
        return json.loads(recv_exact(sock, body_len).decode("utf-8", "replace"))
    finally:
        sock.close()


def collect(args):
    rows = []
    for index, port in enumerate(args.ports):
        try:
            status = query(args.host, port, args.timeout)
        except (OSError, EOFError, ValueError) as exc:
            rows.append({"port": port, "error": str(exc)})
        else:
            status["port"] = port
            rows.append(status)
    return rows


def render(rows):
    head = "%-4s %-6s %-9s %6s %9s %6s %8s %6s %5s" % (
        "id", "port", "role", "term", "voted_for", "votes",
        "last_log", "commit", "apply")
    lines = [head, "-" * len(head)]
    for row in rows:
        if "error" in row:
            lines.append("%-4s %-6d %-9s %6s %9s %6s %8s %6s %5s" % (
                "-", row["port"], "down", "-", "-", "-", "-", "-", "-"))
            continue
        voted_for = row.get("voted_for")
        lines.append("%-4s %-6d %-9s %6d %9s %6d %8d %6d %5d" % (
            row.get("id", "?"),
            row["port"],
            row.get("role", "?"),
            row.get("term", 0),
            "null" if voted_for is None else str(voted_for),
            row.get("votecount", 0),
            row.get("last_log_index", 0),
            row.get("commit_index", 0),
            row.get("apply_index", 0),
        ))
    return "\n".join(lines)


def verdict(rows):
    alive = [r for r in rows if "error" not in r]
    down = [r for r in rows if "error" in r]
    parts = []

    leaders = [r for r in alive if r.get("role") == "leader"]
    if len(leaders) == 1:
        parts.append("leader %s" % leaders[0]["id"])
    elif not leaders:
        parts.append("no leader")
    else:
        parts.append("split brain: " + ",".join(str(r["id"]) for r in leaders))

    if down:
        parts.append("down: " + ",".join(str(r["port"]) for r in down))

    if alive:
        terms = [r.get("term", 0) for r in alive]
        if len(set(terms)) == 1:
            parts.append("term %d" % terms[0])
        else:
            parts.append("term " + "/".join(str(t) for t in terms) + " DRIFT")

        tails = [r.get("last_log_index", 0) for r in alive]
        if len(set(tails)) == 1:
            parts.append("last_log %d" % tails[0])
        else:
            parts.append("last_log " + "/".join(str(t) for t in tails) + " DRIFT")

    return " | ".join(parts)


def main(argv):
    parser = argparse.ArgumentParser(description="show every raft node at once")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--ports", type=lambda v: [int(x) for x in v.split(",")],
                        default=[7001, 7002, 7003])
    parser.add_argument("--timeout", type=float, default=1.0)
    parser.add_argument("--watch", type=float, default=0.0,
                        help="repeat every N seconds")
    parser.add_argument("--json", action="store_true", help="print raw json")
    args = parser.parse_args(argv)

    try:
        while True:
            rows = collect(args)
            if args.json:
                print(json.dumps(rows, ensure_ascii=False, indent=2))
            else:
                if args.watch:
                    sys.stdout.write("\033[H\033[J")
                print(render(rows))
                print()
                print(verdict(rows))
                sys.stdout.flush()
            if not args.watch:
                break
            import time
            time.sleep(args.watch)
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

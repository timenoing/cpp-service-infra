"""解析盘上的 raftlog。

布局（全部大端）：

    [0..15]    文件头   magic(4) + 12 字节保留
    [16..]     帧       [u32 len][u64 index][u64 term][u32 crc][msg]
                        len = 20 + len(msg)
                        crc = crc32(index||term||msg)
"""

import struct
import time
import zlib

MAGIC = 0x524C4F47
HEADER = 16


def read_log(path):
    """返回 [(index, term, msg)]，格式不对直接抛 AssertionError。"""
    with open(path, "rb") as f:
        data = f.read()
    assert len(data) >= HEADER, "%s 比文件头还短: %d" % (path, len(data))
    magic, = struct.unpack(">I", data[0:4])
    assert magic == MAGIC, "%s magic 0x%08x" % (path, magic)

    out, off = [], HEADER
    while off < len(data):
        assert len(data) - off >= 4, "帧长度字段不全 @%d" % off
        n, = struct.unpack(">I", data[off:off + 4])
        assert n >= 20, ("非法 len=%d @%d：这一帧的起点被写成了空洞，"
                         "常见原因是 cut_log 截断后 log_end 没回退" % (n, off))
        assert off + 4 + n <= len(data), "半帧 @%d" % off
        body = data[off + 4:off + 4 + n]
        idx, term, crc = struct.unpack(">QQI", body[:20])
        msg = body[20:]
        want = zlib.crc32(body[:16] + msg) & 0xFFFFFFFF
        assert crc == want, "第 %d 条 crc 不符" % idx
        out.append((idx, term, msg))
        off += 4 + n

    assert [i for i, _, _ in out] == list(range(1, len(out) + 1)), \
        "编号不连续: %s" % [i for i, _, _ in out]
    return out


def wait_log(path, n, timeout=6.0, step=0.05):
    """等到盘上出现第 n 条。

    内存里的 last_log_index 领先磁盘最多一个心跳，而且批写不是原子的，
    读到半帧会抛，所以这里重试到超时。
    """
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            entries = read_log(path)
            if entries and entries[-1][0] >= n:
                return entries
        except (AssertionError, OSError) as e:
            last = e
        time.sleep(step)
    raise AssertionError("%s 在 %.1fs 内没到 %d 条: %s"
                         % (path, timeout, n, last))

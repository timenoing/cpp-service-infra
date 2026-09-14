#include "Message.h"
#include <algorithm>
#include <arpa/inet.h>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <vector>

static int g_failed = 0;

#define CHECK(cond)                                                        \
    do {                                                                   \
        if (!(cond)) {                                                     \
            std::printf("  [FAIL] %s:%d %s\n", __FILE__, __LINE__, #cond); \
            ++g_failed;                                                    \
        }                                                                  \
    } while (0)

static void feed_chunked(net::messagedecode &d, const std::string &bytes,
                         size_t chunk) {
    for (size_t i = 0; i < bytes.size(); i += chunk) {
        size_t n = std::min(chunk, bytes.size() - i);
        d.feed(bytes.data() + i, (ssize_t)n);
    }
}

static bool same(const std::vector<std::string> &a,
                 const std::vector<std::string> &b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i)
        if (a[i] != b[i]) return false;
    return true;
}

int main() {
    std::printf("[case] single whole frame\n");
    {
        net::messagedecode d;
        std::string pkt = net::encode("hello");
        d.feed(pkt.data(), (ssize_t)pkt.size());
        auto v = d.take();
        CHECK(v.size() == 1);
        CHECK(!v.empty() && v[0] == "hello");
        CHECK(!d.brokenmessage());
    }

    std::printf("[case] sticky packets in one feed\n");
    {
        net::messagedecode d;
        std::string pkt =
            net::encode("aaa") + net::encode("bbbb") + net::encode("ccccc");
        d.feed(pkt.data(), (ssize_t)pkt.size());
        auto v = d.take();
        CHECK(v.size() == 3);
        CHECK(v[0] == "aaa" && v[1] == "bbbb" && v[2] == "ccccc");
        CHECK(!d.brokenmessage());
    }

    std::printf("[case] byte-by-byte fragmented header+body\n");
    {
        net::messagedecode d;
        std::string pkt = net::encode("fragment test");
        feed_chunked(d, pkt, 1);
        auto v = d.take();
        CHECK(v.size() == 1);
        CHECK(!v.empty() && v[0] == "fragment test");
        CHECK(!d.brokenmessage());
    }

    std::printf("[case] len=0 empty message\n");
    {
        net::messagedecode d;
        std::string pkt = net::encode("");
        d.feed(pkt.data(), (ssize_t)pkt.size());
        auto v = d.take();
        CHECK(v.size() == 1);
        CHECK(!v.empty() && v[0].empty());
        CHECK(!d.brokenmessage());
    }

    std::printf("[case] exactly 1MB boundary message is legal\n");
    {
        net::messagedecode d;
        std::string big(1024 * 1024, 'x');
        std::string pkt = net::encode(big);
        feed_chunked(d, pkt, 65536);
        auto v = d.take();
        CHECK(v.size() == 1);
        CHECK(!v.empty() && v[0] == big);
        CHECK(!d.brokenmessage());
    }

    std::printf("[case] oversized len header with no body breaks immediately\n");
    {
        net::messagedecode d;
        uint32_t bad = htonl(1024 * 1024 + 1);
        d.feed(reinterpret_cast<char *>(&bad), 4);
        CHECK(d.brokenmessage());
    }

    std::printf("[case] three garbage bytes only wait, not break\n");
    {
        net::messagedecode d;
        d.feed("\xff\xff\xff", 3);
        CHECK(!d.brokenmessage());
        auto v = d.take();
        CHECK(v.empty());
    }

    std::printf("[case] bad frame rejected after error\n");
    {
        net::messagedecode d;
        uint32_t bad = htonl(0xFFFFFFFFu);
        d.feed(reinterpret_cast<char *>(&bad), 4);
        CHECK(d.brokenmessage());
        std::string pkt = net::encode("still alive?");
        d.feed(pkt.data(), (ssize_t)pkt.size());
        CHECK(d.brokenmessage());
        auto v = d.take();
        CHECK(v.empty());
    }

    std::printf("[case] take clears the queue\n");
    {
        net::messagedecode d;
        std::string pkt = net::encode("one") + net::encode("two");
        d.feed(pkt.data(), (ssize_t)pkt.size());
        CHECK(d.take().size() == 2);
        CHECK(d.take().empty());
    }

    std::printf("[case] compaction path keeps parsing correct\n");
    {
        net::messagedecode d;
        std::string expect;
        std::string pkt;
        for (int i = 0; i < 600; ++i) {
            std::string m(1024, 'a' + (char)(i % 26));
            expect += m;
            pkt += net::encode(m);
        }
        feed_chunked(d, pkt, 4096);
        std::string pkt2 = net::encode("after compaction");
        expect += "after compaction";
        feed_chunked(d, pkt2, 1);
        auto v = d.take();
        CHECK(v.size() == 601);
        CHECK(!v.empty() && v.back() == "after compaction");
        std::string joined;
        for (auto &s : v) joined += s;
        CHECK(joined == expect);
        CHECK(!d.brokenmessage());
    }

    std::printf("[case] random fragmentation stress\n");
    {
        std::mt19937 rng(12345);
        std::vector<std::string> msgs;
        std::string stream;
        for (int i = 0; i < 1000; ++i) {
            size_t len = (size_t)(rng() % 8193);
            std::string m(len, '\0');
            for (auto &c : m) c = (char)(rng() % 256);
            msgs.push_back(m);
            stream += net::encode(m);
        }
        std::uniform_int_distribution<size_t> chop(1, 17);
        net::messagedecode d;
        size_t pos = 0;
        while (pos < stream.size()) {
            size_t n = std::min(chop(rng), stream.size() - pos);
            d.feed(stream.data() + pos, (ssize_t)n);
            pos += n;
        }
        auto v = d.take();
        CHECK(same(v, msgs));
        CHECK(!d.brokenmessage());
    }

    if (g_failed == 0) {
        std::printf("ALL PASS\n");
        return 0;
    }
    std::printf("%d CHECK(S) FAILED\n", g_failed);
    return 1;
}

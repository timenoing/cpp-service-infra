#include "mq/raft/raft_router.h"

#include <cstdio>
#include <cstdlib>

// usage: raft_router [nodes]
//   节点 id 为 1..nodes，监听端口为 RAFT_PORT_BASE + id（默认基址 7000）
int main(int argc, char** argv) {
  const uint32_t nodes =
      argc >= 2 ? static_cast<uint32_t>(std::stoul(argv[1])) : 3;

  const char* base_env = std::getenv("RAFT_PORT_BASE");
  const uint32_t port_base =
      (base_env != nullptr && *base_env != '\0')
          ? static_cast<uint32_t>(std::stoul(base_env))
          : 7000u;
  if (port_base == 0 || port_base + nodes > 65535u) {
    std::fprintf(stderr, "router: bad RAFT_PORT_BASE=%s\n",
                 base_env ? base_env : "");
    return 1;
  }

  std::unordered_map<uint32_t, std::pair<std::string, uint32_t>> table;
  for (uint32_t i = 1; i <= nodes; ++i) {
    table.emplace(i, std::make_pair(std::string("127.0.0.1"), port_base + i));
  }

  const int kListenPort = static_cast<int>(port_base);
  Raft_router router(table, kListenPort);
  return router.start(kListenPort) ? 0 : 2;
}

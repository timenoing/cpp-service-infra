#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>

#include "mq/raft/Raftnode.h"

// usage: raft_node <id> [nodes]
//   id    1..nodes
//   nodes 集群节点总数，省略时按 3 处理
// 环境变量 RAFT_DATA_DIR 非空时启用落盘：<dir>/node<id>/{hardstate,raftlog}
int main(int argc, char** argv) {
  if (argc < 2) return 1;
  const uint32_t me = static_cast<uint32_t>(std::stoul(argv[1]));
  const uint32_t nodes =
      argc >= 3 ? static_cast<uint32_t>(std::stoul(argv[2])) : 3;
  if (me < 1 || me > nodes) return 1;

  // 端口基址可配，便于多核并行跑多套集群（默认仍是 7000）。
  const char* base_env = std::getenv("RAFT_PORT_BASE");
  const uint32_t port_base =
      (base_env != nullptr && *base_env != '\0')
          ? static_cast<uint32_t>(std::stoul(base_env))
          : 7000u;
  if (port_base == 0 || port_base + nodes > 65535u) {
    std::fprintf(stderr, "node %u: bad RAFT_PORT_BASE=%s\n", me,
                 base_env ? base_env : "");
    return 1;
  }

  raftconfig cfg;
  cfg.me = me;
  cfg.listen_port = static_cast<uint16_t>(port_base + me);  // base+1 / base+2 / ...
  cfg.router_ip = "127.0.0.1";
  cfg.router_port = static_cast<uint16_t>(port_base);

  std::string head_path;
  std::string log_path;
  if (const char* dir = std::getenv("RAFT_DATA_DIR"); dir != nullptr && *dir != '\0') {
    std::filesystem::path node_dir =
        std::filesystem::path(dir) / ("node" + std::to_string(me));
    std::error_code ec;
    std::filesystem::create_directories(node_dir, ec);
    if (ec) {
      std::fprintf(stderr, "node %u: create_directories %s: %s\n", me,
                   node_dir.string().c_str(), ec.message().c_str());
      return 2;
    }
    head_path = (node_dir / "hardstate").string();
    log_path = (node_dir / "raftlog").string();
  }

  mlog::io::PosixFileIo io;
  raftnode node(nodes, io, head_path, log_path);
  return node.start(cfg) ? 0 : 2;
}

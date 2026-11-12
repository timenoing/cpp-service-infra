#include "mq/raft/Raftnode.h"
int main(int argc, char** argv) {
  if (argc < 2) return 1;
  const uint32_t me = static_cast<uint32_t>(std::stoul(argv[1]));

  std::vector<std::pair<std::string, uint32_t>> map = {
      {"127.0.0.1", 7001},
      {"127.0.0.1", 7002},
      {"127.0.0.1", 7003},
  };

  raftconfig cfg;
  cfg.me = me;
  cfg.listen_port = static_cast<uint16_t>(map[me - 1].second);
  for (uint32_t i = 0; i < map.size(); ++i) {
    if (i + 1 == me) continue;
    cfg.peers.push_back({i + 1, static_cast<uint16_t>(map[i].second), map[i].first});
  }

  mlog::io::PosixFileIo io;
  raftnode node(map.size(), io, "", "", cfg);   // path、log_path 空着
  return node.start(cfg) ? 0 : 2;
}
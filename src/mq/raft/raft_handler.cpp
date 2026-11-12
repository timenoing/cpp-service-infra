#include "mq/raft/raft_handler.h"

#include "Epoll.h"
#include "mq/raft/raft_inbox.h"

void install_raft_handler(Epoll& epoll, Raft_inbox& inbox) {
  epoll.set_handler([&inbox](const std::string& payload) -> std::string {
    inboundmsg m;
    if (!raft_code(payload, m.msg, m.opcode, m.from)) return {};
    m.bytes = payload.size();
    inbox.try_push(std::move(m));
    return {};
  });
}

#ifndef MQ_RAFT_RAFT_HANDLER_H
#define MQ_RAFT_RAFT_HANDLER_H

class Epoll;
class Raft_inbox;

void install_raft_handler(Epoll& epoll, Raft_inbox& inbox);

#endif  // MQ_RAFT_RAFT_HANDLER_H

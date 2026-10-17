#ifndef COMMIT_LOG_H
#define COMMIT_LOG_H
#include<cstdint>
#include <string>
#include<fcntl.h>
#include<errno.h>
#include<sys/stat.h>
#include <unistd.h>
#include <zlib.h>
#include "mq/io/File_io.h"
namespace mlog{
enum class comiitstatus {candidate=0, credited=1,published=2  };
struct commitEntry{
  uint64_t batch_no; //8B
  uint64_t candidate_hw;
  uint64_t epoch;
  uint8_t state;
};
class Commitlog{
  public:
     struct Reply{
  uint64_t last_batch_no=0; //8B
  uint64_t credit_hw=0;
  uint64_t last_epoch=0;
  bool corrupt=false;
     };
     Commitlog(const Commitlog&)=delete;
     Commitlog& operator=(const Commitlog&)=delete;
    commitEntry log;
    explicit Commitlog(std::string path, mlog::io::File_io& io = mlog::io::posix_file_io());
    ~Commitlog();
    bool open_or_create(Reply& out);
    bool append();
    bool sync();
    private:
    std::string path;
    int fd=-1;
    ssize_t offset=0;
    mlog::io::File_io& io_;
};
};
#endif

#ifndef SEGMENT_RECOVERY
#define SEGMENT_RECOVERY
#include "Segment.h"
struct Scan_end{
  enum kind{clean, torn_tail, corrupt, head_bad} kind;
  uint64_t vaild_end=0;
  uint64_t torn_end=0;
  uint8_t corrupt_at=0;
  uint32_t last_epoch=0;
};

class Segment_recovery{
   public:
   Segment_recovery(int inteval,int fd);
   ~Segment_recovery();
   Scan_end segment_scanner();
   void turncate_segment(uint64_t offset);
   void server_idx(uint64_t offset);
   uint64_t find_idx(const uint64_t idx);
   private:
   uint8_t* buf;
   std::vector<uint64_t> idx;
   int inteval;
   int fd=-1;
  uint64_t offset=32;
  uint64_t base_offset=0;
  uint32_t prev_epoch=0;
};
#endif
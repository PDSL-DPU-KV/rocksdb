#include <doca_mmap.h>

#include "dflush/common.h"

struct DMAThread {
  DMAThread(uint32_t max_bufs, uint32_t num_dma_tasks);
  ~DMAThread();

  void run(params_memcpy_t params, doca_mmap* dst_m, doca_mmap* src_m);

  struct doca_buf_inventory* buf_inv;
  struct doca_pe* pe;
  struct doca_dma* dma_ctx;
  struct doca_ctx* ctx;
};
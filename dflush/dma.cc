#include "dma.h"

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_ctx.h>
#include <doca_dma.h>
#include <doca_pe.h>

#include <cstdio>
#include <vector>

static void dma_memcpy_completed_callback(struct doca_dma_task_memcpy* dma_task,
                                          union doca_data task_user_data,
                                          union doca_data ctx_user_data) {
  struct dma_resources* resources = (struct dma_resources*)ctx_user_data.ptr;
  struct doca_task* task = doca_dma_task_memcpy_as_task(dma_task);
  doca_check(doca_task_get_status(task));

  /* Free task */
  doca_task_free(doca_dma_task_memcpy_as_task(dma_task));
}

static void dma_memcpy_error_callback(struct doca_dma_task_memcpy* dma_task,
                                      union doca_data task_user_data,
                                      union doca_data ctx_user_data) {
  printf("dma error!\n");
  struct dma_resources* resources = (struct dma_resources*)ctx_user_data.ptr;
  struct doca_task* task = doca_dma_task_memcpy_as_task(dma_task);
  doca_check(doca_task_get_status(task));

  /* Free task */
  doca_task_free(task);
}

static void dma_state_changed_callback(const union doca_data user_data,
                                       struct doca_ctx* ctx,
                                       enum doca_ctx_states prev_state,
                                       enum doca_ctx_states next_state) {
  (void)ctx;
  (void)prev_state;

  switch (next_state) {
    case DOCA_CTX_STATE_IDLE:
      printf("DMA context has been stopped\n");
      break;
    case DOCA_CTX_STATE_STARTING:
      printf(
          "DMA context entered into starting state. Unexpected transition\n");
      break;
    case DOCA_CTX_STATE_RUNNING:
      printf("DMA context is running\n");
      break;
    case DOCA_CTX_STATE_STOPPING:
      printf(
          "DMA context entered into stopping state. Any inflight tasks "
          "will be flushed\n");
      break;
    default:
      break;
  }
}

DMAThread::DMAThread(uint32_t max_bufs, uint32_t num_dma_tasks) {
  doca_check(doca_buf_inventory_create(max_bufs, &buf_inv));
  doca_check(doca_buf_inventory_start(buf_inv));
  doca_check(doca_pe_create(&pe));
  doca_check(doca_dma_create(dev, &dma_ctx));
  ctx = doca_dma_as_ctx(dma_ctx);
  doca_check(doca_ctx_set_state_changed_cb(ctx, dma_state_changed_callback));
  doca_check(
      doca_dma_task_memcpy_set_conf(dma_ctx, dma_memcpy_completed_callback,
                                    dma_memcpy_error_callback, num_dma_tasks));
  doca_check(doca_pe_connect_ctx(pe, ctx));
  doca_check(doca_ctx_start(ctx));
}

void DMAThread::run(params_memcpy_t params, doca_mmap* dst_m,
                    doca_mmap* src_m) {
  struct doca_dma_task_memcpy* dma_task = nullptr;
  union doca_data task_user_data = {0};
  struct doca_task* task = nullptr;
  std::vector<doca_buf*> dst_bufs;
  std::vector<doca_buf*> src_bufs;

  for (uint64_t i = 0; i < params.copy_n; i++) {
    doca_buf* dst_buf = nullptr;
    doca_buf* src_buf = nullptr;
    auto dst_addr = (char*)params.dst.ptr + i * params.copy_size;
    auto src_addr = (char*)params.src.ptr + i * params.copy_size;
    //   printf("copy %d, size: %ld, dst: %lx, src: %lx\n", i,
    //   params.copy_size,
    //          dst_addr, src_addr);
    doca_check(doca_buf_inventory_buf_get_by_addr(
        buf_inv, dst_m, (void*)dst_addr, params.copy_size, &dst_buf));
    doca_check(doca_buf_inventory_buf_get_by_addr(
        buf_inv, src_m, (void*)src_addr, params.copy_size, &src_buf));
    doca_check(doca_buf_set_data(src_buf, (void*)src_addr, params.copy_size));
    dst_bufs.emplace_back(dst_buf);
    src_bufs.emplace_back(src_buf);
    doca_check(doca_dma_task_memcpy_alloc_init(dma_ctx, src_buf, dst_buf,
                                               task_user_data, &dma_task));
    task = doca_dma_task_memcpy_as_task(dma_task);
    doca_check(doca_task_submit(task));
    //   poll until dma task finished
    while (!doca_pe_progress(pe));
  }

  for (size_t i = 0; i < dst_bufs.size(); i++) {
    doca_buf_dec_refcount(dst_bufs[i], NULL);
    doca_buf_dec_refcount(src_bufs[i], NULL);
  }
}

DMAThread::~DMAThread() {
  doca_check(doca_ctx_stop(ctx));
  doca_check(doca_dma_destroy(dma_ctx));
  doca_check(doca_pe_destroy(pe));
  doca_check(doca_buf_inventory_destroy(buf_inv));
}
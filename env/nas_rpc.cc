#include "nas_rpc.h"

#include <mercury_config.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "env/rpc.h"
#include "rocksdb/env.h"
#include "rpc_engine.h"
#include "rpc_util.h"

namespace ROCKSDB_NAMESPACE {

static hg_id_t open_rpc_id, fopen_rpc_id, close_rpc_id, fseek_rpc_id,
    write_rpc_id, read_rpc_id, fstat_rpc_id, ftruncate_rpc_id, fallocate_rpc_id,
    fdatasync_rpc_id, fsync_rpc_id, rangesync_rpc_id, rename_rpc_id,
    access_rpc_id, unlink_rpc_id, mkdir_rpc_id, rmdir_rpc_id, stat_rpc_id,
    ls_rpc_id, lock_rpc_id;

static void open_rpc(hg_addr_t svr_addr, open_rpc_in_t *in, FutureResponse *fr);
static hg_return_t open_rpc_cb(const struct hg_cb_info *info);
static void fopen_rpc(hg_addr_t svr_addr, fopen_rpc_in_t *in,
                      FutureResponse *fr);
static hg_return_t fopen_rpc_cb(const struct hg_cb_info *info);
static void close_rpc(hg_addr_t svr_addr, close_rpc_in_t *in,
                      FutureResponse *fr);
static hg_return_t close_rpc_cb(const struct hg_cb_info *info);
static void fseek_rpc(hg_addr_t svr_addr, fseek_rpc_in_t *in,
                      FutureResponse *fr);
static hg_return_t fseek_rpc_cb(const struct hg_cb_info *info);
static void write_rpc(hg_addr_t svr_addr, write_rpc_in_t *in,
                      FutureResponse *fr);
static hg_return_t write_rpc_cb(const struct hg_cb_info *info);
static void read_rpc(hg_addr_t svr_addr, read_rpc_in_t *in, FutureResponse *fr);
static hg_return_t read_rpc_cb(const struct hg_cb_info *info);
static void fstat_rpc(hg_addr_t svr_addr, fstat_rpc_in_t *in,
                      FutureResponse *fr);
static hg_return_t fstat_rpc_cb(const struct hg_cb_info *info);
static void ftruncate_rpc(hg_addr_t svr_addr, ftruncate_rpc_in_t *in,
                          FutureResponse *fr);
static hg_return_t ftruncate_rpc_cb(const struct hg_cb_info *info);
static void fallocate_rpc(hg_addr_t svr_addr, fallocate_rpc_in_t *in,
                          FutureResponse *fr);
static hg_return_t fallocate_rpc_cb(const struct hg_cb_info *info);
static void fdatasync_rpc(hg_addr_t svr_addr, fdatasync_rpc_in_t *in,
                          FutureResponse *fr);
static hg_return_t fdatasync_rpc_cb(const struct hg_cb_info *info);
static void fsync_rpc(hg_addr_t svr_addr, fsync_rpc_in_t *in,
                      FutureResponse *fr);
static hg_return_t fsync_rpc_cb(const struct hg_cb_info *info);
static void rangesync_rpc(hg_addr_t svr_addr, rangesync_rpc_in_t *in,
                          FutureResponse *fr);
static hg_return_t rangesync_rpc_cb(const struct hg_cb_info *info);
static void rename_rpc(hg_addr_t svr_addr, rename_rpc_in_t *in,
                       FutureResponse *fr);
static hg_return_t rename_rpc_cb(const struct hg_cb_info *info);
static void access_rpc(hg_addr_t svr_addr, access_rpc_in_t *in,
                       FutureResponse *fr);
static hg_return_t access_rpc_cb(const struct hg_cb_info *info);
static void unlink_rpc(hg_addr_t svr_addr, unlink_rpc_in_t *in,
                       FutureResponse *fr);
static hg_return_t unlink_rpc_cb(const struct hg_cb_info *info);
static void mkdir_rpc(hg_addr_t svr_addr, mkdir_rpc_in_t *in,
                      FutureResponse *fr);
static hg_return_t mkdir_rpc_cb(const struct hg_cb_info *info);
static void rmdir_rpc(hg_addr_t svr_addr, rmdir_rpc_in_t *in,
                      FutureResponse *fr);
static hg_return_t rmdir_rpc_cb(const struct hg_cb_info *info);
static void stat_rpc(hg_addr_t svr_addr, stat_rpc_in_t *in, FutureResponse *fr);
static hg_return_t stat_rpc_cb(const struct hg_cb_info *info);
static void ls_rpc(hg_addr_t svr_addr, ls_rpc_in_t *in, FutureResponse *fr);
static hg_return_t ls_rpc_cb(const struct hg_cb_info *info);
static void lock_rpc(hg_addr_t svr_addr, lock_rpc_in_t *in, FutureResponse *fr);
static hg_return_t lock_rpc_cb(const struct hg_cb_info *info);

static void open_rpc(hg_addr_t svr_addr, open_rpc_in_t *in,
                     FutureResponse *fr) {
  hg_handle_t handle;
  hg_return_t ret;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, open_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, open_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

/* callback triggered upon receipt of rpc response */
static hg_return_t open_rpc_cb(const struct hg_cb_info *info) {
  open_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got open response fd: %d\n", out.fd);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return HG_SUCCESS;
}

static void fopen_rpc(hg_addr_t svr_addr, fopen_rpc_in_t *in,
                      FutureResponse *fr) {
  hg_handle_t handle;
  hg_return_t ret;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, fopen_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, fopen_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

/* callback triggered upon receipt of rpc response */
static hg_return_t fopen_rpc_cb(const struct hg_cb_info *info) {
  fopen_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got fopen response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return HG_SUCCESS;
}

static void close_rpc(hg_addr_t svr_addr, close_rpc_in_t *in,
                      FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, close_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, close_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

/* callback triggered upon receipt of rpc response */
static hg_return_t close_rpc_cb(const struct hg_cb_info *info) {
  close_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got close response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return HG_SUCCESS;
}

static void fseek_rpc(hg_addr_t svr_addr, fseek_rpc_in_t *in,
                      FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, fseek_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, fseek_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

/* callback triggered upon receipt of rpc response */
static hg_return_t fseek_rpc_cb(const struct hg_cb_info *info) {
  fseek_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got fseek response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return HG_SUCCESS;
}

static void write_rpc(hg_addr_t svr_addr, write_rpc_in_t *in,
                      FutureResponse *fr) {
  hg_handle_t handle;
  hg_return_t ret;
  const struct hg_info *hgi;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, write_rpc_id, &handle);

  /* register local target buffer for bulk access */
  hgi = HG_Get_info(handle);
  assert(hgi);
  ret = HG_Bulk_create(hgi->hg_class, 1, (void **)&fr->send_buf, &in->n,
                       HG_BULK_READ_ONLY, &in->bulk_handle);
  assert(ret == 0);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, write_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t write_rpc_cb(const struct hg_cb_info *info) {
  write_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got write response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return HG_SUCCESS;
}

static void read_rpc(hg_addr_t svr_addr, read_rpc_in_t *in,
                     FutureResponse *fr) {
  hg_handle_t handle;
  hg_return_t ret;
  const struct hg_info *hgi;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, read_rpc_id, &handle);

  hgi = HG_Get_info(handle);
  assert(hgi);

  ret = HG_Bulk_create(hgi->hg_class, 1, (void **)&fr->recv_buf, &in->n,
                       HG_BULK_READWRITE, &in->bulk_handle);
  assert(ret == HG_SUCCESS);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, read_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t read_rpc_cb(const struct hg_cb_info *info) {
  read_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got read response size: %ld\n", out.size);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return ret;
}

static void fstat_rpc(hg_addr_t svr_addr, fstat_rpc_in_t *in,
                      FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, fstat_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, fstat_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t fstat_rpc_cb(const struct hg_cb_info *info) {
  fstat_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got fstat response file size: %d, blk size: %d\n", out.st_size,
         out.st_blksize);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return HG_SUCCESS;
}

static void ftruncate_rpc(hg_addr_t svr_addr, ftruncate_rpc_in_t *in,
                          FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, ftruncate_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, ftruncate_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t ftruncate_rpc_cb(const struct hg_cb_info *info) {
  ftruncate_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got ftruncate response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return HG_SUCCESS;
}

static void fallocate_rpc(hg_addr_t svr_addr, fallocate_rpc_in_t *in,
                          FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, fallocate_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, fallocate_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t fallocate_rpc_cb(const struct hg_cb_info *info) {
  fallocate_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got fallocate response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return HG_SUCCESS;
}

static void fdatasync_rpc(hg_addr_t svr_addr, fdatasync_rpc_in_t *in,
                          FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, fdatasync_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, fdatasync_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t fdatasync_rpc_cb(const struct hg_cb_info *info) {
  fdatasync_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got fdatasync response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return HG_SUCCESS;
}

static void fsync_rpc(hg_addr_t svr_addr, fsync_rpc_in_t *in,
                      FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, fsync_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, fsync_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t fsync_rpc_cb(const struct hg_cb_info *info) {
  fsync_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got fsync response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return HG_SUCCESS;
}

static void rangesync_rpc(hg_addr_t svr_addr, rangesync_rpc_in_t *in,
                          FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, rangesync_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, rangesync_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t rangesync_rpc_cb(const struct hg_cb_info *info) {
  rangesync_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got rangesync response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return HG_SUCCESS;
}

static void rename_rpc(hg_addr_t svr_addr, rename_rpc_in_t *in,
                       FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, rename_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, rename_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t rename_rpc_cb(const struct hg_cb_info *info) {
  rename_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got rename response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return HG_SUCCESS;
}

static void access_rpc(hg_addr_t svr_addr, access_rpc_in_t *in,
                       FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, access_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, access_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t access_rpc_cb(const struct hg_cb_info *info) {
  access_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got access response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();

  return HG_SUCCESS;
}

static void unlink_rpc(hg_addr_t svr_addr, unlink_rpc_in_t *in,
                       FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, unlink_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, unlink_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t unlink_rpc_cb(const struct hg_cb_info *info) {
  unlink_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got unlink response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();

  return HG_SUCCESS;
}

static void mkdir_rpc(hg_addr_t svr_addr, mkdir_rpc_in_t *in,
                      FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, mkdir_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, mkdir_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t mkdir_rpc_cb(const struct hg_cb_info *info) {
  mkdir_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got mkdir response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();

  return HG_SUCCESS;
}

static void rmdir_rpc(hg_addr_t svr_addr, rmdir_rpc_in_t *in,
                      FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, rmdir_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, rmdir_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t rmdir_rpc_cb(const struct hg_cb_info *info) {
  rmdir_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got rmdir response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();

  return HG_SUCCESS;
}

static void stat_rpc(hg_addr_t svr_addr, stat_rpc_in_t *in,
                     FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, stat_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, stat_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t stat_rpc_cb(const struct hg_cb_info *info) {
  stat_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got stat response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();

  return HG_SUCCESS;
}

static void ls_rpc(hg_addr_t svr_addr, ls_rpc_in_t *in, FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, ls_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, ls_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

static hg_return_t ls_rpc_cb(const struct hg_cb_info *info) {
  ls_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got ls response, ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);

  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();

  return HG_SUCCESS;
}

static void lock_rpc(hg_addr_t svr_addr, lock_rpc_in_t *in,
                     FutureResponse *fr) {
  hg_return_t ret;
  hg_handle_t handle;

  /* create create handle to represent this rpc operation */
  hg_engine_create_handle(svr_addr, lock_rpc_id, &handle);

  /* Send rpc. Note that we are also transmitting the bulk handle in the
   * input struct.  It was set above.
   */
  ret = HG_Forward(handle, lock_rpc_cb, (void *)fr, in);
  assert(ret == 0);
  (void)ret;

  return;
}

/* callback triggered upon receipt of rpc response */
static hg_return_t lock_rpc_cb(const struct hg_cb_info *info) {
  lock_rpc_out_t out;
  hg_return_t ret;
  FutureResponse *fr = (FutureResponse *)info->arg;

  assert(info->ret == HG_SUCCESS);

  /* decode response */
  ret = HG_Get_output(info->info.forward.handle, &out);
  assert(ret == 0);
  (void)ret;
#ifdef NAS_DEBUG
  printf("Got lock response ret: %d\n", out.ret);
#endif
  fr->func(&out, fr->data);
  /* clean up resources consumed by this rpc */
  HG_Free_output(info->info.forward.handle, &out);
  HG_Destroy(info->info.forward.handle);

  fr->notify();
  return HG_SUCCESS;
}

RPCEngine::RPCEngine(const std::string &svr_addr_string) {
  hg_engine_init(HG_FALSE, "verbs");
  hg_engine_addr_lookup(svr_addr_string.c_str(), &svr_addr);
  // register file system rpcs
  open_rpc_id = open_rpc_register();
  fopen_rpc_id = fopen_rpc_register();
  close_rpc_id = close_rpc_register();
  fseek_rpc_id = fseek_rpc_register();
  write_rpc_id = write_rpc_register();
  read_rpc_id = read_rpc_register();
  fstat_rpc_id = fstat_rpc_register();
  ftruncate_rpc_id = ftruncate_rpc_register();
  fallocate_rpc_id = fallocate_rpc_register();
  fdatasync_rpc_id = fdatasync_rpc_register();
  fsync_rpc_id = fsync_rpc_register();
  rangesync_rpc_id = rangesync_rpc_register();
  rename_rpc_id = rename_rpc_register();
  access_rpc_id = access_rpc_register();
  unlink_rpc_id = unlink_rpc_register();
  mkdir_rpc_id = mkdir_rpc_register();
  rmdir_rpc_id = rmdir_rpc_register();
  stat_rpc_id = stat_rpc_register();
  ls_rpc_id = ls_rpc_register();
  lock_rpc_id = lock_rpc_register();
}

RPCEngine::~RPCEngine() { /* shut down */
  hg_engine_addr_free(svr_addr);
  hg_engine_finalize();
}

int RPCEngine::Open(const char *fname, int flags, uint mode) {
  open_rpc_in_t in;
  in.fname = fname;
  in.flags = flags;
  in.mode = mode;
#ifdef NAS_DEBUG
  printf("send open req, fname: %s, flags: %d, mode: %d\n", fname, flags, mode);
#endif
  int open_fd = -1;
  FutureResponse fr;
  fr.data = &open_fd;
  fr.func = [](void *out, void *result) {
    open_rpc_out_t *rpc_out = (open_rpc_out_t *)out;
    int *res = (int *)result;
    *res = rpc_out->fd;
  };
  open_rpc(svr_addr, &in, &fr);
  fr.wait();

  return open_fd;
}  // Open

bool RPCEngine::Fopen(int fd, const char *mode) {
  fopen_rpc_in_t in;
  in.fd = fd;
  in.mode = mode;
#ifdef NAS_DEBUG
  printf("send fopen req, fd: %d, mode: %s\n", fd, mode);
#endif
  FutureResponse fr;
  bool ret = false;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    fopen_rpc_out_t *rpc_out = (fopen_rpc_out_t *)out;
    bool *res = (bool *)result;
    *res = rpc_out->ret;
  };
  fopen_rpc(svr_addr, &in, &fr);
  fr.wait();

  return ret;
}  // Fopen

int RPCEngine::Close(int fd) {
  close_rpc_in_t in;
  in.fd = fd;
#ifdef NAS_DEBUG
  printf("send close req, fd: %d\n", fd);
#endif
  FutureResponse fr;
  int ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    close_rpc_out_t *rpc_out = (close_rpc_out_t *)out;
    int *res = (int *)result;
    *res = rpc_out->ret;
  };
  close_rpc(svr_addr, &in, &fr);
  fr.wait();

  return ret;
}  // close

int RPCEngine::Fseek(int fd, size_t n) {
  fseek_rpc_in_t in;
  in.fd = fd;
  in.n = n;
#ifdef NAS_DEBUG
  printf("send fseek req, fd: %d, n: %ld\n", fd, n);
#endif
  FutureResponse fr;
  int ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    fseek_rpc_out_t *rpc_out = (fseek_rpc_out_t *)out;
    int *res = (int *)result;
    *res = rpc_out->ret;
  };
  fseek_rpc(svr_addr, &in, &fr);
  fr.wait();

  return ret;
}  // fseek

ssize_t RPCEngine::Fread(int fd, size_t n, char *buffer) {
  read_rpc_in_t in;
  in.fd = fd;
  in.n = n;
  in.offset = -1;
#ifdef NAS_DEBUG
  printf("send fread req, fd: %d, n: %ld\n", fd, n);
#endif
  FutureResponse fr;
  int re = posix_memalign((void **)&fr.recv_buf, 512, in.n);
  assert(re == 0);
  size_t ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    read_rpc_out_t *rpc_out = (read_rpc_out_t *)out;
    size_t *res = (size_t *)result;
    *res = rpc_out->size;
  };
  read_rpc(svr_addr, &in, &fr);
  fr.wait();

  memcpy(buffer, fr.recv_buf, in.n);
  free(fr.recv_buf);
  return ret;
}

ssize_t RPCEngine::Pread(int fd, uint64_t offset, size_t n, char *buffer) {
  read_rpc_in_t in;
  in.fd = fd;
  in.n = n;
  in.offset = offset;
#ifdef NAS_DEBUG
  printf("send pread req, fd: %d, offset: %ld, n: %ld\n", fd, offset, n);
#endif
  FutureResponse fr;
  int re = posix_memalign((void **)&fr.recv_buf, 512, in.n);
  assert(re == 0);
  size_t ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    read_rpc_out_t *rpc_out = (read_rpc_out_t *)out;
    size_t *res = (size_t *)result;
    *res = rpc_out->size;
  };
  read_rpc(svr_addr, &in, &fr);
  fr.wait();
  memcpy(buffer, fr.recv_buf, in.n);
  free(fr.recv_buf);
  return ret;
}

bool RPCEngine::Write(int fd, const char *buffer, size_t n) {
  write_rpc_in_t in;
  in.fd = fd;
  in.n = n;
  in.offset = -1;
#ifdef NAS_DEBUG
  printf("send write req, fd: %d, n: %ld\n", fd, n);
#endif
  FutureResponse fr;
  bool ret = false;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    write_rpc_out_t *rpc_out = (write_rpc_out_t *)out;
    bool *res = (bool *)result;
    *res = rpc_out->ret;
  };
  fr.send_buf = buffer;
  write_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret;
}

bool RPCEngine::PWrite(int fd, const char *buffer, size_t n, uint64_t offset) {
  write_rpc_in_t in;
  in.fd = fd;
  in.n = n;
  in.offset = static_cast<off_t>(offset);
#ifdef NAS_DEBUG
  printf("send pwrite req, fd: %d, n: %ld, offset: %ld\n", fd, n, offset);
#endif
  FutureResponse fr;
  bool ret = false;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    write_rpc_out_t *rpc_out = (write_rpc_out_t *)out;
    bool *res = (bool *)result;
    *res = rpc_out->ret;
  };
  fr.send_buf = buffer;
  write_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret;
}

int RPCEngine::Fstat(int fd, struct stat *stat_buf) {
  fstat_rpc_in_t in;
  in.fd = fd;
#ifdef NAS_DEBUG
  printf("send stat req, fd: %d\n", fd);
#endif
  FutureResponse fr;
  struct stat_ret ret;
  ret.stat_buf = stat_buf;
  ret.ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    fstat_rpc_out_t *rpc_out = (fstat_rpc_out_t *)out;
    struct stat_ret *res = (struct stat_ret *)result;
    res->stat_buf->st_size = rpc_out->st_size;
    res->stat_buf->st_blksize = rpc_out->st_blksize;
    res->stat_buf->st_blocks = rpc_out->st_blocks;
    res->stat_buf->st_dev = rpc_out->st_dev;
    res->stat_buf->st_ino = rpc_out->st_ino;
    res->stat_buf->st_mode = rpc_out->st_mode;
    res->stat_buf->st_mtime = rpc_out->st_mtime_;
    res->ret = rpc_out->ret;
  };
  fstat_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret.ret;
}

int RPCEngine::Ftruncate(int fd, uint64_t size) {
  ftruncate_rpc_in_t in;
  in.fd = fd;
  in.size = size;
#ifdef NAS_DEBUG
  printf("send ftruncate req, fd: %d, size: %ld\n", fd, size);
#endif
  FutureResponse fr;
  int ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    ftruncate_rpc_out_t *rpc_out = (ftruncate_rpc_out_t *)out;
    int *res = (int *)result;
    *res = rpc_out->ret;
  };
  ftruncate_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret;
}

int RPCEngine::Fallocate(int fd, int mode, uint64_t offset, uint64_t len) {
  fallocate_rpc_in_t in;
  in.fd = fd;
  in.mode = mode;
  in.offset = offset;
  in.len = len;
#ifdef NAS_DEBUG
  printf("send fallocate req, fd: %d, mode: %d, offset: %ld, len: %ld\n", fd,
         mode, offset, len);
#endif
  FutureResponse fr;
  int ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    fallocate_rpc_out_t *rpc_out = (fallocate_rpc_out_t *)out;
    int *res = (int *)result;
    *res = rpc_out->ret;
  };
  fallocate_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret;
}

int RPCEngine::Fdatasync(int fd) {
  fdatasync_rpc_in_t in;
  in.fd = fd;
#ifdef NAS_DEBUG
  printf("send fdatasync req, fd: %d\n", fd);
#endif
  FutureResponse fr;
  int ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    fdatasync_rpc_out_t *rpc_out = (fdatasync_rpc_out_t *)out;
    int *res = (int *)result;
    *res = rpc_out->ret;
  };
  fdatasync_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret;
}

int RPCEngine::Fsync(int fd) {
  fsync_rpc_in_t in;
  in.fd = fd;
#ifdef NAS_DEBUG
  printf("send fsync req, fd: %d\n", fd);
#endif
  FutureResponse fr;
  int ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    fsync_rpc_out_t *rpc_out = (fsync_rpc_out_t *)out;
    int *res = (int *)result;
    *res = rpc_out->ret;
  };
  fsync_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret;
}

int RPCEngine::RangeSync(int fd, uint64_t offset, uint64_t count, int flags) {
  rangesync_rpc_in_t in;
  in.fd = fd;
  in.offset = offset;
  in.count = count;
  in.flags = flags;
#ifdef NAS_DEBUG
  printf("send rangesync req, fd: %d, offset: %ld, count: %ld, flags: %d\n", fd,
         offset, count, flags);
#endif
  FutureResponse fr;
  int ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    rangesync_rpc_out_t *rpc_out = (rangesync_rpc_out_t *)out;
    int *res = (int *)result;
    *res = rpc_out->ret;
  };
  rangesync_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret;
}

int RPCEngine::Rename(const char *old_name, const char *new_name) {
  rename_rpc_in_t in;
  in.old_name = old_name;
  in.new_name = new_name;
#ifdef NAS_DEBUG
  printf("send rename req, old: %s, new: %s\n", old_name, new_name);
#endif
  FutureResponse fr;
  int ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    rename_rpc_out_t *rpc_out = (rename_rpc_out_t *)out;
    int *res = (int *)result;
    *res = rpc_out->ret;
  };
  rename_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret;
}

ret_with_errno RPCEngine::Access(const char *name, int type) {
  access_rpc_in_t in;
  in.name = name;
  in.type = type;
#ifdef NAS_DEBUG
  printf("send access req, name: %s, type: %d\n", name, type);
#endif
  FutureResponse fr;
  ret_with_errno ret;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    access_rpc_out_t *rpc_out = (access_rpc_out_t *)out;
    ret_with_errno *res = (ret_with_errno *)result;
    res->ret = rpc_out->ret;
    res->errn = rpc_out->errn;
  };
  access_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret;
}

int RPCEngine::Unlink(const char *name) {
  unlink_rpc_in_t in;
  in.name = name;
#ifdef NAS_DEBUG
  printf("send unlink req, name: %s\n", name);
#endif
  FutureResponse fr;
  int ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    unlink_rpc_out_t *rpc_out = (unlink_rpc_out_t *)out;
    int *res = (int *)result;
    *res = rpc_out->ret;
  };
  unlink_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret;
}

ret_with_errno RPCEngine::Mkdir(const char *name, uint mode) {
  mkdir_rpc_in_t in;
  in.name = name;
  in.mode = mode;
#ifdef NAS_DEBUG
  printf("Send mkdir req, fname: %s, mode: %d\n", name, mode);
#endif
  FutureResponse fr;
  struct ret_with_errno ret;
  ret.ret = -1;
  ret.errn = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    mkdir_rpc_out_t *rpc_out = (mkdir_rpc_out_t *)out;
    ret_with_errno *res = (ret_with_errno *)result;
    res->ret = rpc_out->ret;
    res->errn = rpc_out->errn;
  };
  mkdir_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret;
}

int RPCEngine::Rmdir(const char *name) {
  rmdir_rpc_in_t in;
  in.name = name;
#ifdef NAS_DEBUG
  printf("send rmdir req, name: %s\n", name);
#endif
  FutureResponse fr;
  int ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    rmdir_rpc_out_t *rpc_out = (rmdir_rpc_out_t *)out;
    int *res = (int *)result;
    *res = rpc_out->ret;
  };
  rmdir_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret;
}

int RPCEngine::Stat(const char *name, struct stat *stat_buf) {
  stat_rpc_in_t in;
  in.name = name;
#ifdef NAS_DEBUG
  printf("send stat req, name: %s\n", name);
#endif
  FutureResponse fr;
  struct stat_ret ret;
  ret.stat_buf = stat_buf;
  ret.ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    stat_rpc_out_t *rpc_out = (stat_rpc_out_t *)out;
    struct stat_ret *res = (struct stat_ret *)result;
    res->stat_buf->st_size = rpc_out->st_size;
    res->stat_buf->st_blksize = rpc_out->st_blksize;
    res->stat_buf->st_blocks = rpc_out->st_blocks;
    res->stat_buf->st_dev = rpc_out->st_dev;
    res->stat_buf->st_ino = rpc_out->st_ino;
    res->stat_buf->st_mode = rpc_out->st_mode;
    res->stat_buf->st_mtime = rpc_out->st_mtime_;
    res->ret = rpc_out->ret;
  };
  stat_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret.ret;
}

int RPCEngine::GetChildren(const char *dir_name,
                           std::vector<std::string> *result) {
  ls_rpc_in_t in;
  in.name = dir_name;
#ifdef NAS_DEBUG
  printf("send ls req, dir name: %s\n", dir_name);
#endif
  FutureResponse fr;
  struct ret_with_name_list ret;
  ret.name_list = result;
  ret.ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result1) {
    ls_rpc_out_t *ls_out = (ls_rpc_out_t *)out;
    ret_with_name_list *res = (ret_with_name_list *)result1;
    name_list_t cur = ls_out->list;
    while (cur != nullptr) {
      res->name_list->push_back(cur->name);
      cur = cur->next;
    }
    res->ret = ls_out->ret;
  };
  ls_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret.ret;
}

int RPCEngine::SetLock(int fd, bool lock) {
  lock_rpc_in_t in;
  in.fd = fd;
  in.lock = lock;
#ifdef NAS_DEBUG
  printf("send lock req, fd: %d, lock: %d\n", fd, lock);
#endif
  FutureResponse fr;
  int ret = -1;
  fr.data = &ret;
  fr.func = [](void *out, void *result) {
    lock_rpc_out_t *rpc_out = (lock_rpc_out_t *)out;
    int *res = (int *)result;
    *res = rpc_out->ret;
  };
  lock_rpc(svr_addr, &in, &fr);
  fr.wait();
  return ret;
}

}  // namespace ROCKSDB_NAMESPACE
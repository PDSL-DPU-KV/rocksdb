#ifndef RPC_H
#define RPC_H

#include <cstdlib>

#include "mercury_proc.h"
#define HG_HAS_BOOST
#include <mercury_macros.h>
#include <mercury_proc_string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cassert>
#include <cstdint>

#include "mercury_config.h"
#include "mercury_types.h"

/* file rpcs */
MERCURY_GEN_PROC(open_rpc_out_t, ((int32_t)(fd)))
MERCURY_GEN_PROC(open_rpc_in_t, ((hg_const_string_t)(fname))((int32_t)(flags))(
                                    (uint32_t)(mode)))
MERCURY_GEN_PROC(fopen_rpc_out_t, ((hg_bool_t)(ret)))
MERCURY_GEN_PROC(fopen_rpc_in_t, ((int32_t)(fd))((hg_const_string_t)(mode)))
MERCURY_GEN_PROC(close_rpc_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(close_rpc_in_t, ((int32_t)(fd)))
MERCURY_GEN_PROC(fseek_rpc_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(fseek_rpc_in_t, ((int32_t)(fd))((uint64_t)(n)))
MERCURY_GEN_PROC(read_rpc_out_t, ((int64_t)(size))((hg_string_t)(buffer)))
MERCURY_GEN_PROC(read_rpc_in_t,
                 ((int32_t)(fd))((uint64_t)(n))((int64_t)(offset)))
MERCURY_GEN_PROC(write_rpc_out_t, ((hg_bool_t)(ret)))
MERCURY_GEN_PROC(
    write_rpc_in_t,
    ((int32_t)(fd))((uint64_t)(n))((int64_t)(offset))((hg_bulk_t)(bulk_handle)))
MERCURY_GEN_PROC(fstat_rpc_out_t,
                 ((int32_t)(st_dev))((int32_t)(st_ino))((int32_t)(st_size))(
                     (int32_t)(st_blksize))((int32_t)(st_blocks))(
                     (int32_t)(st_mode))((uint64_t)(st_mtime_))((int32_t)(ret)))
MERCURY_GEN_PROC(fstat_rpc_in_t, ((int32_t)(fd)))
MERCURY_GEN_PROC(ftruncate_rpc_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(ftruncate_rpc_in_t, ((int32_t)(fd))((uint64_t)(size)))
MERCURY_GEN_PROC(fallocate_rpc_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(fallocate_rpc_in_t, ((int32_t)(fd))((int32_t)(mode))(
                                         (uint64_t)(offset))((uint64_t)(len)))
MERCURY_GEN_PROC(fdatasync_rpc_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(fdatasync_rpc_in_t, ((int32_t)(fd)))
MERCURY_GEN_PROC(fsync_rpc_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(fsync_rpc_in_t, ((int32_t)(fd)))
MERCURY_GEN_PROC(rangesync_rpc_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(rangesync_rpc_in_t, ((int32_t)(fd))((uint64_t)(offset))(
                                         (uint64_t)(count))((int32_t)(flags)))
/*file system rpcs*/
MERCURY_GEN_PROC(rename_rpc_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(rename_rpc_in_t,
                 ((hg_const_string_t)(old_name))((hg_const_string_t)(new_name)))
MERCURY_GEN_PROC(access_rpc_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(access_rpc_in_t, ((hg_const_string_t)(name))((int32_t)(type)))
MERCURY_GEN_PROC(unlink_rpc_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(unlink_rpc_in_t, ((hg_const_string_t)(name)))
MERCURY_GEN_PROC(mkdir_rpc_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(mkdir_rpc_in_t, ((hg_const_string_t)(name))((uint32_t)(mode)))
MERCURY_GEN_PROC(rmdir_rpc_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(rmdir_rpc_in_t, ((hg_const_string_t)(name)))
MERCURY_GEN_PROC(stat_rpc_out_t,
                 ((int32_t)(st_dev))((int32_t)(st_ino))((int32_t)(st_size))(
                     (int32_t)(st_blksize))((int32_t)(st_blocks))(
                     (int32_t)(st_mode))((uint64_t)(st_mtime_))((int32_t)(ret)))
MERCURY_GEN_PROC(stat_rpc_in_t, ((hg_const_string_t)(name)))
MERCURY_GEN_PROC(ls_rpc_in_t, ((hg_const_string_t)(name)))

typedef struct name_list {
  hg_string_t name;
  struct name_list *next;
} *name_list_t;

typedef struct {
  int32_t ret;
  name_list_t list;
} ls_rpc_out_t;

static inline hg_return_t hg_proc_ls_rpc_out_t(hg_proc_t proc, void *data) {
  hg_return_t ret;
  ls_rpc_out_t *out = (ls_rpc_out_t *)data;

  hg_size_t length = 0;
  name_list_t tmp = NULL;
  name_list_t prev = NULL;

  switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      // write the ret
      ret = hg_proc_hg_int32_t(proc, &out->ret);
      if (ret != HG_SUCCESS) break;
      tmp = out->list;
      // find out the length of the list
      while (tmp != NULL) {
        tmp = tmp->next;
        length += 1;
      }
      // write the length
      ret = hg_proc_hg_size_t(proc, &length);
      if (ret != HG_SUCCESS) break;
      // write the list
      tmp = out->list;
      while (tmp != NULL) {
        ret = hg_proc_hg_string_t(proc, &tmp->name);
        if (ret != HG_SUCCESS) break;
        tmp = tmp->next;
      }
      break;

    case HG_DECODE:
      // find out the ret
      ret = hg_proc_hg_int32_t(proc, &out->ret);
      if (ret != HG_SUCCESS) break;
      // find out the length of the list
      ret = hg_proc_hg_size_t(proc, &length);
      if (ret != HG_SUCCESS) break;
      // loop and create list elements
      out->list = NULL;
      while (length > 0) {
        tmp = (name_list_t)calloc(1, sizeof(*tmp));
        if (out->list == NULL) {
          out->list = tmp;
        }
        if (prev != NULL) {
          prev->next = tmp;
        }
        ret = hg_proc_hg_string_t(proc, &tmp->name);
        if (ret != HG_SUCCESS) break;
        prev = tmp;
        length -= 1;
      }
      break;

    case HG_FREE:
      tmp = out->list;
      while (tmp != NULL) {
        prev = tmp;
        tmp = prev->next;
        free(prev);
      }
      ret = HG_SUCCESS;
  }
  return ret;
}

struct read_ret {
  char *buffer;
  size_t size;
};

struct stat_ret {
  int ret;
  struct stat *stat_buf;
};

hg_id_t open_rpc_register(void);
hg_id_t fopen_rpc_register(void);
hg_id_t close_rpc_register(void);
hg_id_t fseek_rpc_register(void);
hg_id_t read_rpc_register(void);
hg_id_t write_rpc_register(void);
hg_id_t fstat_rpc_register(void);
hg_id_t ftruncate_rpc_register(void);
hg_id_t fallocate_rpc_register(void);
hg_id_t fdatasync_rpc_register(void);
hg_id_t fsync_rpc_register(void);
hg_id_t rangesync_rpc_register(void);
hg_id_t rename_rpc_register(void);
hg_id_t access_rpc_register(void);
hg_id_t unlink_rpc_register(void);
hg_id_t mkdir_rpc_register(void);
hg_id_t rmdir_rpc_register(void);
hg_id_t stat_rpc_register(void);
hg_id_t ls_rpc_register(void);

inline bool IsSectorAligned(const size_t off, size_t sector_size) {
  assert((sector_size & (sector_size - 1)) == 0);
  return (off & (sector_size - 1)) == 0;
}

#endif /* EXAMPLE_RPC_H */
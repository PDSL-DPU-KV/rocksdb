#include "rpc.h"

#include <aio.h>
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>

#include "mercury.h"
#include "mercury_bulk.h"
#include "mercury_core_types.h"
#include "mercury_types.h"
#include "rpc_engine.h"

static std::map<int32_t, FILE *> fmap;

static hg_return_t open_rpc_handler(hg_handle_t handle);
static hg_return_t fopen_rpc_handler(hg_handle_t handle);
static hg_return_t close_rpc_handler(hg_handle_t handle);
static hg_return_t fseek_rpc_handler(hg_handle_t handle);
static hg_return_t read_rpc_handler(hg_handle_t handle);
static hg_return_t read_rpc_handler_bulk_cb(const struct hg_cb_info *info);
static hg_return_t write_rpc_handler(hg_handle_t handle);
static hg_return_t write_rpc_handler_bulk_cb(const struct hg_cb_info *info);
static hg_return_t fstat_rpc_handler(hg_handle_t handle);
static hg_return_t ftruncate_rpc_handler(hg_handle_t handle);
static hg_return_t fallocate_rpc_handler(hg_handle_t handle);
static hg_return_t fdatasync_rpc_handler(hg_handle_t handle);
static hg_return_t fsync_rpc_handler(hg_handle_t handle);
static hg_return_t rangesync_rpc_handler(hg_handle_t handle);
static hg_return_t rename_rpc_handler(hg_handle_t handle);
static hg_return_t access_rpc_handler(hg_handle_t handle);
static hg_return_t unlink_rpc_handler(hg_handle_t handle);
static hg_return_t mkdir_rpc_handler(hg_handle_t handle);
static hg_return_t rmdir_rpc_handler(hg_handle_t handle);
static hg_return_t stat_rpc_handler(hg_handle_t handle);
static hg_return_t ls_rpc_handler(hg_handle_t handle);
static hg_return_t lock_rpc_handler(hg_handle_t handle);

/* register this particular rpc type with Mercury */
hg_id_t open_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;

  hg_class = hg_engine_get_class();

  tmp = MERCURY_REGISTER(hg_class, "open_rpc", open_rpc_in_t, open_rpc_out_t,
                         open_rpc_handler);

  return (tmp);
}

hg_id_t fopen_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;

  hg_class = hg_engine_get_class();

  tmp = MERCURY_REGISTER(hg_class, "fopen_rpc", fopen_rpc_in_t, fopen_rpc_out_t,
                         fopen_rpc_handler);

  return (tmp);
}

hg_id_t close_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;

  hg_class = hg_engine_get_class();

  tmp = MERCURY_REGISTER(hg_class, "close_rpc", close_rpc_in_t, close_rpc_out_t,
                         close_rpc_handler);

  return (tmp);
}

hg_id_t fseek_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;

  hg_class = hg_engine_get_class();

  tmp = MERCURY_REGISTER(hg_class, "fseek_rpc", fseek_rpc_in_t, fseek_rpc_out_t,
                         fseek_rpc_handler);

  return (tmp);
}

hg_id_t read_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;

  hg_class = hg_engine_get_class();

  tmp = MERCURY_REGISTER(hg_class, "read_rpc", read_rpc_in_t, read_rpc_out_t,
                         read_rpc_handler);

  return (tmp);
}

hg_id_t write_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "write_rpc", write_rpc_in_t, write_rpc_out_t,
                         write_rpc_handler);
  return (tmp);
}

hg_id_t fstat_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "fstat_rpc", fstat_rpc_in_t, fstat_rpc_out_t,
                         fstat_rpc_handler);
  return (tmp);
}

hg_id_t ftruncate_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "ftruncate_rpc", ftruncate_rpc_in_t,
                         ftruncate_rpc_out_t, ftruncate_rpc_handler);
  return (tmp);
}

hg_id_t fallocate_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "fallocate_rpc", fallocate_rpc_in_t,
                         fallocate_rpc_out_t, fallocate_rpc_handler);
  return (tmp);
}

hg_id_t fdatasync_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "fdatasync_rpc", fdatasync_rpc_in_t,
                         fdatasync_rpc_out_t, fdatasync_rpc_handler);
  return (tmp);
}

hg_id_t fsync_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "fsync_rpc", fsync_rpc_in_t, fsync_rpc_out_t,
                         fsync_rpc_handler);
  return (tmp);
}

hg_id_t rangesync_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "rangesync_rpc", rangesync_rpc_in_t,
                         rangesync_rpc_out_t, rangesync_rpc_handler);
  return (tmp);
}

hg_id_t rename_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "rename_rpc", rename_rpc_in_t,
                         rename_rpc_out_t, rename_rpc_handler);
  return (tmp);
}

hg_id_t access_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "access_rpc", access_rpc_in_t,
                         access_rpc_out_t, access_rpc_handler);
  return (tmp);
}

hg_id_t unlink_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "unlink_rpc", unlink_rpc_in_t,
                         unlink_rpc_out_t, unlink_rpc_handler);
  return (tmp);
}

hg_id_t mkdir_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "mkdir_rpc", mkdir_rpc_in_t, mkdir_rpc_out_t,
                         mkdir_rpc_handler);
  return (tmp);
}

hg_id_t rmdir_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "rmdir_rpc", rmdir_rpc_in_t, rmdir_rpc_out_t,
                         rmdir_rpc_handler);
  return (tmp);
}

hg_id_t stat_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "stat_rpc", stat_rpc_in_t, stat_rpc_out_t,
                         stat_rpc_handler);
  return (tmp);
}

hg_id_t ls_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "ls_rpc", ls_rpc_in_t, ls_rpc_out_t,
                         ls_rpc_handler);
  return (tmp);
}

hg_id_t lock_rpc_register(void) {
  hg_class_t *hg_class;
  hg_id_t tmp;
  hg_class = hg_engine_get_class();
  tmp = MERCURY_REGISTER(hg_class, "lock_rpc", lock_rpc_in_t, lock_rpc_out_t,
                         lock_rpc_handler);
  return (tmp);
}

/* callback/handler triggered upon receipt of rpc request */
static hg_return_t open_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  open_rpc_in_t in;
  open_rpc_out_t out;
  int fd;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got open RPC request with fname: %s, flags: %d, mode: %d\n", in.fname,
         in.flags, in.mode);
#endif
  /* open file */
  fd = open(in.fname, in.flags, in.mode);
  if (fd < 0) {
    printf("fail to open file! %s\n", strerror(errno));
  }
  out.fd = fd;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t fopen_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  fopen_rpc_in_t in;
  fopen_rpc_out_t out;
  FILE *file = nullptr;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got fopen RPC request with fd: %d, mode: %s\n", in.fd, in.mode);
#endif
  /* fopen file */
  if (!fmap[in.fd]) {
    do {
      file = fdopen(in.fd, in.mode);
    } while (file == nullptr && errno == EINTR);
    if (file == nullptr) {
      printf("fail to fopen file! %s\n", strerror(errno));
      out.ret = false;
      goto res;
    }
    out.ret = true;
    fmap[in.fd] = file;
  } else {
    printf("already fopen file! %s\n", strerror(errno));
    out.ret = false;
  }

res:
  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t close_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  close_rpc_in_t in;
  close_rpc_out_t out;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got close RPC request with fd: %d\n", in.fd);
#endif
  /* close file */
  res = close(in.fd);
  if (res < 0) {
    printf("fail to close file! %s\n", strerror(errno));
  } else {
    fmap.erase(in.fd);
  }
  out.ret = res;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t fseek_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  fseek_rpc_in_t in;
  fseek_rpc_out_t out;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got fseek RPC request with fd: %d, n: %lu\n", in.fd, in.n);
#endif
  /* fseek file */
  if (!fmap[in.fd]) {
    printf("fail to fseek file %d! not open!\n", in.fd);
    out.ret = -1;
  } else {
    FILE *file = fmap[in.fd];
    res = fseek(file, in.n, SEEK_CUR);
    if (res < 0) {
      printf("fail to fseek file to %ld! %s\n", in.n, strerror(errno));
      out.ret = -1;
    } else {
      out.ret = res;
    }
  }

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

struct read_state {
  read_rpc_out_t out;
  char *local_buffer;
  hg_bulk_t local_bulk_handle;
  hg_handle_t handle;
};

static hg_return_t read_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  read_rpc_in_t in;
  read_rpc_out_t out;
  const struct hg_info *hgi;
  hg_bulk_t local_buffer_handle;
  read_state *state = (read_state *)malloc(sizeof(read_state));

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got read RPC request with fd: %d, n: %lu, offset: %lu\n", in.fd, in.n,
         in.offset);
#endif
  if (in.offset < 0) {
    state->local_buffer = (char *)calloc(1, in.n);
  } else {
    int res = posix_memalign((void **)&state->local_buffer, 512, in.n);
    assert(res == 0);
  }

  char *ptr = state->local_buffer;

  /* open file */
  if (in.offset < 0) {
    // fread
    if (!fmap[in.fd]) {
      printf("while fread file %d! not open!\n", in.fd);
      out.size = -1;
    } else {
      FILE *file = fmap[in.fd];
      size_t r = 0;
      do {
        clearerr(file);
        r = fread_unlocked(ptr, 1, in.n, file);
      } while (r == 0 && ferror(file) && errno == EINTR);
      out.size = r;
      if (r < in.n) {
        if (feof(file)) {
          // We leave status as ok if we hit the end of the file
          // We also clear the error so that the reads can continue
          // if a new data is written to the file
          clearerr(file);
        } else {
          // A partial read with an error: return a non-ok status
          // An error: return a non-ok status
          printf("while fread file! fd:%d, err:%s\n", in.fd, strerror(errno));
          out.size = -1;
        }
      }
    }
  } else {
    // pread
    ssize_t r = -1;
    size_t left = in.n;
    size_t offset = in.offset;
    while (left > 0) {
      r = pread(in.fd, ptr, left, static_cast<off_t>(offset));
      if (r <= 0) {
        if (r == -1 && errno == EINTR) {
          continue;
        }
        break;
      }
      ptr += r;
      offset += r;
      left -= r;
      if (!IsSectorAligned(r, 4096)) {
        // Bytes reads don't fill sectors. Should only happen at the end
        // of the file.
        break;
      }
    }
    if (r < 0) {
      // An error: return a non-ok status
      printf("while pread file! fd:%d, err:%s\n", in.fd, strerror(errno));
      out.size = -1;
    } else {
      out.size = in.n - left;
    }
  }
#ifdef NAS_DEBUG
  printf("read size: %ld\n", out.size);
#endif
  hgi = HG_Get_info(handle);
  assert(hgi);

  ret = HG_Bulk_create(hgi->hg_class, 1, (void **)&state->local_buffer, &in.n,
                       HG_BULK_READ_ONLY, &local_buffer_handle);
  assert(ret == HG_SUCCESS);
  state->local_bulk_handle = local_buffer_handle;
  state->handle = handle;
  state->out = out;

  ret = HG_Bulk_transfer(hgi->context, read_rpc_handler_bulk_cb, state,
                         HG_BULK_PUSH, hgi->addr, in.bulk_handle, 0,
                         local_buffer_handle, 0, in.n, HG_OP_ID_IGNORE);
  assert(ret == HG_SUCCESS);
  return ret;
}

static hg_return_t read_rpc_handler_bulk_cb(const struct hg_cb_info *info) {
  hg_return_t ret;
  read_state *state = (read_state *)info->arg;

  ret = HG_Respond(state->handle, NULL, NULL, &state->out);
  assert(ret == HG_SUCCESS);

  HG_Bulk_free(state->local_bulk_handle);
  free(state->local_buffer);
  free(state);

  return ret;
}

struct write_state {
  write_rpc_in_t in;
  char *src;
  hg_bulk_t local_bulk_handle;
  hg_handle_t handle;
};

static hg_return_t write_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  write_rpc_in_t in;
  const struct hg_info *hgi;
  write_state *state = (write_state *)malloc(sizeof(write_state));

  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got write RPC request with fd: %d, n: %lu, offset: %lu\n", in.fd,
         in.n, in.offset);
#endif
  state->handle = handle;
  state->in = in;
  if (state->in.offset < 0) {
    state->src = (char *)calloc(1, in.n);
  } else {
    int res = posix_memalign((void **)&state->src, 512, state->in.n);
    assert(res == 0);
  }
  /* register local target buffer for bulk access */
  hgi = HG_Get_info(handle);
  assert(hgi);
  ret = HG_Bulk_create(hgi->hg_class, 1, (void **)&state->src, &in.n,
                       HG_BULK_WRITE_ONLY, &state->local_bulk_handle);
  assert(ret == 0);

  /* initiate bulk transfer from client to server */
  ret = HG_Bulk_transfer(hgi->context, write_rpc_handler_bulk_cb, state,
                         HG_BULK_PULL, hgi->addr, in.bulk_handle, 0,
                         state->local_bulk_handle, 0, in.n, HG_OP_ID_IGNORE);
  assert(ret == 0);
  (void)ret;
  return ret;
}

static hg_return_t write_rpc_handler_bulk_cb(const struct hg_cb_info *info) {
  hg_return_t ret;
  write_rpc_out_t out;
  const size_t kLimit1Gb = 1UL << 30;
  write_state *state = (write_state *)info->arg;

  const char *src = state->src;
  size_t left = state->in.n;
  ssize_t done = 0;
  while (left != 0) {
    size_t bytes_to_write = std::min(left, kLimit1Gb);
    if (state->in.offset >= 0) {
#ifdef NAS_DEBUG
      printf("pwrite! bytes_to_write: %ld, offset: %ld\n", bytes_to_write,
             state->in.offset);
#endif
      done = pwrite(state->in.fd, src, bytes_to_write,
                    static_cast<off_t>(state->in.offset));
    } else {
#ifdef NAS_DEBUG
      printf("write! bytes_to_write: %ld\n", bytes_to_write);
#endif
      done = write(state->in.fd, src, bytes_to_write);
    }
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      printf("write error: %s\n", strerror(errno));
      out.ret = false;
      goto res;
    }
    left -= done;
    src += done;
    if (state->in.offset >= 0) {
      state->in.offset += done;
    }
  }
  out.ret = true;

res:
  ret = HG_Respond(state->handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);

  HG_Bulk_free(state->local_bulk_handle);
  free(state->src);
  free(state);

  return ret;
}

static hg_return_t fstat_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  fstat_rpc_in_t in;
  fstat_rpc_out_t out;
  struct stat stat_buf;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got fstat RPC request with fd: %d\n", in.fd);
#endif
  /* fstat file */
  res = fstat(in.fd, &stat_buf);
  if (res < 0) {
    printf("fail to stat file! %s\n", strerror(errno));
    out.ret = -1;
  }
  out.st_ino = stat_buf.st_ino;
  out.st_blksize = stat_buf.st_blksize;
  out.st_blocks = stat_buf.st_blocks;
  out.st_size = stat_buf.st_size;
  out.st_dev = stat_buf.st_dev;
  out.st_mode = stat_buf.st_mode;
  out.st_mtime_ = stat_buf.st_mtime;
#ifdef NAS_DEBUG
  printf("stat buf, file size: %ld, block size: %ld\n", stat_buf.st_size,
         stat_buf.st_blksize);
#endif
  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t ftruncate_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  ftruncate_rpc_in_t in;
  ftruncate_rpc_out_t out;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got ftruncate RPC request with fd: %d, size: %ld\n", in.fd, in.size);
#endif
  /* fstat file */
  res = ftruncate(in.fd, in.size);
  if (res < 0) {
    printf("fail to ftruncate file! %s\n", strerror(errno));
  }
  out.ret = res;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t fallocate_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  fallocate_rpc_in_t in;
  fallocate_rpc_out_t out;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf(
      "Got fallocate RPC request with fd: %d, mode: %d, offset: %lu, len: "
      "%lu\n",
      in.fd, in.mode, in.offset, in.len);
#endif
  /* fallocate file */
  res = fallocate(in.fd, in.mode, in.offset, in.len);
  if (res < 0) {
    printf("fail to fallocate file! %s\n", strerror(errno));
  }
  out.ret = res;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t fdatasync_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  fdatasync_rpc_in_t in;
  fdatasync_rpc_out_t out;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got fdatasync RPC request with fd: %d\n", in.fd);
#endif
  /* fallocate file */
  res = fdatasync(in.fd);
  if (res < 0) {
    printf("fail to fdatasync file! %s\n", strerror(errno));
  }
  out.ret = res;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t fsync_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  fsync_rpc_in_t in;
  fsync_rpc_out_t out;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got fsync RPC request with fd: %d\n", in.fd);
#endif
  /* fallocate file */
  res = fsync(in.fd);
  if (res < 0) {
    printf("fail to fsync file! %s\n", strerror(errno));
  }
  out.ret = res;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t rangesync_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  rangesync_rpc_in_t in;
  rangesync_rpc_out_t out;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf(
      "Got rangesync RPC request with fd: %d, offset: %lu, count: %lu, "
      "flags: %d\n",
      in.fd, in.offset, in.count, in.flags);
#endif
  /* fallocate file */
  res = sync_file_range(in.fd, in.offset, in.count, in.flags);
  if (res < 0) {
    printf("fail to rangesync file! %s\n", strerror(errno));
  }
  out.ret = res;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t rename_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  rename_rpc_in_t in;
  rename_rpc_out_t out;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got rename RPC request with old_name: %s, new_name: %s\n",
         in.old_name, in.new_name);
#endif
  /* fallocate file */
  res = rename(in.old_name, in.new_name);
  if (res < 0) {
    printf("fail to rename file! %s\n", strerror(errno));
  }
  out.ret = res;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t access_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  access_rpc_in_t in;
  access_rpc_out_t out;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got access RPC request with name: %s, type: %d\n", in.name, in.type);
#endif
  /* fallocate file */
  res = access(in.name, in.type);
  if (res < 0) {
    printf("fail to access file! %s\n", strerror(errno));
  }
  out.ret = res;
  out.errn = errno;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t unlink_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  unlink_rpc_in_t in;
  unlink_rpc_out_t out;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got unlink RPC request with name: %s\n", in.name);
#endif
  /* fallocate file */
  res = unlink(in.name);
  if (res < 0) {
    printf("fail to unlink file! %s\n", strerror(errno));
  }
  out.ret = res;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t mkdir_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  mkdir_rpc_in_t in;
  mkdir_rpc_out_t out;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got mkdir RPC request with name: %s, mode: %d\n", in.name, in.mode);
#endif
  /* fallocate file */
  res = mkdir(in.name, in.mode);
  if (res < 0) {
    printf("While mkdir! dir: %s, error: %s\n", in.name, strerror(errno));
  }
  out.ret = res;
  out.errn = errno;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t rmdir_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  rmdir_rpc_in_t in;
  rmdir_rpc_out_t out;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got rmdir RPC request with name: %s\n", in.name);
#endif
  /* fallocate file */
  res = rmdir(in.name);
  if (res < 0) {
    printf("fail to rmdir file! %s\n", strerror(errno));
  }
  out.ret = res;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t stat_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  stat_rpc_in_t in;
  stat_rpc_out_t out;
  struct stat buf;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got stat RPC request with name: %s\n", in.name);
#endif
  /* fallocate file */
  res = stat(in.name, &buf);
  if (res < 0) {
    printf("fail to stat file! %s\n", strerror(errno));
  }
  out.ret = res;
  out.st_size = buf.st_size;
  out.st_blksize = buf.st_blksize;
  out.st_blocks = buf.st_blocks;
  out.st_dev = buf.st_dev;
  out.st_ino = buf.st_ino;
  out.st_mode = buf.st_mode;
  out.st_mtime_ = buf.st_mtime;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t ls_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  ls_rpc_in_t in;
  ls_rpc_out_t out;
  out.list = nullptr;
  out.ret = 0;
  name_list_t cur, neww;
  struct dirent *entry;
  int pre_close_errno;
  int close_result;
  bool first = true;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got ls RPC request with name: %s\n", in.name);
#endif
  DIR *d = opendir(in.name);
  if (d == nullptr) {
    switch (errno) {
      case EACCES:
      case ENOENT:
      case ENOTDIR:
        printf("fail to open dir! dir: %s, error: %s\n", in.name,
               strerror(errno));
        break;
      default:
        printf("While opendir %s, error: %s", in.name, strerror(errno));
    }
    out.ret = -1;
    out.list = nullptr;
    goto res;
  }

  // reset errno before calling readdir()
  errno = 0;
  while ((entry = readdir(d)) != nullptr) {
    // filter out '.' and '..' directory entries
    // which appear only on some platforms
    const bool ignore =
        entry->d_type == DT_DIR &&
        (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0);
    if (!ignore) {
      neww = (name_list *)calloc(1, sizeof(name_list));
      neww->name = entry->d_name;
      neww->next = nullptr;
      if (first) {
        out.list = neww;
        cur = neww;
        first = false;
      } else {
        cur->next = neww;
        cur = cur->next;
      }
    }
    errno = 0;  // reset errno if readdir() success
  }

  // always attempt to close the dir
  pre_close_errno = errno;  // errno may be modified by closedir
  close_result = closedir(d);

  if (pre_close_errno != 0) {
    // error occurred during readdir
    printf("While readdir %s, error: %s", in.name, strerror(pre_close_errno));
    out.ret = -1;
    out.list = nullptr;
    goto res;
  }

  if (close_result != 0) {
    // error occurred during closedir
    printf("While closedir %s, error: %s", in.name, strerror(errno));
    out.ret = -1;
    out.list = nullptr;
    goto res;
  }

res:
  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  return ret;
}

static hg_return_t lock_rpc_handler(hg_handle_t handle) {
  hg_return_t ret;
  lock_rpc_in_t in;
  lock_rpc_out_t out;
  int res;

  /* decode input */
  ret = HG_Get_input(handle, &in);
  assert(ret == HG_SUCCESS);
#ifdef NAS_DEBUG
  printf("Got lock RPC request with fd: %d\n", in.fd);
#endif
  /* fallocate file */
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (in.lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;  // Lock/unlock entire file
  res = fcntl(in.fd, F_SETLK, &f);
  if (res < 0) {
    printf("fail to lock file! %s\n", strerror(errno));
  }
  out.ret = res;

  ret = HG_Respond(handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;

  return ret;
}
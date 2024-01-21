

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <variant>

#include "op.hh"
#include "util/mercury_wrapper.hh"

static std::map<int32_t, FILE *> fmap;

inline bool IsSectorAligned(const size_t off, size_t sector_size) {
  assert((sector_size & (sector_size - 1)) == 0);
  return (off & (sector_size - 1)) == 0;
}

int open_rpc_handler(open_args &args) {
#ifdef NAS_DEBUG
  printf("Got open RPC request with fname: %s, flags: %d, mode: %d\n",
         args.name.c_str(), args.flags, args.mode);
#endif
  int fd = open(args.name.c_str(), args.flags, args.mode);
  if (fd < 0) {
    printf("fail to open file! %s\n", strerror(errno));
    return -1;
  }
  return fd;
}

int close_rpc_handler(int fd) {
#ifdef NAS_DEBUG
  printf("Got close RPC request with fd: %d\n", fd);
#endif
  int ret = close(fd);
  if (ret < 0) {
    printf("fail to close file! %s\n", strerror(errno));
  } else {
    fmap.erase(fd);
  }
  return ret;
}

std::string read_rpc_handler(read_args &args) {
  ssize_t ret;
  std::string res;
  char *local_buffer;
#ifdef NAS_DEBUG
  printf(
      "Got read RPC request with fd: %d, n: %lu, offset: %lu, "
      "use_direct_io: %d\n",
      args.fd, args.n, args.offset, args.use_direct_io);
#endif
  if (args.use_direct_io) {
    int re = posix_memalign((void **)&local_buffer, 512, args.n);
    assert(re == 0);
  } else {
    local_buffer = (char *)calloc(1, args.n);
  }

  char *ptr = local_buffer;
  /* open file */
  if (args.offset < 0) {
    // printf("fread!\n");
    if (!fmap[args.fd]) {
      printf("while fread file %d! not open!\n", args.fd);
      ret = -1;
    } else {
      FILE *file = fmap[args.fd];
      size_t r = 0;
      do {
        clearerr(file);
        r = fread_unlocked(ptr, 1, args.n, file);
      } while (r == 0 && ferror(file) && errno == EINTR);
      ret = r;
      if (r < args.n) {
        if (feof(file)) {
          // We leave status as ok if we hit the end of the file
          // We also clear the error so that the reads can continue
          // if a new data is written to the file
          clearerr(file);
        } else {
          // A partial read with an error: return a non-ok status
          // An error: return a non-ok status
          printf("while fread file! fd:%d, err:%s\n", args.fd, strerror(errno));
          ret = -1;
        }
      }
    }
  } else {
    // printf("pread!\n");
    ssize_t r = -1;
    size_t left = args.n;
    ssize_t offset = args.offset;
    while (left > 0) {
      r = pread(args.fd, ptr, left, static_cast<off_t>(offset));
      if (r <= 0) {
        if (r == -1 && errno == EINTR) {
          continue;
        }
        break;
      }
      ptr += r;
      offset += r;
      left -= r;
      if (!IsSectorAligned(r, 512)) {
        // Bytes reads don't fill sectors. Should only happen at the end
        // of the file.
        break;
      }
    }
    if (r < 0) {
      // An error: return a non-ok status
      printf("while pread file! fd:%d, err:%s\n", args.fd, strerror(errno));
      ret = -1;
    } else {
      ret = args.n - left;
    }
  }

  if (ret < 0) {
    res = "failed";
  } else {
    res = std::string(local_buffer, ret);
  }
#ifdef NAS_DEBUG
  for (uint i = 0; i < res.size(); i++) {
    printf("%x\n", res[i]);
  }
#endif
  free(local_buffer);
  return res;
}

bool write_rpc_handler(write_args &args) {
  const size_t kLimit1Gb = 1UL << 30;
  char *local_buffer = nullptr;
  const char *src;
#ifdef NAS_DEBUG
  printf(
      "Got write RPC request with fd: %d, n: %lu, offset: %lu, "
      "use_direct_io: %d\n",
      args.fd, args.n, args.offset, args.use_direct_io);
#endif
  if (args.use_direct_io) {
    int res = posix_memalign((void **)&local_buffer, 512, args.buffer.size());
    assert(res == 0);
    memcpy(local_buffer, args.buffer.c_str(), args.buffer.size());
    src = local_buffer;
    // printf("use direct io!\n");
  } else {
    src = args.buffer.c_str();
  }
#ifdef NAS_DEBUG
  for (uint i = 0; i < args.buffer.size(); i++) {
    printf("%x\n", args.buffer[i]);
  }
#endif
  size_t left = args.n;
  ssize_t done = 0;
  while (left != 0) {
    size_t bytes_to_write = std::min(left, kLimit1Gb);
    if (args.offset >= 0) {
      // printf("pwrite! src len: %ld\n", bytes_to_write);
      done = pwrite(args.fd, src, bytes_to_write, args.offset);
    } else {
      // printf("write! src len: %ld\n", bytes_to_write);
      done = write(args.fd, src, bytes_to_write);
    }
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      printf("write error! %s\n", strerror(errno));
      return false;
    }
    left -= done;
    src += done;
    if (args.offset >= 0) {
      args.offset += done;
    }
  }

  if (args.use_direct_io) {
    free(local_buffer);
  }

  return true;
}

bool fopen_rpc_handler(fopen_args &args) {
  FILE *file = nullptr;
#ifdef NAS_DEBUG
  printf("Got fopen RPC request with fd: %d, mode: %s\n", args.fd,
         args.mode.c_str());
#endif
  /* fopen file */
  if (!fmap[args.fd]) {
    do {
      file = fdopen(args.fd, args.mode.c_str());
    } while (file == nullptr && errno == EINTR);
    if (file == nullptr) {
      printf("fail to fopen file! %s\n", strerror(errno));
      return false;
    }
    fmap[args.fd] = file;
    return true;
  } else {
    printf("already fopen file! %s\n", strerror(errno));
    return false;
  }
}

int fseek_rpc_handler(fseek_args &args) {
  int res;
#ifdef NAS_DEBUG
  printf("Got fseek RPC request with fd: %d, n: %lu\n", args.fd, args.n);
#endif
  /* fseek file */
  if (!fmap[args.fd]) {
    printf("fail to fseek file %d! not open!\n", args.fd);
    return -1;
  } else {
    FILE *file = fmap[args.fd];
    res = fseek(file, args.n, SEEK_CUR);
    if (res < 0) {
      printf("fail to fseek file to %ld! %s\n", args.n, strerror(errno));
    }
    return res;
  }
}

stat_ret fstat_rpc_handler(int fd) {
  int res;
  struct stat stat_buf;
  struct stat_ret stat_ret;
#ifdef NAS_DEBUG
  printf("Got fstat RPC request with fd: %d\n", fd);
#endif
  /* fstat file */
  res = fstat(fd, &stat_buf);
  if (res < 0) {
    printf("fail to stat file! %s\n", strerror(errno));
    stat_ret.ret = -1;
  } else {
    stat_ret.ret = 0;
    stat_ret.st_blksize = stat_buf.st_blksize;
    stat_ret.st_blocks = stat_buf.st_blocks;
    stat_ret.st_size = stat_buf.st_size;
    stat_ret.st_dev = stat_buf.st_dev;
    stat_ret.st_ino = stat_buf.st_ino;
    stat_ret.st_mode = stat_buf.st_mode;
    stat_ret.st_mtime_ = stat_buf.st_mtime;
  }
  return stat_ret;
}

int ftruncate_rpc_handler(ftruncate_args &args) {
  int res;
#ifdef NAS_DEBUG
  printf("Got ftruncate RPC request with fd: %d, size: %ld\n", args.fd,
         args.size);
#endif
  /* fstat file */
  res = ftruncate(args.fd, args.size);
  if (res < 0) {
    printf("fail to ftruncate file! %s\n", strerror(errno));
  }
  return res;
}

int fallocate_rpc_handler(fallocate_args &args) {
  int res;
#ifdef NAS_DEBUG
  printf(
      "Got fallocate RPC request with fd: %d, mode: %d, offset: %lu, len: "
      "%lu\n",
      args.fd, args.mode, args.offset, args.len);
#endif
  /* fallocate file */
  res = fallocate(args.fd, args.mode, args.offset, args.len);
  if (res < 0) {
    printf("fail to fallocate file! %s\n", strerror(errno));
  }
  return res;
}

int fdatasync_rpc_handler(int fd) {
  int res;
#ifdef NAS_DEBUG
  printf("Got fdatasync RPC request with fd: %d\n", fd);
#endif
  /* fdatasync file */
  res = fdatasync(fd);
  if (res < 0) {
    printf("fail to fdatasync file! %s\n", strerror(errno));
  }
  return res;
}

int fsync_rpc_handler(int fd) {
  int res;
#ifdef NAS_DEBUG
  printf("Got fsync RPC request with fd: %d\n", fd);
#endif
  /* fdatasync file */
  res = fsync(fd);
  if (res < 0) {
    printf("fail to fsync file! %s\n", strerror(errno));
  }
  return res;
}

int rangesync_rpc_handler(rangesync_args &args) {
  int res;
#ifdef NAS_DEBUG
  printf(
      "Got rangesync RPC request with fd: %d, offset: %lu, count: %lu, "
      "flags: %d\n",
      args.fd, args.offset, args.count, args.flags);
#endif
  /* rangesync file */
  res = sync_file_range(args.fd, args.offset, args.count, args.flags);
  if (res < 0) {
    printf("fail to rangesync file! %s\n", strerror(errno));
  }
  return res;
}

int rename_rpc_handler(rename_args &args) {
  int res;
#ifdef NAS_DEBUG
  printf("Got rename RPC request with old_name: %s, new_name: %s\n",
         args.old_name.c_str(), args.new_name.c_str());
#endif
  /* rename file */
  res = rename(args.old_name.c_str(), args.new_name.c_str());
  if (res < 0) {
    printf("fail to rename file! %s\n", strerror(errno));
  }
  return res;
}

ret_with_errno access_rpc_handler(access_args &args) {
  int res;
  ret_with_errno ret;
#ifdef NAS_DEBUG
  printf("Got access RPC request with name: %s, type: %d\n", args.name.c_str(),
         args.type);
#endif
  /* fallocate file */
  res = access(args.name.c_str(), args.type);
  if (res < 0) {
    printf("fail to access file! %s\n", strerror(errno));
  }
  ret.ret = res;
  ret.errn = errno;
  return ret;
}

int unlink_rpc_handler(std::string name) {
  int res;
#ifdef NAS_DEBUG
  printf("Got unlink RPC request with name: %s\n", name.c_str());
#endif
  /* fallocate file */
  res = unlink(name.c_str());
  if (res < 0) {
    printf("fail to unlink file! %s\n", strerror(errno));
  }
  return res;
}

int rmdir_rpc_handler(std::string name) {
  int res;
#ifdef NAS_DEBUG
  printf("Got rmdir RPC request with name: %s\n", name.c_str());
#endif
  /* fallocate file */
  res = rmdir(name.c_str());
  if (res < 0) {
    printf("fail to rmdir file! %s\n", strerror(errno));
  }
  return res;
}

ret_with_errno mkdir_rpc_handler(mkdir_args &args) {
  int res;
  ret_with_errno ret;
#ifdef NAS_DEBUG
  printf("Got mkdir RPC request with name: %s, mode: %d\n", args.name.c_str(),
         args.mode);
#endif
  /* fallocate file */
  res = mkdir(args.name.c_str(), args.mode);
  if (res < 0) {
    printf("fail to mkdir file! %s\n", strerror(errno));
  }
  ret.ret = res;
  ret.errn = errno;
  return ret;
}

stat_ret stat_rpc_handler(std::string name) {
  int res;
  struct stat stat_buf;
  struct stat_ret stat_ret;
#ifdef NAS_DEBUG
  printf("Got stat RPC request with name: %s\n", name.c_str());
#endif
  /* fallocate file */
  res = stat(name.c_str(), &stat_buf);
  if (res < 0) {
    printf("fail to stat file! %s\n", strerror(errno));
    stat_ret.ret = -1;
  } else {
    stat_ret.ret = 0;
    stat_ret.st_blksize = stat_buf.st_blksize;
    stat_ret.st_blocks = stat_buf.st_blocks;
    stat_ret.st_size = stat_buf.st_size;
    stat_ret.st_dev = stat_buf.st_dev;
    stat_ret.st_ino = stat_buf.st_ino;
    stat_ret.st_mode = stat_buf.st_mode;
    stat_ret.st_mtime_ = stat_buf.st_mtime;
  }
  return stat_ret;
}

ls_ret ls_rpc_handler(std::string name) {
  struct ls_ret ret;
  struct dirent *entry;
  int pre_close_errno;
  int close_result;
#ifdef NAS_DEBUG
  printf("Got ls RPC request with name: %s\n", name.c_str());
#endif
  DIR *d = opendir(name.c_str());
  if (d == nullptr) {
    switch (errno) {
      case EACCES:
      case ENOENT:
      case ENOTDIR:
        printf("fail to open dir! dir: %s, error: %s\n", name.c_str(),
               strerror(errno));
        break;
      default:
        printf("While opendir %s, error: %s", name.c_str(), strerror(errno));
    }
    ret.ret = -1;
    return ret;
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
      ret.result.push_back(entry->d_name);
    }
    errno = 0;  // reset errno if readdir() success
  }

  // always attempt to close the dir
  pre_close_errno = errno;  // errno may be modified by closedir
  close_result = closedir(d);

  if (pre_close_errno != 0) {
    // error occurred during readdir
    printf("While readdir %s, error: %s", name.c_str(),
           strerror(pre_close_errno));
    ret.ret = -1;
    return ret;
  }

  if (close_result != 0) {
    // error occurred during closedir
    printf("While closedir %s, error: %s", name.c_str(), strerror(errno));
    ret.ret = -1;
    return ret;
  }

  ret.ret = 0;
  return ret;
}

int lock_rpc_handler(lock_args &args) {
  int res;
#ifdef NAS_DEBUG
  printf("Got lock RPC request with fd: %d\n", args.fd);
#endif
  /* lock file */
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (args.lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;  // Lock/unlock entire file
  res = fcntl(args.fd, F_SETLK, &f);
  if (res < 0) {
    printf("fail to lock file! %s\n", strerror(errno));
  }
  return res;
}

auto rpc_handler(RpcRequest &req) -> RpcResponse {
  RpcResponse resp;
  resp.type = req.type;
  if (req.type == OPEN) {
    auto args = std::get_if<open_args>(&req.args);
    assert(args != nullptr);
    resp.result = open_rpc_handler(*args);
  } else if (req.type == CLOSE) {
    auto args2 = std::get_if<int>(&req.args);
    assert(args2 != nullptr);
    resp.result = close_rpc_handler(*args2);
  } else if (req.type == READ) {
    auto args3 = std::get_if<read_args>(&req.args);
    assert(args3 != nullptr);
    resp.result = read_rpc_handler(*args3);
  } else if (req.type == WRITE) {
    auto args4 = std::get_if<write_args>(&req.args);
    assert(args4 != nullptr);
    resp.result = write_rpc_handler(*args4);
  } else if (req.type == FOPEN) {
    auto args5 = std::get_if<fopen_args>(&req.args);
    assert(args5 != nullptr);
    resp.result = fopen_rpc_handler(*args5);
  } else if (req.type == FSEEK) {
    auto args6 = std::get_if<fseek_args>(&req.args);
    assert(args6 != nullptr);
    resp.result = fseek_rpc_handler(*args6);
  } else if (req.type == FSTAT) {
    auto args7 = std::get_if<int>(&req.args);
    assert(args7 != nullptr);
    resp.result = fstat_rpc_handler(*args7);
  } else if (req.type == FTRUNCATE) {
    auto args8 = std::get_if<ftruncate_args>(&req.args);
    assert(args8 != nullptr);
    resp.result = ftruncate_rpc_handler(*args8);
  } else if (req.type == FALLOCATE) {
    auto args9 = std::get_if<fallocate_args>(&req.args);
    assert(args9 != nullptr);
    resp.result = fallocate_rpc_handler(*args9);
  } else if (req.type == FDATASYNC) {
    auto args10 = std::get_if<int>(&req.args);
    assert(args10 != nullptr);
    resp.result = fdatasync_rpc_handler(*args10);
  } else if (req.type == FSYNC) {
    auto args11 = std::get_if<int>(&req.args);
    assert(args11 != nullptr);
    resp.result = fsync_rpc_handler(*args11);
  } else if (req.type == RANGESYNC) {
    auto args12 = std::get_if<rangesync_args>(&req.args);
    assert(args12 != nullptr);
    resp.result = rangesync_rpc_handler(*args12);
  } else if (req.type == RENAME) {
    auto args13 = std::get_if<rename_args>(&req.args);
    assert(args13 != nullptr);
    resp.result = rename_rpc_handler(*args13);
  } else if (req.type == ACCESS) {
    auto args14 = std::get_if<access_args>(&req.args);
    assert(args14 != nullptr);
    resp.result = access_rpc_handler(*args14);
  } else if (req.type == UNLINK) {
    auto args15 = std::get_if<std::string>(&req.args);
    assert(args15 != nullptr);
    resp.result = unlink_rpc_handler(*args15);
  } else if (req.type == RMDIR) {
    auto args16 = std::get_if<std::string>(&req.args);
    assert(args16 != nullptr);
    resp.result = rmdir_rpc_handler(*args16);
  } else if (req.type == MKDIR) {
    auto args17 = std::get_if<mkdir_args>(&req.args);
    assert(args17 != nullptr);
    resp.result = mkdir_rpc_handler(*args17);
  } else if (req.type == STAT) {
    auto args18 = std::get_if<std::string>(&req.args);
    assert(args18 != nullptr);
    resp.result = stat_rpc_handler(*args18);
  } else if (req.type == LS) {
    auto args19 = std::get_if<std::string>(&req.args);
    assert(args19 != nullptr);
    resp.result = ls_rpc_handler(*args19);
  } else if (req.type == LOCK) {
    auto args20 = std::get_if<lock_args>(&req.args);
    assert(args20 != nullptr);
    resp.result = lock_rpc_handler(*args20);
  } else {
    printf("unkown op type! %d\n", req.type);
  }
  return resp;
}

int main(void) {
  MercuryEngine engine("ofi+verbs://192.168.200.10:12345", true);
  printf("server address, ofi+verbs://192.168.200.10:12345\n");
  engine.define("op", [&](const Handle &h) -> void {
    auto req = h.get_payload().as<RpcRequest>();
    auto resp = rpc_handler(req);
    h.respond(resp);
  });
  engine.progress();
  return (0);
}
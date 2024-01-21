
#include <sys/stat.h>
#include <sys/types.h>

#include <cereal/types/string.hpp>
#include <cereal/types/variant.hpp>
#include <cereal/types/vector.hpp>
#include <string>
#include <variant>
#include <vector>

struct open_args {
  std::string name;
  int flags;
  uint mode;
  template <typename A>
  void serialize(A &ar) {
    ar(name, flags, mode);
  }
};

struct read_args {
  int fd;
  uint64_t n;
  int64_t offset;
  bool use_direct_io;
  template <typename A>
  void serialize(A &ar) {
    ar(fd, n, offset, use_direct_io);
  }
};

struct write_args {
  int fd;
  uint64_t n;
  int64_t offset;
  bool use_direct_io;
  std::string buffer;
  template <typename A>
  void serialize(A &ar) {
    ar(fd, n, offset, use_direct_io, buffer);
  }
};

struct fopen_args {
  int fd;
  std::string mode;
  template <typename A>
  void serialize(A &ar) {
    ar(fd, mode);
  }
};

struct fseek_args {
  int fd;
  uint64_t n;
  template <typename A>
  void serialize(A &ar) {
    ar(fd, n);
  }
};

struct ftruncate_args {
  int fd;
  uint64_t size;
  template <typename A>
  void serialize(A &ar) {
    ar(fd, size);
  }
};

struct fallocate_args {
  int fd;
  int32_t mode;
  uint64_t offset;
  uint64_t len;
  template <typename A>
  void serialize(A &ar) {
    ar(fd, mode, offset, len);
  }
};

struct rangesync_args {
  int fd;
  uint64_t offset;
  uint64_t count;
  int32_t flags;
  template <typename A>
  void serialize(A &ar) {
    ar(fd, offset, count, flags);
  }
};

struct rename_args {
  std::string old_name;
  std::string new_name;
  template <typename A>
  void serialize(A &ar) {
    ar(old_name, new_name);
  }
};

struct access_args {
  std::string name;
  int32_t type;
  template <typename A>
  void serialize(A &ar) {
    ar(name, type);
  }
};

struct mkdir_args {
  std::string name;
  uint32_t mode;
  template <typename A>
  void serialize(A &ar) {
    ar(name, mode);
  }
};

struct lock_args {
  int fd;
  bool lock;
  template <typename A>
  void serialize(A &ar) {
    ar(fd, lock);
  }
};

struct ret_with_errno {
  int ret;
  int errn;
  template <typename A>
  void serialize(A &ar) {
    ar(ret, errn);
  }
};

struct stat_ret {
  int ret;
  int32_t st_ino;
  int32_t st_dev;
  int32_t st_size;
  int32_t st_blksize;
  int32_t st_blocks;
  int32_t st_mode;
  uint64_t st_mtime_;
  template <typename A>
  void serialize(A &ar) {
    ar(ret, st_ino, st_dev, st_size, st_blksize, st_blocks, st_mode, st_mtime_);
  }
};

struct ls_ret {
  int ret;
  std::vector<std::string> result;
  template <typename A>
  void serialize(A &ar) {
    ar(ret, result);
  }
};

enum OpType {
  OPEN,
  CLOSE,
  READ,
  WRITE,
  FOPEN,
  FSEEK,
  FSTAT,
  FTRUNCATE,
  FALLOCATE,
  FDATASYNC,
  FSYNC,
  RANGESYNC,
  RENAME,
  ACCESS,
  UNLINK,
  RMDIR,
  MKDIR,
  STAT,
  LS,
  LOCK,
};

using RpcArgs =
    std::variant<int, std::string, open_args, read_args, write_args, fopen_args,
                 fseek_args, ftruncate_args, fallocate_args, rangesync_args,
                 rename_args, access_args, mkdir_args, lock_args>;

using RpcResult =
    std::variant<int, bool, std::string, stat_ret, ls_ret, ret_with_errno>;

struct RpcRequest {
  OpType type;
  RpcArgs args;

  template <typename A>
  void serialize(A &ar) {
    ar(type, args);
  }
};

struct RpcResponse {
  OpType type;
  RpcResult result;
  template <typename A>
  void serialize(A &ar) {
    ar(type, result);
  }
};
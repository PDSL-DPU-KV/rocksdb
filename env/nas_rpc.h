#include <sys/stat.h>

#include <cstddef>
#include <vector>

#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

class RPCEngine {
 public:
  int Open(const std::string& fname, int flags, uint mode);  // Open

  int Close(int fd);  // close

  size_t Fread(int fd, size_t n, char* buffer);  // fread

  int Fseek(int fd, size_t n);  // fseek

  size_t Pread(int fd, uint64_t offset, size_t n, char* buffer);  // pread

  int Write(int fd, const char* buffer, size_t n);  // write

  bool PWrite(int fd, const char* buffer, size_t n, uint64_t offset);  // pwrite

  int Fstat(int fd, struct stat* stat_buf);  // fstat

  int Ftruncate(int fd, uint64_t size);  // ftruncate

  int Fallocate(int fd, int mode, uint64_t offset, uint64_t len);  // fallocate

  int Fdatasync(int fd);  // fdatasync

  int Fsync(int fd);  // fsync

  int RangeSync(int fd, uint64_t offset, uint64_t count,
                int flags);  // sync_file_range

  bool IsSyncFileRangeSupported(int fd);

  int Rename(const char* old_name, const char* new_name);  // rename

  int Access(const char* name, int type);  // access

  int Unlink(const char* name);  // unlink

  int Mkdir(const char* name, uint mode);  // mkdir

  int Rmdir(const char* name);  // rmdir

  int Stat(const char* name, struct stat* stat_buf);  // stat

  int GetChildren(const char* dir_name, std::vector<std::string>* result);
};
}  // namespace ROCKSDB_NAMESPACE
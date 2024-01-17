#include <mercury.h>
#include <mercury_types.h>
#include <sys/stat.h>

#include <cstddef>
#include <map>
#include <vector>

#include "env/rpc.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

class RPCEngine {
 public:
  RPCEngine(const std::string& svr_addr_string);
  ~RPCEngine();

 public:
  int Open(const char* fname, int flags, uint mode);                   // open
  bool Fopen(int fd, const char* mode);                                // fdopen
  int Close(int fd);                                                   // close
  int Fseek(int fd, uint64_t n);                                       // fseek
  ssize_t Fread(int fd, size_t n, char* buffer);                       // fread
  ssize_t Pread(int fd, uint64_t offset, size_t n, char* buffer);      // pread
  bool Write(int fd, const char* buffer, size_t n);                    // write
  bool PWrite(int fd, const char* buffer, size_t n, uint64_t offset);  // pwrite
  int Fstat(int fd, struct stat* stat_buf);                            // fstat
  int Ftruncate(int fd, uint64_t size);                            // ftruncate
  int Fallocate(int fd, int mode, uint64_t offset, uint64_t len);  // fallocate
  int Fdatasync(int fd);                                           // fdatasync
  int Fsync(int fd);                                               // fsync
  int RangeSync(int fd, uint64_t offset, uint64_t count,
                int flags);                                // sync_file_range
  int Rename(const char* old_name, const char* new_name);  // rename
  ret_with_errno Access(const char* name, int type);       // access
  int Unlink(const char* name);                            // unlink
  ret_with_errno Mkdir(const char* name, uint mode);       // mkdir
  int Rmdir(const char* name);                             // rmdir
  int Stat(const char* name, struct stat* stat_buf);       // stat
  int GetChildren(const char* dir_name,
                  std::vector<std::string>* result);  // ls
  int SetLock(int fd, bool lock);                     // fcntl(F_SETLK)

 private:
  hg_addr_t svr_addr;
};
}  // namespace ROCKSDB_NAMESPACE
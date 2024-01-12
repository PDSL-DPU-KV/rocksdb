#include "nas_rpc.h"

#include <cstdio>

namespace ROCKSDB_NAMESPACE {

int RPCEngine::Open(const std::string& fname, int flags, uint mode) {
  return 0;
}  // Open

int RPCEngine::Close(int fd) { return 0; }  // close

ssize_t RPCEngine::Fread(int fd, size_t n, char* buffer) { return 0; }  // fread

int RPCEngine::Fseek(int fd, size_t n) { return 0; }  // fseek

ssize_t RPCEngine::Pread(int fd, uint64_t offset, size_t n, char* buffer) {
  ssize_t s = 0;
  return s;
}

ssize_t RPCEngine::Write(int fd, const char* buffer, size_t n) {
  return 0;
}  // write

bool RPCEngine::PWrite(int fd, const char* buffer, size_t n, uint64_t offset) {
  return true;
}  // pwrite

int RPCEngine::Fstat(int fd, struct stat* stat_buf) { return 0; }  // fstat

int RPCEngine::Ftruncate(int fd, uint64_t size) { return 0; }  // ftruncate

int RPCEngine::Fallocate(int fd, int mode, uint64_t offset, uint64_t len) {
  return 0;
}  // fallocate

int RPCEngine::Fdatasync(int fd) { return 0; }  // fdatasync

int RPCEngine::Fsync(int fd) { return 0; }  // fsync

int RPCEngine::RangeSync(int fd, uint64_t offset, uint64_t count, int flags) {
  return 0;
}  // sync_file_range

bool RPCEngine::IsSyncFileRangeSupported(int fd) { return true; }

int RPCEngine::Rename(const char* old_name, const char* new_name) {
  return 0;
}  // rename

int RPCEngine::Access(const char* name, int type) { return 0; }  // access

int RPCEngine::Unlink(const char* name) { return 0; }  // unlink

int RPCEngine::Mkdir(const char* name, uint mode) { return 0; }  // mkdir

int RPCEngine::Rmdir(const char* name) { return 0; }  // rmdir

int RPCEngine::Stat(const char* name, struct stat* stat_buf) {
  return 0;
}  // stat

int RPCEngine::GetChildren(const char* dir_name,
                           std::vector<std::string>* result) {
  return 0;
}

}  // namespace ROCKSDB_NAMESPACE
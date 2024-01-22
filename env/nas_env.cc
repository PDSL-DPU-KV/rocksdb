
#include "env/nas_env.h"

#include <bits/types/FILE.h>
#include <fcntl.h>
#include <linux/falloc.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>

#include "env/emulated_clock.h"
#include "env/io_posix.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

// list of pathnames that are locked
// Only used for error message.
struct LockHoldingInfo {
  int64_t acquire_time;
  uint64_t acquiring_thread;
};
static std::map<std::string, LockHoldingInfo> locked_files;
static port::Mutex mutex_locked_files;

class NasFileLock : public FileLock {
 public:
  int fd_ = /*invalid*/ -1;
  std::string filename;

  void Clear() {
    fd_ = -1;
    filename.clear();
  }

  virtual ~NasFileLock() override {
    // Check for destruction without UnlockFile
    assert(fd_ == -1);
  }
};

inline mode_t GetDBFileMode(bool allow_non_owner_access) {
  return allow_non_owner_access ? 0644 : 0600;
}

class NasSequentailFile : public FSSequentialFile {
 public:
  NasSequentailFile(int fd, const std::string& fname,
                    std::shared_ptr<RPCEngine> rpc_engine)
      : fd_(fd), fname_(fname), rpc_engine_(rpc_engine){};
  ~NasSequentailFile() override { rpc_engine_->Close(fd_); };

 public:
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& /*opts*/,
                          Slice* result, char* scratch,
                          IODebugContext* /*dbg*/) override {
    assert(use_direct_io());
    assert(IsSectorAligned(offset, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(n, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(scratch, GetRequiredBufferAlignment()));
    ssize_t res = rpc_engine_->Pread(fd_, offset, n, scratch, use_direct_io());
    if (res < 0) {
      *result = Slice(scratch, 0);
      return IOStatus::IOError("pread failed!", fname_);
    }
    *result = Slice(scratch, res);
    return IOStatus::OK();
  };
  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override {
    assert(result != nullptr && !use_direct_io());
    ssize_t res = rpc_engine_->Fread(fd_, n, scratch, use_direct_io());
    if (res < 0) {
      *result = Slice(scratch, 0);
      return IOStatus::IOError("fread failed!", fname_);
    }
    *result = Slice(scratch, res);
    return IOStatus::OK();
  };
  IOStatus Skip(uint64_t n) override {
    int res = rpc_engine_->Fseek(fd_, n);
    if (res) {
      return IOStatus::IOError("fseek failed!", fname_);
    }
    return IOStatus::OK();
  };

 private:
  int fd_;
  const std::string fname_;
  std::shared_ptr<RPCEngine> rpc_engine_;
};

class NasRandomAccessFile : public FSRandomAccessFile {
 public:
  NasRandomAccessFile(int fd, const std::string& fname,
                      std::shared_ptr<RPCEngine> rpc_engine,
                      size_t logical_block_size, const EnvOptions& options)
      : fd_(fd),
        fname_(fname),
        rpc_engine_(rpc_engine),
        logical_sector_size_(logical_block_size),
        use_direct_io_(options.use_direct_reads){};
  ~NasRandomAccessFile() override { rpc_engine_->Close(fd_); };
  virtual bool use_direct_io() const override { return use_direct_io_; }
  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }

 public:
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    if (use_direct_io()) {
      assert(IsSectorAligned(offset, GetRequiredBufferAlignment()));
      assert(IsSectorAligned(n, GetRequiredBufferAlignment()));
      assert(IsSectorAligned(scratch, GetRequiredBufferAlignment()));
    }
    ssize_t res = rpc_engine_->Pread(fd_, offset, n, scratch, use_direct_io());
    if (res < 0) {
      *result = Slice(scratch, 0);
      return IOStatus::IOError("pread failed!", fname_);
    }
    *result = Slice(scratch, res);
    return IOStatus::OK();
  };

 private:
  int fd_;
  const std::string fname_;
  std::shared_ptr<RPCEngine> rpc_engine_;
  size_t logical_sector_size_;
  bool use_direct_io_;
};

class NasWritableFile : public FSWritableFile {
 public:
  NasWritableFile(int fd, const std::string& fname, size_t logical_block_size,
                  std::shared_ptr<RPCEngine> rpc_engine,
                  const FileOptions& options)
      : fd_(fd),
        fname_(fname),
        filesize_(0),
        logical_sector_size_(logical_block_size),
        rpc_engine_(rpc_engine),
        use_direct_io_(options.use_direct_writes) {
    allow_fallocate_ = options.allow_fallocate;
    fallocate_with_keep_size_ = options.fallocate_with_keep_size;
    sync_file_range_supported_ = true;
  };
  ~NasWritableFile() override {
    if (fd_ > 0) {
      rpc_engine_->Close(fd_);
    }
  };
  virtual bool use_direct_io() const override { return use_direct_io_; }
  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }

 public:
  using FSWritableFile::Append;
  IOStatus Append(const Slice& data, const IOOptions& /*opts*/,
                  IODebugContext* /*dbg*/) override {
    if (use_direct_io()) {
      assert(IsSectorAligned(data.size(), GetRequiredBufferAlignment()));
      assert(IsSectorAligned(data.data(), GetRequiredBufferAlignment()));
    }
    const char* src = data.data();
    size_t nbytes = data.size();
    ssize_t res = rpc_engine_->Write(fd_, src, nbytes, use_direct_io());
    if (!res) {
      return IOStatus::IOError("Append failed!", fname_);
    }
    filesize_ += nbytes;
    return IOStatus::OK();
  };
  using FSWritableFile::PositionedAppend;
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& /*opts*/,
                            IODebugContext* /*dbg*/) override {
    if (use_direct_io()) {
      assert(IsSectorAligned(offset, GetRequiredBufferAlignment()));
      assert(IsSectorAligned(data.size(), GetRequiredBufferAlignment()));
      assert(IsSectorAligned(data.data(), GetRequiredBufferAlignment()));
    }
    assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
    const char* src = data.data();
    size_t nbytes = data.size();
    if (!rpc_engine_->PWrite(fd_, src, nbytes, offset, use_direct_io())) {
      return IOStatus::IOError("positioned append failed!", fname_);
    }
    filesize_ += offset + nbytes;
    return IOStatus::OK();
  };
  IOStatus Truncate(uint64_t size, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    int res = rpc_engine_->Ftruncate(fd_, size);
    if (res < 0) {
      return IOStatus::IOError(
          "fail to truncate file to size " + std::to_string(size), fname_);
    }
    filesize_ = size;
    return IOStatus::OK();
  };
  IOStatus Close(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    size_t block_size;
    size_t last_allocated_block;
    GetPreallocationStatus(&block_size, &last_allocated_block);
    if (last_allocated_block > 0) {
      int dummy __attribute__((__unused__));
      dummy = rpc_engine_->Ftruncate(fd_, filesize_);
#if defined(ROCKSDB_FALLOCATE_PRESENT) && defined(FALLOC_FL_PUNCH_HOLE)
      struct stat file_stats;
      int result = rpc_engine_->Fstat(fd_, &file_stats);
      if (result == 0 &&
          (file_stats.st_size + file_stats.st_blksize - 1) /
                  file_stats.st_blksize !=
              file_stats.st_blocks / (file_stats.st_blksize / 512)) {
        if (allow_fallocate_) {
          rpc_engine_->Fallocate(
              fd_, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, filesize_,
              block_size * last_allocated_block - filesize_);
        }
      }
#endif
    }
    int res = rpc_engine_->Close(fd_);
    if (res < 0) {
      return IOStatus::IOError("Close failed", fname_);
    }
    fd_ = -1;
    return IOStatus::OK();
  };
  IOStatus Flush(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  };
  IOStatus Sync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    int res = rpc_engine_->Fdatasync(fd_);
    if (res < 0) {
      return IOStatus::IOError("Sync failed!", fname_);
    }
    return IOStatus::OK();
  };  // sync data
  IOStatus Fsync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    int res = rpc_engine_->Fsync(fd_);
    if (res < 0) {
      return IOStatus::IOError("Fsync failed!", fname_);
    }
    return IOStatus::OK();
  };
  bool IsSyncThreadSafe() const override { return true; }
  uint64_t GetFileSize(const IOOptions& /*opts*/,
                       IODebugContext* /*dbg*/) override {
    return filesize_;
  };
#ifdef ROCKSDB_FALLOCATE_PRESENT
  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
    int res;
    assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
    assert(len <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
    if (allow_fallocate_) {
      res = rpc_engine_->Fallocate(
          fd_, fallocate_with_keep_size_ ? FALLOC_FL_KEEP_SIZE : 0, offset,
          len);
      if (res != 0) {
        return IOStatus::IOError("While fallocate offset " +
                                     std::to_string(offset) + " len " +
                                     std::to_string(len),
                                 fname_);
      }
    }
    return IOStatus::OK();
  };
#endif

  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions& opts,
                     IODebugContext* dbg) override {
#ifdef ROCKSDB_RANGESYNC_PRESENT
    assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
    assert(nbytes <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
    if (sync_file_range_supported_) {
      int ret;
      if (strict_bytes_per_sync_) {
        // Specifying `SYNC_FILE_RANGE_WAIT_BEFORE` together with an
        // offset/length that spans all bytes written so far tells
        // `sync_file_range` to wait for any outstanding writeback requests to
        // finish before issuing a new one.
        ret = rpc_engine_->RangeSync(
            fd_, 0, static_cast<off_t>(offset + nbytes),
            SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE);
      } else {
        ret = rpc_engine_->RangeSync(fd_, static_cast<off_t>(offset),
                                     static_cast<off_t>(nbytes),
                                     SYNC_FILE_RANGE_WRITE);
      }
      if (ret != 0) {
        return IOError("While sync_file_range returned " + std::to_string(ret),
                       fname_, errno);
      }
      return IOStatus::OK();
    }
#endif  // ROCKSDB_RANGESYNC_PRESENT
    return FSWritableFile::RangeSync(offset, nbytes, opts, dbg);
  }

 private:
  int fd_;
  const std::string fname_;
  size_t filesize_;
  size_t logical_sector_size_;
  std::shared_ptr<RPCEngine> rpc_engine_;
  bool use_direct_io_;
  bool allow_fallocate_;
  bool fallocate_with_keep_size_;
  bool sync_file_range_supported_;
};

class NasRandomRWFile : public FSRandomRWFile {
 public:
  NasRandomRWFile(int fd, const std::string& fname,
                  std::shared_ptr<RPCEngine> rpc_engine,
                  const FileOptions& /*options*/)
      : fd_(fd), fname_(fname), rpc_engine_(rpc_engine){};
  ~NasRandomRWFile() override {
    if (fd_ > 0) {
      rpc_engine_->Close(fd_);
    }
  };

 public:
  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& /*opts*/,
                 IODebugContext* /*dbg*/) override {
    const char* src = data.data();
    size_t nbytes = data.size();
    if (!rpc_engine_->PWrite(fd_, src, nbytes, offset, use_direct_io())) {
      return IOStatus::IOError("While write random read/write file at offset " +
                                   std::to_string(offset),
                               fname_);
    }
    return IOStatus::OK();
  }
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*opts*/,
                Slice* result, char* scratch,
                IODebugContext* /*dbg*/) const override {
    ssize_t res = rpc_engine_->Pread(fd_, offset, n, scratch, use_direct_io());
    if (res < 0) {
      *result = Slice(scratch, 0);
      return IOStatus::IOError("While reading random read/write file offset " +
                                   std::to_string(offset) + " len " +
                                   std::to_string(n),
                               fname_);
    }
    *result = Slice(scratch, res);
    return IOStatus::OK();
  };
  IOStatus Flush(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }
  IOStatus Sync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    int res = rpc_engine_->Fdatasync(fd_);
    if (res < 0) {
      return IOStatus::IOError("Sync failed!", fname_);
    }
    return IOStatus::OK();
  };  // sync data
  IOStatus Fsync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    int res = rpc_engine_->Fsync(fd_);
    if (res < 0) {
      return IOStatus::IOError("Fsync failed!", fname_);
    }
    return IOStatus::OK();
  };
  IOStatus Close(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    // may not need
    int res = rpc_engine_->Close(fd_);
    if (res < 0) {
      return IOStatus::IOError("While close random read/write file", fname_);
    }
    fd_ = -1;
    return IOStatus::OK();
  };

 private:
  int fd_;
  const std::string fname_;
  std::shared_ptr<RPCEngine> rpc_engine_;
};

class NasDirectory : public FSDirectory {
 public:
  NasDirectory(int fd, const std::string& dir_name,
               std::shared_ptr<RPCEngine> rpc_engine)
      : fd_(fd), dir_name_(dir_name), rpc_engine_(rpc_engine){};
  ~NasDirectory() override {
    if (fd_ >= 0) {
      rpc_engine_->Close(fd_);
    }
  };
  IOStatus FsyncWithDirOptions(
      const IOOptions& /*opts*/, IODebugContext* /*dbg*/,
      const DirFsyncOptions& dir_fsync_options) override {
    assert(fd_ > 0);  // Check use after close
    int res = rpc_engine_->Fsync(fd_);
    if (res < 0) {
      return IOStatus::IOError("While fsync", "a directory" + dir_name_);
    }
    return IOStatus::OK();
  }
  IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override {
    return FsyncWithDirOptions(opts, dbg, DirFsyncOptions());
  }
  IOStatus Close(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    int res = rpc_engine_->Close(fd_);
    if (res < 0) {
      return IOStatus::IOError("While closing directory ", dir_name_);
    }
    return IOStatus::OK();
  }

 private:
  int fd_;
  const std::string dir_name_;
  std::shared_ptr<RPCEngine> rpc_engine_;
};

IOStatus RemoteFileSystem::NewSequentialFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* /*dbg*/) {
  result->reset();
  // int flags = O_RDONLY;  // flags |= O_CLOEXEC ???
  // if (file_opts.use_direct_reads) {
  //   flags |= O_DIRECT;
  // }
  int flags = 0;  // flags |= O_CLOEXEC ???
  if (file_opts.use_direct_reads) {
    flags += 16384;
  }
  int fd = rpc_engine->Open(fname.c_str(), flags,
                            GetDBFileMode(allow_non_owner_access_));
  if (fd < 0) {
    return IOStatus::IOError("Open failed!\n", fname);
  }
  if (!file_opts.use_direct_reads) {
    bool ret = rpc_engine->Fopen(fd, "r");
    if (!ret) {
      rpc_engine->Close(fd);
      return IOStatus::IOError("While opening file for sequentially read\n",
                               fname);
    }
  }
  result->reset(new NasSequentailFile(fd, fname, rpc_engine));
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::NewRandomAccessFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* /*dbg*/) {
  result->reset();
  // int flags = O_RDONLY;
  // if (file_opts.use_direct_reads) {
  //   flags |= O_DIRECT;
  // }
  int flags = 0;
  if (file_opts.use_direct_reads) {
    flags += 16384;
  }
  int fd = rpc_engine->Open(fname.c_str(), flags,
                            GetDBFileMode(allow_non_owner_access_));
  if (fd < 0) {
    return IOStatus::IOError("Open failed!\n", fname);
  }
  result->reset(new NasRandomAccessFile(fd, fname, rpc_engine, kDefaultPageSize,
                                        file_opts));
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::NewRandomRWFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSRandomRWFile>* result, IODebugContext* /*dbg*/) {
  result->reset();
  // int flags = O_RDWR;
  int flags = 2;
  int fd = rpc_engine->Open(fname.c_str(), flags,
                            GetDBFileMode(allow_non_owner_access_));
  if (fd < 0) {
    return IOStatus::IOError("while open file for random read/write", fname);
  }
  result->reset(new NasRandomRWFile(fd, fname, rpc_engine, file_opts));
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::OpenWritableFile(
    const std::string& fname, const FileOptions& options, bool reopen,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* /*dbg*/) {
  result->reset();
  IOStatus s;
  int fd = -1;
  // int flags = (reopen) ? (O_CREAT | O_APPEND | O_WRONLY)
  //                      : (O_CREAT | O_TRUNC | O_WRONLY);
  // if (options.use_direct_writes) {
  //   flags |= O_DIRECT;
  // }
  int flags = (reopen) ? 1089 : 577;
  if (options.use_direct_writes) {
    flags += 16384;
  }

  fd = rpc_engine->Open(fname.c_str(), flags,
                        GetDBFileMode(allow_non_owner_access_));
  if (fd < 0) {
    return IOStatus::IOError("Open failed!", fname);
  }
  result->reset(
      new NasWritableFile(fd, fname, kDefaultPageSize, rpc_engine, options));
  return s;
}

IOStatus RemoteFileSystem::NewWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  return OpenWritableFile(fname, options, false, result, dbg);
}

IOStatus RemoteFileSystem::ReopenWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  return OpenWritableFile(fname, options, true, result, dbg);
}

IOStatus RemoteFileSystem::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    const FileOptions& file_opts, std::unique_ptr<FSWritableFile>* result,
    IODebugContext* dbg) {
  result->reset();
  int fd = -1;
  // int flags = O_WRONLY;
  // if (file_opts.use_direct_writes) {
  //   flags |= O_DIRECT;
  // }
  int flags = 1;
  if (file_opts.use_direct_writes) {
    flags += 16384;
  }

  fd = rpc_engine->Open(old_fname.c_str(), flags,
                        GetDBFileMode(allow_non_owner_access_));
  if (fd < 0) {
    return IOStatus::IOError("reopen file for write", old_fname);
  }

  int res = rpc_engine->Rename(old_fname.c_str(), fname.c_str());
  if (res != 0) {
    rpc_engine->Close(fd);
    return IOStatus::IOError("rename file to " + fname, old_fname);
  }

  result->reset(
      new NasWritableFile(fd, fname, kDefaultPageSize, rpc_engine, file_opts));
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::NewDirectory(const std::string& name,
                                        const IOOptions& /*opts*/,
                                        std::unique_ptr<FSDirectory>* result,
                                        IODebugContext* /*dbg*/) {
  result->reset();
  int flags = 0;
  int fd = rpc_engine->Open(name.c_str(), flags, 0);
  if (fd < 0) {
    // todo: upper caller may need errno
    return IOStatus::IOError("While open directory", name);
  } else {
    result->reset(new NasDirectory(fd, name, rpc_engine));
  }
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::FileExists(const std::string& fname,
                                      const IOOptions& /*opts*/,
                                      IODebugContext* /*dbg*/) {
  struct ret_with_errno ret = rpc_engine->Access(fname.c_str(), F_OK);
  if (ret.ret == 0) {
    return IOStatus::OK();
    return IOStatus::IOError("While access file", fname);
  }
  int err = ret.errn;
  switch (err) {
    case EACCES:
    case ELOOP:
    case ENAMETOOLONG:
    case ENOENT:
    case ENOTDIR:
      return IOStatus::NotFound();
    default:
      assert(err == EIO || err == ENOMEM);
      return IOStatus::IOError("Unexpected error(" + std::to_string(err) +
                               ") accessing file `" + fname + "' ");
  }
}

IOStatus RemoteFileSystem::GetChildren(const std::string& dir,
                                       const IOOptions& opts,
                                       std::vector<std::string>* result,
                                       IODebugContext* /*dbg*/) {
  result->clear();
  int res = rpc_engine->GetChildren(dir.c_str(), result);
  if (res < 0) {
    // todo: upper caller may need errno
    return IOStatus::IOError("While get children", dir);
  }
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::DeleteFile(const std::string& fname,
                                      const IOOptions& /*opts*/,
                                      IODebugContext* /*dbg*/) {
  if (rpc_engine->Unlink(fname.c_str()) != 0) {
    // todo: upper caller may need errno
    return IOStatus::IOError("while unlink() file", fname);
  }
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::CreateDir(const std::string& name,
                                     const IOOptions& /*opts*/,
                                     IODebugContext* /*dbg*/) {
  struct ret_with_errno ret = rpc_engine->Mkdir(name.c_str(), 0755);
  if (ret.ret != 0) {
    return IOError("While mkdir", name, ret.errn);
  }
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::CreateDirIfMissing(const std::string& name,
                                              const IOOptions& /*opts*/,
                                              IODebugContext* /*dbg*/) {
  struct ret_with_errno ret = rpc_engine->Mkdir(name.c_str(), 0755);
  if (ret.ret != 0) {
    if (ret.errn != EEXIST) {
      return IOError("While mkdir if missing", name, ret.errn);
    } else if (!DirExists(name)) {  // Check that name is actually a
                                    // directory.
      // Message is taken from mkdir
      return IOStatus::IOError("`" + name + "' exists but is not a directory");
    }
  }
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::DeleteDir(const std::string& name,
                                     const IOOptions& /*opts*/,
                                     IODebugContext* /*dbg*/) {
  if (rpc_engine->Rmdir(name.c_str()) != 0) {
    // todo: upper caller may need errno
    return IOStatus::IOError("file rmdir", name);
  }
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::GetFileSize(const std::string& fname,
                                       const IOOptions& /*opts*/,
                                       uint64_t* size,
                                       IODebugContext* /*dbg*/) {
  struct stat sbuf;
  struct stat_ret ret = rpc_engine->Stat(fname.c_str(), &sbuf);
  if (ret.ret != 0) {
    *size = 0;
    return IOError("while stat a file for size", fname, ret.errn);
  } else {
    *size = sbuf.st_size;
  }
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::GetFileModificationTime(const std::string& fname,
                                                   const IOOptions& /*opts*/,
                                                   uint64_t* file_mtime,
                                                   IODebugContext* /*dbg*/) {
  struct stat s;
  struct stat_ret ret = rpc_engine->Stat(fname.c_str(), &s);
  if (ret.ret != 0) {
    return IOError("while stat a file for modification time", fname, ret.errn);
  }
  *file_mtime = static_cast<uint64_t>(s.st_mtime);
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::RenameFile(const std::string& src,
                                      const std::string& target,
                                      const IOOptions& /*opts*/,
                                      IODebugContext* /*dbg*/) {
  if (rpc_engine->Rename(src.c_str(), target.c_str()) != 0) {
    // todo: upper caller may need errno
    return IOStatus::IOError("While renaming a file to " + target, src);
  }
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::LockFile(const std::string& fname,
                                    const IOOptions& /*opts*/, FileLock** lock,
                                    IODebugContext* /*dbg*/) {
  *lock = nullptr;

  LockHoldingInfo lhi;
  int64_t current_time = 0;
  // Ignore status code as the time is only used for error message.
  SystemClock::Default()->GetCurrentTime(&current_time).PermitUncheckedError();
  lhi.acquire_time = current_time;
  lhi.acquiring_thread = Env::Default()->GetThreadID();

  mutex_locked_files.Lock();
  // If it already exists in the locked_files set, then it is already locked,
  // and fail this lock attempt. Otherwise, insert it into locked_files.
  // This check is needed because fcntl() does not detect lock conflict
  // if the fcntl is issued by the same thread that earlier acquired
  // this lock.
  // We must do this check *before* opening the file:
  // Otherwise, we will open a new file descriptor. Locks are associated with
  // a process, not a file descriptor and when *any* file descriptor is
  // closed, all locks the process holds for that *file* are released
  const auto it_success = locked_files.insert({fname, lhi});
  if (it_success.second == false) {
    LockHoldingInfo prev_info = it_success.first->second;
    mutex_locked_files.Unlock();
    errno = ENOLCK;
    // Note that the thread ID printed is the same one as the one in
    // posix logger, but posix logger prints it hex format.
    return IOStatus::IOError("lock hold by current process, acquire time " +
                                 std::to_string(prev_info.acquire_time) +
                                 " acquiring thread " +
                                 std::to_string(prev_info.acquiring_thread),
                             fname);
  }

  IOStatus result = IOStatus::OK();
  int fd;
  // int flags = O_RDWR | O_CREAT;
  int flags = 66;
  fd = rpc_engine->Open(fname.c_str(), flags, 0644);
  if (fd < 0) {
    result = IOStatus::IOError("while open a file for lock", fname);
  } else if (LockOrUnlock(fd, true) == -1) {
    result = IOStatus::IOError("While lock file", fname);
    rpc_engine->Close(fd);
  } else {
    NasFileLock* my_lock = new NasFileLock;
    my_lock->fd_ = fd;
    my_lock->filename = fname;
    *lock = my_lock;
  }
  if (!result.ok()) {
    // If there is an error in locking, then remove the pathname from
    // locked_files. (If we got this far, it did not exist in locked_files
    // before this call.)
    locked_files.erase(fname);
  }

  mutex_locked_files.Unlock();
  return result;
}

IOStatus RemoteFileSystem::UnlockFile(FileLock* lock, const IOOptions& /*opts*/,
                                      IODebugContext* /*dbg*/) {
  NasFileLock* my_lock = reinterpret_cast<NasFileLock*>(lock);
  IOStatus result;
  mutex_locked_files.Lock();
  // If we are unlocking, then verify that we had locked it earlier,
  // it should already exist in locked_files. Remove it from locked_files.
  if (locked_files.erase(my_lock->filename) != 1) {
    errno = ENOLCK;
    result = IOError("unlock", my_lock->filename, errno);
  } else if (LockOrUnlock(my_lock->fd_, false) == -1) {
    result = IOError("unlock", my_lock->filename, errno);
  }
  rpc_engine->Close(my_lock->fd_);
  my_lock->Clear();
  delete my_lock;
  mutex_locked_files.Unlock();
  return result;
}

IOStatus RemoteFileSystem::GetAbsolutePath(const std::string& db_path,
                                           const IOOptions& /*opts*/,
                                           std::string* output_path,
                                           IODebugContext* /*dbg*/) {
  if (!db_path.empty() && db_path[0] == '/') {
    *output_path = db_path;
    return IOStatus::OK();
  }

  return IOStatus::NotSupported("GetAbsolutePath");
}

IOStatus RemoteFileSystem::GetTestDirectory(const IOOptions& /*opts*/,
                                            std::string* result,
                                            IODebugContext* /*dbg*/) {
  *result = "/tmp/rocksdb-test";
  return IOStatus::OK();
}

IOStatus RemoteFileSystem::IsDirectory(const std::string& path,
                                       const IOOptions& /*opts*/, bool* is_dir,
                                       IODebugContext* /*dbg*/) {
  int fd = -1;
  // int flags = O_RDONLY;
  int flags = 0;
  fd = rpc_engine->Open(path.c_str(), flags, 0);
  if (fd < 0) {
    return IOStatus::IOError("While open for IsDirectory()", path);
  }
  IOStatus io_s;
  struct stat sbuf;
  if (rpc_engine->Fstat(fd, &sbuf) < 0) {
    io_s = IOStatus::IOError("While doing stat for IsDirectory()", path);
  }
  rpc_engine->Close(fd);
  if (io_s.ok() && nullptr != is_dir) {
    *is_dir = S_ISDIR(sbuf.st_mode);
  }
  return io_s;
}

std::shared_ptr<FileSystem> NewRemoteFileSystem(RPCEngine* rpc_engine) {
  STATIC_AVOID_DESTRUCTION(std::shared_ptr<FileSystem>, instance)
  (std::make_shared<RemoteFileSystem>(rpc_engine));
  return instance;
}

}  // namespace ROCKSDB_NAMESPACE
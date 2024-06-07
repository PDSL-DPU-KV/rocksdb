#include "nas_rpc.h"

#include <cstdio>
#include <variant>

#include "util/mercury_wrapper.hh"

RPCEngine::RPCEngine(const std::string &svr_addr_string)
    : engine(MercuryEngine("verbs", false)),
      bg_worker(std::thread([&]() { engine.progress(); })),
      ep(engine.lookup(svr_addr_string.c_str())),
      rp(engine.define("op")) {}

RPCEngine::~RPCEngine() {
  engine.stop();
  bg_worker.join();
}

ret_with_errno RPCEngine::Open(const char *fname, int flags, uint mode) {
  open_args args;
  args.name = fname;
  args.flags = flags;
  args.mode = mode;
  RpcRequest req;
  req.type = OPEN;
  req.args = args;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<ret_with_errno>(&resp.result);
  return *ret;
}

int RPCEngine::Close(int fd) {
  RpcRequest req;
  req.type = CLOSE;
  req.args = fd;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<int>(&resp.result);
  return *ret;
}

ssize_t RPCEngine::Read(int fd, uint64_t offset, uint64_t n, char *buffer,
                        bool use_direct_io) {
  read_args args;
  args.fd = fd;
  args.n = n;
  args.offset = offset;
  args.use_direct_io = use_direct_io;
#ifdef NAS_DEBUG
  printf("Send read req, fd: %d, n: %ld, offset: %ld, direct: %d\n", fd, n,
         offset, use_direct_io);
#endif
  RpcRequest req;
  req.type = READ;
  req.args = args;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<std::string>(&resp.result);
  if (*ret == "failed") return -1;
  memcpy(buffer, ret->c_str(), ret->size());
#ifdef NAS_DEBUG
  for (uint i = 0; i < ret->size(); i++) {
    printf("%x\n", ret->at(i));
  }
#endif
  return ret->size();
}

ssize_t RPCEngine::Fread(int fd, uint64_t n, char *buffer, bool use_direct_io) {
  return Read(fd, -1, n, buffer, use_direct_io);
}

ssize_t RPCEngine::Pread(int fd, uint64_t offset, uint64_t n, char *buffer,
                         bool use_direct_io) {
  return Read(fd, offset, n, buffer, use_direct_io);
}

bool RPCEngine::Write(int fd, uint64_t n, int64_t offset, const char *buffer,
                      bool use_direct_io) {
  write_args args;
  args.fd = fd;
  args.n = n;
  args.offset = offset;
  args.use_direct_io = use_direct_io;
  args.buffer = std::string(buffer, n);
#ifdef NAS_DEBUG
  printf("Send write req, fd: %d, n: %ld, offset: %ld, direct: %d\n", fd, n,
         offset, use_direct_io);
  for (uint i = 0; i < n; i++) {
    printf("%x\n", buffer[i]);
  }
#endif
  RpcRequest req;
  req.type = WRITE;
  req.args = args;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<bool>(&resp.result);
  return *ret;
}

bool RPCEngine::Write(int fd, const char *buffer, size_t n,
                      bool use_direct_io) {
  return Write(fd, n, -1, buffer, use_direct_io);
}

bool RPCEngine::PWrite(int fd, const char *buffer, size_t n, uint64_t offset,
                       bool use_direct_io) {
  return Write(fd, n, offset, buffer, use_direct_io);
}

bool RPCEngine::Fopen(int fd, const char *mode) {
  fopen_args args;
  args.fd = fd;
  args.mode = mode;
  RpcRequest req;
  req.type = FOPEN;
  req.args = args;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<bool>(&resp.result);
  return *ret;
}

int RPCEngine::Fseek(int fd, uint64_t n) {
  fseek_args args;
  args.fd = fd;
  args.n = n;
  RpcRequest req;
  req.type = FSEEK;
  req.args = args;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<int>(&resp.result);
  return *ret;
}

int RPCEngine::Fstat(int fd, struct stat *stat_buf) {
  RpcRequest req;
  req.type = FSTAT;
  req.args = fd;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<stat_ret>(&resp.result);
  stat_buf->st_blksize = ret->st_blksize;
  stat_buf->st_blocks = ret->st_blocks;
  stat_buf->st_size = ret->st_size;
  stat_buf->st_dev = ret->st_dev;
  stat_buf->st_ino = ret->st_ino;
  stat_buf->st_mode = ret->st_mode;
  stat_buf->st_mtime = ret->st_mtime_;
  return ret->ret;
}

int RPCEngine::Ftruncate(int fd, uint64_t size) {
  ftruncate_args args;
  args.fd = fd;
  args.size = size;
  RpcRequest req;
  req.type = FTRUNCATE;
  req.args = args;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<int>(&resp.result);
  return *ret;
}

int RPCEngine::Fallocate(int fd, int mode, uint64_t offset, uint64_t len) {
  fallocate_args args;
  args.fd = fd;
  args.mode = mode;
  args.offset = offset;
  args.len = len;
  RpcRequest req;
  req.type = FALLOCATE;
  req.args = args;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<int>(&resp.result);
  return *ret;
}

int RPCEngine::Fdatasync(int fd) {
  RpcRequest req;
  req.type = FDATASYNC;
  req.args = fd;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<int>(&resp.result);
  return *ret;
}

int RPCEngine::Fsync(int fd) {
  RpcRequest req;
  req.type = FSYNC;
  req.args = fd;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<int>(&resp.result);
  return *ret;
}

int RPCEngine::RangeSync(int fd, uint64_t offset, uint64_t count, int flags) {
  rangesync_args args;
  args.fd = fd;
  args.offset = offset;
  args.count = count;
  args.flags = flags;
  RpcRequest req;
  req.type = RANGESYNC;
  req.args = args;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<int>(&resp.result);
  return *ret;
}

ret_with_errno RPCEngine::Rename(const char *old_name, const char *new_name) {
  rename_args args;
  args.old_name = old_name;
  args.new_name = new_name;
  RpcRequest req;
  req.type = RENAME;
  req.args = args;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<ret_with_errno>(&resp.result);
  return *ret;
}

ret_with_errno RPCEngine::Access(const char *name, int type) {
  access_args args;
  args.name = name;
  args.type = type;
  RpcRequest req;
  req.type = ACCESS;
  req.args = args;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<ret_with_errno>(&resp.result);
  return *ret;
}

int RPCEngine::Unlink(const char *name) {
  RpcRequest req;
  req.type = UNLINK;
  req.args = std::string(name);
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<int>(&resp.result);
  return *ret;
}

ret_with_errno RPCEngine::Mkdir(const char *name, uint mode) {
  mkdir_args args;
  args.name = name;
  args.mode = mode;
  RpcRequest req;
  req.type = MKDIR;
  req.args = args;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<ret_with_errno>(&resp.result);
  return *ret;
}

int RPCEngine::Rmdir(const char *name) {
  RpcRequest req;
  req.type = RMDIR;
  req.args = std::string(name);
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<int>(&resp.result);
  return *ret;
}

stat_ret RPCEngine::Stat(const char *name, struct stat *stat_buf) {
  RpcRequest req;
  req.type = STAT;
  req.args = std::string(name);
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<stat_ret>(&resp.result);
  stat_buf->st_blksize = ret->st_blksize;
  stat_buf->st_blocks = ret->st_blocks;
  stat_buf->st_size = ret->st_size;
  stat_buf->st_dev = ret->st_dev;
  stat_buf->st_ino = ret->st_ino;
  stat_buf->st_mode = ret->st_mode;
  stat_buf->st_mtime = ret->st_mtime_;
  return *ret;
}

int RPCEngine::GetChildren(const char *dir_name,
                           std::vector<std::string> *result) {
  RpcRequest req;
  req.type = LS;
  req.args = std::string(dir_name);
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<ls_ret>(&resp.result);
  *result = ret->result;
  return ret->ret;
}

int RPCEngine::SetLock(int fd, bool lock) {
  lock_args args;
  args.fd = fd;
  args.lock = lock;
  RpcRequest req;
  req.type = LOCK;
  req.args = args;
  Callable rpc = rp.on(ep);
  FutureResponsePayload *future_resp = rpc.forward(req);
  future_resp->wait();
  auto resp = future_resp->as<RpcResponse>();
  delete future_resp;
  auto ret = std::get_if<int>(&resp.result);
  return *ret;
}

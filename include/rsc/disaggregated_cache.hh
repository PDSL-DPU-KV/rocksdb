#pragma once

#include <infiniband/verbs.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include "allocator.hh"
#include "rdma/connection.hh"
#include "rdma/fixed_buffer_pool.hh"
#include "rdma/resources.hh"
#include "rocksdb/slice.h"
#include "sharded_map.hh"
#include "utilities/memory.hh"
#include "utilities/misc.hh"
#include "utilities/timer.hh"

namespace sc {

struct HostMeta {
  AddrType remote_addr;
  uint32_t value_length;
  uint32_t reserved;  // maybe we need to store something else, like server id
};

//! These concepts are the core components of our disaggregated cache,
//! which you can change with whatever you want.

// clang-format off
template<typename T>
concept RemoteMemoryAllocator = requires(T a, SizeType n, AddrType addr) {
  { a.Allocate(n) } -> std::same_as<std::optional<AddrType>>;
  { a.Deallocate(addr, n) } -> std::same_as<void>;
  { a.PolicyType() } -> std::same_as<std::string>;
};

template<typename T>
concept HostMetaIndex = requires(T i, std::string key, HostMeta value) {
  { i.Get(key) } -> std::same_as<std::optional<HostMeta>>;
  { i.Set(key, value) } -> std::same_as<void>;
  { i.Delete(key) } -> std::same_as<std::optional<HostMeta>>;
  { i.Size() } -> std::same_as<uint32_t>;
};
// clang-format on

static_assert(RemoteMemoryAllocator<Allocator<FirstFitPolicy>>, "?");
static_assert(HostMetaIndex<ShardedMap<std::string, HostMeta>>, "?");

template <RemoteMemoryAllocator Allocator,
          HostMetaIndex Index = ShardedMap<std::string, HostMeta>>
class DisaggregatedCache {
  inline static constexpr uint8_t default_n_shards = 31;
  inline static constexpr uint32_t default_n_wr = 64;
  inline static constexpr rdma::ConnCtxInitAttr default_attr{
      .max_cqe = default_n_wr * 2,
      .max_send_wr = default_n_wr,
      .max_recv_wr = default_n_wr,
      .max_send_sge = 1,
      .max_recv_sge = 1,
      .max_inline_data = 0,
      .qp_type = IBV_QPT_RC,
      .sig_all = false,
      .use_same_cq = true,
      .use_srq = false,
  };

 public:
  //* Handle hold the async result of the DisaggregatedCache.
  class Handle;
  //* Daemon is a server-side class which will hold the memory.
  friend class RemoteDaemon;

 public:
  DisaggregatedCache() {}

  ~DisaggregatedCache() {
    running_ = false;
    if (cq_poller_.joinable()) {
      cq_poller_.join();
    }
    if (buffer_pool_ != nullptr) {
      delete buffer_pool_;
    }
    if (connector_.IsConnected()) {
      connector_.Disconnect();
    }
    if (allocator_ != nullptr) {
      delete allocator_;
    }
    if (index_ != nullptr) {
      delete index_;
    }
  }

  auto Initialize(const char* host, const char* port, uint32_t max_value_size,
                  uint32_t concurrency_hint) -> bool {
    auto builder = [this](sc::rdma::ConnHandle*, rdma_cm_id* id) -> bool {
      ctx_ = rdma::ConnContext::Builder(id, default_attr);
      return true;
    };
    if (not connector_.ConnectAndEstablish(host, port, builder)) {
      ERROR("fail to connect with remote daemon");
      return false;
    }
    TRACE("addr: {} length: {} rkey: {}", RemoteMR()->Address(),
          RemoteMR()->Length(), RemoteMR()->RKey());
    allocator_ = new Allocator(RemoteMR()->Address(), RemoteMR()->Length());
    index_ = new Index(default_n_shards);
    buffer_pool_ = new rdma::FixedBufferPool(default_n_wr, max_value_size,
                                             concurrency_hint * 2);
    if (not buffer_pool_->RegisterBuffer(ctx_->GetPD(),
                                         rdma::FixedBufferPool::local_access)) {
      return false;
    }
    running_ = true;
    cq_poller_ = std::thread(&DisaggregatedCache::PollCQ, this);
    return true;
  }

 public:
  //* Get()
  /// 1. Locate the meta in the host memory map by key
  /// 2. Fetch the value through transport by remote address asynchronously
  /// 3. Return the Handle to the caller
  /// 4. The caller shall hold the handle and wait for the result.
  auto Get(const std::string& key) -> std::optional<std::unique_ptr<Handle>> {
    auto remote_meta = index_->Get(key);
    if (not remote_meta.has_value()) {
      return std::nullopt;
    }
    std::unique_ptr<Handle> h(new Handle(this, remote_meta.value()));
    if (not h->AcquireBuffer()) {
      return std::nullopt;
    }
    if (not h->InitiateRemoteOp(IBV_WR_RDMA_READ)) {
      return std::nullopt;
    }
    h->Wait();
    INFO("GET");
    DEBUG("FROM REMOTE key {},value length {} {}", key, h->Size(),
          rocksdb::Slice((char*)h->Value(), h->Size()).ToASCII());
    return std::optional{std::move(h)};
  }

  //* Set()
  /// 1. Allocate remote memory
  /// 2. Write remote value
  /// 3. Insert host meta index
  auto Set(const std::string& key, void* value, uint32_t length) -> bool {
    INFO("SET");
    DEBUG("TO REMOTE key {}, value length {} {}", key, length,
          rocksdb::Slice((char*)value, length).ToASCII());
    util::TraceTimer t;
    t.Tick();  // 1
    auto addr = allocator_->Allocate(length);
    t.Tick();  // 2
    if (not addr.has_value()) {
      return false;
    }
    HostMeta meta{
        .remote_addr = addr.value(),
        .value_length = length,
        .reserved = 0,
    };
    Handle h(this, std::move(meta));
    t.Tick();  // 3
    if (not h.AcquireBuffer(value)) {
      allocator_->Deallocate(meta.remote_addr, meta.value_length);
      return false;
    }
    t.Tick();  // 4
    if (not h.InitiateRemoteOp(IBV_WR_RDMA_WRITE)) {
      allocator_->Deallocate(meta.remote_addr, meta.value_length);
      return false;
    }
    t.Tick();  // 5
    index_->Set(key, meta);
    t.Tick();  // 6
    h.Wait();
    t.Tick();
    TRACE("{}", t);
    return true;
  }

  //* Delete()
  /// 1. Locate in host meta index
  /// 2. Deallocate the remote memory region
  //! BUG the concurrent problem here?
  auto Delete(const std::string& key) -> void {
    DEBUG("DELETE REMOTE key {}", key);
    auto o = index_->Delete(key);
    if (o.has_value()) {
      allocator_->Deallocate(o.value().remote_addr, o.value().value_length);
    }
  }

  //* TODO: Evict()

 private:
  constexpr auto RemoteMR(/*id*/) -> rdma::RemoteMR* {
    return (rdma::RemoteMR*)connector_.GetReceivedData().first;
  }

  auto PollCQ() -> void {
    while (running_.load(std::memory_order_relaxed)) {
      if (auto wc = ctx_->GetCQ()->Poll(); wc.has_value()) {
        auto sw = (std::atomic_flag*)wc->wr_id;
        if (sw != nullptr) {
          sw->notify_all();
        }
      }
      util::Pause();
    }
  }

 private:
  // nomovable
  DisaggregatedCache(DisaggregatedCache&&) = delete;
  auto operator=(DisaggregatedCache&&) -> DisaggregatedCache& = delete;
  // nocopyable
  DisaggregatedCache(const DisaggregatedCache&) = delete;
  auto operator=(const DisaggregatedCache&) -> DisaggregatedCache& = delete;

 private:
  Allocator* allocator_{nullptr};
  Index* index_{nullptr};
  rdma::FixedBufferPool* buffer_pool_{nullptr};
  rdma::SimpleConnector connector_;
  rdma::ConnContext* ctx_{nullptr};
  std::thread cq_poller_;
  std::atomic_bool running_;
};

template <RemoteMemoryAllocator Allocator, HostMetaIndex Index>
class DisaggregatedCache<Allocator, Index>::Handle {
 public:
  auto IsReady() -> bool { return done_.test(std::memory_order_acquire); }
  auto Wait() -> void { done_.wait(true, std::memory_order_relaxed); }
  auto Size() -> uint32_t { return meta_.value_length; }
  auto Value() -> const void* { return buffer_; }

 public:
  // NOTICE: only the DisaggregatedCache can construct the handle.
  Handle(DisaggregatedCache* cache, const HostMeta& meta)
      : cache_(cache), meta_(meta) {}
  Handle(DisaggregatedCache* cache, HostMeta&& meta)
      : cache_(cache), meta_(std::move(meta)) {}
  ~Handle() {
    if (buffer_ != nullptr) {
      cache_->buffer_pool_->Deallocate(buffer_);
    }
  }

 private:
  constexpr static auto local_access = IBV_ACCESS_LOCAL_WRITE;

 private:
  auto AcquireBuffer() -> bool {
    buffer_ = cache_->buffer_pool_->Allocate();
    return buffer_ != nullptr;
  }
  auto AcquireBuffer(const void* data_to_send) -> bool {
    buffer_ = cache_->buffer_pool_->Allocate();
    if (buffer_ != nullptr) {
      memcpy(buffer_, data_to_send, meta_.value_length);
    }
    return buffer_ != nullptr;
  }

  auto InitiateRemoteOp(ibv_wr_opcode opcode) -> bool {
    auto mr = cache_->buffer_pool_->GetLocalMR();
    ibv_sge sge{(uint64_t)buffer_, cache_->buffer_pool_->Buffer_size(),
                mr->LKey()};
    ibv_send_wr wr;
    memset(&wr, 0, sizeof(ibv_send_wr));
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.opcode = opcode;
    wr.num_sge = 1;
    wr.sg_list = &sge;
    wr.wr.rdma.remote_addr = meta_.remote_addr;
    wr.wr.rdma.rkey = cache_->RemoteMR()->RKey();
    wr.wr_id = (uint64_t)&done_;
    return cache_->ctx_->GetQP()->PostSend(&wr) == nullptr;
  }

 private:
  // nomovable
  Handle(Handle&&) = delete;
  auto operator=(Handle&&) -> Handle& = delete;
  // nocopyable
  Handle(const Handle&) = delete;
  auto operator=(const Handle&) -> Handle& = delete;

 private:
  std::atomic_flag done_ = ATOMIC_FLAG_INIT;
  DisaggregatedCache* cache_;
  HostMeta meta_;
  void* buffer_{nullptr};

 private:
  friend class DisaggregatedCache<Allocator, Index>;
};

class RemoteDaemon {
 public:
  inline static constexpr uint32_t default_n_wr = 64;
  inline static constexpr rdma::ConnCtxInitAttr default_attr{
      .max_cqe = default_n_wr * 2,
      .max_send_wr = default_n_wr,
      .max_recv_wr = default_n_wr,
      .max_send_sge = 1,
      .max_recv_sge = 1,
      .max_inline_data = 0,
      .qp_type = IBV_QPT_RC,
      .sig_all = false,
      .use_same_cq = true,
      .use_srq = false,
  };

 public:
  RemoteDaemon() {}
  ~RemoteDaemon() {
    if (mr_ != nullptr) {
      delete mr_;
    }
    if (memory_ != nullptr) {
      util::Dealloc(memory_, size_);
    }
    if (ctx_ != nullptr) {
      delete ctx_;
    }
  };

 public:
  auto Initialize(const char* host, const char* port, uint32_t size) -> bool {
    size_ = size;
    memory_ = util::Alloc(size_);
    if (memory_ == nullptr) {
      ERROR("fail to allocate memory with size {}", size_);
      return false;
    }
    auto builder = [this](sc::rdma::ConnHandle* h, rdma_cm_id* id) -> bool {
      ctx_ = rdma::ConnContext::Builder(id, default_attr);
      mr_ = ctx_->GetPD()->RegisterMemory(memory_, size_, remote_access);
      if (mr_ == nullptr) {
        return false;
      }
      auto data = mr_->ToRemoteMR();
      TRACE("addr: {} length: {} rkey: {}", data.Address(), data.Length(),
            data.RKey());
      h->SetDataToSend(&data, sizeof(rdma::RemoteMR));
      return true;
    };
    if (not acceptor_.ListenAndAccept(host, port, builder)) {
      ERROR("fail to listen and accept connection request");
      return false;
    }
    return true;
  }
  auto Hold() -> void { acceptor_.WaitForDisconnection(); }

 private:
  static constexpr auto remote_access =
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE;

 private:
  // nocopyable
  RemoteDaemon(const RemoteDaemon&) = delete;
  auto operator=(const RemoteDaemon&) -> RemoteDaemon& = delete;
  // nomovable
  RemoteDaemon(RemoteDaemon&&) = delete;
  auto operator=(RemoteDaemon&&) -> RemoteDaemon& = delete;

 private:
  rdma::ConnContext* ctx_{nullptr};
  rdma::LocalMR* mr_{nullptr};
  void* memory_{nullptr};
  uint32_t size_{0};
  rdma::SimpleAcceptor acceptor_;
};

}  // namespace sc
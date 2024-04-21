//  Author: Ding Chen

#include "cache/remote_secondary_cache.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>

#include "memory/allocator.h"
#include "memory/memory_allocator_impl.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rsc/disaggregated_cache.hh"
#include "rsc/slab_policy.hh"
#include "util/compression.h"
#include "util/spdlogger.h"
#include "util/string_util.h"
namespace ROCKSDB_NAMESPACE {

namespace {
// A distinct pointer value for marking "puppet" cache entries
struct Puppet {
  char val[8] = "kPuppet";
};
const Puppet kPuppet{};
Cache::ObjectPtr const kPuppetObj = const_cast<Puppet*>(&kPuppet);
}  // namespace

RemoteSecondaryCache::RemoteSecondaryCache(
    const RemoteSecondaryCacheOptions& opts)
    : cache_(opts.LRUCacheOptions::MakeSharedCache()),
      cache_options_(opts),
      cache_res_mgr_(std::make_shared<ConcurrentCacheReservationManager>(
          std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
              cache_))) {
  assert(opts.addr != "" and opts.port != "" and opts.max_value_size != 0 and
         opts.threads != 0);
  DEBUG("opts.addr {} port {} mvs {} threads {}", opts.addr, opts.port,
        opts.max_value_size, opts.threads);
  std::call_once(flag, [opts]() {
    d_cache.Initialize(opts.addr.c_str(), opts.port.c_str(),
                       opts.max_value_size, opts.threads);
  });
}

RemoteSecondaryCache::~RemoteSecondaryCache() {
  assert(cache_res_mgr_->GetTotalReservedCacheSize() == 0);
}

bool RemoteSecondaryCache::Release(const Slice& key, Cache::Handle* handle,
                                   bool erase_if_last_ref) {
  auto erase = cache_->Release(handle, erase_if_last_ref);
  if (erase) {
    d_cache.Delete(key.ToString());
  }
  return erase;
}

std::unique_ptr<SecondaryCacheResultHandle> RemoteSecondaryCache::Lookup(
    const Slice& key, const Cache::CacheItemHelper* helper,
    Cache::CreateContext* create_context, bool /*wait*/, bool advise_erase,
    bool& kept_in_sec_cache) {
  assert(helper);
  TRACE("secondary lookup!");
  std::unique_ptr<SecondaryCacheResultHandle> handle;
  kept_in_sec_cache = false;
  Cache::Handle* lru_handle = cache_->Lookup(key);

  if (lru_handle == nullptr) {
    return nullptr;
  }

  void* handle_value = cache_->Value(lru_handle);
  if (handle_value == nullptr) {
    Release(key, lru_handle, /*erase_if_last_ref=*/false);
    return nullptr;
  }

  size_t handle_value_charge{0};
  handle_value_charge = cache_->GetCharge(lru_handle);

  Status s;
  Cache::ObjectPtr value{nullptr};
  size_t charge{0};
  MemoryAllocator* allocator = cache_options_.memory_allocator.get();

  PERF_COUNTER_ADD(remote_sec_cache_lookup_real_count, 1)
  auto d_handle = d_cache.Get(key.ToString());
  assert(d_handle.has_value());
  auto hd = std::move(d_handle.value());
  assert(hd->Size() == handle_value_charge);
  PERF_COUNTER_ADD(remote_sec_cache_lookup_bytes, handle_value_charge);
  hd->Wait();
  s = helper->create_cb(Slice((const char*)hd->Value(), handle_value_charge),
                        create_context, allocator, &value, &charge);
  if (!s.ok()) {
    Release(key, lru_handle, /*erase_if_last_ref=*/true);
    return nullptr;
  }

  if (advise_erase) {
    Release(key, lru_handle, /*erase_if_last_ref=*/true);
    // Insert a dummy handle.
    cache_->Insert(key, nullptr, GetHelper(), 0).PermitUncheckedError();
  } else {
    kept_in_sec_cache = true;
    Release(key, lru_handle, /*erase_if_last_ref=*/false);
  }
  handle.reset(new RemoteSecondaryCacheResultHandle(value, charge));
  return handle;
}

Status RemoteSecondaryCache::Insert(const Slice& key, Cache::ObjectPtr value,
                                    const Cache::CacheItemHelper* helper) {
  if (value == nullptr) {
    return Status::InvalidArgument();
  }

  Cache::Handle* lru_handle = cache_->Lookup(key);
  TRACE("secondary insert key {}", key.ToASCII());

  auto internal_helper = GetHelper();
  if (lru_handle == nullptr) {
    // PERF_COUNTER_ADD(Remote_sec_cache_insert_dummy_count, 1);
    // Insert a dummy handle if the handle is evicted for the first time.
    TRACE("secondary insert a dummy entry");
    return cache_->Insert(key, /*obj=*/nullptr, internal_helper,
                          /*charge=*/0);
  } else {
    // Maybe we should free the handle when insert with same key.
    Release(key, lru_handle, /*erase_if_last_ref=*/true);
  }

  size_t size = (*helper->size_cb)(value);
  PERF_COUNTER_ADD(remote_sec_cache_insert_bytes, size);
  char* buf = new char[size];
  auto s = (*helper->saveto_cb)(value, 0, size, buf);
  if (!s.ok()) {
    return s;
  }
  PERF_COUNTER_ADD(remote_sec_cache_insert_real_count, 1);
  d_cache.Set(key.ToString(), buf, size);  // TODO avoid this extra memcpy
  delete[] buf;

  // CacheAllocationPtr* buf = new CacheAllocationPtr(std::move(ptr));

  return cache_->Insert(key, kPuppetObj, &kNoopCacheItemHelper, size);
}

void RemoteSecondaryCache::Erase(const Slice& key) {
  d_cache.Delete(key.ToString());
  cache_->Erase(key);
}

Status RemoteSecondaryCache::SetCapacity(size_t capacity) {
  MutexLock l(&capacity_mutex_);
  cache_options_.capacity = capacity;
  cache_->SetCapacity(capacity);
  return Status::OK();
}

Status RemoteSecondaryCache::GetCapacity(size_t& capacity) {
  MutexLock l(&capacity_mutex_);
  capacity = cache_options_.capacity;
  return Status::OK();
}

std::string RemoteSecondaryCache::GetPrintableOptions() const {
  std::string ret;
  ret.reserve(20000);
  ret.append(cache_->GetPrintableOptions());
  return ret;
}

const Cache::CacheItemHelper* RemoteSecondaryCache::GetHelper() const {
  static const Cache::CacheItemHelper kHelper{
      CacheEntryRole::kMisc,
      [](Cache::ObjectPtr obj, MemoryAllocator* /*alloc*/) {
        delete static_cast<CacheAllocationPtr*>(obj);
        obj = nullptr;
      }};
  return &kHelper;
}

std::shared_ptr<SecondaryCache>
RemoteSecondaryCacheOptions::MakeSharedSecondaryCache() const {
  return std::make_shared<RemoteSecondaryCache>(*this);
}

Status RemoteSecondaryCache::Deflate(size_t decrease) {
  return cache_res_mgr_->UpdateCacheReservation(decrease, /*increase=*/true);
}

Status RemoteSecondaryCache::Inflate(size_t increase) {
  return cache_res_mgr_->UpdateCacheReservation(increase, /*increase=*/false);
}

}  // namespace ROCKSDB_NAMESPACE

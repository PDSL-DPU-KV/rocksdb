//  Author: Ding Chen

#include "cache/remote_secondary_cache.h"

#include <algorithm>
#include <cstdint>
#include <memory>

#include "memory/memory_allocator_impl.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/cache.h"
#include "util/compression.h"
#include "util/spdlogger.h"
#include "util/string_util.h"
namespace ROCKSDB_NAMESPACE {

RemoteSecondaryCache::RemoteSecondaryCache(
    const RemoteSecondaryCacheOptions& opts)
    : cache_(opts.LRUCacheOptions::MakeSharedCache()),
      cache_options_(opts),
      cache_res_mgr_(std::make_shared<ConcurrentCacheReservationManager>(
          std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
              cache_))) {
  DEBUG("opts.addr {} port {} mvs {} threads {}", opts.addr, opts.port,
        opts.max_value_size, opts.concurrency_hint);
  auto s = d_cache_.Initialize(opts.addr.c_str(), opts.port.c_str(),
                               opts.max_value_size, opts.concurrency_hint);
  assert(s);
}

RemoteSecondaryCache::~RemoteSecondaryCache() {
  assert(cache_res_mgr_->GetTotalReservedCacheSize() == 0);
}

std::unique_ptr<SecondaryCacheResultHandle> RemoteSecondaryCache::Lookup(
    const Slice& key, const Cache::CacheItemHelper* helper,
    Cache::CreateContext* create_context, bool /*wait*/, bool advise_erase,
    bool& kept_in_sec_cache) {
  assert(helper);
  DEBUG("secondary lookup!");
  std::unique_ptr<SecondaryCacheResultHandle> handle;
  kept_in_sec_cache = false;
  Cache::Handle* lru_handle = cache_->Lookup(key);
  num_lookups_++;
  if (lru_handle == nullptr) {
    return nullptr;
  }

  void* handle_value = cache_->Value(lru_handle);
  if (handle_value == nullptr) {
    cache_->Release(lru_handle, /*erase_if_last_ref=*/false);
    return nullptr;
  }

  CacheAllocationPtr* ptr{nullptr};
  size_t handle_value_charge{0};
  ptr = reinterpret_cast<CacheAllocationPtr*>(handle_value);
  handle_value_charge = cache_->GetCharge(lru_handle);

  MemoryAllocator* allocator = cache_options_.memory_allocator.get();

  Status s;
  Cache::ObjectPtr value{nullptr};
  size_t charge{0};
  s = helper->create_cb(Slice(ptr->get(), handle_value_charge), create_context,
                        allocator, &value, &charge);
  if (!s.ok()) {
    cache_->Release(lru_handle, /*erase_if_last_ref=*/true);
    return nullptr;
  }

  if (advise_erase) {
    cache_->Release(lru_handle, /*erase_if_last_ref=*/true);
    // Insert a dummy handle.
    cache_
        ->Insert(key, /*obj=*/nullptr, GetHelper(),
                 /*charge=*/0)
        .PermitUncheckedError();
  } else {
    kept_in_sec_cache = true;
    cache_->Release(lru_handle, /*erase_if_last_ref=*/false);
  }
  handle.reset(new RemoteSecondaryCacheResultHandle(value, charge));
  return handle;
}

Status RemoteSecondaryCache::Insert(const Slice& key, Cache::ObjectPtr value,
                                    const Cache::CacheItemHelper* helper) {
  if (value == nullptr) {
    return Status::InvalidArgument();
  }

  Cache::Handle* lru_handle = cache_->Lookup(key);  // TODO
  DEBUG("secondary insert key {}", key.ToASCII());

  auto internal_helper = GetHelper();
  if (lru_handle == nullptr) {
    // PERF_COUNTER_ADD(Remote_sec_cache_insert_dummy_count, 1);
    // Insert a dummy handle if the handle is evicted for the first time.
    DEBUG("secondary insert a dummy entry");
    return cache_->Insert(key, /*obj=*/nullptr, internal_helper,
                          /*charge=*/0);
  } else {
    // Maybe we should free the handle when insert with same key.
    cache_->Release(lru_handle, /*erase_if_last_ref=*/true);
  }

  size_t size = (*helper->size_cb)(value);

  CacheAllocationPtr ptr =
      AllocateBlock(size, cache_options_.memory_allocator.get());

  Status s = (*helper->saveto_cb)(value, 0, size, ptr.get());
  if (!s.ok()) {
    return s;
  }

  // PERF_COUNTER_ADD(Remote_sec_cache_insert_real_count, 1);
  CacheAllocationPtr* buf = new CacheAllocationPtr(std::move(ptr));
  num_inserts_++;
  DEBUG("secondary real insert");
  return cache_->Insert(key, buf, internal_helper, size);
}

void RemoteSecondaryCache::Erase(const Slice& key) { cache_->Erase(key); }

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

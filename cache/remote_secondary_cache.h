// Author: Ding Chen

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "cache/cache_reservation_manager.h"
#include "cache/lru_cache.h"
#include "memory/memory_allocator_impl.h"
#include "rocksdb/cache.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/compression.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class RemoteSecondaryCacheResultHandle : public SecondaryCacheResultHandle {
 public:
  RemoteSecondaryCacheResultHandle(Cache::ObjectPtr value, size_t size)
      : value_(value), size_(size) {}
  ~RemoteSecondaryCacheResultHandle() override = default;

  RemoteSecondaryCacheResultHandle(const RemoteSecondaryCacheResultHandle&) =
      delete;
  RemoteSecondaryCacheResultHandle& operator=(
      const RemoteSecondaryCacheResultHandle&) = delete;

  bool IsReady() override { return true; }

  void Wait() override {}

  Cache::ObjectPtr Value() override { return value_; }

  size_t Size() override { return size_; }

 private:
  Cache::ObjectPtr value_;
  size_t size_;
};

// The RemoteSecondaryCache is a concrete implementation of
// rocksdb::SecondaryCache.
//
// When a block is found from RemoteSecondaryCache::Lookup, we check whether
// there is a dummy block with the same key in the primary cache.
// 1. If the dummy block exits, we erase the block from
//    RemoteSecondaryCache and insert it into the primary cache.
// 2. If not, we just insert a dummy block into the primary cache
//    (charging the actual size of the block) and don not erase the block from
//    RemoteSecondaryCache. A standalone handle is returned to the caller.
//
// When a block is evicted from the primary cache, we check whether
// there is a dummy block with the same key in RemoteSecondaryCache.
// 1. If the dummy block exits, the block is inserted into
//    RemoteSecondaryCache.
// 2. If not, we just insert a dummy block (size 0) in RemoteSecondaryCache.
//
// Users can also cast a pointer to RemoteSecondaryCache and call methods on
// it directly, especially custom methods that may be added
// in the future.  For example -
// std::unique_ptr<rocksdb::SecondaryCache> cache =
//      NewRemoteSecondaryCache(opts);
// static_cast<RemoteSecondaryCache*>(cache.get())->Erase(key);

class RemoteSecondaryCache : public SecondaryCache {
 public:
  explicit RemoteSecondaryCache(const RemoteSecondaryCacheOptions& opts);
  ~RemoteSecondaryCache() override;

  const char* Name() const override { return "RemoteSecondaryCache"; }

  Status Insert(const Slice& key, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper) override;

  std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CacheItemHelper* helper,
      Cache::CreateContext* create_context, bool /*wait*/, bool advise_erase,
      bool& kept_in_sec_cache) override;

  bool SupportForceErase() const override { return true; }

  void Erase(const Slice& key) override;

  void WaitAll(std::vector<SecondaryCacheResultHandle*> /*handles*/) override {}

  Status SetCapacity(size_t capacity) override;

  Status GetCapacity(size_t& capacity) override;

  Status Deflate(size_t decrease) override;

  Status Inflate(size_t increase) override;

  std::string GetPrintableOptions() const override;

  size_t TEST_GetUsage() { return cache_->GetUsage(); }

  uint32_t num_inserts() { return num_inserts_; }

  uint32_t num_lookups() {return num_lookups_; }

 private:
  friend class RemoteSecondaryCacheTestBase;

  // TODO: clean up to use cleaner interfaces in typed_cache.h
  const Cache::CacheItemHelper* GetHelper() const;
  std::shared_ptr<Cache> cache_;
  RemoteSecondaryCacheOptions cache_options_;
  mutable port::Mutex capacity_mutex_;
  std::shared_ptr<ConcurrentCacheReservationManager> cache_res_mgr_;

  uint32_t num_inserts_{0};
  uint32_t num_lookups_{0};
};

}  // namespace ROCKSDB_NAMESPACE

#pragma once

#include <atomic>
#include <mutex>
#include <stdexcept>

#include "policy.hh"
#include "util/spdlogger.h"

namespace sc {

constexpr static const uint32_t cache_line_size = 64;

class SlabPolicy {
 public:
  using RMemInfo = sc::RMemRange;

 public:
  SlabPolicy(AddrType addr, SizeType size, SizeType elem_size = 8388608,
             uint32_t n_slab = 4)
      : base_(addr),
        size_(size),
        elem_size_(elem_size),
        n_slab_(n_slab),
        slab_size_(size / n_slab),
        n_elem_(slab_size_ / elem_size) {
    DEBUG("slab_size_ {} size {}", slab_size_, size);
    if (slab_size_ % elem_size != 0 or size % n_slab != 0) {
      throw std::invalid_argument(
          "invalid slab number or invalid element size");
    }
    handles_ = new SlabHandle[n_slab_];
    for (uint32_t i = 0; i < n_slab_; i++) {
      auto handle = &handles_[i];
      handle->base = base_ + i * slab_size_;
      handle->entries = new SlabEntry[n_elem_];
      for (uint32_t j = 0; j < n_elem_; j++) {
        auto entry = &handle->entries[j];
        entry->addr = handle->base + j * elem_size_;
        entry->next_free = j + 1 == n_elem_ ? nullptr : &handle->entries[j + 1];
      }
      handle->first_free = handle->entries;
    }
  }

  ~SlabPolicy() {
    if (handles_ != nullptr) {
      for (uint32_t i = 0; i < n_slab_; i++) {
        auto handle = &handles_[i];
        if (handle->entries != nullptr) {
          delete[] handle->entries;
        }
      }
      delete[] handles_;
    }
  }

 public:
  auto Allocate(SizeType n) -> std::optional<AddrType> {
    // std::scoped_lock<std::mutex> lock(m_);
    if (n > elem_size_) {
      ERROR("expected element size {} actual {}", elem_size_, n);
      throw std::runtime_error("actual size larger than element size");
    }
    auto cur_handle = &handles_[pt_idx_.fetch_add(1) % n_slab_];
    // we may need to steal some entry from other slab
    SlabEntry *next_free = nullptr;
    SlabEntry *target = nullptr;
    do {
      if (cur_handle->first_free.load(std::memory_order_relaxed) == nullptr) {
        uint32_t i = 0;
        auto origin = cur_handle;
        auto offset = cur_handle - handles_;
        do {
          i++;
          cur_handle = &handles_[(offset + i) % n_slab_];
        } while (cur_handle != origin and
                 cur_handle->first_free.load(std::memory_order_relaxed) ==
                     nullptr);
        if (cur_handle == origin) {  // if true, we meet an oom
          return std::nullopt;
        }
      }
      target = cur_handle->first_free.load(std::memory_order_acquire);
      next_free = (target == nullptr) ? nullptr : target->next_free;
    } while (not cur_handle->first_free.compare_exchange_weak(
        target, next_free, std::memory_order_relaxed,
        std::memory_order_acquire));
    used_.fetch_add(1, std::memory_order_acquire);
    target->next_free = nullptr;
    return target->addr;
  }

  auto Deallocate(AddrType addr, SizeType n) -> void {
    // std::scoped_lock<std::mutex> lock(m_);
    // if (n != elem_size_) {
    // throw std::runtime_error("invalid memory region");
    // }
    // locate which slab it belongs to.
    auto slab_idx = (addr - base_) / slab_size_;
    auto handle = &handles_[slab_idx];
    auto entry_idx = (addr - handle->base) / elem_size_;
    TRACE("addr {} slab {} entry {}", addr, slab_idx, entry_idx);
    SlabEntry *entry = &handle->entries[entry_idx];
    SlabEntry *origin = nullptr;
    do {
      origin = handle->first_free.load(std::memory_order_acquire);
      entry->next_free = origin;
    } while (not handle->first_free.compare_exchange_weak(
        origin, entry, std::memory_order_relaxed, std::memory_order_acquire));
    used_.fetch_add(-1, std::memory_order_acquire);
  }

 private:
  SlabPolicy(const SlabPolicy &) = delete;
  auto operator=(SlabPolicy &) -> SlabPolicy & = delete;
  SlabPolicy(SlabPolicy &&) = delete;
  auto operator=(const SlabPolicy &&) -> SlabPolicy & = delete;

 private:
  struct SlabEntry {
    SlabEntry *next_free;
    AddrType addr;
  };
  struct alignas(cache_line_size) SlabHandle {
    std::atomic<SlabEntry *> first_free;  // first free entry
    AddrType base;                        // base remote address
    SlabEntry *entries;                   // n slab entries
  };

 private:
  AddrType base_;
  SizeType size_;

  SizeType elem_size_;
  uint32_t n_slab_;
  SizeType slab_size_;
  uint32_t n_elem_;

  std::atomic_uint32_t pt_idx_{0};
  std::atomic_uint32_t used_{0};  // used entry number

  SlabHandle *handles_{nullptr};

  // std::mutex m_;

#ifdef ENABLE_TEST
  friend class SlabPolicyTest;
  friend class SlabAllocatorTest;
#endif
  friend struct fmt::formatter<SlabPolicy>;
};

}  // namespace sc

/// formatters
template <>
struct fmt::formatter<sc::SlabPolicy> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext &ctx) const {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const sc::SlabPolicy &s, FormatContext &ctx) const {
    fmt::format_to(ctx.out(),
                   "Base: {}, Size: {}, Elem Size: {}, Slab Size: {}\n",
                   s.base_, s.size_, s.elem_size_, s.slab_size_);
    for (uint32_t i = 0; i < s.n_slab_; i++) {
      fmt::format_to(ctx.out(), "Slab {}:", i);
      auto handle = &s.handles_[i];
      auto entry = handle->first_free.load();
      while (entry != nullptr) {
        fmt::format_to(ctx.out(), " {}", entry->addr);
        entry = entry->next_free;
      }
      fmt::format_to(ctx.out(), "\n");
    }
    return ctx.out();
  }
};

/// static checkers
static_assert(sc::PolicyType<sc::SlabPolicy>, "unsatisfied");

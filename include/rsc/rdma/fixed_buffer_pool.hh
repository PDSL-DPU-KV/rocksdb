#pragma once

#include <exception>

#include "resources.hh"
#include "rsc/utilities/memory.hh"
#include "rsc/utilities/misc.hh"

namespace sc::rdma {

constexpr static const uint32_t cache_line_size = 64;

class FixedBufferPool {
 public:
  constexpr static const int local_access = IBV_ACCESS_LOCAL_WRITE;
  constexpr static const int remote_read_access =
      local_access | IBV_ACCESS_REMOTE_READ;
  constexpr static const int remote_write_access =
      local_access | IBV_ACCESS_REMOTE_WRITE;
  constexpr static const int remote_access =
      local_access | IBV_ACCESS_REMOTE_READ | IB_UVERBS_ACCESS_REMOTE_WRITE;
  constexpr static const uint32_t default_handle_number = 4;

 public:
  FixedBufferPool(uint32_t n_buffer, uint32_t buffer_size,
                  uint32_t n_handle = default_handle_number)
      : n_buffer_(n_buffer),
        buffer_size_(util::AlignUp(buffer_size, sizeof(Entry) * 2)),
        n_handle_(n_handle),
        handle_size_(n_buffer_ * buffer_size_ / n_handle),
        handles_(n_handle_) {
    if (n_buffer % n_handle != 0) {
      throw std::invalid_argument(
          "the number of buffers shall be divisible by the number of handles");
    }
    TRACE("n buffer: {}, buffer size: {}, n handle: {}, handle size: {}",
          n_buffer_, buffer_size_, n_handle_, handle_size_);
    uint64_t total_size = n_buffer_ * buffer_size_;
    memory_ = util::Alloc(total_size);
    if (memory_ == nullptr) {
      ERROR("fail to allocator memory with size {} because \"{}\"", total_size,
            util::ErrnoString());
      throw std::runtime_error("no enough memory or other error");
    }
    auto p = (char*)(memory_);
    auto n_buffer_per_handle = n_buffer_ / n_handle_;
    for (uint32_t i = 0; i < n_handle_; i++) {
      auto& handle = handles_[i];
      auto entries = (Entry*)(p + (handle_size_ * i));
      handle.first_free = entries;
      handle.offset = i;
      for (uint32_t j = 0; j < n_buffer_per_handle; j++) {
        entries[j].next_free =
            ((j == n_buffer_per_handle - 1) ? nullptr : &entries[j + 1]);
      }
    }
  }
  ~FixedBufferPool() {
    if (mr_ != nullptr) {
      delete mr_;
    }
    if (memory_ != nullptr) {
      util::Dealloc(memory_, n_buffer_ * buffer_size_);
    }
  }

 public:
  auto GetLocalMR() const -> LocalMR* { return mr_; }
  auto RegisterBuffer(PD* pd, int access_flag = local_access) -> bool {
    mr_ = pd->RegisterMemory(memory_, n_buffer_ * buffer_size_, access_flag);
    return mr_ != nullptr;
  }

 public:
  auto Allocate() -> void* {
    auto handle =
        &handles_[pt_idx_.fetch_add(1, std::memory_order::acquire) % n_handle_];
    Entry* next_free = nullptr;
    Entry* target = nullptr;
    do {
      if (handle->first_free.load(std::memory_order_relaxed) == nullptr) {
        uint32_t i = handle->offset;
        uint32_t origin = i;
        do {
          handle = &handles_[(++i) % n_handle_];
        } while (i != origin and
                 handle->first_free.load(std::memory_order_relaxed) == nullptr);
        if (i == origin) {
          return nullptr;
        }
      }
      target = handle->first_free.load(std::memory_order_acquire);
      next_free = (target == nullptr) ? nullptr : target->next_free;
    } while (not handle->first_free.compare_exchange_weak(
        target, next_free, std::memory_order_relaxed,
        std::memory_order_acquire));
    used_.fetch_add(1, std::memory_order::acquire);
    target->next_free = nullptr;
    return target;
  }

  auto Deallocate(void* p) -> void {
    auto handle_idx = ((char*)p - (char*)memory_) / handle_size_;
    auto handle = &handles_[handle_idx];
    auto entry = (Entry*)p;
    auto origin = (Entry*)nullptr;
    do {
      origin = handle->first_free.load(std::memory_order_acquire);
      entry->next_free = origin;
    } while (not handle->first_free.compare_exchange_weak(
        origin, entry, std::memory_order_relaxed, std::memory_order_acquire));
    used_.fetch_add(-1, std::memory_order_acquire);
  }

  uint32_t Buffer_size() { return buffer_size_; }

 private:
  // nomovable
  FixedBufferPool(FixedBufferPool&&) = delete;
  auto operator=(FixedBufferPool&&) -> FixedBufferPool& = delete;

  // nocopyable
  FixedBufferPool(const FixedBufferPool&) = delete;
  auto operator=(const FixedBufferPool&) -> FixedBufferPool& = delete;

 private:
  struct Entry {
    Entry* next_free;
  };
  // avoid false positive
  struct alignas(cache_line_size) Handle {
    std::atomic<Entry*> first_free;
    uint32_t offset;
  };

 private:
  uint32_t n_buffer_;
  uint32_t buffer_size_;
  uint32_t n_handle_;
  uint32_t handle_size_;
  std::atomic_uint32_t pt_idx_{0};
  std::atomic_uint32_t used_{0};
  LocalMR* mr_{nullptr};
  void* memory_{nullptr};
  std::vector<Handle> handles_{};
};

}  // namespace sc::rdma
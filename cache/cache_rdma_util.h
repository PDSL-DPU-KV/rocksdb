// Author: Ding Chen

#ifndef _CACHE_RDMA_UTIL_H_
#define _CACHE_RDMA_UTIL_H_

#include <atomic>

#if defined(__x86_64__)
#include <emmintrin.h>
#define PAUSE _mm_pause()
#elif defined(__aarch64__)
#define PAUSE asm volatile("yield" ::: "memory")
#else
#define PAUSE
#endif

inline auto pause_() -> void { PAUSE; }

class SpinWait {
 public:
  SpinWait() = default;
  ~SpinWait() = default;

 public:
  auto wait() -> void {
    while (not b.load(std::memory_order_acquire)) {
      pause_();
    }
  }

  auto notify() -> void { b.store(true, std::memory_order_release); }

  auto reset() -> void { b = false; }

 private:
  std::atomic_bool b{false};
};

#endif
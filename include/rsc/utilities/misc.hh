#pragma once

#if defined(__x86_64__)
#include <emmintrin.h>
#define PAUSE _mm_pause()
#elif defined(__aarch64__)
#define PAUSE asm volatile("yield" ::: "memory")
#else
#define PAUSE
#endif

#include <cstdint>
#include <cassert>

namespace sc::util {

inline auto Pause() -> void { PAUSE; }

inline auto AlignUp(uint64_t x, uint64_t n) -> uint64_t {
  assert(not(n & (n - 1)));
  return (x + n - 1) & ~(n - 1);
}

}  // namespace sc::util
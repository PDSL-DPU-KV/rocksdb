#pragma once

#include <atomic>
#include <condition_variable>

#if defined(__x86_64__)
#include <emmintrin.h>
#define PAUSE _mm_pause()
#elif defined(__aarch64__)
#define PAUSE asm volatile("yield" ::: "memory")
#else
#define PAUSE
#endif

inline auto pause_() -> void { PAUSE; }

class BlockWait {
 public:
  BlockWait() = default;
  ~BlockWait() = default;

 public:
  auto wait() -> void {
    std::unique_lock g(mu);
    cv.wait(g, [this]() { return ok; });
  }
  auto notify() -> void {
    std::unique_lock g(mu);
    ok = true;
    cv.notify_all();
  }

 private:
  std::condition_variable cv;
  std::mutex mu;
  bool ok{false};
};

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

 private:
  std::atomic_bool b{false};
};

class Spinlock {
 public:
  Spinlock() = default;
  ~Spinlock() = default;

 public:
  auto lock() -> void {
    while (true) {
      if (not b.exchange(true, std::memory_order_acquire)) {
        return;
      }
      while (b.load(std::memory_order_relaxed)) {
        pause_();
      }
    }
  }

  auto try_lock() -> bool {
    return not b.load(std::memory_order_relaxed) and
           not b.exchange(true, std::memory_order_acquire);
  }

  auto unlock() -> void { b.store(false, std::memory_order_release); }

 private:
  std::atomic_bool b{false};
};
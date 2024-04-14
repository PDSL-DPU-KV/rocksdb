#pragma once

#include <unistd.h>

#include <cstdint>

#include "rsc/utilities/errno.hh"
#include "rsc/utilities/literal.hh"
#include "util/spdlogger.h"

using namespace sc::util::literal;

namespace sc::util {

inline auto align(uint64_t x, uint64_t base) -> uint64_t {
  return (((x) + (base)-1) & ~(base - 1));
}

#ifdef USE_HUGEPAGE
#include <sys/mman.h>
constexpr static uint64_t huge_page_size = 2_MB;
#endif

inline auto Alloc(uint32_t len) -> void * {
#ifdef USE_HUGEPAGE
  auto p = mmap(nullptr, align(len, huge_page_size), PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  return (p == MAP_FAILED) ? nullptr : p;
#else
  return new char[len];
#endif
}

#ifdef USE_HUGEPAGE
inline auto Dealloc(void *p, uint32_t len) -> void {
  if (auto rc = munmap(p, align(len, huge_page_size)); rc != 0) {
    ERROR("fail to unmap {} because \"{}\"", p, ErrnoString());
  }
#else
inline auto Dealloc(void *p, [[maybe_unused]] uint32_t len) -> void {
  delete[] (char *)p;
#endif
}

}  // namespace sc::util
#pragma once

// #include <absl/container/flat_hash_set.h>
#include <fmt/core.h>
#include <fmt/format.h>

#include <mutex>
#include <unordered_set>

#include "first_fit_policy.hh"
#include "policy.hh"
#include "slab_policy.hh"
#include "util/spdlogger.h"

namespace sc {

template <PolicyType T>
class Allocator {
 public:
  template <typename... Args>
  Allocator(Args &&...args) : p_(std::make_unique<T>(std::move(args)...)) {}
  Allocator(std::unique_ptr<T> p) : p_(std::move(p)) {}
  ~Allocator() {}

 public:
  auto Allocate(SizeType size) -> std::optional<AddrType>;

  auto Deallocate(AddrType addr, SizeType size) -> void;

  auto PolicyType() -> std::string { return typeid(T).name(); }

 private:
  Allocator(const Allocator &) = delete;
  auto operator=(const Allocator &) -> Allocator & = delete;
  Allocator(Allocator &&) = delete;
  auto operator=(Allocator &&) -> Allocator & = delete;

 private:
  std::mutex mu_;
  std::unordered_set<RMemRange> occupied_;
  std::unique_ptr<T> p_;

#ifdef ENABLE_TEST
  friend class FirstFitAllocatorTest;
#endif
  friend class fmt::formatter<Allocator<T>>;
};

}  // namespace sc

/// helper functions

template <sc::PolicyType T>
struct fmt::formatter<sc::Allocator<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext &ctx) const {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const sc::Allocator<T> &a, FormatContext &ctx) const {
    return fmt::format_to(ctx.out(), "free list: {}\noccupied list: {}",
                          *a.p_.get(), fmt::join(a.occupied_, " "));
  }
};

/// implementations
namespace sc {

template <PolicyType T>
auto Allocator<T>::Allocate(SizeType size) -> std::optional<AddrType> {
  std::lock_guard<std::mutex> l(mu_);
  auto o = p_->Allocate(size);
  if (o.has_value()) {
    auto [_, ok] = occupied_.insert(RMemRange{o.value(), size});  //
    assert(ok);
    TRACE("allocate address {} size {}", o.value(), size);
  }
  return o;
}

template <PolicyType T>
auto Allocator<T>::Deallocate(AddrType addr, SizeType size) -> void {
  std::lock_guard<std::mutex> l(mu_);
  if (auto iter = occupied_.find({addr, size}); iter != occupied_.end()) {
    TRACE("deallocate address {} size {}", addr, size);
    p_->Deallocate(addr, size);
    occupied_.erase(iter);
    return;
  }
  ERROR("invalid memory address {} with size {}", (void *)addr, size);
  throw std::runtime_error("invalid memory address");
}

}  // namespace sc

/// instantiation
namespace sc {

using FirstFitAllocator = Allocator<FirstFitPolicy>;
using SlabAllocator = Allocator<SlabPolicy>;

}
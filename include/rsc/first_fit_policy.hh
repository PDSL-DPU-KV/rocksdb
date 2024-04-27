#pragma once

#include <fmt/format.h>

#include <list>
#include <string>

#include "policy.hh"
#include "util/spdlogger.h"

namespace sc {

class FirstFitPolicy {
 public:
  FirstFitPolicy(AddrType addr, SizeType size, SizeType value_size) : addr_(addr), size_(size) {
    DEBUG("initialize with address {} size {}", addr, size);
    free_list_.push_back({addr, addr + size});
  }
  ~FirstFitPolicy() {}

  auto Allocate(SizeType n) -> std::optional<AddrType> {
    for (auto iter = free_list_.begin(); iter != free_list_.end(); iter++) {
      if (n <= iter->Length()) {  // found first fit
        auto addr = iter->lower;
        iter->lower += n;
        if (iter->Length() == 0) {
          free_list_.erase(iter);
        }
        used_ += n;
        // DEBUG("{}", gen_debug());
        return {addr};
      }
    }
    return {};  // oom
  }

  auto Deallocate(AddrType addr, SizeType n) -> void {
    auto cur = RMemRange{addr, addr + n};
    auto less = [](const RMemRange &lhs, const RMemRange &rhs) -> bool {
      return lhs.lower < rhs.lower;
    };
    auto iter =
        std::lower_bound(free_list_.begin(), free_list_.end(), cur, less);
    auto prev_iter = std::make_reverse_iterator(iter);
    bool connect_with_next =
        iter != free_list_.end() and cur.upper == iter->lower;
    bool connect_with_prev =
        prev_iter != free_list_.rend() and prev_iter->upper == cur.lower;
    if (connect_with_next and connect_with_prev) {
      prev_iter->upper = iter->upper;
      free_list_.erase(iter);
    } else if (connect_with_prev) {
      prev_iter->upper = cur.upper;
    } else if (connect_with_next) {
      iter->lower = cur.lower;
    } else {
      free_list_.insert(iter, std::move(cur));
    }
    used_ -= n;

    // DEBUG("{}", gen_debug());
  }

 private:
  std::string gen_debug() {
    std::string debug = "free_list: total " + std::to_string(size_) + ", ";
    for (auto &t : free_list_) {
      debug += "(" + std::to_string(t.lower - addr_) + ", " +
               std::to_string(t.upper - addr_) + "), ";
    }
    return debug;
  };

  FirstFitPolicy(const FirstFitPolicy &) = delete;
  auto operator=(FirstFitPolicy &) -> FirstFitPolicy & = delete;
  FirstFitPolicy(FirstFitPolicy &&) = delete;
  auto operator=(const FirstFitPolicy &&) -> FirstFitPolicy & = delete;

 private:
  std::list<RMemRange> free_list_;
  [[maybe_unused]] AddrType addr_;
  [[maybe_unused]] SizeType size_;
  SizeType used_{0};

#ifdef ENABLE_TEST
  friend class FirstFitPolicyTest;
  friend class FirstFitAllocatorTest;
#endif
  friend struct fmt::formatter<FirstFitPolicy>;
};

}  // namespace sc

/// formatters
template <>
struct fmt::formatter<sc::FirstFitPolicy> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext &ctx) const {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const sc::FirstFitPolicy &p, FormatContext &ctx) const {
    return fmt::format_to(ctx.out(), "{}", fmt::join(p.free_list_, " -> "));
  }
};

/// static checkers
static_assert(sc::PolicyType<sc::FirstFitPolicy>, "unsatisfied");

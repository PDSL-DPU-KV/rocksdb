#pragma once

#include <fmt/format.h>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>

namespace sc {

using AddrType = std::uint64_t;
using SizeType = std::size_t;

// clang-format off
template <typename T>
concept PolicyType = requires(T p, SizeType n, AddrType addr) {
  { p.Allocate(n) } -> std::same_as<std::optional<AddrType>>;
  { p.Deallocate(addr, n) } -> std::same_as<void>;
};
// clang-format on

struct RMemRange {
  AddrType lower;  // start addr
  AddrType upper;  // end addr

  auto Length() -> SizeType { return upper - lower; }
};

}  // namespace sc

/// helpers

template <>
struct std::hash<sc::RMemRange> {
  auto operator()(const sc::RMemRange &info) const -> size_t {
    return info.lower;
  }
};

template <>
struct std::equal_to<sc::RMemRange> {
  auto operator()(const sc::RMemRange &lhs, const sc::RMemRange &rhs) const
      -> bool {
    return lhs.lower == rhs.lower;
  }
};

/// formaters
template <>
struct fmt::formatter<sc::RMemRange> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext &ctx) const {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const sc::RMemRange &info, FormatContext &ctx) const {
    return fmt::format_to(ctx.out(), "({}, {})", info.lower, info.upper);
  }
};
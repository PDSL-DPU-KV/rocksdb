#pragma once

#include <fmt/format.h>

#include <chrono>
#include <vector>

namespace sc::util {

using seconds = std::chrono::seconds;
using milliseconds = std::chrono::milliseconds;
using microseconds = std::chrono::microseconds;
using nanoseconds = std::chrono::nanoseconds;
using clock = std::chrono::high_resolution_clock;  // monotonic clock
using time_point = std::chrono::time_point<clock>;
template <typename T>
using duration = std::chrono::duration<T>;

class TraceTimer {
 public:
  TraceTimer() = default;
  ~TraceTimer() = default;

 public:
  auto Tick() -> void { trace_.emplace_back(clock::now()); }

  auto Reset() -> void { trace_.resize(0); }

  auto NPeriods() const -> size_t { return trace_.size() == 1 ? 0 : trace_.size() - 1; }

  template <typename T>
  auto Period(uint32_t i) const -> uint64_t {
    return std::chrono::duration_cast<T>(trace_.at(i + 1) - trace_.at(i)).count();
  }

  template <typename T>
  auto Elapsed() const -> uint64_t {
    return std::chrono::duration_cast<T>(trace_.back() - trace_.front()).count();
  }

 private:
  std::vector<time_point> trace_;

  friend struct fmt::formatter<TraceTimer>;
};

}  // namespace sc::util

template <>
struct fmt::formatter<sc::util::TraceTimer> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext &ctx) const {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const sc::util::TraceTimer &t, FormatContext &ctx) const {
    format_to(ctx.out(), "trace in us:");
    for (uint32_t i = 0; i < t.NPeriods(); i++) {
      format_to(ctx.out(), " {}", t.Period<sc::util::microseconds>(i));
    }
    return ctx.out();
  }
};

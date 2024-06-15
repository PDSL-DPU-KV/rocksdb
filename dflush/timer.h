#pragma once

#include <chrono>
#include <iostream>
#include <vector>

using timer_clock = std::chrono::high_resolution_clock; // monotonic clock
using time_point = std::chrono::time_point<timer_clock>;
template <typename T> using duration = std::chrono::duration<T>;

class Timer {
public:
    Timer() = default;
    ~Timer() = default;

public:
    auto begin() -> void { begin_ = timer_clock::now(); }
    auto end() -> void { end_ = timer_clock::now(); }
    auto reset() -> void { begin_ = end_ = {}; }
    auto elapsed() { return end_ - begin_; }

private:
    time_point begin_{};
    time_point end_{};
};

#if defined(__x86_64__)

inline auto tsc() -> uint64_t {
    uint64_t a, d;
    asm volatile("rdtsc" : "=a"(a), "=d"(d));
    return (d << 32) | a;
}

inline auto tscp() -> uint64_t {
    uint64_t a, d;
    asm volatile("rdtscp" : "=a"(a), "=d"(d));
    return (d << 32) | a;
}

/// Notice:
///    Before you use this timer, you shall check your cpu have constant_tsc
///    flag by using command: `cat /proc/cpuinfo | grep constant_tsc`.
class ClockTimer {
public:
    ClockTimer() = default;
    ~ClockTimer() = default;

public:
    auto begin() -> void { begin_ = tscp(); }
    auto end() -> void { end_ = tscp(); }
    auto reset() -> void { begin_ = end_ = 0; }
    auto elapsed() -> uint64_t { return end_ - begin_; }

private:
    uint64_t begin_{};
    uint64_t end_{};
};

#endif

class TraceTimer {
public:
    TraceTimer() = default;
    ~TraceTimer() = default;

public:
    auto tick() -> void { trace_.emplace_back(timer_clock::now()); }

    auto reset() -> void { trace_.resize(0); }

    auto nPeriod() -> size_t {
        uint32_t n = trace_.size();
        return n > 0 ? n - 1 : 0;
    }

    auto period(uint32_t i) { return trace_.at(i + 1) - trace_.at(i); }

    auto elapsed() { return trace_.back() - trace_.front(); }

    auto print_all() {
        for (uint32_t i = 0; i < nPeriod(); i++) {
            // std::cerr << period(i) << " ";
        }
        // std::cerr << elapsed() << std::endl;
    }

private:
    std::vector<time_point> trace_;
};
#pragma once

#include <new>

class Noncopyable {
 protected:
  Noncopyable() noexcept = default;
  ~Noncopyable() noexcept = default;

 public:
  Noncopyable(const Noncopyable &) = delete;
  auto operator=(const Noncopyable &) -> Noncopyable & = delete;
};

class Nonmovable {
 protected:
  Nonmovable() noexcept = default;
  ~Nonmovable() noexcept = default;

 public:
  Nonmovable(Noncopyable &&) = delete;
  auto operator=(Noncopyable &&) -> Noncopyable & = delete;
};

#ifdef __cpp_lib_hardware_interference_size
using std::hardware_constructive_interference_size;
using std::hardware_destructive_interference_size;
#else
constexpr std::size_t hardware_constructive_interference_size = 64;
constexpr std::size_t hardware_destructive_interference_size = 64;
#endif

constexpr std::size_t cache_line_size = hardware_destructive_interference_size;
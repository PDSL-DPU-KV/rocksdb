#pragma once

#include <chrono>
#include <iterator>
#include <random>
#include <utility>

namespace sc::util::rng {

class RandomIntGenerator {
 public:
  RandomIntGenerator(uint64_t lower, uint64_t upper)
      : seed_(std::chrono::high_resolution_clock::now()
                  .time_since_epoch()
                  .count()),
        gen_(seed_),
        dist_(lower, upper) {}
  ~RandomIntGenerator();

 public:
  auto Get() -> uint64_t { return dist_(gen_); }

 private:
  RandomIntGenerator(const RandomIntGenerator&) = delete;
  auto operator=(const RandomIntGenerator&) -> RandomIntGenerator& = delete;
  RandomIntGenerator(RandomIntGenerator&&) = delete;
  auto operator=(RandomIntGenerator&&) -> RandomIntGenerator& = delete;

 private:
  uint64_t seed_;
  std::mt19937_64 gen_;
  std::uniform_int_distribution<std::mt19937::result_type> dist_;
};

}  // namespace sc::util::rng
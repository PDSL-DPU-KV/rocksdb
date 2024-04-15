#pragma once

#include <concepts>
#include <mutex>
#include <optional>

#ifdef USE_FLAT_HASH_SET
#include <absl/container/flat_hash_set.h>
#else
#include <unordered_set>
#endif

#include "util/spdlogger.h"

//! Well, this is a quite simple sharded set.
//! This is specialized for our requirements:
//!     1. value is extremely small, which can be appended to the key or be
//!     stored along with key
//!     2. can bear with std::mutex.

namespace sc {

template <typename Key, typename Value>
concept ShardedMapItemPolicy = requires(Key t) {
  requires std::regular_invocable<std::hash<Key>, Key>;
  requires std::regular_invocable<std::equal_to<Key>, Key, Key>;
  requires std::is_default_constructible_v<Value>;
  requires std::is_copy_constructible_v<Value>;
  requires std::is_move_constructible_v<Value>;
};

// clang-format off
template <typename Key, typename Value>
requires ShardedMapItemPolicy<Key, Value>
// clang-format on
class ShardedMap {
  struct Item {
    Key key;
    Value value;
  };

  struct ItemHasher {
    auto operator()(const Item& item) const -> size_t {
      return std::hash<Key>()(item.key);
    }
  };

  struct ItemPredictor {
    auto operator()(const Item& lhs, const Item& rhs) const -> bool {
      return std::equal_to<Key>()(lhs.key, rhs.key);
    }
  };

 public:
  ShardedMap(uint8_t n_shards) : n_shards_(n_shards) {
    shards_ = new std::decay_t<decltype(shards_[0])>[n_shards_];
    latches_ = new std::decay_t<decltype(latches_[0])>[n_shards_];
  }
  ~ShardedMap() {
    delete[] shards_;
    delete[] latches_;
  }

 public:
  auto Set(Key&& key, Value&& value) -> void {
    auto idx = std::hash<Key>()(key) % n_shards_;
    std::lock_guard<std::mutex> g(latches_[idx]);
    auto [_, ok] = shards_[idx].insert(Item{std::move(key), std::move(value)});
    if (not ok) {
      ERROR("? duplicate item");
    }
  }
  auto Set(const Key& key, const Value& value) -> void {
    auto idx = std::hash<Key>()(key) % n_shards_;
    std::lock_guard<std::mutex> g(latches_[idx]);
    auto [_, ok] = shards_[idx].insert(Item{key, value});
    if (not ok) {
      ERROR("? duplicate item");
    }
  }
  auto Get(const Key& key) -> std::optional<Value> {
    auto idx = std::hash<Key>()(key) % n_shards_;
    std::lock_guard<std::mutex> g(latches_[idx]);
    auto iter = shards_[idx].find(Item{key, {}});
    return (iter == shards_[idx].end()) ? std::nullopt
                                        : std::optional<Value>(iter->value);
  }
  auto Delete(const Key& key) -> std::optional<Value> {
    auto idx = std::hash<Key>()(key) % n_shards_;
    std::lock_guard<std::mutex> g(latches_[idx]);
    auto iter = shards_[idx].find(Item{key, {}});
    if (iter == shards_[idx].end()) {
      return std::nullopt;
    }
    auto&& value = std::make_move_iterator(iter)->value;
    shards_[idx].erase(iter);
    return std::optional{std::move(value)};
  }
  auto Size() -> uint32_t {
    uint32_t size = 0;
    for (uint8_t i = 0; i < n_shards_; i++) {
      std::lock_guard<std::mutex> g(latches_[i]);
      size += shards_[i].size();
    }
    return size;
  }

 private:
  // nocopyable
  ShardedMap(const ShardedMap&) = delete;
  auto operator=(const ShardedMap&) -> ShardedMap& = delete;
  // nomovable
  ShardedMap(ShardedMap&&) = delete;
  auto operator=(ShardedMap&&) -> ShardedMap& = delete;

 private:
#ifdef USE_FLAT_HASH_SET
  absl::flat_hash_set<Item, ItemHasher, ItemPredictor>* shards_;
#else
  std::unordered_set<Item, ItemHasher, ItemPredictor>* shards_;
#endif
  std::mutex* latches_;
  uint8_t n_shards_;
};

}  // namespace sc

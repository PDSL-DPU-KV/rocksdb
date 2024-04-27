#include <gflags/gflags.h>

#include <algorithm>
#include <chrono>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>

#include "rsc/allocator.hh"
#include "rsc/disaggregated_cache.hh"
using namespace sc;
using namespace std::chrono_literals;

auto FillRandom(std::string& s) -> void {
  static constexpr auto chars =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  thread_local static auto rng = std::mt19937{std::random_device{}()};
  thread_local static auto dist =
      std::uniform_int_distribution{{}, std::strlen(chars) - 1};
  std::generate_n(s.begin(), s.length(), [&]() { return chars[dist(rng)]; });
}

DEFINE_int32(n_loop, 10000, "bench loop number");
DEFINE_int32(n_thread, 1, "bench worker thread number");
DEFINE_string(addr, "192.168.200.53", "server address");
DEFINE_string(port, "10086", "server port");
DEFINE_int32(value_size, 1_KB, "value size");
DEFINE_int32(key_size, 128, "key size");

auto main(int argc, char* argv[]) -> int {
  spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern("%t %+");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto cache = sc::DisaggregatedCache<SlabAllocator>();
  if (not cache.Initialize(FLAGS_addr.c_str(), FLAGS_port.c_str(),
                           FLAGS_value_size, FLAGS_n_thread)) {
    return -1;
  }

  std::mutex mutex_;
  std::unordered_map<std::string, std::string> kvs;

  auto fn = [&]() {
    auto key = std::string(FLAGS_key_size, '\0');
    auto value = std::string(FLAGS_value_size, '\0');
    long double avg_lat = 0;
    for (int32_t i = 0; i < FLAGS_n_loop; i++) {
    redo:
      FillRandom(key);
      FillRandom(value);
      {
        std::scoped_lock<std::mutex> lock(mutex_);
        if (kvs.contains(key)) goto redo;
        kvs[key] = value;
      }
      auto tik = std::chrono::high_resolution_clock::now();
      if (not cache.Set(key, value.data(), value.length())) {
        CRITICAL("too fast");
        std::this_thread::sleep_for(1ms);
        continue;
      }
      auto tok = std::chrono::high_resolution_clock::now();
      avg_lat +=
          std::chrono::duration_cast<std::chrono::microseconds>(tok - tik)
              .count();
    }
    INFO("{}", avg_lat / FLAGS_n_loop);
  };

  std::vector<std::thread> workers;

  for (int32_t i = 0; i < FLAGS_n_thread; i++) {
    workers.push_back(std::thread(fn));
  }

  for (int32_t i = 0; i < FLAGS_n_thread; i++) {
    workers[i].join();
  }

  auto rng = std::mt19937{std::random_device{}()};
  std::uniform_int_distribution<> dist(0, 1);
  std::unordered_map<std::string, std::string> kvs_d;
  for (auto& [k, v] : kvs) {
    auto t = dist(rng);
    if (t == 0) {
      kvs_d[k] = v;
      cache.Delete(k);
    }
  }

  for (auto& [k, v] : kvs) {
    auto handle = cache.Get(k);
    if (kvs_d.contains(k)) {
      assert(not handle.has_value());
      continue;
    }
    assert(handle.has_value());
    handle.value()->Wait();
    auto va = std::string((const char*)handle.value()->Value(),
                          handle.value()->Size());
    if (va != v) {
      DEBUG("v {}, va {}", v, va);
    }
    assert(v == va);
  }

  return 0;
}

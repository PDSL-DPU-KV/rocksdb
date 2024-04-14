#include <gflags/gflags.h>

#include <algorithm>
#include <random>

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
DEFINE_int32(value_size, 4_KB, "value size");
DEFINE_int32(key_size, 128, "key size");

auto main(int argc, char* argv[]) -> int {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto cache = DisaggregatedCache<FirstFitAllocator>();
  if (not cache.Initialize(FLAGS_addr.c_str(), FLAGS_port.c_str(),
                           FLAGS_value_size, FLAGS_n_thread)) {
    return -1;
  }

  auto fn = [&]() {
    auto key = std::string(FLAGS_key_size, '\0');
    auto value = std::string(FLAGS_value_size, '\0');
    long double avg_lat = 0;
    for (int32_t i = 0; i < FLAGS_n_loop; i++) {
      FillRandom(key);
      FillRandom(value);
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

  return 0;
}

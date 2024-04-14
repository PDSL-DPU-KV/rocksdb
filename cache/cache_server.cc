#include <gflags/gflags.h>

#include "rsc/disaggregated_cache.hh"
#include "spdlog/cfg/env.h"
#include "util/spdlogger.h"

using namespace sc;

DEFINE_int32(size, 1_GB, "remote memory region size");
DEFINE_string(addr, "192.168.200.53", "server address");
DEFINE_string(port, "10086", "server port");

auto main(int argc, char* argv[]) -> int {
  spdlog::cfg::load_env_levels();
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto daemon = RemoteDaemon();
  if (not daemon.Initialize(FLAGS_addr.c_str(), FLAGS_port.c_str(),
                            FLAGS_size)) {
    return -1;
  }
  daemon.Hold();

  return 0;
}
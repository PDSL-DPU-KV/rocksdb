#include <gflags/gflags.h>
#include <csignal>

#include "cache_rdma.h"
#include "util/spdlogger.h"

DEFINE_string(addr, "192.168.200.53", "server address");
DEFINE_string(port, "10086", "server port");

cache_rdma_handle h;

void set_signal(sigset_t* sigset) {
  sigemptyset(sigset);
  sigaddset(sigset, SIGINT);
  pthread_sigmask(SIG_BLOCK, sigset, nullptr);
}

int main(int argc, char** argv) {
  int signum = -1;
  sigset_t sigset;
  set_signal(&sigset);

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  INFO("addr: {}, port: {}", FLAGS_addr, FLAGS_port);

  cache_rdma_init(&h, 1);
  cache_rdma_listen(h, FLAGS_addr.c_str(), FLAGS_port.c_str());
  sigwait(&sigset, &signum);

  

  INFO("Got signal: {}\n", signum);
  cache_rdma_fini(h);

  return 0;
}
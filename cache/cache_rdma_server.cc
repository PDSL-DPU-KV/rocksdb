#include <csignal>
#include <cstdio>

#include "cache_rdma.h"

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

  cache_rdma_init(&h, 1);
  cache_rdma_listen(h, argv[1], argv[2]);
  sigwait(&sigset, &signum);
  printf("Got signal: %d\n", signum);
  cache_rdma_fini(h);

  return 0;
}
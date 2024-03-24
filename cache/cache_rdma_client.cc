#include <gflags/gflags.h>
#include <spdlog/spdlog.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "cache_rdma.h"
#include "cache_rdma_util.h"

DEFINE_string(addr, "192.168.200.53", "server address");
DEFINE_string(port, "10086", "server port");

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void init_allocator(connection_handle c, void *arg) {
  // printf("init allocator!\n");
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  spdlog::info("addr: {}, port: {}", FLAGS_addr, FLAGS_port);

  cache_rdma_handle h;
  cache_rdma_init(&h, 1);
  connection_handle c = cache_rdma_connect(
      h, FLAGS_addr.c_str(), FLAGS_port.c_str(), init_allocator, NULL);

  sleep(3);

  // SpinWait sw;
  // auto sw_cb = [](connection_handle, void *cb_args) {
  //   ((SpinWait *)cb_args)->notify();
  // };
  // // read after write
  // cache_rdma_mr send_mr = cache_rdma_alloc_buf(h, 4096);
  // spdlog::info("alloc ok!");
  // memcpy(cache_rdma_get_buf_addr(send_mr), "hello", 5);
  // uint64_t start_time = NowMicros();
  // // for (int i = 0; i < 1000; i++) {
  // cache_rdma_op(c, RDMA_WRITE, send_mr, 4096, sw_cb, &sw);
  // sw.wait();
  // sw.reset();
  // spdlog::info("write ok!");
  // // }
  // uint64_t end_time = NowMicros();
  // // printf("rdma write: %lu us\n", end_time - start_time);
  // cache_rdma_mr recv_mr = cache_rdma_alloc_buf(h, 4096);
  // cache_rdma_op(c, RDMA_READ, recv_mr, 0, sw_cb, &sw);
  // sw.wait();
  // sw.reset();
  // spdlog::info("read ok!");
  // // printf("%.5s\n", cache_rdma_get_buf_addr(recv_mr));
  // // write with offset
  // memcpy(cache_rdma_get_buf_addr(send_mr), "world", 5);
  // cache_rdma_op(c, RDMA_WRITE, send_mr, 4096, sw_cb, &sw);
  // sw.wait();
  // sw.reset();
  // cache_rdma_op(c, RDMA_READ, recv_mr, 4096, sw_cb, &sw);
  // sw.wait();
  // sw.reset();
  // // printf("%.5s\n", cache_rdma_get_buf_addr(recv_mr));
  // cache_rdma_free_mr(send_mr);
  // cache_rdma_free_mr(recv_mr);

  cache_rdma_disconnect(c);
  cache_rdma_fini(h);

  return 0;
}
#ifndef RDMA_COMMON_H
#define RDMA_COMMON_H

#include <infiniband/verbs.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>

#include <cstdint>
#include <cstdio>

#define MAX_BUF_SIZE (16 * 1024)
#define MAX_Q_NUM (4096U)
#define TEST_NZ(x)                                                 \
  do {                                                             \
    int rc;                                                        \
    if ((rc = (x))) {                                              \
      fprintf(stderr, "error: " #x " failed with rc = %d.\n", rc); \
      exit(-1);                                                    \
    }                                                              \
  } while (0)
#define TEST_Z(x) TEST_NZ(!(x))

struct rdma_buffer_attr {
  uint64_t address;
  uint32_t length;
  union stag {
    /* if we send, we call it local stags */
    uint32_t local_stag;
    /* if we receive, we call it remote stag */
    uint32_t remote_stag;
  } stag;
};

struct rdma_connection {
  struct rdma_event_channel *cm_ec{nullptr};
  struct rdma_cm_id *id;
  char *send_buf;
  struct ibv_mr *send_mr;
  char *recv_buf;
  struct ibv_mr *recv_mr;
  struct rdma_buffer_attr *recv_buf_attr;
  struct rdma_buffer_attr *remote_buffer_attr;
};

/* resolves a given destination name to sin_addr */
int get_addr(const char *dst, struct sockaddr *addr);
void show_buffer_attr(struct rdma_buffer_attr *attr, bool is_local);
int process_work_completion_events(struct ibv_comp_channel *comp_channel,
                                   struct ibv_wc *wc, int max_wc);
int process_rdma_cm_event(struct rdma_event_channel *echannel,
                          enum rdma_cm_event_type expected_event,
                          struct rdma_cm_event **cm_event);
#endif
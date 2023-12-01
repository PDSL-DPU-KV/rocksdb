#include "cache_rdma.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>

#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#define CACHE_SIZE (1 << 30U)
#define TIMEOUT_IN_MS (500U)
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
  struct cache_rdma *self;
  struct rdma_cm_id *cm_id;
  struct ibv_qp *qp;
  bool is_server;
  union {
    struct {
      char *local_buf;
      struct ibv_mr *local_mr;
      struct rdma_buffer_attr *cache_attr;
    } s;
    struct {
      struct rdma_buffer_attr *remote_cache_attr;
      cache_rdma_connect_cb connect;
      void *connect_arg;
    } c;
  } u;
};

struct cq_poller_ctx {
  struct cache_rdma *self;
  struct ibv_cq *cq;
  pthread_t poller;
};

struct cache_rdma {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct rdma_event_channel *ec;
  pthread_t cm_poller;
  bool has_server;
  uint32_t thread_num;
  struct cq_poller_ctx *cq_pollers;
};

struct client_req_ctx {
  struct rdma_connection *conn;
  op_cb cb;
  void *cb_arg;
};

// --- alloc & free ---
cache_rdma_mr cache_rdma_alloc_buf(cache_rdma_handle h, uint64_t size) {
  struct cache_rdma *self = (struct cache_rdma *)h;
  char *buf = (char *)malloc(size);
  return ibv_reg_mr(self->pd, buf, size, IBV_ACCESS_LOCAL_WRITE);
}

char *cache_rdma_get_buf_addr(cache_rdma_mr mr) {
  return (char *)(((struct ibv_mr *)mr)->addr);
}

void cache_rdma_free_mr(cache_rdma_mr _mr) {
  struct ibv_mr *mr = (struct ibv_mr *)_mr;
  char *buf = (char *)mr->addr;
  ibv_dereg_mr(mr);
  free(buf);
}

// --- cm poller ---
static void *rdma_cq_poller(void *arg);
static void show_buffer_attr(struct rdma_buffer_attr *attr) {
  printf("---------------------------------------------------------\n");
  printf("cache attr, addr: %p , len: %u , stag : 0x%x \n",
         (void *)attr->address, (unsigned int)attr->length,
         attr->stag.remote_stag);
  printf("---------------------------------------------------------\n");
}

static void register_memory(struct cache_rdma *self,
                            struct rdma_connection *conn) {
  if (!conn->is_server) {
    conn->u.c.remote_cache_attr =
        (struct rdma_buffer_attr *)malloc(sizeof(struct rdma_buffer_attr));
  } else {
    conn->u.s.local_buf = (char *)malloc(CACHE_SIZE);
    printf("malloc finished!\n");
    TEST_Z(conn->u.s.local_mr =
               ibv_reg_mr(self->pd, conn->u.s.local_buf, CACHE_SIZE,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                              IBV_ACCESS_REMOTE_WRITE));
    conn->u.s.cache_attr =
        (struct rdma_buffer_attr *)malloc(sizeof(rdma_buffer_attr));
    conn->u.s.cache_attr->address = (uint64_t)conn->u.s.local_mr->addr;
    conn->u.s.cache_attr->length = (uint32_t)conn->u.s.local_mr->length;
    conn->u.s.cache_attr->stag.local_stag = (uint32_t)conn->u.s.local_mr->lkey;
    show_buffer_attr(conn->u.s.cache_attr);
  }
}

static int create_connetion(struct cache_rdma *self, struct rdma_cm_id *cm_id) {
  struct rdma_connection *conn = (struct rdma_connection *)cm_id->context;
  // --- build context ---
  if (self->ctx == NULL) {
    self->ctx = cm_id->verbs;
    TEST_Z(self->pd = ibv_alloc_pd(self->ctx));
    TEST_Z(self->cq = ibv_create_cq(self->ctx, 3 * MAX_Q_NUM /* max_conn_num */,
                                    NULL, NULL, 0));
    self->cq_pollers = (struct cq_poller_ctx *)calloc(
        self->thread_num, sizeof(struct cq_poller_ctx));
    for (size_t i = 0; i < self->thread_num; i++) {
      self->cq_pollers[i].self = self;
      self->cq_pollers[i].cq = self->cq;
      pthread_create(&self->cq_pollers[i].poller, NULL, rdma_cq_poller,
                     &self->cq_pollers[i]);
    }
  }
  // assume only have one context
  assert(self->ctx == cm_id->verbs);
  // --- build qp ---
  struct ibv_qp_init_attr qp_attr;
  memset(&qp_attr, 0, sizeof(struct ibv_qp_init_attr));
  qp_attr.send_cq = self->cq;
  qp_attr.recv_cq = self->cq;
  qp_attr.qp_type = IBV_QPT_RC;

  qp_attr.cap.max_send_wr = MAX_Q_NUM;
  qp_attr.cap.max_recv_wr = MAX_Q_NUM;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_sge = 1;
  TEST_NZ(rdma_create_qp(cm_id, self->pd, &qp_attr));
  conn->qp = cm_id->qp;

  register_memory(self, conn);

  return 0;
}

static inline int on_addr_resolved(struct cache_rdma *self,
                                   struct rdma_cm_id *cm_id) {
  printf("addr resolved!\n");
  TEST_NZ(create_connetion(self, cm_id));
  TEST_NZ(rdma_resolve_route(cm_id, TIMEOUT_IN_MS));
  return 0;
}

static inline int on_route_resolved(
    __attribute__((unused)) struct cache_rdma *self, struct rdma_cm_id *cm_id) {
  printf("route resolved!\n");
  struct rdma_conn_param cm_params;
  memset(&cm_params, 0, sizeof(cm_params));
  cm_params.responder_resources = 16;
  cm_params.initiator_depth = 16;
  cm_params.retry_count = 7;
  cm_params.rnr_retry_count = 7;  // '7' indicates retry infinitely
  TEST_NZ(rdma_connect(cm_id, &cm_params));
  return 0;
}

static inline int on_connect_request(struct cache_rdma *self,
                                     struct rdma_cm_id *cm_id) {
  printf("new connection!\n");
  struct rdma_connection *conn = (struct rdma_connection *)malloc(
                             sizeof(struct rdma_connection)),
                         *lconn = (struct rdma_connection *)cm_id->context;
  *conn = (struct rdma_connection){self, cm_id, NULL, true};
  cm_id->context = conn;
  TEST_NZ(create_connetion(self, cm_id));
  struct rdma_conn_param cm_params;
  memset(&cm_params, 0, sizeof(cm_params));
  cm_params.responder_resources = 16;
  cm_params.initiator_depth = 16;
  cm_params.retry_count = 7;
  cm_params.rnr_retry_count = 7;  // '7' indicates retry infinitely
  cm_params.private_data = (void *)conn->u.s.cache_attr;
  cm_params.private_data_len = sizeof(struct rdma_buffer_attr);
  TEST_NZ(rdma_accept(cm_id, &cm_params));
  return 0;
}

static inline int on_connect_error(
    __attribute__((unused)) struct cache_rdma *self, struct rdma_cm_id *cm_id) {
  printf("connect error!\n");
  struct rdma_connection *conn = (struct rdma_connection *)cm_id->context;
  if (!conn->is_server) {
    free(conn);
  }
  return 0;
}

static inline int on_established(
    __attribute__((unused)) struct cache_rdma *self, struct rdma_cm_id *cm_id,
    const void *param) {
  struct rdma_connection *conn = (struct rdma_connection *)cm_id->context;
  if (conn->is_server) {
    struct sockaddr_in *addr = (struct sockaddr_in *)rdma_get_peer_addr(cm_id);
    printf("server: connection established from peer %s:%u.\n",
           inet_ntoa(addr->sin_addr), ntohs(addr->sin_port));
  } else {
    conn->u.c.remote_cache_attr->address =
        ((struct rdma_buffer_attr *)param)->address;
    conn->u.c.remote_cache_attr->length =
        ((struct rdma_buffer_attr *)param)->length;
    conn->u.c.remote_cache_attr->stag.remote_stag =
        ((struct rdma_buffer_attr *)param)->stag.local_stag;
    show_buffer_attr(conn->u.c.remote_cache_attr);
    if (conn->u.c.connect) conn->u.c.connect(conn, conn->u.c.connect_arg);
  }
  return 0;
}

static inline int on_disconnect(struct rdma_cm_id *cm_id) {
  struct rdma_connection *conn = (struct rdma_connection *)cm_id->context;
  if (conn->is_server) {
    struct sockaddr_in *addr = (struct sockaddr_in *)rdma_get_peer_addr(cm_id);
    printf("server: peer %s:%u disconnected.\n", inet_ntoa(addr->sin_addr),
           ntohs(addr->sin_port));
    ibv_dereg_mr(conn->u.s.local_mr);
    free(conn->u.s.cache_attr);
    free(conn->u.s.local_buf);
  } else {
    free(conn->u.c.remote_cache_attr);
  }
  rdma_destroy_qp(cm_id);
  rdma_destroy_id(cm_id);
  free(conn);
  return 0;
}

static void *rdma_cm_poller(void *_self) {
  printf("start cm poller!\n");
  struct cache_rdma *self = (struct cache_rdma *)_self;
  struct rdma_cm_event *event = NULL;
  while (self->ec) {
    if (rdma_get_cm_event(self->ec, &event) == 0) {
      struct rdma_cm_id *cm_id = event->id;
      enum rdma_cm_event_type event_type = event->event;
      if (event_type != RDMA_CM_EVENT_ESTABLISHED) rdma_ack_cm_event(event);
      switch (event_type) {
        case RDMA_CM_EVENT_ADDR_RESOLVED:
          on_addr_resolved(self, cm_id);
          break;
        case RDMA_CM_EVENT_ROUTE_RESOLVED:
          on_route_resolved(self, cm_id);
          break;
        case RDMA_CM_EVENT_UNREACHABLE:
        case RDMA_CM_EVENT_REJECTED:
          on_connect_error(self, cm_id);
          break;
        case RDMA_CM_EVENT_CONNECT_REQUEST:
          on_connect_request(self, cm_id);
          break;
        case RDMA_CM_EVENT_ESTABLISHED:
          on_established(self, cm_id, event->param.conn.private_data);
          rdma_ack_cm_event(event);
          break;
        case RDMA_CM_EVENT_DISCONNECTED:
          on_disconnect(cm_id);
          break;
        default:
          break;
      }
    }
  }
  printf("cm poller exit!\n");
  return NULL;
}

// --- client ---
connection_handle cache_rdma_connect(cache_rdma_handle h, char *addr_str,
                                     char *port_str,
                                     cache_rdma_connect_cb connect_cb,
                                     void *connect_arg) {
  printf("connect to server!\n");
  struct cache_rdma *self = (struct cache_rdma *)h;
  struct rdma_connection *conn =
      (struct rdma_connection *)malloc(sizeof(struct rdma_connection));
  conn->u.c.connect = connect_cb;
  conn->u.c.connect_arg = connect_arg;
  *conn = (struct rdma_connection){self, NULL, NULL, false};
  struct addrinfo *addr;
  TEST_NZ(getaddrinfo(addr_str, port_str, NULL, &addr));
  TEST_NZ(rdma_create_id(self->ec, &conn->cm_id, NULL, RDMA_PS_TCP));
  conn->cm_id->context = conn;
  TEST_NZ(rdma_resolve_addr(conn->cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS));
  freeaddrinfo(addr);
  return conn;
}

void cache_rdma_op(connection_handle c, op_code op, cache_rdma_mr _src_mr,
                   uint64_t dst_off, op_cb cb, void *cb_arg) {
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  struct rdma_connection *conn = (struct rdma_connection *)(c);
  struct ibv_mr *src_mr = (struct ibv_mr *)_src_mr;
  struct client_req_ctx *ctx =
      (struct client_req_ctx *)malloc(sizeof(struct client_req_ctx));
  ctx->conn = conn;
  ctx->cb = cb;
  ctx->cb_arg = cb_arg;

  assert(!conn->is_server);

  sge.addr = (uintptr_t)src_mr->addr;
  sge.length = src_mr->length;
  sge.lkey = src_mr->lkey;

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)ctx;
  wr.opcode = (op == op_code::RDMA_READ) ? IBV_WR_RDMA_READ : IBV_WR_RDMA_WRITE;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = conn->u.c.remote_cache_attr->address + dst_off;
  wr.wr.rdma.rkey = conn->u.c.remote_cache_attr->stag.remote_stag;

  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void cache_rdma_disconnect(connection_handle h) {
  struct rdma_connection *conn = (struct rdma_connection *)h;
  TEST_NZ(rdma_disconnect(conn->cm_id));
}

// --- server ---
void cache_rdma_listen(cache_rdma_handle h, char *addr_str, char *port_str) {
  struct cache_rdma *self = (struct cache_rdma *)h;
  struct rdma_connection *conn =
      (struct rdma_connection *)malloc(sizeof(struct rdma_connection));
  *conn = (struct rdma_connection){self, NULL, NULL, true};
  struct addrinfo *addr;
  TEST_NZ(getaddrinfo(addr_str, port_str, NULL, &addr));
  TEST_NZ(rdma_create_id(self->ec, &conn->cm_id, NULL, RDMA_PS_TCP));
  TEST_NZ(rdma_bind_addr(conn->cm_id, addr->ai_addr));
  TEST_NZ(rdma_listen(conn->cm_id, 10));
  conn->cm_id->context = conn;
  freeaddrinfo(addr);
  printf("cache rdma listening on %s %s.\n", addr_str, port_str);
}

// --- cq poller ---
static inline void on_client_op(struct ibv_wc *wc) {
  if (wc->status != IBV_WC_SUCCESS) {
    fprintf(stderr, "on_client_op: status is %s, op: %d\n",
            ibv_wc_status_str(wc->status), wc->opcode);
  }
  struct client_req_ctx *ctx = (struct client_req_ctx *)wc->wr_id;
  if (ctx->cb) ctx->cb(ctx->conn, ctx->cb_arg);
  free(ctx);
}

#define MAX_ENTRIES_PER_POLL 128
static void *rdma_cq_poller(void *arg) {
  printf("start cq poller!\n");
  struct cq_poller_ctx *ctx = (struct cq_poller_ctx *)arg;
  struct ibv_wc wc[MAX_ENTRIES_PER_POLL];
  while (ctx->cq) {
    int rc = ibv_poll_cq(ctx->cq, MAX_ENTRIES_PER_POLL, wc);
    if (rc < 0) {
      fprintf(stderr, "failed to poll cq for wc due to %d \n", rc);
      return NULL;
    }
    for (int i = 0; i < rc; i++) {
      switch (wc[i].opcode) {
        case IBV_WC_RDMA_READ:
        case IBV_WC_RDMA_WRITE:
          on_client_op(wc + i);
          break;
        default:
          fprintf(stderr, "cache_rdma: unknown event %u \n.", wc[i].opcode);
          break;
      }
    }
  }
  printf("cq poller exit!\n");
  return NULL;
}

// --- init & fini ---
void cache_rdma_init(cache_rdma_handle *h, uint32_t thread_num) {
  struct cache_rdma *self =
      (struct cache_rdma *)malloc(sizeof(struct cache_rdma));
  memset(self, 0, sizeof(struct cache_rdma));
  self->ec = rdma_create_event_channel();
  if (!self->ec) {
    fprintf(stderr, "fail to create event channel.\n");
    exit(-1);
  }
  int flag = fcntl(self->ec->fd, F_GETFL);
  fcntl(self->ec->fd, F_SETFL, flag | O_NONBLOCK);
  TEST_NZ(pthread_create(&self->cm_poller, NULL, rdma_cm_poller, (void *)self));
  self->thread_num = thread_num;
  *h = self;
}

void cache_rdma_fini(cache_rdma_handle h) {
  struct cache_rdma *self = (struct cache_rdma *)h;
  rdma_destroy_event_channel(self->ec);
  self->ec = NULL;
  TEST_NZ(pthread_join(self->cm_poller, NULL));
  if (self->ctx) {
    for (size_t i = 0; i < self->thread_num; i++) {
      self->cq_pollers[i].cq = NULL;
      TEST_NZ(pthread_join(self->cq_pollers[i].poller, NULL));
    }
    ibv_destroy_cq(self->cq);
    ibv_dealloc_pd(self->pd);
    free(self->cq_pollers);
  }
}
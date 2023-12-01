// Author: Ding Chen

#ifndef _CACHE_RDMA_H_
#define _CACHE_RDMA_H_

#include <cstdint>

typedef void *cache_rdma_handle;
typedef void *connection_handle;
typedef void *cache_rdma_mr;

typedef void (*cache_rdma_connect_cb)(connection_handle h, void *cb_arg);
typedef void (*op_cb)(connection_handle h, void *cb_arg);

enum op_code { RDMA_READ, RDMA_WRITE };

cache_rdma_mr cache_rdma_alloc_buf(cache_rdma_handle h, uint64_t size);
char *cache_rdma_get_buf_addr(cache_rdma_mr mr);
void cache_rdma_free_mr(cache_rdma_mr _mr);

void cache_rdma_listen(cache_rdma_handle h, char *addr_str, char *port_str);
connection_handle cache_rdma_connect(cache_rdma_handle h, char *addr_str,
                                     char *port_str,
                                     cache_rdma_connect_cb connect_cb,
                                     void *connect_arg);
void cache_rdma_op(connection_handle c, op_code op, cache_rdma_mr src_mr,
                   uint64_t dst_off, op_cb cb, void *cb_arg);
void cache_rdma_disconnect(connection_handle h);

void cache_rdma_init(cache_rdma_handle *h, uint32_t thread_num);
void cache_rdma_fini(cache_rdma_handle h);

#endif
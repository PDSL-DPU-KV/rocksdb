#include <arpa/inet.h>
#include <rdma/rdma_cma.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>

#include "dpu_compaction_common.h"
#include "rocksdb/options.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class DPUCompactionService : public CompactionService {
 public:
  DPUCompactionService(const std::string& svr_addr_string)
      : svr_addr(svr_addr_string){};
  ~DPUCompactionService(){};

  // Returns the name of this compaction service.
  const char* Name() const override { return "DPUCompactionService"; };

  // Start the remote compaction with `compaction_service_input`, which can be
  // passed to `DB::OpenAndCompact()` on the remote side. `info` provides the
  // information the user might want to know, which includes `job_id`.
  CompactionServiceJobStatus StartV2(
      const CompactionServiceJobInfo& info,
      const std::string& compaction_service_input) override;

  // Wait for remote compaction to finish.
  CompactionServiceJobStatus WaitForCompleteV2(
      const CompactionServiceJobInfo& info,
      std::string* compaction_service_result) override;

 private:
  void ConnectToServer(struct rdma_connection* conn) {
    struct rdma_cm_event* cm_event = NULL;
    auto addr = StringSplit(svr_addr, ':');
    assert(addr.size() == 2);
    struct sockaddr_in serv_addr {};
    bzero(&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    get_addr(addr[0].c_str(), (struct sockaddr*)&serv_addr);
    serv_addr.sin_port = htons(std::stoi(addr[1]));
    // create event channel, cm id, pd, cq, qp
    struct rdma_event_channel* cm_event_channel = rdma_create_event_channel();
    if (!cm_event_channel) {
      printf("fail to create event channel! errno: %d\n", -errno);
      return;
    }
    struct rdma_cm_id* cm_client_id = nullptr;
    TEST_NZ(rdma_create_id(cm_event_channel, &cm_client_id, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_resolve_addr(cm_client_id, NULL, (struct sockaddr*)&serv_addr,
                              3000));
    TEST_NZ(process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_ADDR_RESOLVED,
                                  &cm_event));
    TEST_NZ(rdma_ack_cm_event(cm_event));
    TEST_NZ(rdma_resolve_route(cm_client_id, 3000));
    TEST_NZ(process_rdma_cm_event(cm_event_channel,
                                  RDMA_CM_EVENT_ROUTE_RESOLVED, &cm_event));
    TEST_NZ(rdma_ack_cm_event(cm_event));
    conn->id = cm_client_id;
    conn->cm_ec = cm_event_channel;
    cm_client_id->context = conn;
    CreateConnection(conn);
    struct rdma_conn_param cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    cm_params.responder_resources = 16;
    cm_params.initiator_depth = 16;
    cm_params.retry_count = 7;
    cm_params.rnr_retry_count = 7;  // '7' indicates retry infinitely
    cm_params.private_data = (void*)conn->recv_buf_attr;
    cm_params.private_data_len = sizeof(struct rdma_buffer_attr);
    TEST_NZ(rdma_connect(cm_client_id, &cm_params));
    TEST_NZ(process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_ESTABLISHED,
                                  &cm_event));
    const void* param = cm_event->param.conn.private_data;
    conn->remote_buffer_attr =
        (struct rdma_buffer_attr*)malloc(sizeof(struct rdma_buffer_attr));
    conn->remote_buffer_attr->address =
        ((struct rdma_buffer_attr*)param)->address;
    conn->remote_buffer_attr->length =
        ((struct rdma_buffer_attr*)param)->length;
    conn->remote_buffer_attr->stag.remote_stag =
        ((struct rdma_buffer_attr*)param)->stag.local_stag;
    show_buffer_attr(conn->remote_buffer_attr, false);
    TEST_NZ(rdma_ack_cm_event(cm_event));
  }

  void CreateConnection(struct rdma_connection* conn) {
    TEST_Z(conn->id->pd = ibv_alloc_pd(conn->id->verbs));
    TEST_Z(conn->id->recv_cq_channel =
               ibv_create_comp_channel(conn->id->verbs));
    conn->id->send_cq_channel = conn->id->recv_cq_channel;
    TEST_Z(conn->id->recv_cq =
               ibv_create_cq(conn->id->verbs, 3 * MAX_Q_NUM, NULL,
                             conn->id->recv_cq_channel, 0));
    conn->id->send_cq = conn->id->recv_cq;
    TEST_NZ(ibv_req_notify_cq(conn->id->recv_cq, 0));
    struct ibv_qp_init_attr attr;
    bzero(&attr, sizeof(struct ibv_qp_init_attr));
    attr.cap.max_recv_sge = 1;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_wr = MAX_Q_NUM;
    attr.cap.max_send_wr = MAX_Q_NUM;
    attr.qp_type = IBV_QPT_RC;
    attr.recv_cq = conn->id->recv_cq;
    attr.send_cq = conn->id->send_cq;
    TEST_NZ(rdma_create_qp(conn->id, conn->id->pd, &attr));
    // register buffer for receive compaction param
    conn->recv_buf = (char*)malloc(MAX_BUF_SIZE);
    TEST_Z(conn->recv_mr =
               ibv_reg_mr(conn->id->pd, conn->recv_buf, MAX_BUF_SIZE,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                              IBV_ACCESS_REMOTE_WRITE));
    conn->recv_buf_attr =
        (struct rdma_buffer_attr*)malloc(sizeof(rdma_buffer_attr));
    conn->recv_buf_attr->address = (uint64_t)conn->recv_mr->addr;
    conn->recv_buf_attr->length = (uint32_t)conn->recv_mr->length;
    conn->recv_buf_attr->stag.local_stag = (uint32_t)conn->recv_mr->lkey;
    show_buffer_attr(conn->recv_buf_attr, true);
    // pre-post the receive buffer
    struct ibv_sge recv_sge;
    recv_sge.addr = (uint64_t)conn->recv_mr->addr;
    recv_sge.length = (uint32_t)conn->recv_mr->length;
    recv_sge.lkey = (uint32_t)conn->recv_mr->lkey;
    struct ibv_recv_wr recv_wr, *bad_recv_wr = NULL;
    bzero(&recv_wr, sizeof(recv_wr));
    recv_wr.num_sge = 1;
    recv_wr.sg_list = &recv_sge;
    TEST_NZ(ibv_post_recv(conn->id->qp, &recv_wr, &bad_recv_wr));
  }

  void Disconnect(struct rdma_connection* conn) {
    struct rdma_cm_event* cm_event;
    TEST_NZ(rdma_disconnect(conn->id));
    ibv_dereg_mr(conn->send_mr);
    ibv_dereg_mr(conn->recv_mr);
    free(conn->send_buf);
    free(conn->recv_buf);
    free(conn->recv_buf_attr);
    free(conn->remote_buffer_attr);
    free(conn);
    // rdma_destroy_qp(conn->id);
    // ibv_destroy_comp_channel(conn->id->recv_cq_channel);
    // ibv_destroy_cq(conn->id->recv_cq);
    // ibv_dealloc_pd(conn->id->pd);
    // rdma_destroy_id(conn->id);
  }

 private:
  const std::string svr_addr;
  std::map<uint64_t, rdma_connection*> conn_map;
};

std::shared_ptr<CompactionService> NewDPUCompactionService(
    const std::string& svr_addr_string);

}  // namespace ROCKSDB_NAMESPACE
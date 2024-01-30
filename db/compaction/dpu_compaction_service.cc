#include "dpu_compaction_service.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "db/compaction/dpu_compaction_common.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

static std::atomic_int job_num = 0;

CompactionServiceJobStatus DPUCompactionService::StartV2(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_service_input) {
  struct ibv_wc wc;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  struct rdma_connection* conn =
      (struct rdma_connection*)malloc(sizeof(struct rdma_connection));
  ConnectToServer(conn);
  conn->send_buf = (char*)calloc(1, MAX_BUF_SIZE);
  char* ptr = conn->send_buf;
  *(size_t*)ptr = info.db_name.size();
  ptr += sizeof(size_t);
  memcpy(ptr, info.db_name.c_str(), info.db_name.size());
  ptr += info.db_name.size();
  *(size_t*)ptr = job_num++;
  ptr += sizeof(size_t);
  *(size_t*)ptr = compaction_service_input.size();
  ptr += sizeof(size_t);
  memcpy(ptr, compaction_service_input.c_str(),
         compaction_service_input.size());
  size_t total_size = 3 * sizeof(size_t) + info.db_name.size() +
                      compaction_service_input.size();
  printf("send total size: %ld\n", total_size);
  TEST_Z(conn->send_mr =
             ibv_reg_mr(conn->id->pd, conn->send_buf, MAX_BUF_SIZE,
                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                            IBV_ACCESS_REMOTE_WRITE));
  sge.addr = (uintptr_t)conn->send_mr->addr;
  sge.length = conn->send_mr->length;
  sge.lkey = conn->send_mr->lkey;
  memset(&wr, 0, sizeof(wr));
  // wr.wr_id = (uintptr_t)ctx;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = conn->remote_buffer_attr->address;
  wr.wr.rdma.rkey = conn->remote_buffer_attr->stag.remote_stag;
  TEST_NZ(ibv_post_send(conn->id->qp, &wr, &bad_wr));
  // process wc, one for send and one for recv
  if (process_work_completion_events(conn->id->send_cq_channel, &wc, 2) != 2) {
    printf("We failed to get 1 work completions (RDMA_SEND to client)\n");
  }
  conn_map[info.job_id] = conn;
  return CompactionServiceJobStatus::kSuccess;
};

CompactionServiceJobStatus DPUCompactionService::WaitForCompleteV2(
    const CompactionServiceJobInfo& info,
    std::string* compaction_service_result) {
  struct rdma_connection* conn = conn_map[info.job_id];
  char* ptr = conn->recv_buf;
  size_t recv_size = *(size_t*)ptr;
  printf("recv total size: %ld\n", recv_size);
  ptr += sizeof(size_t);
  *compaction_service_result = std::string(ptr, recv_size);
  Disconnect(conn);
  conn_map.erase(info.job_id);
  if (*compaction_service_result == "failed") {
    return CompactionServiceJobStatus::kFailure;
  }
  return CompactionServiceJobStatus::kSuccess;
};

std::shared_ptr<CompactionService> NewDPUCompactionService(
    const std::string& svr_addr_string) {
  return std::shared_ptr<CompactionService>(
      new DPUCompactionService(svr_addr_string));
}

}  // namespace ROCKSDB_NAMESPACE
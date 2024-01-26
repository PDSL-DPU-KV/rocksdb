#include "dpu_compaction_service.h"

#include <netinet/sctp.h>
#include <sys/socket.h>

#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "rocksdb/options.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

static std::atomic_int job_num = 0;

CompactionServiceJobStatus DPUCompactionService::StartV2(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_service_input) {
  auto addr = StringSplit(svr_addr, ':');
  assert(addr.size() == 2);
  int sockfd = ConnectToServer(addr[0].c_str(), stoi(addr[1]));
  // simple serilization
  void* send_buf = calloc(1, MAX_BUF_SIZE);
  char* ptr = (char*)send_buf;
  size_t total_size = info.db_name.size() + compaction_service_input.size() +
                      4 * sizeof(size_t);
  if (total_size > MAX_BUF_SIZE) {
    printf("input size bigger than MAX BUF SIZE!, input size: %ld\n",
           total_size);
  }
#ifdef COMP_DEBUG
  printf(
      "Send compaction request, total size: %ld, db_name: %s, job_id: %d, "
      "input size: %ld\n",
      total_size, info.db_name.c_str(), job_num.load(),
      compaction_service_input.size());
#endif
  *(size_t*)ptr = total_size;
  ptr += sizeof(size_t);
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
  // send
  size_t send_size =
      sctp_sendmsg(sockfd, send_buf, MAX_BUF_SIZE, NULL, 0, 0, 0, 0, 0, 0);
  // receive
  void* recv_buf = calloc(1, MAX_BUF_SIZE);
  ssize_t recv_size = 0;
  while (recv_size < MAX_BUF_SIZE) {
    recv_size = sctp_recvmsg(sockfd, recv_buf, MAX_BUF_SIZE, NULL, 0, 0, 0);
    if (recv_size == -1) {
      printf("recv error: %s\n", strerror(errno));
    }
  }
  mr_map[info.job_id] = recv_buf;
  free(send_buf);
  sock_list.push_back(sockfd);
#ifdef COMP_DEBUG
  printf("info job id: %ld, received size: %ld, socket: %d\n", info.job_id,
         recv_size, sockfd);
#endif
  return CompactionServiceJobStatus::kSuccess;
};

CompactionServiceJobStatus DPUCompactionService::WaitForCompleteV2(
    const CompactionServiceJobInfo& info,
    std::string* compaction_service_result) {
  void* recv_buf = mr_map[info.job_id];
  int result_size = *(size_t*)recv_buf;
  *compaction_service_result =
      std::string((char*)recv_buf + sizeof(size_t), result_size);
  free(recv_buf);
  mr_map.erase(info.job_id);
#ifdef COMP_DEBUG
  printf("result size: %ld\n", compaction_service_result->size());
#endif
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
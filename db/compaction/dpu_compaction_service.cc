#include "dpu_compaction_service.h"

#include <cstdio>

#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

CompactionServiceJobStatus DPUCompactionService::StartV2(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_service_input) {
  compaction_args args;
  args.db_name = info.db_name;
  args.job_id = info.job_id;
  args.input = compaction_service_input;
  printf("Send compaction rpc req!\n");
  Callable rpc = rp.on(ep);
  FutureResponsePayload* future_resp = rpc.forward(args);
  future_resp->wait();
  auto resp = future_resp->as<std::string>();
  delete future_resp;
  if (resp == "failed") {
    return CompactionServiceJobStatus::kFailure;
  }
  result[info.job_id] = resp;
  return CompactionServiceJobStatus::kSuccess;
};

CompactionServiceJobStatus DPUCompactionService::WaitForCompleteV2(
    const CompactionServiceJobInfo& info,
    std::string* compaction_service_result) {
  *compaction_service_result = result[info.job_id];
  result.erase(info.job_id);
  return CompactionServiceJobStatus::kSuccess;
};

std::shared_ptr<CompactionService> NewDPUCompactionService(
    const std::string& svr_addr_string) {
  return std::shared_ptr<CompactionService>(
      new DPUCompactionService(svr_addr_string));
}

}  // namespace ROCKSDB_NAMESPACE
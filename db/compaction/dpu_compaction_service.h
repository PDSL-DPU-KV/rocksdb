#include <memory>
#include <thread>

#include "dpu_compaction_op.h"
#include "rocksdb/options.h"
#include "util/mercury_wrapper.hh"

namespace ROCKSDB_NAMESPACE {

class DPUCompactionService : public CompactionService {
 public:
  DPUCompactionService(const std::string& svr_addr_string)
      : engine(MercuryEngine("verbs", false)),
        bg_worker(std::thread([&]() { engine.progress(); })),
        ep(engine.lookup(svr_addr_string.c_str())),
        rp(engine.define("compaction")){};

  ~DPUCompactionService() {
    engine.stop();
    bg_worker.join();
  };

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
  MercuryEngine engine;
  std::thread bg_worker;
  MercuryEndpoint ep;
  RemoteProcedure rp;
  std::map<uint64_t, std::string> result;
};

std::shared_ptr<CompactionService> NewDPUCompactionService(
    const std::string& svr_addr_string);

}  // namespace ROCKSDB_NAMESPACE
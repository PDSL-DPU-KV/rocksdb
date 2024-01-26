#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>

#include "rocksdb/options.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

#define MAX_BUF_SIZE (8 * 1024)

class DPUCompactionService : public CompactionService {
 public:
  DPUCompactionService(const std::string& svr_addr_string)
      : svr_addr(svr_addr_string){};
  ~DPUCompactionService() {
    for (uint i = 0; i < sock_list.size(); i++) {
      Disconnect(sock_list[i]);
    }
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
  int ConnectToServer(const char* ip, int port) {
    struct hostent* host;
    host = gethostbyname(ip);
    assert(host != nullptr);
    struct sockaddr_in serv_addr {};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    serv_addr.sin_addr = *((struct in_addr*)host->h_addr);
    int sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_SCTP);
    assert(sockfd != -1);
    int res =
        connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(struct sockaddr));
    assert(res == 0);
    return sockfd;
  }

  void Disconnect(int sockfd) { close(sockfd); }

 private:
  const std::string svr_addr;
  std::map<uint64_t, void*> mr_map;
  std::vector<int> sock_list;
};

std::shared_ptr<CompactionService> NewDPUCompactionService(
    const std::string& svr_addr_string);

}  // namespace ROCKSDB_NAMESPACE
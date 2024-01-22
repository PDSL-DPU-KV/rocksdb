#include <unistd.h>

#include <cstdio>
#include <memory>

#include "dpu_compaction_op.h"
#include "env/nas_env.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_factory.h"
#include "util/mercury_wrapper.hh"

static std::shared_ptr<ROCKSDB_NAMESPACE::Env> env;

std::string compaction_rpc_handler(compaction_args &args) {
  printf("Got compaction rpc request!, db_name: %s\n", args.db_name.c_str());
  rocksdb::OpenAndCompactOptions options;
  rocksdb::CompactionServiceOptionsOverride options_override;
  options_override.table_factory = std::shared_ptr<rocksdb::TableFactory>(
      new rocksdb::BlockBasedTableFactory());
  options_override.env = env.get();
  std::string output;
  rocksdb::Status s = rocksdb::DB::OpenAndCompact(
      options, args.db_name, args.db_name + "/" + std::to_string(args.job_id),
      args.input, &output, options_override);
  if (!s.ok()) {
    output = "failed";
  }
  return output;
}

int main(int argc, char *argv[]) {
  const char *comp_svr_addr_string;
  const char *fs_svr_addr_string;
  if (argc < 3) {
    printf(
        "Usage is: %s <compaction svr address string> <fs svr address "
        "string>\n",
        argv[0]);
    return (0);
  }
  comp_svr_addr_string = argv[1];
  fs_svr_addr_string = argv[2];

  MercuryEngine engine(comp_svr_addr_string, true);
  printf("compaction server address, %s\n", comp_svr_addr_string);
  // new nas env for compaction server
  RPCEngine *rpc_engine = new RPCEngine(fs_svr_addr_string);
  env = NewCompositeEnv(rocksdb::NewRemoteFileSystem(rpc_engine));
  // define compaction handler
  engine.define("compaction", [&](const Handle &h) -> void {
    auto req = h.get_payload().as<compaction_args>();
    auto resp = compaction_rpc_handler(req);
    h.respond(resp);
  });
  engine.progress();
  return (0);
}

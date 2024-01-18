#include <assert.h>
#include <stdio.h>
#include <unistd.h>

#include "rpc.h"
#include "rpc_engine.h"

/* example server program.  Starts HG engine, registers the example RPC type,
 * and then executes indefinitely.
 */

int main(void) {
  hg_engine_init(HG_TRUE, "ofi+verbs://192.168.200.10:12345");

  hg_engine_print_self_addr();

  /* register RPC for file operations */
  open_rpc_register();
  fopen_rpc_register();
  close_rpc_register();
  fseek_rpc_register();
  read_rpc_register();
  write_rpc_register();
  fstat_rpc_register();
  ftruncate_rpc_register();
  fallocate_rpc_register();
  fdatasync_rpc_register();
  fsync_rpc_register();
  rangesync_rpc_register();
  rename_rpc_register();
  access_rpc_register();
  unlink_rpc_register();
  mkdir_rpc_register();
  rmdir_rpc_register();
  stat_rpc_register();
  ls_rpc_register();
  lock_rpc_register();

  /* this would really be something waiting for shutdown notification */
  while (1) sleep(1);

  hg_engine_finalize();

  return (0);
}
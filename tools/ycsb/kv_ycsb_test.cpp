#include "kv_ycsb.h"

#include <string>

kv_ycsb_handle workload;

namespace ycsbc {
struct Option {
  uint64_t num_items, operation_cnt;
  uint32_t value_size;
  // uint32_t ssd_num;
  // uint32_t producer_num;
  // uint32_t concurrent_io_num;
  bool seq_read, fill, seq_write, del, rnd_read, rnd_write;
  // char json_config_file[1024];
  std::string workload_file;
};
#define _KV_MSG_ALIGN(size) ((size) & 0x3 ? ((size) & ~0x3) + 0x4 : (size))
struct kv_msg {
// msg_types:
#define KV_MSG_OK (0U)
#define KV_MSG_GET (1U)
#define KV_MSG_SET (2U)
#define KV_MSG_DEL (3U)
#define KV_MSG_TEST (128U)
#define KV_MSG_OUTDATED (254U)
#define KV_MSG_ERR (255U)
    uint8_t type;
    uint8_t key_len;
    uint16_t hop;
    uint32_t value_len;
    // struct kv_ds_q_info q_info;
    uint8_t data[0];
// data:
// uint8_t key[key_length]
// uint8_t _[key_length - _KV_MSG_ALIGN(key_length)];
// uint8_t value[value_len]; //must be 4 bytes aligned
#define KV_MSG_KEY(msg) ((msg)->data)
#define KV_MSG_VALUE(msg) ((msg)->data + _KV_MSG_ALIGN((msg)->key_len))
#define KV_MSG_SIZE(msg) (sizeof(struct kv_msg) + _KV_MSG_ALIGN((msg)->key_len) + (msg)->value_len)
};
}  // namespace ycsbc

int main() {
  ycsbc::Option opt;
  opt.workload_file = "./workloads/workloada.spec";
  opt.num_items = 1000;
  opt.operation_cnt = 1000;
  opt.value_size = 4096;

  kv_ycsb_init(&workload, opt.workload_file.c_str(), &opt.num_items, &opt.operation_cnt,
               &opt.value_size);

  ycsbc::kv_msg* msg;
  msg = (ycsbc::kv_msg*)malloc(16 + opt.value_size + sizeof(struct ycsbc::kv_msg));

  
  enum kv_ycsb_operation op = kv_ycsb_next(workload, false, KV_MSG_KEY(msg), KV_MSG_VALUE(msg));


  kv_ycsb_fini(workload);
}
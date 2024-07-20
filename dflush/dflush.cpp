#include <arpa/inet.h>
#include <doca_buf.h>
#include <doca_buf_array.h>
#include <doca_buf_inventory.h>
#include <doca_ctx.h>
#include <doca_dev.h>
#include <doca_dma.h>
#include <doca_dpa.h>
#include <doca_mmap.h>
#include <doca_pe.h>
#include <doca_sync_event.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

#include "../util/work_queue.h"
#include "common.h"
#include "db/builder.h"
#include "db/seqno_to_time_mapping.h"
#include "db/version_edit.h"
#include "dma.h"
#include "dpa.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "stdio.h"
#include "threadpool.h"

doca_dev* dev;
doca_dpa* dpa;

const uint32_t nthreads_task = 32;
const uint32_t nthreads_memcpy = 4;
const bool DPA_MEMCPY = 0;
const bool use_optimized = 1;

ROCKSDB_NAMESPACE::WorkQueue<DMAThread*> DMAThread_pool;
ROCKSDB_NAMESPACE::WorkQueue<DPAThreads*> DPAThreads_pool;

void run_memcpy_dma(params_memcpy_t params, doca_mmap* dst_m,
                    doca_mmap* src_m) {
  DMAThread* dt = nullptr;
  DMAThread_pool.pop(dt);
  assert(params.copy_size <= max_dma_buffer_size());
  dt->run(params, dst_m, src_m);
  DMAThread_pool.push(dt);
}

void run_memcpy_dpa(const params_memcpy_t& params) {
  DPAThreads* dt = nullptr;
  DPAThreads_pool.pop(dt);
  dt->set_params(params);
  dt->trigger_all();
  dt->wait_all();
  DPAThreads_pool.push(dt);
}

int parse_file_meta(rocksdb::FileMetaData* meta, char* ptr) {
  int recv_size = 0;

  uint32_t n = *(uint32_t*)ptr;
  ptr += sizeof(uint32_t);
  recv_size += sizeof(uint32_t);
  if (n > 0) {
    std::string str(ptr, n);
    str.push_back('\0');
    meta->smallest.set_InternalKey(str);
    ptr += n;
    recv_size += n;
  } else {
    meta->smallest.set_InternalKey(std::string());
  }

  n = *(uint32_t*)ptr;
  ptr += sizeof(uint32_t);
  recv_size += sizeof(uint32_t);
  if (n > 0) {
    std::string str(ptr, n);
    str.push_back('\0');
    meta->largest.set_InternalKey(str);
    ptr += n;
    recv_size += n;
  } else {
    meta->largest.set_InternalKey(std::string());
  }

  meta->compensated_file_size = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  recv_size += sizeof(uint64_t);

  meta->num_entries = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  recv_size += sizeof(uint64_t);

  meta->num_deletions = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  recv_size += sizeof(uint64_t);

  meta->raw_key_size = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  recv_size += sizeof(uint64_t);

  meta->raw_value_size = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  recv_size += sizeof(uint64_t);

  meta->num_range_deletions = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  recv_size += sizeof(uint64_t);

  meta->compensated_range_deletion_size = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  recv_size += sizeof(uint64_t);

  meta->refs = *(int*)ptr;
  ptr += sizeof(int);
  recv_size += sizeof(int);

  meta->being_compacted = *(bool*)ptr;
  ptr += sizeof(bool);
  recv_size += sizeof(bool);

  meta->init_stats_from_file = *(bool*)ptr;
  ptr += sizeof(bool);
  recv_size += sizeof(bool);

  meta->marked_for_compaction = *(bool*)ptr;
  ptr += sizeof(bool);
  recv_size += sizeof(bool);

  meta->oldest_blob_file_number = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  recv_size += sizeof(uint64_t);

  meta->oldest_ancester_time = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  recv_size += sizeof(uint64_t);

  meta->file_creation_time = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  recv_size += sizeof(uint64_t);

  meta->epoch_number = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  recv_size += sizeof(uint64_t);

  n = *(uint32_t*)ptr;
  ptr += sizeof(uint32_t);
  recv_size += sizeof(uint32_t);
  if (n > 0) {
    std::string str(ptr, n);
    str.push_back('\0');
    meta->file_checksum = str;
    ptr += n;
    recv_size += n;
  } else {
    meta->file_checksum = std::string();
  }

  n = *(uint32_t*)ptr;
  ptr += sizeof(uint32_t);
  recv_size += sizeof(uint32_t);
  if (n > 0) {
    std::string str(ptr, n);
    str.push_back('\0');
    meta->file_checksum_func_name = str;
    ptr += n;
    recv_size += n;
  } else {
    meta->file_checksum_func_name = std::string();
  }

  meta->tail_size = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  recv_size += sizeof(uint64_t);

  return recv_size;
}

int set_file_meta(rocksdb::FileMetaData* meta, char* ptr) {
  int send_size = 0;

  std::string send_small_str = (meta->smallest).get_InternalKey();
  *(uint32_t*)ptr = (uint32_t)(send_small_str.size());
  ptr += sizeof(uint32_t);
  send_size += sizeof(uint32_t);
  if ((uint32_t)(send_small_str).size() > 0) {
    memcpy(ptr, (send_small_str).c_str(), (uint32_t)(send_small_str).size());
    ptr += (send_small_str).size();
    send_size += (send_small_str).size();
  }
  printf("meta.smallest.DebugString:%s\n",
         meta->smallest.DebugString(true).c_str());

  std::string send_large_str = (meta->largest).get_InternalKey();
  *(uint32_t*)ptr = (uint32_t)(send_large_str).size();
  ptr += sizeof(uint32_t);
  send_size += sizeof(uint32_t);
  if ((uint32_t)(send_large_str).size() > 0) {
    memcpy(ptr, (send_large_str).c_str(), (uint32_t)(send_large_str).size());
    ptr += (send_large_str).size();
    send_size += (send_large_str).size();
  }
  printf("meta.largest.DebugString:%s\n",
         meta->largest.DebugString(true).c_str());

  printf("SEND: total_size after str: %d\n", send_size);
  *(uint64_t*)ptr = meta->compensated_file_size;
  ptr += sizeof(uint64_t);
  send_size += sizeof(uint64_t);

  *(uint64_t*)ptr = meta->num_entries;
  ptr += sizeof(uint64_t);
  send_size += sizeof(uint64_t);

  *(uint64_t*)ptr = meta->num_deletions;
  ptr += sizeof(uint64_t);
  send_size += sizeof(uint64_t);

  *(uint64_t*)ptr = meta->raw_key_size;
  ptr += sizeof(uint64_t);
  send_size += sizeof(uint64_t);

  *(uint64_t*)ptr = meta->raw_value_size;
  ptr += sizeof(uint64_t);
  send_size += sizeof(uint64_t);

  *(uint64_t*)ptr = meta->num_range_deletions;
  ptr += sizeof(uint64_t);
  send_size += sizeof(uint64_t);

  *(uint64_t*)ptr = meta->compensated_range_deletion_size;
  ptr += sizeof(uint64_t);
  send_size += sizeof(uint64_t);

  *(int*)ptr = meta->refs;
  ptr += sizeof(int);
  send_size += sizeof(int);

  *(bool*)ptr = meta->being_compacted;
  ptr += sizeof(bool);
  send_size += sizeof(bool);

  *(bool*)ptr = meta->init_stats_from_file;
  ptr += sizeof(bool);
  send_size += sizeof(bool);

  *(bool*)ptr = meta->marked_for_compaction;
  ptr += sizeof(bool);
  send_size += sizeof(bool);

  *(uint64_t*)ptr = meta->oldest_blob_file_number;
  ptr += sizeof(uint64_t);
  send_size += sizeof(uint64_t);

  *(uint64_t*)ptr = meta->oldest_ancester_time;
  ptr += sizeof(uint64_t);
  send_size += sizeof(uint64_t);

  *(uint64_t*)ptr = meta->file_creation_time;
  ptr += sizeof(uint64_t);
  send_size += sizeof(uint64_t);

  *(uint64_t*)ptr = meta->epoch_number;
  ptr += sizeof(uint64_t);
  send_size += sizeof(uint64_t);

  *(uint32_t*)ptr = (uint32_t)(meta->file_checksum).size();
  ptr += sizeof(uint32_t);
  send_size += sizeof(uint32_t);
  strncpy(ptr, (meta->file_checksum).c_str(),
          (uint32_t)(meta->file_checksum).size());
  ptr += (meta->file_checksum).size();
  send_size += (meta->file_checksum).size();

  *(uint32_t*)ptr = (uint32_t)(meta->file_checksum_func_name).size();
  ptr += sizeof(uint32_t);
  send_size += sizeof(uint32_t);
  strncpy(ptr, (meta->file_checksum_func_name).c_str(),
          (uint32_t)(meta->file_checksum_func_name).size());
  ptr += (meta->file_checksum_func_name).size();
  send_size += (meta->file_checksum_func_name).size();

  *(uint64_t*)ptr = meta->tail_size;
  ptr += sizeof(uint64_t);
  send_size += sizeof(uint64_t);

  return send_size;
}

void DeSerializeReq(char* buffer, rocksdb::MetaReq* req) {
  char* ptr = buffer;

  // TrisectionPoint
  req->TrisectionPoint[0] = *(uintptr_t*)ptr;
  ptr += sizeof(uintptr_t);
  req->TrisectionPoint[1] = *(uintptr_t*)ptr;
  ptr += sizeof(uintptr_t);
  req->TrisectionPoint[2] = *(uintptr_t*)ptr;
  ptr += sizeof(uintptr_t);
  printf("TrisectionPoint: %lu %lu %lu\n", req->TrisectionPoint[0],
         req->TrisectionPoint[1], req->TrisectionPoint[2]);

  // mems
  req->num_entries = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  req->Node_head = *(uintptr_t*)ptr;
  ptr += sizeof(uintptr_t);
  req->mt_buf = *(uintptr_t*)ptr;
  ptr += sizeof(uintptr_t);
  printf("total_size after mems: %ld\n", ptr - buffer);

  req->Node_heads.push_back(req->Node_head);
  req->Node_heads.push_back(req->TrisectionPoint[0]);
  req->Node_heads.push_back(req->TrisectionPoint[1]);
  req->Node_heads.push_back(req->TrisectionPoint[2]);
  req->Node_heads.push_back(0);

  // meta_
  int meta_size = parse_file_meta(&req->file_meta, ptr);
  ptr += meta_size;
  printf("total_size after meta_: %ld\n", ptr - buffer);

  // new_versions_NewFileNumber
  req->new_versions_NewFileNumber = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);

  // seqno_to_time_mapping
  req->seqno_to_time_mapping = *(rocksdb::SeqnoToTimeMapping*)ptr;
  ptr += sizeof(rocksdb::SeqnoToTimeMapping);

  // kUnknownColumnFamily
  req->kUnknownColumnFamily = *(uint32_t*)ptr;
  ptr += sizeof(uint32_t);

  // full_history_ts_low
  uint32_t n = *(uint32_t*)ptr;
  ptr += sizeof(uint32_t);
  if (n > 0) {
    std::string str(ptr, n);
    str.push_back('\0');
    req->str_full_history_ts_low = str;
    ptr += n;
  } else {
    req->str_full_history_ts_low = "";
  }

  // io_priority
  req->io_priority = (rocksdb::Env::IOPriority*)ptr;
  ptr += sizeof(rocksdb::Env::IOPriority);

  // paranoid_file_checks
  req->paranoid_file_checks = *(bool*)ptr;
  ptr += sizeof(bool);

  // job_id
  req->job_id = *(int*)ptr;
  ptr += sizeof(int);

  // snapshots
  n = *(uint32_t*)ptr;
  ptr += sizeof(uint32_t);
  if (n > 0) {
    req->snapshots.assign(ptr, ptr + n * sizeof(rocksdb::SequenceNumber));
    ptr += n * sizeof(rocksdb::SequenceNumber);
  }

  // earliest_write_conflict_snapshot
  req->earliest_write_conflict_snapshot = *(rocksdb::SequenceNumber*)ptr;
  ptr += sizeof(rocksdb::SequenceNumber);

  // job_snapshot
  req->job_snapshot = *(rocksdb::SequenceNumber*)ptr;
  ptr += sizeof(rocksdb::SequenceNumber);

  // timestamp_size
  req->timestamp_size = *(size_t*)ptr;
  ptr += sizeof(size_t);

  // tboptions_ioptions_cf_paths
  n = *(uint32_t*)ptr;
  ptr += sizeof(uint32_t);
  if (n > 0) {
    for (uint32_t i = 0; i < n; i++) {
      rocksdb::DbPath newPath;
      newPath.path = ((rocksdb::DbPath_struct*)ptr)->path;
      newPath.target_size = ((rocksdb::DbPath_struct*)ptr)->target_size;
      req->tboptions_ioptions_cf_paths.push_back(newPath);
      ptr += sizeof(rocksdb::DbPath_struct);
    }
  }

  req->smallest_seqno = *(rocksdb::SequenceNumber*)ptr;
  ptr += sizeof(rocksdb::SequenceNumber);
  req->largest_seqno = *(rocksdb::SequenceNumber*)ptr;
  ptr += sizeof(rocksdb::SequenceNumber);

  // cfd_GetName
  n = *(uint32_t*)ptr;
  ptr += sizeof(uint32_t);
  if (n > 0) {
    std::string str(ptr, n);
    str.push_back('\0');
    req->cfd_GetName = str;
    ptr += n;
  } else {
    req->cfd_GetName = "";
  }

  // dbname
  n = *(uint32_t*)ptr;
  ptr += sizeof(uint32_t);
  if (n > 0) {
    std::string str(ptr, n);
    str.push_back('\0');
    req->dbname = str;
    ptr += n;
  } else {
    req->dbname = "";
  }

  printf("total_size after dbname : %ld\n", ptr - buffer);
  auto mmap_desc_size = *(uint64_t*)ptr;
  printf("mmap_desc_size:%ld\n", mmap_desc_size);
  ptr += sizeof(uint64_t);
  req->mmap_desc.assign(ptr, mmap_desc_size);
  printf("total_size: %ld\n", ptr - buffer);
}

int SerializeResult(char* buffer, rocksdb::MetaResult* result) {
  char* ptr = buffer;
  int result_size = 0;

  int send_size = set_file_meta(&result->file_meta, ptr);
  ptr += send_size;
  result_size += send_size;
  printf("SEND: total_size after meta: %ld\n", ptr - buffer);

  *(rocksdb::Status*)ptr = result->status;
  ptr += sizeof(rocksdb::Status);
  result_size += sizeof(rocksdb::Status);

  *(uint64_t*)ptr = result->num_input_entries;
  ptr += sizeof(uint64_t);
  result_size += sizeof(uint64_t);

  *(uint64_t*)ptr = result->memtable_payload_bytes;
  ptr += sizeof(uint64_t);
  result_size += sizeof(uint64_t);

  *(uint64_t*)ptr = result->memtable_garbage_bytes;
  ptr += sizeof(uint64_t);
  result_size += sizeof(uint64_t);

  *(uint64_t*)ptr = result->packed_number_and_path_id;
  ptr += sizeof(uint64_t);
  result_size += sizeof(uint64_t);

  *(uint64_t*)ptr = result->file_size;
  ptr += sizeof(uint64_t);
  result_size += sizeof(uint64_t);

  *(rocksdb::SequenceNumber*)ptr = result->smallest_seqno;
  ptr += sizeof(rocksdb::SequenceNumber);
  result_size += sizeof(rocksdb::SequenceNumber);

  *(rocksdb::SequenceNumber*)ptr = result->largest_seqno;
  ptr += sizeof(rocksdb::SequenceNumber);
  result_size += sizeof(rocksdb::SequenceNumber);
  return result_size;
}

void RunJob(int client_fd) {
  // get and deserialize message from host
  printf("RunJob!\n");
  char buffer[1024];
  rocksdb::MetaReq req;
  rocksdb::MetaResult result;
  auto read_size = read(client_fd, buffer, 1024);
  assert(read_size == 1024);
  DeSerializeReq(buffer, &req);

  // copy memtable from host
  params_memcpy_t params;
  if (DPA_MEMCPY) {
    params.copy_size = 140 * 1024;  // 单次单线程copy大小为140KB
    params.region_size = 140 * 1024 * 1024;  // 总共copy大小为140MB
    params.piece_size = params.region_size / nthreads_memcpy;  // 分给八个线程
    params.copy_n = params.piece_size / params.copy_size;  // 每个线程的copy次数
    params.memcpy_mode = ASYNC;                            // copy模式
  } else {
    params.copy_size = max_dma_buffer_size();  // 单次单线程copy大小为140KB
    params.region_size = 140 * 1024 * 1024;  // 总共copy大小为140MB
    params.piece_size = params.region_size;  // 分给八个线程
    params.copy_n = params.piece_size / params.copy_size;  // 每个线程的copy次数
    params.memcpy_mode = DMA;                              // copy模式
  }

  uint64_t offset;
  doca_mmap* dst_m;
  doca_mmap* src_m;

  std::tie(dst_m, params.dst) = alloc_mem(Location::Host, params.region_size);
  std::tie(src_m, params.src) =
      alloc_mem_from_export(Location::Host, params.region_size, req.mmap_desc);
  params.src.ptr = req.mt_buf;
  offset = params.dst.ptr - params.src.ptr;

  auto a = std::chrono::high_resolution_clock::now();
  if (DPA_MEMCPY) {
    run_memcpy_dpa(params);
  } else {
    run_memcpy_dma(params, dst_m, src_m);
  }
  auto b = std::chrono::high_resolution_clock::now();
  printf("memcpytime:%lu\n",
         std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count() /
             1000 / 1000);

  // build sstable
  printf("meta.smallest.DebugString:%s\n",
         req.file_meta.smallest.DebugString(true).c_str());
  printf("meta.largest.DebugString:%s\n",
         req.file_meta.largest.DebugString(true).c_str());

  auto a_point = std::chrono::high_resolution_clock::now();
  rocksdb::BuildTable_new(offset, &req, &result, use_optimized);
  auto b_point = std::chrono::high_resolution_clock::now();
  uint64_t buildtable_time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(b_point - a_point)
          .count();
  printf("builder over, buildtable time:%lu\n", buildtable_time / 1000 / 1000);

  printf(
      "Flush_result:   num_input_entries:%ld memtable_payload_bytes:%ld "
      "memtable_garbage_bytes:%ld packed_number_and_path_id:%ld file_size:%ld "
      "largest_seqno:%ld smallest_seqno:%ld\n",
      result.num_input_entries, result.memtable_payload_bytes,
      result.memtable_garbage_bytes, result.packed_number_and_path_id,
      result.file_size, result.largest_seqno, result.smallest_seqno);
  printf("meta.smallest.DebugString:%s\n",
         result.file_meta.smallest.DebugString(true).c_str());
  printf("meta.largest.DebugString:%s\n",
         result.file_meta.largest.DebugString(true).c_str());

  // serialize and return result to host
  char result_buffer[1024];
  auto result_size = SerializeResult(result_buffer, &result);
  auto send_size = send(client_fd, result_buffer, result_size, 0);
  assert(send_size == result_size);
  free_mem(params.dst, dst_m);
  doca_check(doca_mmap_stop(src_m));
  doca_check(doca_mmap_destroy(src_m));
}

static int PrepareConn(struct sockaddr_in* server_addr) {
  auto server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    perror("socket failed");
    exit(EXIT_FAILURE);
  }

  int opt = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
                 sizeof(opt))) {
    perror("setsockopt");
    exit(EXIT_FAILURE);
  }

  if (bind(server_fd, (struct sockaddr*)server_addr, sizeof(*server_addr)) <
      0) {
    perror("bind failed");
    exit(EXIT_FAILURE);
  }
  if (listen(server_fd, 3) < 0) {
    perror("listen");
    exit(EXIT_FAILURE);
  }
  return server_fd;
}

int main() {
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(10086);
  auto server_fd = PrepareConn(&server_addr);

  if (DPA_MEMCPY) {
    // DPA COPY
    attach_device(dflush_app);
    params_memcpy_t params;
    params.copy_size = 140 * 1024;  // 单次单线程copy大小为140KB
    params.region_size = 140 * 1024 * 1024;  // 总共copy大小为140MB
    params.piece_size = params.region_size / nthreads_memcpy;  // 分给八个线程
    params.copy_n = params.piece_size / params.copy_size;  // 每个线程的copy次数
    params.memcpy_mode = ASYNC;                            // copy模式

    DPAThreads_pool.setMaxSize(nthreads_task);
    for (uint64_t i = 0; i < nthreads_task; ++i) {
      DPAThreads_pool.push(new DPAThreads(nthreads_memcpy, params, 1));
    }
  } else {
    // DMA COPY
    open_device(&dma_task_is_supported);
    auto num_dma_tasks = 140 * 1024 * 1024 / max_dma_buffer_size();
    auto max_bufs = num_dma_tasks * 2;
    printf("max_bufs: %ld, dev: %p\n", max_bufs, dev);
    DMAThread_pool.setMaxSize(nthreads_task);
    for (uint32_t i = 0; i < nthreads_task; ++i) {
      DMAThread_pool.push(new DMAThread(max_bufs, num_dma_tasks));
    }
  }

  ThreadPool tp(nthreads_task);
  while (true) {
    socklen_t address_size = sizeof(server_addr);
    auto client_fd =
        accept(server_fd, (struct sockaddr*)&server_addr, &address_size);
    if (client_fd < 0) {
      printf("fail to accept!\n");
      exit(EXIT_FAILURE);
    }
    tp.enqueue(RunJob, client_fd);
  }

  if (DPA_MEMCPY) {
    detach_device();
  } else {
    close_device();
  }
  return 0;
}

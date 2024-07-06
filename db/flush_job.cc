//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/flush_job.h"

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <vector>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <chrono>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "logging/event_logger.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "table/two_level_iterator.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {
int Build_Table_num = 0;
std::chrono::milliseconds time_sum(0);

struct DbPath_struct {
  char path[128];
  uint64_t target_size;
};
int send_meta(FileMetaData* meta,char* ptr) {
  int send_size = 0;

  std::string send_small_str = (meta->smallest).get_InternalKey();
  *(uint32_t*)ptr = (uint32_t)(send_small_str).size();
  ptr += sizeof(uint32_t);
  send_size += sizeof(uint32_t);
  if ((uint32_t)(send_small_str).size() > 0) {
    memcpy(ptr, (send_small_str).c_str(), (uint32_t)(send_small_str).size());
    // strncpy(ptr,(send_small_str).c_str(),(uint32_t)(send_small_str).size());
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
    // strncpy(ptr,(send_large_str).c_str(),(uint32_t)(send_large_str).size());
    ptr += (send_large_str).size();
    send_size += (send_large_str).size();
  }
  printf("meta.largest.DebugString:%s\n",
         meta->largest.DebugString(true).c_str());
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
  strncpy(ptr,(meta->file_checksum).c_str(),(uint32_t)(meta->file_checksum).size());
  ptr += (meta->file_checksum).size();
  send_size += (meta->file_checksum).size();

  *(uint32_t*)ptr = (uint32_t)(meta->file_checksum_func_name).size();
  ptr += sizeof(uint32_t);
  send_size += sizeof(uint32_t);
  strncpy(ptr,(meta->file_checksum_func_name).c_str(),(uint32_t)(meta->file_checksum_func_name).size());
  ptr += (meta->file_checksum_func_name).size();
  send_size += (meta->file_checksum_func_name).size();

  *(uint64_t*)ptr = meta->tail_size;
  ptr += sizeof(uint64_t);
  send_size += sizeof(uint64_t);

  return send_size;
}

int recv_meta(FileMetaData* meta, char* ptr) {
  int recv_size=0;

  uint32_t n = *(uint32_t*)ptr;
  ptr += sizeof(uint32_t);
  recv_size += sizeof(uint32_t);
  printf("recv n = %d\n",n);
  if (n > 0) {
    std::string str(ptr, n);
    // str.push_back('\0');
    meta->smallest.set_InternalKey(str);
    ptr += n;
    recv_size += n;
    for (int i = 0; i < 24; i++) {
      printf("%d ",str.c_str()[i]);
    }
  } else {
    meta->smallest.set_InternalKey(std::string());
  }
  // printf("meta.smallest.DebugString:%s\n",
  //        meta->smallest.DebugString(true).c_str());
  
  n = *(uint32_t*)ptr;
  ptr += sizeof(uint32_t);
  recv_size += sizeof(uint32_t);
  if (n > 0) {
    std::string str(ptr, n);
    // str.push_back('\0');
    meta->largest.set_InternalKey(str);
    ptr += n;
    recv_size +=n;
  } else {
    meta->largest.set_InternalKey(std::string());
  }
  // printf("meta.largest.DebugString:%s\n",
  //        meta->largest.DebugString(true).c_str());
  
// printf("RECV: total_size after str: %d\n", recv_size);
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
  recv_size +=sizeof(uint32_t);
  if (n > 0) {
    std::string str(ptr, n);
    str.push_back('\0');
    meta->file_checksum = str;
    ptr += n;
    recv_size +=n;
  } else {
    meta->file_checksum = std::string();
  }

  n = *(uint32_t*)ptr;
  ptr += sizeof(uint32_t);
  recv_size +=sizeof(uint32_t);
  if (n > 0) {
    std::string str(ptr, n);
    str.push_back('\0');
    meta->file_checksum_func_name = str;
    ptr += n;
    recv_size +=n;
  } else {
    meta->file_checksum_func_name = std::string();
  }

  meta->tail_size = *(uint64_t*)ptr;
  ptr += sizeof(uint64_t);
  recv_size += sizeof(uint64_t);

  return recv_size;
}
  const char* GetFlushReasonString(FlushReason flush_reason) {
    switch (flush_reason) {
      case FlushReason::kOthers:
        return "Other Reasons";
      case FlushReason::kGetLiveFiles:
        return "Get Live Files";
      case FlushReason::kShutDown:
        return "Shut down";
      case FlushReason::kExternalFileIngestion:
        return "External File Ingestion";
      case FlushReason::kManualCompaction:
        return "Manual Compaction";
      case FlushReason::kWriteBufferManager:
        return "Write Buffer Manager";
      case FlushReason::kWriteBufferFull:
        return "Write Buffer Full";
      case FlushReason::kTest:
        return "Test";
      case FlushReason::kDeleteFiles:
        return "Delete Files";
      case FlushReason::kAutoCompaction:
        return "Auto Compaction";
      case FlushReason::kManualFlush:
        return "Manual Flush";
      case FlushReason::kErrorRecovery:
        return "Error Recovery";
      case FlushReason::kErrorRecoveryRetryFlush:
        return "Error Recovery Retry Flush";
      case FlushReason::kWalFull:
        return "WAL Full";
      default:
        return "Invalid";
    }
  }

  FlushJob::FlushJob(
      const std::string& dbname, ColumnFamilyData* cfd,
      const ImmutableDBOptions& db_options,
      const MutableCFOptions& mutable_cf_options, uint64_t max_memtable_id,
      const FileOptions& file_options, VersionSet* versions,
      InstrumentedMutex* db_mutex, std::atomic<bool>* shutting_down,
      std::vector<SequenceNumber> existing_snapshots,
      SequenceNumber earliest_write_conflict_snapshot,
      SnapshotChecker* snapshot_checker, JobContext* job_context,
      FlushReason flush_reason, LogBuffer* log_buffer, FSDirectory* db_directory,
      FSDirectory* output_file_directory, CompressionType output_compression,
      Statistics* stats, EventLogger* event_logger, bool measure_io_stats,
      const bool sync_output_directory, const bool write_manifest,
      Env::Priority thread_pri, const std::shared_ptr<IOTracer>& io_tracer,
      const SeqnoToTimeMapping& seqno_time_mapping, const std::string& db_id,
      const std::string& db_session_id, std::string full_history_ts_low,
      BlobFileCompletionCallback* blob_callback)
    : dbname_(dbname),
    db_id_(db_id),
    db_session_id_(db_session_id),
    cfd_(cfd),
    db_options_(db_options),
    mutable_cf_options_(mutable_cf_options),
    max_memtable_id_(max_memtable_id),
    file_options_(file_options),
    versions_(versions),
    db_mutex_(db_mutex),
    shutting_down_(shutting_down),
    existing_snapshots_(std::move(existing_snapshots)),
    earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
    snapshot_checker_(snapshot_checker),
    job_context_(job_context),
    flush_reason_(flush_reason),
    log_buffer_(log_buffer),
    db_directory_(db_directory),
    output_file_directory_(output_file_directory),
    output_compression_(output_compression),
    stats_(stats),
    event_logger_(event_logger),
    measure_io_stats_(measure_io_stats),
    sync_output_directory_(sync_output_directory),
    write_manifest_(write_manifest),
    edit_(nullptr),
    base_(nullptr),
    pick_memtable_called(false),
    thread_pri_(thread_pri),
    io_tracer_(io_tracer),
    clock_(db_options_.clock),
    full_history_ts_low_(std::move(full_history_ts_low)),
    blob_callback_(blob_callback),
    db_impl_seqno_time_mapping_(seqno_time_mapping) {
    // Update the thread status to indicate flush.
    ReportStartedFlush();
    TEST_SYNC_POINT("FlushJob::FlushJob()");
  }

  FlushJob::~FlushJob() { ThreadStatusUtil::ResetThreadStatus(); }

  void FlushJob::ReportStartedFlush() {
    ThreadStatusUtil::SetEnableTracking(db_options_.enable_thread_tracking);
    ThreadStatusUtil::SetColumnFamily(cfd_);
    ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_FLUSH);
    ThreadStatusUtil::SetThreadOperationProperty(ThreadStatus::COMPACTION_JOB_ID,
                                                 job_context_->job_id);

    IOSTATS_RESET(bytes_written);
  }

  void FlushJob::ReportFlushInputSize(const autovector<MemTable*>& mems) {
    uint64_t input_size = 0;
    for (auto* mem : mems) {
      input_size += mem->ApproximateMemoryUsage();
    }
    ThreadStatusUtil::IncreaseThreadOperationProperty(
        ThreadStatus::FLUSH_BYTES_MEMTABLES, input_size);
  }

  void FlushJob::RecordFlushIOStats() {
    RecordTick(stats_, FLUSH_WRITE_BYTES, IOSTATS(bytes_written));
    ThreadStatusUtil::IncreaseThreadOperationProperty(
        ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
    IOSTATS_RESET(bytes_written);
  }
  void FlushJob::PickMemTable() {
    db_mutex_->AssertHeld();
    assert(!pick_memtable_called);
    pick_memtable_called = true;

    // Maximum "NextLogNumber" of the memtables to flush.
    // When mempurge feature is turned off, this variable is useless
    // because the memtables are implicitly sorted by increasing order of creation
    // time. Therefore mems_->back()->GetNextLogNumber() is already equal to
    // max_next_log_number. However when Mempurge is on, the memtables are no
    // longer sorted by increasing order of creation time. Therefore this variable
    // becomes necessary because mems_->back()->GetNextLogNumber() is no longer
    // necessarily equal to max_next_log_number.
    uint64_t max_next_log_number = 0;

    // Save the contents of the earliest memtable as a new Table
    cfd_->imm()->PickMemtablesToFlush(max_memtable_id_, &mems_,
                                      &max_next_log_number);
    if (mems_.empty()) {
      return;
    }

    ReportFlushInputSize(mems_);

    // entries mems are (implicitly) sorted in ascending order by their created
    // time. We will use the first memtable's `edit` to keep the meta info for
    // this flush.
    MemTable* m = mems_[0];
    edit_ = m->GetEdits();
    edit_->SetPrevLogNumber(0);
    // SetLogNumber(log_num) indicates logs with number smaller than log_num
    // will no longer be picked up for recovery.
    edit_->SetLogNumber(max_next_log_number);
    edit_->SetColumnFamily(cfd_->GetID());

    // path 0 for level 0 file.
    meta_.fd = FileDescriptor(versions_->NewFileNumber(), 0, 0);
    meta_.epoch_number = cfd_->NewEpochNumber();

    base_ = cfd_->current();
    base_->Ref();  // it is likely that we do not need this reference
  }

  Status FlushJob::Run(LogsWithPrepTracker* prep_tracker, FileMetaData* file_meta,
                       bool* switched_to_mempurge) {
    TEST_SYNC_POINT("FlushJob::Start");
    db_mutex_->AssertHeld();
    assert(pick_memtable_called);
    // Mempurge threshold can be dynamically changed.
    // For sake of consistency, mempurge_threshold is
    // saved locally to maintain consistency in each
    // FlushJob::Run call.
    double mempurge_threshold =
      mutable_cf_options_.experimental_mempurge_threshold;

    AutoThreadOperationStageUpdater stage_run(ThreadStatus::STAGE_FLUSH_RUN);
    if (mems_.empty()) {
      ROCKS_LOG_BUFFER(log_buffer_, "[%s] Nothing in memtable to flush",
                       cfd_->GetName().c_str());
      return Status::OK();
    }

    // I/O measurement variables
    PerfLevel prev_perf_level = PerfLevel::kEnableTime;
    uint64_t prev_write_nanos = 0;
    uint64_t prev_fsync_nanos = 0;
    uint64_t prev_range_sync_nanos = 0;
    uint64_t prev_prepare_write_nanos = 0;
    uint64_t prev_cpu_write_nanos = 0;
    uint64_t prev_cpu_read_nanos = 0;
    if (measure_io_stats_) {
      prev_perf_level = GetPerfLevel();
      SetPerfLevel(PerfLevel::kEnableTime);
      prev_write_nanos = IOSTATS(write_nanos);
      prev_fsync_nanos = IOSTATS(fsync_nanos);
      prev_range_sync_nanos = IOSTATS(range_sync_nanos);
      prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
      prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
      prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
    }
    Status mempurge_s = Status::NotFound("No MemPurge.");
    if ((mempurge_threshold > 0.0) &&
        (flush_reason_ == FlushReason::kWriteBufferFull) && (!mems_.empty()) &&
        MemPurgeDecider(mempurge_threshold) && !(db_options_.atomic_flush)) {
      cfd_->SetMempurgeUsed();
      mempurge_s = MemPurge();
      if (!mempurge_s.ok()) {
        // Mempurge is typically aborted when the output
        // bytes cannot be contained onto a single output memtable.
        if (mempurge_s.IsAborted()) {
          ROCKS_LOG_INFO(db_options_.info_log, "Mempurge process aborted: %s\n",
                         mempurge_s.ToString().c_str());
        }
        else {
          // However the mempurge process can also fail for
          // other reasons (eg: new_mem->Add() fails).
          ROCKS_LOG_WARN(db_options_.info_log, "Mempurge process failed: %s\n",
                         mempurge_s.ToString().c_str());
        }
      }
      else {
        if (switched_to_mempurge) {
          *switched_to_mempurge = true;
        }
        else {
          // The mempurge process was successful, but no switch_to_mempurge
          // pointer provided so no way to propagate the state of flush job.
          ROCKS_LOG_WARN(db_options_.info_log,
                         "Mempurge process succeeded"
                         "but no 'switched_to_mempurge' ptr provided.\n");
        }
      }
    }
    Status s;
    if (mempurge_s.ok()) {
      base_->Unref();
      s = Status::OK();
    }
    else {
      // This will release and re-acquire the mutex.
      s = WriteLevel0Table();
    }

    if (s.ok() && cfd_->IsDropped()) {
      s = Status::ColumnFamilyDropped("Column family dropped during compaction");
    }
    if ((s.ok() || s.IsColumnFamilyDropped()) &&
        shutting_down_->load(std::memory_order_acquire)) {
      s = Status::ShutdownInProgress("Database shutdown");
    }

    if (!s.ok()) {
      cfd_->imm()->RollbackMemtableFlush(mems_, meta_.fd.GetNumber());
    }
    else if (write_manifest_) {
      TEST_SYNC_POINT("FlushJob::InstallResults");
      // Replace immutable memtable with the generated Table
      s = cfd_->imm()->TryInstallMemtableFlushResults(
          cfd_, mutable_cf_options_, mems_, prep_tracker, versions_, db_mutex_,
          meta_.fd.GetNumber(), &job_context_->memtables_to_free, db_directory_,
          log_buffer_, &committed_flush_jobs_info_,
          !(mempurge_s.ok()) /* write_edit : true if no mempurge happened (or if aborted),
                                but 'false' if mempurge successful: no new min log number
                                or new level 0 file path to write to manifest. */);
    }

    if (s.ok() && file_meta != nullptr) {
      *file_meta = meta_;
    }
    RecordFlushIOStats();

    // When measure_io_stats_ is true, the default 512 bytes is not enough.
    auto stream = event_logger_->LogToBuffer(log_buffer_, 1024);
    stream << "job" << job_context_->job_id << "event"
      << "flush_finished";
    stream << "output_compression"
      << CompressionTypeToString(output_compression_);
    stream << "lsm_state";
    stream.StartArray();
    auto vstorage = cfd_->current()->storage_info();
    for (int level = 0; level < vstorage->num_levels(); ++level) {
      stream << vstorage->NumLevelFiles(level);
    }
    stream.EndArray();

    const auto& blob_files = vstorage->GetBlobFiles();
    if (!blob_files.empty()) {
      assert(blob_files.front());
      stream << "blob_file_head" << blob_files.front()->GetBlobFileNumber();

      assert(blob_files.back());
      stream << "blob_file_tail" << blob_files.back()->GetBlobFileNumber();
    }

    stream << "immutable_memtables" << cfd_->imm()->NumNotFlushed();

    if (measure_io_stats_) {
      if (prev_perf_level != PerfLevel::kEnableTime) {
        SetPerfLevel(prev_perf_level);
      }
      stream << "file_write_nanos" << (IOSTATS(write_nanos) - prev_write_nanos);
      stream << "file_range_sync_nanos"
        << (IOSTATS(range_sync_nanos) - prev_range_sync_nanos);
      stream << "file_fsync_nanos" << (IOSTATS(fsync_nanos) - prev_fsync_nanos);
      stream << "file_prepare_write_nanos"
        << (IOSTATS(prepare_write_nanos) - prev_prepare_write_nanos);
      stream << "file_cpu_write_nanos"
        << (IOSTATS(cpu_write_nanos) - prev_cpu_write_nanos);
      stream << "file_cpu_read_nanos"
        << (IOSTATS(cpu_read_nanos) - prev_cpu_read_nanos);
    }

    TEST_SYNC_POINT("FlushJob::End");
    return s;
  }

  void FlushJob::Cancel() {
    db_mutex_->AssertHeld();
    assert(base_ != nullptr);
    base_->Unref();
  }

  Status FlushJob::MemPurge() {
    Status s;
    db_mutex_->AssertHeld();
    db_mutex_->Unlock();
    assert(!mems_.empty());

    // Measure purging time.
    const uint64_t start_micros = clock_->NowMicros();
    const uint64_t start_cpu_micros = clock_->CPUMicros();

    MemTable* new_mem = nullptr;
    // For performance/log investigation purposes:
    // look at how much useful payload we harvest in the new_mem.
    // This value is then printed to the DB log.
    double new_mem_capacity = 0.0;

    // Create two iterators, one for the memtable data (contains
    // info from puts + deletes), and one for the memtable
    // Range Tombstones (from DeleteRanges).
    // TODO: plumb Env::IOActivity
    ReadOptions ro;
    ro.total_order_seek = true;
    Arena arena;
    std::vector<InternalIterator*> memtables;
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
      range_del_iters;
    for (MemTable* m : mems_) {
      memtables.push_back(m->NewIterator(ro, &arena));
      auto* range_del_iter = m->NewRangeTombstoneIterator(
          ro, kMaxSequenceNumber, true /* immutable_memtable */);
      if (range_del_iter != nullptr) {
        range_del_iters.emplace_back(range_del_iter);
      }
    }

    assert(!memtables.empty());
    SequenceNumber first_seqno = kMaxSequenceNumber;
    SequenceNumber earliest_seqno = kMaxSequenceNumber;
    // Pick first and earliest seqno as min of all first_seqno
    // and earliest_seqno of the mempurged memtables.
    for (const auto& mem : mems_) {
      first_seqno = mem->GetFirstSequenceNumber() < first_seqno
        ? mem->GetFirstSequenceNumber()
        : first_seqno;
      earliest_seqno = mem->GetEarliestSequenceNumber() < earliest_seqno
        ? mem->GetEarliestSequenceNumber()
        : earliest_seqno;
    }

    ScopedArenaIterator iter(
        NewMergingIterator(&(cfd_->internal_comparator()), memtables.data(),
          static_cast<int>(memtables.size()), &arena));

    auto* ioptions = cfd_->ioptions();

    // Place iterator at the First (meaning most recent) key node.
    iter->SeekToFirst();

    const std::string* const full_history_ts_low = &(cfd_->GetFullHistoryTsLow());
    std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
        new CompactionRangeDelAggregator(&(cfd_->internal_comparator()),
          existing_snapshots_,
          full_history_ts_low));
    for (auto& rd_iter : range_del_iters) {
      range_del_agg->AddTombstones(std::move(rd_iter));
    }

    // If there is valid data in the memtable,
    // or at least range tombstones, copy over the info
    // to the new memtable.
    if (iter->Valid() || !range_del_agg->IsEmpty()) {
      // MaxSize is the size of a memtable.
      size_t maxSize = mutable_cf_options_.write_buffer_size;
      std::unique_ptr<CompactionFilter> compaction_filter;
      if (ioptions->compaction_filter_factory != nullptr &&
          ioptions->compaction_filter_factory->ShouldFilterTableFileCreation(
            TableFileCreationReason::kFlush)) {
        CompactionFilter::Context ctx;
        ctx.is_full_compaction = false;
        ctx.is_manual_compaction = false;
        ctx.column_family_id = cfd_->GetID();
        ctx.reason = TableFileCreationReason::kFlush;
        compaction_filter =
          ioptions->compaction_filter_factory->CreateCompactionFilter(ctx);
        if (compaction_filter != nullptr &&
            !compaction_filter->IgnoreSnapshots()) {
          s = Status::NotSupported(
              "CompactionFilter::IgnoreSnapshots() = false is not supported "
              "anymore.");
          return s;
        }
      }

      new_mem = new MemTable((cfd_->internal_comparator()), *(cfd_->ioptions()),
                             mutable_cf_options_, cfd_->write_buffer_mgr(),
                             earliest_seqno, cfd_->GetID());
      assert(new_mem != nullptr);

      Env* env = db_options_.env;
      assert(env);
      MergeHelper merge(
          env, (cfd_->internal_comparator()).user_comparator(),
          (ioptions->merge_operator).get(), compaction_filter.get(),
          ioptions->logger, true /* internal key corruption is not ok */,
          existing_snapshots_.empty() ? 0 : existing_snapshots_.back(),
          snapshot_checker_);
      assert(job_context_);
      SequenceNumber job_snapshot_seq = job_context_->GetJobSnapshotSequence();
      const std::atomic<bool> kManualCompactionCanceledFalse{ false };
      CompactionIterator c_iter(
          iter.get(), (cfd_->internal_comparator()).user_comparator(), &merge,
          kMaxSequenceNumber, &existing_snapshots_,
          earliest_write_conflict_snapshot_, job_snapshot_seq, snapshot_checker_,
          env, ShouldReportDetailedTime(env, ioptions->stats),
          true /* internal key corruption is not ok */, range_del_agg.get(),
          nullptr, ioptions->allow_data_in_errors,
          ioptions->enforce_single_del_contracts,
          /*manual_compaction_canceled=*/kManualCompactionCanceledFalse,
          /*compaction=*/nullptr, compaction_filter.get(),
          /*shutting_down=*/nullptr, ioptions->info_log, full_history_ts_low);

      // Set earliest sequence number in the new memtable
      // to be equal to the earliest sequence number of the
      // memtable being flushed (See later if there is a need
      // to update this number!).
      new_mem->SetEarliestSequenceNumber(earliest_seqno);
      // Likewise for first seq number.
      new_mem->SetFirstSequenceNumber(first_seqno);
      SequenceNumber new_first_seqno = kMaxSequenceNumber;

      c_iter.SeekToFirst();

      // Key transfer
      for (; c_iter.Valid(); c_iter.Next()) {
        const ParsedInternalKey ikey = c_iter.ikey();
        const Slice value = c_iter.value();
        new_first_seqno =
          ikey.sequence < new_first_seqno ? ikey.sequence : new_first_seqno;

        // Should we update "OldestKeyTime" ???? -> timestamp appear
        // to still be an "experimental" feature.
        s = new_mem->Add(
            ikey.sequence, ikey.type, ikey.user_key, value,
            nullptr,   // KV protection info set as nullptr since it
                       // should only be useful for the first add to
                       // the original memtable.
            false,     // : allow concurrent_memtable_writes_
                       // Not seen as necessary for now.
            nullptr,   // get_post_process_info(m) must be nullptr
                       // when concurrent_memtable_writes is switched off.
            nullptr);  // hint, only used when concurrent_memtable_writes_
        // is switched on.
        if (!s.ok()) {
          break;
        }

        // If new_mem has size greater than maxSize,
        // then rollback to regular flush operation,
        // and destroy new_mem.
        if (new_mem->ApproximateMemoryUsage() > maxSize) {
          s = Status::Aborted("Mempurge filled more than one memtable.");
          new_mem_capacity = 1.0;
          break;
        }
      }

      // Check status and propagate
      // potential error status from c_iter
      if (!s.ok()) {
        c_iter.status().PermitUncheckedError();
      }
      else if (!c_iter.status().ok()) {
        s = c_iter.status();
      }

      // Range tombstone transfer.
      if (s.ok()) {
        auto range_del_it = range_del_agg->NewIterator();
        for (range_del_it->SeekToFirst(); range_del_it->Valid();
             range_del_it->Next()) {
          auto tombstone = range_del_it->Tombstone();
          new_first_seqno =
            tombstone.seq_ < new_first_seqno ? tombstone.seq_ : new_first_seqno;
          s = new_mem->Add(
              tombstone.seq_,        // Sequence number
              kTypeRangeDeletion,    // KV type
              tombstone.start_key_,  // Key is start key.
              tombstone.end_key_,    // Value is end key.
              nullptr,               // KV protection info set as nullptr since it
                                     // should only be useful for the first add to
                                     // the original memtable.
              false,                 // : allow concurrent_memtable_writes_
                                     // Not seen as necessary for now.
              nullptr,               // get_post_process_info(m) must be nullptr
                        // when concurrent_memtable_writes is switched off.
              nullptr);  // hint, only used when concurrent_memtable_writes_
          // is switched on.

          if (!s.ok()) {
            break;
          }

          // If new_mem has size greater than maxSize,
          // then rollback to regular flush operation,
          // and destroy new_mem.
          if (new_mem->ApproximateMemoryUsage() > maxSize) {
            s = Status::Aborted(Slice("Mempurge filled more than one memtable."));
            new_mem_capacity = 1.0;
            break;
          }
        }
      }

      // If everything happened smoothly and new_mem contains valid data,
      // decide if it is flushed to storage or kept in the imm()
      // memtable list (memory).
      if (s.ok() && (new_first_seqno != kMaxSequenceNumber)) {
        // Rectify the first sequence number, which (unlike the earliest seq
        // number) needs to be present in the new memtable.
        new_mem->SetFirstSequenceNumber(new_first_seqno);

        // The new_mem is added to the list of immutable memtables
        // only if it filled at less than 100% capacity and isn't flagged
        // as in need of being flushed.
        if (new_mem->ApproximateMemoryUsage() < maxSize &&
            !(new_mem->ShouldFlushNow())) {
          // Construct fragmented memtable range tombstones without mutex
          new_mem->ConstructFragmentedRangeTombstones();
          db_mutex_->Lock();
          uint64_t new_mem_id = mems_[0]->GetID();

          new_mem->SetID(new_mem_id);
          new_mem->SetNextLogNumber(mems_[0]->GetNextLogNumber());

          // This addition will not trigger another flush, because
          // we do not call SchedulePendingFlush().
          cfd_->imm()->Add(new_mem, &job_context_->memtables_to_free);
          new_mem->Ref();
          // Piggyback FlushJobInfo on the first flushed memtable.
          db_mutex_->AssertHeld();
          meta_.fd.file_size = 0;
          mems_[0]->SetFlushJobInfo(GetFlushJobInfo());
          db_mutex_->Unlock();
        }
        else {
          s = Status::Aborted(Slice("Mempurge filled more than one memtable."));
          new_mem_capacity = 1.0;
          if (new_mem) {
            job_context_->memtables_to_free.push_back(new_mem);
          }
        }
      }
      else {
        // In this case, the newly allocated new_mem is empty.
        assert(new_mem != nullptr);
        job_context_->memtables_to_free.push_back(new_mem);
      }
    }

    // Reacquire the mutex for WriteLevel0 function.
    db_mutex_->Lock();

    // If mempurge successful, don't write input tables to level0,
    // but write any full output table to level0.
    if (s.ok()) {
      TEST_SYNC_POINT("DBImpl::FlushJob:MemPurgeSuccessful");
    }
    else {
      TEST_SYNC_POINT("DBImpl::FlushJob:MemPurgeUnsuccessful");
    }
    const uint64_t micros = clock_->NowMicros() - start_micros;
    const uint64_t cpu_micros = clock_->CPUMicros() - start_cpu_micros;
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Mempurge lasted %" PRIu64
                   " microseconds, and %" PRIu64
                   " cpu "
                   "microseconds. Status is %s ok. Perc capacity: %f\n",
                   cfd_->GetName().c_str(), job_context_->job_id, micros,
                   cpu_micros, s.ok() ? "" : "not", new_mem_capacity);

    return s;
  }

  bool FlushJob::MemPurgeDecider(double threshold) {
    // Never trigger mempurge if threshold is not a strictly positive value.
    if (!(threshold > 0.0)) {
      return false;
    }
    if (threshold > (1.0 * mems_.size())) {
      return true;
    }
    // Payload and useful_payload (in bytes).
    // The useful payload ratio of a given MemTable
    // is estimated to be useful_payload/payload.
    uint64_t payload = 0, useful_payload = 0, entry_size = 0;

    // Local variables used repetitively inside the for-loop
    // when iterating over the sampled entries.
    Slice key_slice, value_slice;
    ParsedInternalKey res;
    SnapshotImpl min_snapshot;
    std::string vget;
    Status mget_s, parse_s;
    MergeContext merge_context;
    SequenceNumber max_covering_tombstone_seq = 0, sqno = 0,
      min_seqno_snapshot = 0;
    bool get_res, can_be_useful_payload, not_in_next_mems;

    // If estimated_useful_payload is > threshold,
    // then flush to storage, else MemPurge.
    double estimated_useful_payload = 0.0;
    // Cochran formula for determining sample size.
    // 95% confidence interval, 7% precision.
    //    n0 = (1.96*1.96)*0.25/(0.07*0.07) = 196.0
    // TODO: plumb Env::IOActivity
    double n0 = 196.0;
    ReadOptions ro;
    ro.total_order_seek = true;

    // Iterate over each memtable of the set.
    for (auto mem_iter = std::begin(mems_); mem_iter != std::end(mems_);
         mem_iter++) {
      MemTable* mt = *mem_iter;

      // Else sample from the table.
      uint64_t nentries = mt->num_entries();
      // Corrected Cochran formula for small populations
      // (converges to n0 for large populations).
      uint64_t target_sample_size =
        static_cast<uint64_t>(ceil(n0 / (1.0 + (n0 / nentries))));
      std::unordered_set<const char*> sentries = {};
      // Populate sample entries set.
      mt->UniqueRandomSample(target_sample_size, &sentries);

      // Estimate the garbage ratio by comparing if
      // each sample corresponds to a valid entry.
      for (const char* ss : sentries) {
        key_slice = GetLengthPrefixedSlice(ss);
        parse_s = ParseInternalKey(key_slice, &res, true /*log_err_key*/);
        if (!parse_s.ok()) {
          ROCKS_LOG_WARN(db_options_.info_log,
                         "Memtable Decider: ParseInternalKey did not parse "
                         "key_slice %s successfully.",
                         key_slice.data());
        }

        // Size of the entry is "key size (+ value size if KV entry)"
        entry_size = key_slice.size();
        if (res.type == kTypeValue) {
          value_slice =
            GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
          entry_size += value_slice.size();
        }

        // Count entry bytes as payload.
        payload += entry_size;

        LookupKey lkey(res.user_key, kMaxSequenceNumber);

        // Paranoia: zero out these values just in case.
        max_covering_tombstone_seq = 0;
        sqno = 0;

        // Pick the oldest existing snapshot that is more recent
        // than the sequence number of the sampled entry.
        min_seqno_snapshot = kMaxSequenceNumber;
        for (SequenceNumber seq_num : existing_snapshots_) {
          if (seq_num > res.sequence && seq_num < min_seqno_snapshot) {
            min_seqno_snapshot = seq_num;
          }
        }
        min_snapshot.number_ = min_seqno_snapshot;
        ro.snapshot =
          min_seqno_snapshot < kMaxSequenceNumber ? &min_snapshot : nullptr;

        // Estimate if the sample entry is valid or not.
        get_res = mt->Get(lkey, &vget, /*columns=*/nullptr, /*timestamp=*/nullptr,
                          &mget_s, &merge_context, &max_covering_tombstone_seq,
                          &sqno, ro, true /* immutable_memtable */);
        if (!get_res) {
          ROCKS_LOG_WARN(
              db_options_.info_log,
              "Memtable Get returned false when Get(sampled entry). "
              "Yet each sample entry should exist somewhere in the memtable, "
              "unrelated to whether it has been deleted or not.");
        }

        // TODO(bjlemaire): evaluate typeMerge.
        // This is where the sampled entry is estimated to be
        // garbage or not. Note that this is a garbage *estimation*
        // because we do not include certain items such as
        // CompactionFitlers triggered at flush, or if the same delete
        // has been inserted twice or more in the memtable.

        // Evaluate if the entry can be useful payload
        // Situation #1: entry is a KV entry, was found in the memtable mt
        //               and the sequence numbers match.
        can_be_useful_payload = (res.type == kTypeValue) && get_res &&
          mget_s.ok() && (sqno == res.sequence);

        // Situation #2: entry is a delete entry, was found in the memtable mt
        //               (because gres==true) and no valid KV entry is found.
        //               (note: duplicate delete entries are also taken into
        //               account here, because the sequence number 'sqno'
        //               in memtable->Get(&sqno) operation is set to be equal
        //               to the most recent delete entry as well).
        can_be_useful_payload |=
          ((res.type == kTypeDeletion) || (res.type == kTypeSingleDeletion)) &&
          mget_s.IsNotFound() && get_res && (sqno == res.sequence);

        // If there is a chance that the entry is useful payload
        // Verify that the entry does not appear in the following memtables
        // (memtables with greater memtable ID/larger sequence numbers).
        if (can_be_useful_payload) {
          not_in_next_mems = true;
          for (auto next_mem_iter = mem_iter + 1;
               next_mem_iter != std::end(mems_); next_mem_iter++) {
            if ((*next_mem_iter)
                    ->Get(lkey, &vget, /*columns=*/nullptr, /*timestamp=*/nullptr,
                      &mget_s, &merge_context, &max_covering_tombstone_seq,
                      &sqno, ro, true /* immutable_memtable */)) {
              not_in_next_mems = false;
              break;
            }
          }
          if (not_in_next_mems) {
            useful_payload += entry_size;
          }
        }
      }
      if (payload > 0) {
        // We use the estimated useful payload ratio to
        // evaluate how many of the memtable bytes are useful bytes.
        estimated_useful_payload +=
          (mt->ApproximateMemoryUsage()) * (useful_payload * 1.0 / payload);

        ROCKS_LOG_INFO(db_options_.info_log,
                       "Mempurge sampling [CF %s] - found garbage ratio from "
                       "sampling: %f. Threshold is %f\n",
                       cfd_->GetName().c_str(),
                       (payload - useful_payload) * 1.0 / payload, threshold);
      }
      else {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Mempurge sampling: null payload measured, and collected "
                       "sample size is %zu\n.",
                       sentries.size());
      }
    }
    // We convert the total number of useful payload bytes
    // into the proportion of memtable necessary to store all these bytes.
    // We compare this proportion with the threshold value.
    return ((estimated_useful_payload / mutable_cf_options_.write_buffer_size) <
            threshold);
  }

  int ConnectToServer() {
    auto client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0) {
      printf("\n Socket creation error \n");
      return -1;
    }

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;

    serv_addr.sin_port = htons(10086 );
  if (inet_pton(AF_INET, "192.168.2.21", &serv_addr.sin_addr) <= 0) {
      printf("\nInvalid address/ Address not supported \n");
      return -1;
    }

    if ((connect(client_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr))) <
        0) {
      printf("\nConnection Failed \n");
      return -1;
    }
    return client_fd;
  }

  Status FlushJob::WriteLevel0Table() {
    AutoThreadOperationStageUpdater stage_updater(
        ThreadStatus::STAGE_FLUSH_WRITE_L0);
    db_mutex_->AssertHeld();
    const uint64_t start_micros = clock_->NowMicros();
    const uint64_t start_cpu_micros = clock_->CPUMicros();
    Status s;

    SequenceNumber smallest_seqno = mems_.front()->GetEarliestSequenceNumber();
    if (!db_impl_seqno_time_mapping_.Empty()) {
      // make a local copy, as the seqno_time_mapping from db_impl is not thread
      // safe, which will be used while not holding the db_mutex.
      seqno_to_time_mapping_ = db_impl_seqno_time_mapping_.Copy(smallest_seqno);
    }

    std::vector<BlobFileAddition> blob_file_additions;

    {
      auto write_hint = cfd_->CalculateSSTWriteHint(0);
      Env::IOPriority io_priority = GetRateLimiterPriorityForWrite();
      db_mutex_->Unlock();
      if (log_buffer_) {
        log_buffer_->FlushBufferToLog();
      }
      // memtables and range_del_iters store internal iterators over each data
      // memtable and its associated range deletion memtable, respectively, at
      // corresponding indexes.
      std::vector<InternalIterator*> memtables;
      std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
        range_del_iters;
      ReadOptions ro;
      ro.total_order_seek = true;
      ro.io_activity = Env::IOActivity::kFlush;
      Arena arena;
      uint64_t total_num_entries = 0, total_num_deletes = 0;
      uint64_t total_data_size = 0;
      size_t total_memory_usage = 0;
      // Used for testing:
      uint64_t mems_size = mems_.size();
      if (mems_size != 1) {
        fprintf(stderr,"mems_size:%d\n",mems_size);
      }
      (void)mems_size;  // avoids unused variable error when
      // TEST_SYNC_POINT_CALLBACK not used.
      TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table:num_memtables",
                               &mems_size);
      assert(job_context_);
/******************************************************************************************************************************* */
/******************************************************************************************************************************* */
      assert(mems_size == 1); // 我自己打印出来是一直等于 1 的
#ifdef DFLUSH
    uintptr_t mt_buf_head=0;
    uintptr_t Node_head=0;
    bool mt_flag_=false;
      static uint64_t nums = 0;
#endif
      for (MemTable* m : mems_) {
        ROCKS_LOG_INFO(
            db_options_.info_log,
            "[%s] [JOB %d] Flushing memtable with next log file: %" PRIu64 "\n",
            cfd_->GetName().c_str(), job_context_->job_id, m->GetNextLogNumber());
        memtables.push_back(m->NewIterator(ro, &arena));
        memtables[memtables.size() - 1]->SeekToFirst();
#ifdef DFLUSH
        Node_head = (uintptr_t)memtables[memtables.size() - 1]->Current();
        // printf("nums:%ld, head_key_ptr:%lx\n", nums, (uintptr_t)memtables[memtables.size() - 1]->key().data());
        // printf("nums:%ld, key1:%ld\n", nums, *(uint64_t*)memtables[memtables.size() - 1]->key().data());
        memtables[memtables.size() - 1]->Next();
        // printf("nums:%ld, next_key_ptr:%lx\n", nums, (uintptr_t)memtables[memtables.size() - 1]->key().data());
        // printf("nums:%ld, key2:%ld\n", nums++, *(uint64_t*)memtables[memtables.size() - 1]->key().data());
        mt_buf_head = (uintptr_t)m->get_mt_buf_();
        mt_flag_ = m->get_mt_flag_();
#endif
        auto* range_del_iter = m->NewRangeTombstoneIterator(
            ro, kMaxSequenceNumber, true /* immutable_memtable */);
        if (range_del_iter != nullptr) {
          range_del_iters.emplace_back(range_del_iter);
        }
        total_num_entries += m->num_entries();
        total_num_deletes += m->num_deletes();
        total_data_size += m->get_data_size();
        total_memory_usage += m->ApproximateMemoryUsage();
      }

#ifdef DFLUSH
      // 把必要信息传到 DPU 端
      // char buffer[1024];
      // char* ptr = buffer;
      // *(uintptr_t*)ptr = Node_head;
      // ptr += sizeof(uintptr_t);
      // *(uintptr_t*)ptr = mt_buf_head;
      // ptr += sizeof(uintptr_t);
      // *(uint64_t*)ptr = FLAGS_env->mmap_export_desc.size();
      // ptr += sizeof(uint64_t);
      // memcpy(ptr, FLAGS_env->mmap_export_desc.data(), FLAGS_env->mmap_export_desc.size());
      // uint64_t total_size = 2 * sizeof(uintptr_t) + sizeof(uint64_t) + FLAGS_env->mmap_export_desc.size();
      // auto client_fd = ConnectToServer();
      // printf("mt_buf_head:%lx, key_head:%lx, mt_flag_:%d\n", mt_buf_head, Node_head, mt_flag_);
      // send(client_fd, buffer, total_size, 0);

      // read(client_fd, buffer, 1024); //通过 read 阻塞
#endif

      event_logger_->Log() << "job" << job_context_->job_id << "event"
        << "flush_started"
        << "num_memtables" << mems_.size() << "num_entries"
        << total_num_entries << "num_deletes"
        << total_num_deletes << "total_data_size"
        << total_data_size << "memory_usage"
        << total_memory_usage << "flush_reason"
        << GetFlushReasonString(flush_reason_);

      {
        ScopedArenaIterator iter(
            NewMergingIterator(&cfd_->internal_comparator(), memtables.data(),
              static_cast<int>(memtables.size()), &arena));
        ROCKS_LOG_INFO(db_options_.info_log,
                       "[%s] [JOB %d] Level-0 flush table #%" PRIu64 ": started",
                       cfd_->GetName().c_str(), job_context_->job_id,
                       meta_.fd.GetNumber());

        TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table:output_compression",
                                 &output_compression_);
        int64_t _current_time = 0;
        auto status = clock_->GetCurrentTime(&_current_time);
        // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
        if (!status.ok()) {
          ROCKS_LOG_WARN(
              db_options_.info_log,
              "Failed to get current time to populate creation_time property. "
              "Status: %s",
              status.ToString().c_str());
        }
        const uint64_t current_time = static_cast<uint64_t>(_current_time);

        uint64_t oldest_key_time = mems_.front()->ApproximateOldestKeyTime();

        // It's not clear whether oldest_key_time is always available. In case
        // it is not available, use current_time.
        uint64_t oldest_ancester_time = std::min(current_time, oldest_key_time);

        TEST_SYNC_POINT_CALLBACK(
            "FlushJob::WriteLevel0Table:oldest_ancester_time",
            &oldest_ancester_time);
        meta_.oldest_ancester_time = oldest_ancester_time;
        meta_.file_creation_time = current_time;

        uint64_t num_input_entries = 0;
        uint64_t memtable_payload_bytes = 0;
        uint64_t memtable_garbage_bytes = 0;
        IOStatus io_s;

        const std::string* const full_history_ts_low =
          (full_history_ts_low_.empty()) ? nullptr : &full_history_ts_low_;
        TableBuilderOptions tboptions(
            *cfd_->ioptions(), mutable_cf_options_, cfd_->internal_comparator(),
            cfd_->int_tbl_prop_collector_factories(), output_compression_,
            mutable_cf_options_.compression_opts, cfd_->GetID(), cfd_->GetName(),
            0 /* level */, false /* is_bottommost */,
            TableFileCreationReason::kFlush, oldest_key_time, current_time,
            db_id_, db_session_id_, 0 /* target_file_size */,
            meta_.fd.GetNumber());
        const SequenceNumber job_snapshot_seq =
          job_context_->GetJobSnapshotSequence();
        const ReadOptions read_options(Env::IOActivity::kFlush);

/******************************************************************************************************************************* */
/******************************************************************************************************************************* */
#ifdef DFLUSH


    // InternalIterator* iter = NewIterator(&new_m,ro, &arena,cfd_internal_comparator);
    /* test */
    iter->SeekToFirst();
    Slice key = iter->key();
    printf("key:%ld\n", *(uint64_t*)key.data());
    Slice value = iter->value();
    printf("value:%ld\n", *(uint64_t*)value.data());
    iter->Next();
    key = iter->key();
    value = iter->value();
    printf("key:%ld\n",*(uint64_t*)key.data());
    printf("value:%ld\n", *(uint64_t*)value.data());
    iter->SeekToFirst();


        
    // 把必要信息传到 DPU 端
    char buffer[1024];
    char* ptr = buffer;
    uint64_t total_size = 0;

    // mems
    *(uintptr_t*)ptr = Node_head;
    ptr += sizeof(uintptr_t);
    *(uintptr_t*)ptr = mt_buf_head; 
    ptr += sizeof(uintptr_t);
    total_size += 2 * sizeof(uintptr_t);
    printf("total_size after mems: %ld\n", total_size);
    printf("check check mt_buf_head:%lx, key_head:%lx, mt_flag_:%d\n",
           mt_buf_head, Node_head, mt_flag_);

    // meta_
    int meta_size = send_meta(&meta_, ptr);    // memcpy((FileMetaData*)ptr,&meta_,  sizeof(FileMetaData));
    ptr += meta_size;
    total_size += meta_size;
    printf("total_size after meta: %ld\n", total_size);
    
    // new_versions_NewFileNumber
    *(uint64_t*)ptr = versions_->NewFileNumber();
    ptr += sizeof(uint64_t);
    total_size += sizeof(uint64_t);
    
    // seqno_to_time_mapping_
    memcpy((SeqnoToTimeMapping*)ptr, &seqno_to_time_mapping_, sizeof(SeqnoToTimeMapping));
    ptr += sizeof(SeqnoToTimeMapping);
    total_size += sizeof(SeqnoToTimeMapping);

    // // kUnknownColumnFamily
    *(uint32_t*)ptr =
        TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;
    ptr += sizeof(uint32_t);
    total_size += sizeof(uint32_t);

    // full_history_ts_low
    if(full_history_ts_low!=nullptr){
      *(uint32_t*)ptr = (uint32_t)(*full_history_ts_low).size();
      ptr += sizeof(uint32_t);
      total_size += sizeof(uint32_t);
    if((uint32_t)(*full_history_ts_low).size()>0){
      memcpy(ptr,full_history_ts_low,(uint32_t)(*full_history_ts_low).size());
      ptr += (*full_history_ts_low).size();
      total_size += (*full_history_ts_low).size();
    }
    }else
    {
       *(uint32_t*)ptr = (uint32_t)0;
       ptr += sizeof(uint32_t);
       total_size += sizeof(uint32_t);
    }
    

    // io_priority
    *(Env::IOPriority*)ptr =io_priority;
    ptr += sizeof(Env::IOPriority);
    total_size += sizeof(Env::IOPriority);

    // paranoid_file_checks
    *(bool*)ptr =mutable_cf_options_.paranoid_file_checks;
    ptr += sizeof(bool);
    total_size += sizeof(bool);

    // job_id
    *(int*)ptr =job_context_->job_id;
    ptr += sizeof(int);
    total_size += sizeof(int);

    // snapshots
    *(uint32_t*)ptr = (uint32_t)existing_snapshots_.size();
    ptr += sizeof(uint32_t);
    total_size += sizeof(uint32_t);
    if((uint32_t)existing_snapshots_.size()>0){
      std::copy(existing_snapshots_.begin(), existing_snapshots_.end(), (SequenceNumber*)ptr);
      ptr += sizeof(SequenceNumber)*existing_snapshots_.size();
      total_size += sizeof(SequenceNumber) * existing_snapshots_.size();
    }

    // earliest_write_conflict_snapshot
    *(SequenceNumber*)ptr = earliest_write_conflict_snapshot_;
    ptr += sizeof(SequenceNumber);
    total_size += sizeof(SequenceNumber);

    // job_snapshot
    *(SequenceNumber*)ptr = job_context_->GetJobSnapshotSequence();
    ptr += sizeof(SequenceNumber);
    total_size += sizeof(SequenceNumber);

    // timestamp_size
    size_t timestamp_size=tboptions.internal_comparator.user_comparator()->timestamp_size();
    *(size_t*)ptr = timestamp_size;
    ptr += sizeof(size_t);
    total_size += sizeof(size_t);

    // tboptions_ioptions_cf_paths
    *(uint32_t*)ptr = (uint32_t)tboptions.ioptions.cf_paths.size();
    ptr += sizeof(uint32_t);
    total_size += sizeof(uint32_t);

    if ((uint32_t)tboptions.ioptions.cf_paths.size() > 0) {
      DbPath_struct send_tboptions_ioptions_cf_paths
          [(uint32_t)tboptions.ioptions.cf_paths.size()];
      for (int i = 0; i < tboptions.ioptions.cf_paths.size(); i++) {
        std::strcpy(send_tboptions_ioptions_cf_paths[i].path,tboptions.ioptions.cf_paths[i].path.c_str());
        send_tboptions_ioptions_cf_paths[i].target_size =
            tboptions.ioptions.cf_paths[i].target_size;
        
        *(DbPath_struct*)ptr = send_tboptions_ioptions_cf_paths[i];
        ptr += sizeof(DbPath_struct);
        total_size += sizeof(DbPath_struct);
      }
    }

    // meta_.fd.smallest_seqno
    *(SequenceNumber*)ptr = meta_.fd.smallest_seqno;
    ptr += sizeof(SequenceNumber);
    total_size += sizeof(SequenceNumber);
    *(SequenceNumber*)ptr = meta_.fd.largest_seqno;
    ptr += sizeof(SequenceNumber);
    total_size += sizeof(SequenceNumber);
    
    // cfd_GetName
    *(uint32_t*)ptr = (uint32_t)(cfd_->GetName()).size();
    ptr += sizeof(uint32_t);
    total_size += sizeof(uint32_t);
    strncpy(ptr,(cfd_->GetName()).c_str(),(uint32_t)(cfd_->GetName()).size());
    ptr += (cfd_->GetName()).size();
    total_size += (cfd_->GetName()).size();

    // dbname
    *(uint32_t*)ptr = (uint32_t)(dbname_).size();
    ptr += sizeof(uint32_t);
    total_size += sizeof(uint32_t);
    strncpy(ptr,(dbname_).c_str(),(uint32_t)(dbname_).size());
    ptr += (dbname_).size();
    total_size += (dbname_).size();
    printf("total_size after dbname: %ld %ld\n", total_size, ptr - buffer);

    
    // FLAGS_env->mmap_export_desc
    *(uint64_t*)ptr = FLAGS_env->mmap_export_desc.size();
    ptr += sizeof(uint64_t);
    total_size += sizeof(uint64_t) + FLAGS_env->mmap_export_desc.size();
    printf("total_size: %ld %ld\n",total_size,ptr - buffer);
    
    memcpy(ptr, FLAGS_env->mmap_export_desc.data(), FLAGS_env->mmap_export_desc.size());

    auto client_fd = ConnectToServer();
    printf("mt_buf_head:%lx, key_head:%lx, mt_flag_:%d\n", mt_buf_head,
           Node_head, mt_flag_);

    std::chrono::steady_clock::time_point start =
        std::chrono::steady_clock::now();
    printf("AAAAAAAAAAAA\n\n");
    send(client_fd, buffer, total_size, 0);
        printf("BBBBBBBBBBBBBBBBBB\n\n");
        read(client_fd, buffer, 1024);
            printf("CCCCCCCCCCCCCCC\n\n");
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::chrono::milliseconds duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "    本次时间间隔: " << static_cast<double>(duration.count()) << " 毫秒"
              << std::endl;
    
    time_sum += duration;
    Build_Table_num++;
    double averageDuration =
        static_cast<double>(time_sum.count()) / Build_Table_num;
    std::cout << "次数：" << Build_Table_num << "    平均时间间隔: " << averageDuration << " 毫秒" << std::endl;

    ptr = buffer;

    meta_size = recv_meta(&meta_, ptr);
    ptr += meta_size;
    printf("RECV: total_size after meta_: %ld\n", ptr - buffer);

    s = *(rocksdb::Status*)ptr;
    ptr += sizeof(rocksdb::Status);

    num_input_entries = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);

    memtable_payload_bytes = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);

    memtable_garbage_bytes = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);

    meta_.fd.packed_number_and_path_id = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);

    meta_.fd.file_size = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);

    meta_.fd.smallest_seqno = *(rocksdb::SequenceNumber*)ptr;
    ptr += sizeof(rocksdb::SequenceNumber);
            
    meta_.fd.largest_seqno = *(rocksdb::SequenceNumber*)ptr;
    ptr += sizeof(rocksdb::SequenceNumber);

#endif

  // printf("meta.smallest.DebugString:%s\n", meta_.smallest.DebugString(true).c_str());
  // printf("meta.largest.DebugString:%s\n",meta_.largest.DebugString(true).c_str());
/******************************************************************************************************************************* */
/******************************************************************************************************************************* */
/******************************************************************************************************************************* */
    s = BuildTable(dbname_, versions_, db_options_, tboptions, file_options_,
               read_options, cfd_->table_cache(), iter.get(),
               std::move(range_del_iters), &meta_, &blob_file_additions,
               existing_snapshots_, earliest_write_conflict_snapshot_,
               job_snapshot_seq, snapshot_checker_,
               mutable_cf_options_.paranoid_file_checks,
               cfd_->internal_stats(), &io_s, io_tracer_,
               BlobFileCreationReason::kFlush, seqno_to_time_mapping_,
               event_logger_, job_context_->job_id, io_priority,
               &table_properties_, write_hint, full_history_ts_low,
               blob_callback_, base_, &num_input_entries,
               &memtable_payload_bytes, &memtable_garbage_bytes);
    /****************************************************************************************** */
    //   Status return_status;
    //   BuildTable_new(  // versions_->current_next_file_number()
    //       mems_, &meta_, versions_->NewFileNumber(), seqno_to_time_mapping_,
    //       TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
    //       mutable_cf_options_.paranoid_file_checks, job_context_->job_id,
    //      earliest_write_conflict_snapshot_,
    //       job_snapshot_seq, timestamp_size, tboptions.ioptions.cf_paths,
    //       cfd_->GetName(), dbname_,  
    //       // 在实际情况中，以下参数需要回传
    //       &return_status, &num_input_entries, &memtable_payload_bytes,
    //       &memtable_garbage_bytes, &meta_.fd.packed_number_and_path_id,
    //       &meta_.fd.file_size,&meta_.fd.smallest_seqno,&meta_.fd.largest_seqno);
    //   s = return_status;
    // /****************************************************************************************** */


      printf(
      "Flush_result:   num_input_entries:%ld memtable_payload_bytes:%ld "
      "memtable_garbage_bytes:%ld packed_number_and_path_id:%ld file_size:%ld "
      "largest_seqno:%ld smallest_seqno:%ld\n",
      num_input_entries, memtable_payload_bytes, memtable_garbage_bytes,
      meta_.fd.packed_number_and_path_id, meta_.fd.file_size, meta_.fd.largest_seqno, meta_.fd.smallest_seqno);
/******************************************************************************************************************************* */
/******************************************************************************************************************************* */
/******************************************************************************************************************************* */
        // TODO: Cleanup io_status in BuildTable and table builders
        assert(!s.ok() || io_s.ok());
        io_s.PermitUncheckedError();
        if (num_input_entries != total_num_entries && s.ok()) {
          std::string msg = "Expected " + std::to_string(total_num_entries) +
            " entries in memtables, but read " +
            std::to_string(num_input_entries);
          ROCKS_LOG_WARN(db_options_.info_log, "[%s] [JOB %d] Level-0 flush %s",
                         cfd_->GetName().c_str(), job_context_->job_id,
                         msg.c_str());
          if (db_options_.flush_verify_memtable_count) {
            s = Status::Corruption(msg);
          }
        }
        if (tboptions.reason == TableFileCreationReason::kFlush) {
          TEST_SYNC_POINT("DBImpl::FlushJob:Flush");
          RecordTick(stats_, MEMTABLE_PAYLOAD_BYTES_AT_FLUSH,
                     memtable_payload_bytes);
          RecordTick(stats_, MEMTABLE_GARBAGE_BYTES_AT_FLUSH,
                     memtable_garbage_bytes);
        }
        LogFlush(db_options_.info_log);
      }
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] [JOB %d] Level-0 flush table #%" PRIu64 ": %" PRIu64
                       " bytes %s"
                       "%s",
                       cfd_->GetName().c_str(), job_context_->job_id,
                       meta_.fd.GetNumber(), meta_.fd.GetFileSize(),
                       s.ToString().c_str(),
                       meta_.marked_for_compaction ? " (needs compaction)" : "");

      if (s.ok() && output_file_directory_ != nullptr && sync_output_directory_) {
        s = output_file_directory_->FsyncWithDirOptions(
            IOOptions(), nullptr,
            DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
      }
      TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table", &mems_);
      db_mutex_->Lock();
    }
    base_->Unref();

    // Note that if file_size is zero, the file has been deleted and
    // should not be added to the manifest.
    const bool has_output = meta_.fd.GetFileSize() > 0;

    if (s.ok() && has_output) {
      // TEST_SYNC_POINT("DBImpl::FlushJob:SSTFileCreated");
      // if we have more than 1 background thread, then we cannot
      // insert files directly into higher levels because some other
      // threads could be concurrently producing compacted files for
      // that key range.
      // Add file to L0
      edit_->AddFile(0 /* level */, meta_.fd.GetNumber(), meta_.fd.GetPathId(),
                     meta_.fd.GetFileSize(), meta_.smallest, meta_.largest,
                     meta_.fd.smallest_seqno, meta_.fd.largest_seqno,
                     meta_.marked_for_compaction, meta_.temperature,
                     meta_.oldest_blob_file_number, meta_.oldest_ancester_time,
                     meta_.file_creation_time, meta_.epoch_number,
                     meta_.file_checksum, meta_.file_checksum_func_name,
                     meta_.unique_id, meta_.compensated_range_deletion_size,
                     meta_.tail_size);
      
      // edit_->SetBlobFileAdditions(std::move(blob_file_additions));
    }
    // Piggyback FlushJobInfo on the first first flushed memtable.
    mems_[0]->SetFlushJobInfo(GetFlushJobInfo());

    // Note that here we treat flush as level 0 compaction in internal stats
    InternalStats::CompactionStats stats(CompactionReason::kFlush, 1);
    const uint64_t micros = clock_->NowMicros() - start_micros;
    const uint64_t cpu_micros = clock_->CPUMicros() - start_cpu_micros;
    stats.micros = micros;
    stats.cpu_micros = cpu_micros;

    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Flush lasted %" PRIu64
                   " microseconds, and %" PRIu64 " cpu microseconds.\n",
                   cfd_->GetName().c_str(), job_context_->job_id, micros,
                   cpu_micros);

    if (has_output) {
      stats.bytes_written = meta_.fd.GetFileSize();
      stats.num_output_files = 1;
    }

    const auto& blobs = edit_->GetBlobFileAdditions();
    for (const auto& blob : blobs) {
      stats.bytes_written_blob += blob.GetTotalBlobBytes();
    }

    stats.num_output_files_blob = static_cast<int>(blobs.size());

    RecordTimeToHistogram(stats_, FLUSH_TIME, stats.micros);
    cfd_->internal_stats()->AddCompactionStats(0 /* level */, thread_pri_, stats);
    cfd_->internal_stats()->AddCFStats(
        InternalStats::BYTES_FLUSHED,
        stats.bytes_written + stats.bytes_written_blob);
    RecordFlushIOStats();

    FlushMetrics metrics;
    metrics.total_bytes = stats.bytes_written;
    metrics.memtable_ratio = 0.0;
    for (auto mem : mems_) {
      metrics.memtable_ratio += (double)mem->ApproximateMemoryUsage() /
        mutable_cf_options_.write_buffer_size;
    }
    auto vfs = cfd_->current()->storage_info();
    metrics.l0_files = vfs->NumLevelFiles(vfs->base_level());
    metrics.memtable_ratio /= mems_.size();
    metrics.write_out_bandwidth = stats.bytes_written / stats.micros;

    db_options_.flush_stats->push_back(metrics);

    return s;
  }

  Env::IOPriority FlushJob::GetRateLimiterPriorityForWrite() {
    if (versions_ && versions_->GetColumnFamilySet() &&
        versions_->GetColumnFamilySet()->write_controller()) {
      WriteController* write_controller =
        versions_->GetColumnFamilySet()->write_controller();
      if (write_controller->IsStopped() || write_controller->NeedsDelay()) {
        return Env::IO_USER;
      }
    }

    return Env::IO_HIGH;
  }

  std::unique_ptr<FlushJobInfo> FlushJob::GetFlushJobInfo() const {
    db_mutex_->AssertHeld();
    std::unique_ptr<FlushJobInfo> info(new FlushJobInfo{});
    info->cf_id = cfd_->GetID();
    info->cf_name = cfd_->GetName();

    const uint64_t file_number = meta_.fd.GetNumber();
    info->file_path =
      MakeTableFileName(cfd_->ioptions()->cf_paths[0].path, file_number);
    info->file_number = file_number;
    info->oldest_blob_file_number = meta_.oldest_blob_file_number;
    info->thread_id = db_options_.env->GetThreadID();
    info->job_id = job_context_->job_id;
    info->smallest_seqno = meta_.fd.smallest_seqno;
    info->largest_seqno = meta_.fd.largest_seqno;
    info->table_properties = table_properties_;
    info->flush_reason = flush_reason_;
    info->blob_compression_type = mutable_cf_options_.blob_compression_type;

    // Update BlobFilesInfo.
    for (const auto& blob_file : edit_->GetBlobFileAdditions()) {
      BlobFileAdditionInfo blob_file_addition_info(
          BlobFileName(cfd_->ioptions()->cf_paths.front().path,
            blob_file.GetBlobFileNumber()) /*blob_file_path*/,
          blob_file.GetBlobFileNumber(), blob_file.GetTotalBlobCount(),
          blob_file.GetTotalBlobBytes());
      info->blob_file_addition_infos.emplace_back(
          std::move(blob_file_addition_info));
    }
    return info;
  }

}  // namespace ROCKSDB_NAMESPACE

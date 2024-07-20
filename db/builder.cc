//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include <atomic>
#include <vector>

#include "db/blob/blob_file_builder.h"
#include "db/compaction/compaction_iterator.h"
#include "db/db_with_timestamp_test_util.h"
#include "db/event_helpers.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/memtable.h"
#include "db/merge_helper.h"
#include "db/output_validator.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "dflush/common.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "table/merging_iterator.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/stop_watch.h"
#include "util/work_queue.h"

namespace ROCKSDB_NAMESPACE {
std::chrono::milliseconds meta_send_time_sum(0);
int Build_Table_num = 0;

class TableFactory;

TableBuilder* NewTableBuilder(const TableBuilderOptions& tboptions,
                              WritableFileWriter* file) {
  assert((tboptions.column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         tboptions.column_family_name.empty());
  return tboptions.ioptions.table_factory->NewTableBuilder(tboptions, file);
}

Status BuildTable(
    const std::string& dbname, VersionSet* versions,
    const ImmutableDBOptions& db_options, const TableBuilderOptions& tboptions,
    const FileOptions& file_options, const ReadOptions& read_options,
    TableCache* table_cache, InternalIterator* iter,
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
        range_del_iters,
    FileMetaData* meta, std::vector<BlobFileAddition>* blob_file_additions,
    std::vector<SequenceNumber> snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    SequenceNumber job_snapshot, SnapshotChecker* snapshot_checker,
    bool paranoid_file_checks, InternalStats* internal_stats,
    IOStatus* io_status, const std::shared_ptr<IOTracer>& io_tracer,
    BlobFileCreationReason blob_creation_reason,
    const SeqnoToTimeMapping& seqno_to_time_mapping, EventLogger* event_logger,
    int job_id, const Env::IOPriority io_priority,
    TableProperties* table_properties, Env::WriteLifeTimeHint write_hint,
    const std::string* full_history_ts_low,
    BlobFileCompletionCallback* blob_callback, Version* version,
    uint64_t* num_input_entries, uint64_t* memtable_payload_bytes,
    uint64_t* memtable_garbage_bytes) {
  assert((tboptions.column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         tboptions.column_family_name.empty());
  auto& mutable_cf_options = tboptions.moptions;
  auto& ioptions = tboptions.ioptions;
  // Reports the IOStats for flush for every following bytes.
  const size_t kReportFlushIOStatsEvery = 1048576;
  OutputValidator output_validator(
      tboptions.internal_comparator,
      /*enable_order_check=*/
      mutable_cf_options.check_flush_compaction_key_order,
      /*enable_hash=*/paranoid_file_checks);
  Status s;
  meta->fd.file_size = 0;
  iter->SeekToFirst();
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&tboptions.internal_comparator,
                                       snapshots, full_history_ts_low));
  uint64_t num_unfragmented_tombstones = 0;
  uint64_t total_tombstone_payload_bytes = 0;
  for (auto& range_del_iter : range_del_iters) {
    num_unfragmented_tombstones +=
        range_del_iter->num_unfragmented_tombstones();
    total_tombstone_payload_bytes +=
        range_del_iter->total_tombstone_payload_bytes();
    range_del_agg->AddTombstones(std::move(range_del_iter));
  }

  std::string fname = TableFileName(ioptions.cf_paths, meta->fd.GetNumber(),
                                    meta->fd.GetPathId());
  std::vector<std::string> blob_file_paths;
  std::string file_checksum = kUnknownFileChecksum;
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;
  EventHelpers::NotifyTableFileCreationStarted(ioptions.listeners, dbname,
                                               tboptions.column_family_name,
                                               fname, job_id, tboptions.reason);
  Env* env = db_options.env;
  assert(env);
  FileSystem* fs = db_options.fs.get();
  assert(fs);

  TableProperties tp;
  bool table_file_created = false;
  if (iter->Valid() || !range_del_agg->IsEmpty()) {
    std::unique_ptr<CompactionFilter> compaction_filter;
    if (ioptions.compaction_filter_factory != nullptr &&
        ioptions.compaction_filter_factory->ShouldFilterTableFileCreation(
            tboptions.reason)) {
      CompactionFilter::Context context;
      context.is_full_compaction = false;
      context.is_manual_compaction = false;
      context.column_family_id = tboptions.column_family_id;
      context.reason = tboptions.reason;
      compaction_filter =
          ioptions.compaction_filter_factory->CreateCompactionFilter(context);
      if (compaction_filter != nullptr &&
          !compaction_filter->IgnoreSnapshots()) {
        s.PermitUncheckedError();
        return Status::NotSupported(
            "CompactionFilter::IgnoreSnapshots() = false is not supported "
            "anymore.");
      }
    }

    TableBuilder* builder;
    std::unique_ptr<WritableFileWriter> file_writer;
    {
      std::unique_ptr<FSWritableFile> file;
#ifndef NDEBUG
      bool use_direct_writes = file_options.use_direct_writes;
      TEST_SYNC_POINT_CALLBACK("BuildTable:create_file", &use_direct_writes);
#endif  // !NDEBUG
      IOStatus io_s = NewWritableFile(fs, fname, &file, file_options);
      assert(s.ok());
      s = io_s;
      if (io_status->ok()) {
        *io_status = io_s;
      }
      if (!s.ok()) {
        EventHelpers::LogAndNotifyTableFileCreationFinished(
            event_logger, ioptions.listeners, dbname,
            tboptions.column_family_name, fname, job_id, meta->fd,
            kInvalidBlobFileNumber, tp, tboptions.reason, s, file_checksum,
            file_checksum_func_name);
        return s;
      }

      table_file_created = true;
      FileTypeSet tmp_set = ioptions.checksum_handoff_file_types;
      file->SetIOPriority(io_priority);
      file->SetWriteLifeTimeHint(write_hint);
      file_writer.reset(new WritableFileWriter(
          std::move(file), fname, file_options, ioptions.clock, io_tracer,
          ioptions.stats, ioptions.listeners,
          ioptions.file_checksum_gen_factory.get(),
          tmp_set.Contains(FileType::kTableFile), false));

      builder = NewTableBuilder(tboptions, file_writer.get());
    }

    auto ucmp = tboptions.internal_comparator.user_comparator();
    MergeHelper merge(
        env, ucmp, ioptions.merge_operator.get(), compaction_filter.get(),
        ioptions.logger, true /* internal key corruption is not ok */,
        snapshots.empty() ? 0 : snapshots.back(), snapshot_checker);

    std::unique_ptr<BlobFileBuilder> blob_file_builder(
        (mutable_cf_options.enable_blob_files &&
         tboptions.level_at_creation >=
             mutable_cf_options.blob_file_starting_level &&
         blob_file_additions)
            ? new BlobFileBuilder(
                  versions, fs, &ioptions, &mutable_cf_options, &file_options,
                  tboptions.db_id, tboptions.db_session_id, job_id,
                  tboptions.column_family_id, tboptions.column_family_name,
                  io_priority, write_hint, io_tracer, blob_callback,
                  blob_creation_reason, &blob_file_paths, blob_file_additions)
            : nullptr);

    const std::atomic<bool> kManualCompactionCanceledFalse{false};
    CompactionIterator c_iter(
        iter, ucmp, &merge, kMaxSequenceNumber, &snapshots,
        earliest_write_conflict_snapshot, job_snapshot, snapshot_checker, env,
        ShouldReportDetailedTime(env, ioptions.stats),
        true /* internal key corruption is not ok */, range_del_agg.get(),
        blob_file_builder.get(), ioptions.allow_data_in_errors,
        ioptions.enforce_single_del_contracts,
        /*manual_compaction_canceled=*/kManualCompactionCanceledFalse,
        /*compaction=*/nullptr, compaction_filter.get(),
        /*shutting_down=*/nullptr, db_options.info_log, full_history_ts_low);
    c_iter.SeekToFirst();
    for (; c_iter.Valid(); c_iter.Next()) {
      const Slice& key = c_iter.key();
      const Slice& value = c_iter.value();
      const ParsedInternalKey& ikey = c_iter.ikey();
      // Generate a rolling 64-bit hash of the key and values
      // Note :
      // Here "key" integrates 'sequence_number'+'kType'+'user key'.
      s = output_validator.Add(key, value);
      if (!s.ok()) {
        break;
      }
      builder->Add(key, value);

      s = meta->UpdateBoundaries(key, value, ikey.sequence, ikey.type);
      if (!s.ok()) {
        break;
      }

      // TODO(noetzli): Update stats after flush, too.
      if (io_priority == Env::IO_HIGH &&
          IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
        ThreadStatusUtil::SetThreadOperationProperty(
            ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
      }
    }
    if (!s.ok()) {
      c_iter.status().PermitUncheckedError();
    } else if (!c_iter.status().ok()) {
      s = c_iter.status();
    }

    if (s.ok()) {
      auto range_del_it = range_del_agg->NewIterator();
      Slice last_tombstone_start_user_key{};
      for (range_del_it->SeekToFirst(); range_del_it->Valid();
           range_del_it->Next()) {
        auto tombstone = range_del_it->Tombstone();
        auto kv = tombstone.Serialize();
        builder->Add(kv.first.Encode(), kv.second);
        InternalKey tombstone_end = tombstone.SerializeEndKey();
        meta->UpdateBoundariesForRange(kv.first, tombstone_end, tombstone.seq_,
                                       tboptions.internal_comparator);
        if (version) {
          if (last_tombstone_start_user_key.empty() ||
              ucmp->CompareWithoutTimestamp(last_tombstone_start_user_key,
                                            range_del_it->start_key()) < 0) {
            SizeApproximationOptions approx_opts;
            approx_opts.files_size_error_margin = 0.1;
            meta->compensated_range_deletion_size += versions->ApproximateSize(
                approx_opts, read_options, version, kv.first.Encode(),
                tombstone_end.Encode(), 0 /* start_level */, -1 /* end_level */,
                TableReaderCaller::kFlush);
          }
          last_tombstone_start_user_key = range_del_it->start_key();
        }
      }
    }

    TEST_SYNC_POINT("BuildTable:BeforeFinishBuildTable");
    const bool empty = builder->IsEmpty();
    if (num_input_entries != nullptr) {
      *num_input_entries =
          c_iter.num_input_entry_scanned() + num_unfragmented_tombstones;
    }
    if (!s.ok() || empty) {
      builder->Abandon();
    } else {
      std::string seqno_time_mapping_str;
      seqno_to_time_mapping.Encode(
          seqno_time_mapping_str, meta->fd.smallest_seqno,
          meta->fd.largest_seqno, meta->file_creation_time);
      builder->SetSeqnoTimeTableProperties(
          seqno_time_mapping_str,
          ioptions.compaction_style == CompactionStyle::kCompactionStyleFIFO
              ? meta->file_creation_time
              : meta->oldest_ancester_time);
      s = builder->Finish();
    }
    if (io_status->ok()) {
      *io_status = builder->io_status();
    }

    if (s.ok() && !empty) {
      uint64_t file_size = builder->FileSize();
      meta->fd.file_size = file_size;
      meta->tail_size = builder->GetTailSize();
      meta->marked_for_compaction = builder->NeedCompact();
      assert(meta->fd.GetFileSize() > 0);
      tp = builder
               ->GetTableProperties();  // refresh now that builder is finished
      if (memtable_payload_bytes != nullptr &&
          memtable_garbage_bytes != nullptr) {
        const CompactionIterationStats& ci_stats = c_iter.iter_stats();
        uint64_t total_payload_bytes = ci_stats.total_input_raw_key_bytes +
                                       ci_stats.total_input_raw_value_bytes +
                                       total_tombstone_payload_bytes;
        uint64_t total_payload_bytes_written =
            (tp.raw_key_size + tp.raw_value_size);
        // Prevent underflow, which may still happen at this point
        // since we only support inserts, deletes, and deleteRanges.
        if (total_payload_bytes_written <= total_payload_bytes) {
          *memtable_payload_bytes = total_payload_bytes;
          *memtable_garbage_bytes =
              total_payload_bytes - total_payload_bytes_written;
        } else {
          *memtable_payload_bytes = 0;
          *memtable_garbage_bytes = 0;
        }
      }
      if (table_properties) {
        *table_properties = tp;
      }
    }
    delete builder;

    // Finish and check for file errors
    TEST_SYNC_POINT("BuildTable:BeforeSyncTable");
    if (s.ok() && !empty) {
      StopWatch sw(ioptions.clock, ioptions.stats, TABLE_SYNC_MICROS);
      *io_status = file_writer->Sync(ioptions.use_fsync);
    }
    TEST_SYNC_POINT("BuildTable:BeforeCloseTableFile");
    if (s.ok() && io_status->ok() && !empty) {
      *io_status = file_writer->Close();
    }
    if (s.ok() && io_status->ok() && !empty) {
      // Add the checksum information to file metadata.
      meta->file_checksum = file_writer->GetFileChecksum();
      meta->file_checksum_func_name = file_writer->GetFileChecksumFuncName();
      file_checksum = meta->file_checksum;
      file_checksum_func_name = meta->file_checksum_func_name;
      // Set unique_id only if db_id and db_session_id exist
      if (!tboptions.db_id.empty() && !tboptions.db_session_id.empty()) {
        if (!GetSstInternalUniqueId(tboptions.db_id, tboptions.db_session_id,
                                    meta->fd.GetNumber(), &(meta->unique_id))
                 .ok()) {
          // if failed to get unique id, just set it Null
          meta->unique_id = kNullUniqueId64x2;
        }
      }
    }

    if (s.ok()) {
      s = *io_status;
    }

    if (blob_file_builder) {
      if (s.ok()) {
        s = blob_file_builder->Finish();
      } else {
        blob_file_builder->Abandon(s);
      }
      blob_file_builder.reset();
    }

    // TODO Also check the IO status when create the Iterator.

    TEST_SYNC_POINT("BuildTable:BeforeOutputValidation");
    if (s.ok() && !empty) {
      // Verify that the table is usable
      // We set for_compaction to false and don't OptimizeForCompactionTableRead
      // here because this is a special case after we finish the table building.
      // No matter whether use_direct_io_for_flush_and_compaction is true,
      // the goal is to cache it here for further user reads.
      std::unique_ptr<InternalIterator> it(table_cache->NewIterator(
          read_options, file_options, tboptions.internal_comparator, *meta,
          nullptr /* range_del_agg */, mutable_cf_options.prefix_extractor,
          nullptr,
          (internal_stats == nullptr) ? nullptr
                                      : internal_stats->GetFileReadHist(0),
          TableReaderCaller::kFlush, /*arena=*/nullptr,
          /*skip_filter=*/false, tboptions.level_at_creation,
          MaxFileSizeForL0MetaPin(mutable_cf_options),
          /*smallest_compaction_key=*/nullptr,
          /*largest_compaction_key*/ nullptr,
          /*allow_unprepared_value*/ false,
          mutable_cf_options.block_protection_bytes_per_key));
      s = it->status();
      if (s.ok() && paranoid_file_checks) {
        OutputValidator file_validator(tboptions.internal_comparator,
                                       /*enable_order_check=*/true,
                                       /*enable_hash=*/true);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
          // Generate a rolling 64-bit hash of the key and values
          file_validator.Add(it->key(), it->value()).PermitUncheckedError();
        }
        s = it->status();
        if (s.ok() && !output_validator.CompareValidator(file_validator)) {
          s = Status::Corruption("Paranoid checksums do not match");
        }
      }
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (!s.ok() || meta->fd.GetFileSize() == 0) {
    TEST_SYNC_POINT("BuildTable:BeforeDeleteFile");

    constexpr IODebugContext* dbg = nullptr;

    if (table_file_created) {
      Status ignored = fs->DeleteFile(fname, IOOptions(), dbg);
      ignored.PermitUncheckedError();
    }

    assert(blob_file_additions || blob_file_paths.empty());

    if (blob_file_additions) {
      for (const std::string& blob_file_path : blob_file_paths) {
        Status ignored = DeleteDBFile(&db_options, blob_file_path, dbname,
                                      /*force_bg=*/false, /*force_fg=*/false);
        ignored.PermitUncheckedError();
        TEST_SYNC_POINT("BuildTable::AfterDeleteFile");
      }
    }
  }

  Status status_for_listener = s;
  if (meta->fd.GetFileSize() == 0) {
    fname = "(nil)";
    if (s.ok()) {
      status_for_listener = Status::Aborted("Empty SST file not kept");
    }
  }
  // Output to event logger and fire events.
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger, ioptions.listeners, dbname, tboptions.column_family_name,
      fname, job_id, meta->fd, meta->oldest_blob_file_number, tp,
      tboptions.reason, status_for_listener, file_checksum,
      file_checksum_func_name);

  return s;
}

const uint64_t parallel_threads = 4;

class NewMemTable {
 private:
 public:
  uintptr_t Node_head_;
  uintptr_t End_iter_;
  uint64_t offset_;
  const MemTable::KeyComparator comparator_;
  std::unique_ptr<FragmentedRangeTombstoneList>
      fragmented_range_tombstone_list_;
  std::atomic_bool is_range_del_table_empty_;

  NewMemTable(uintptr_t Node_head, uintptr_t End_iter, uint64_t offset,
              InternalKeyComparator& c)
      : comparator_(c) {
    Node_head_ = Node_head;
    End_iter_ = End_iter;
    offset_ = offset;
    is_range_del_table_empty_ = true;
  }

  ~NewMemTable() {}
};

class NewMemTableIterator : public InternalIterator {
 private:
  DynamicBloom* bloom_;
  const MemTable::KeyComparator comparator_;
  // MemTableRep::Iterator* iter_;
  bool valid_;
  bool arena_mode_;
  Status status_;

  uintptr_t Node_head_;
  uintptr_t Node_iter_;
  uintptr_t End_iter_;
  uint64_t offset_;

 public:
  NewMemTableIterator(const NewMemTable& mem, const ReadOptions& read_options,
                      InternalKeyComparator& c, Arena* arena,
                      bool use_range_del_table = false)
      : bloom_(nullptr),
        comparator_(c),
        valid_(false),
        arena_mode_(arena != nullptr),
        status_(Status::OK()),
        Node_head_(mem.Node_head_),
        Node_iter_(0),
        End_iter_(mem.End_iter_),
        offset_(mem.offset_) {}
  // No copying allowed
  NewMemTableIterator(const NewMemTableIterator&) = delete;
  void operator=(const NewMemTableIterator&) = delete;
  ~NewMemTableIterator() override {};

  bool Valid() const override {
    return (Node_iter_ != End_iter_) && (Node_iter_ != offset_) && status_.ok();
  }
  void Seek(const Slice& k) override { printf("1 该函数尚未实现！\n"); }
  void SeekForPrev(const Slice& k) override { printf("2 该函数尚未实现！\n"); }
  void SeekToFirst() override {
    // printf("Node_head:%lx, offset:%lx\n", Node_head_, offset_);
    Node_iter_ = Node_head_ + offset_;
    // printf("SeekToFirst Node_iter_:%lu\n", Node_iter_);
    // if (*((uintptr_t*)(Node_iter_)) != 0) {
    //   valid_ = true;
    // }
    // else {
    //   valid_ = false;
    // }
  }
  void SeekToLast() override { printf("3 该函数尚未实现！\n"); }
  void* Current() override { return this; }
  void Next() override {
    // static uint64_t time = 0;
    // auto time0 = std::chrono::high_resolution_clock::now();
    Node_iter_ = *(uintptr_t*)(Node_iter_) + offset_;

    // if (Node_iter_ == offset_) {
    //   valid_ = false;
    // }
    // auto time1 = std::chrono::high_resolution_clock::now();
    // nexttime += std::chrono::duration<double>(time1 - time0).count();
    // printf("dram time:%lu\n",
    // std::chrono::duration_cast<std::chrono::nanoseconds>(time1 -
    // time0).count());
  }
  bool NextAndGetResult(IterateResult* result) override {
    printf("4 该函数尚未实现！\n");
    return true;
  }
  void Prev() override {
    printf("5 该函数尚未实现！\n");
    return;
  }
  Slice key() const override {
    assert(Valid());
    rocksdb::Slice key = rocksdb::GetLengthPrefixedSlice(
        (const char*)(Node_iter_ + sizeof(uintptr_t)));
    // rocksdb::Slice key_and_vallue = rocksdb::GetLengthPrefixedSlice(
    //     (const char*)(Node_iter_ + sizeof(uintptr_t)));
    // rocksdb::GetLengthPrefixedSlice(&key_and_vallue, &key);
    // auto time1 = std::chrono::high_resolution_clock::now();
    // getkeytime += std::chrono::duration_cast<std::chrono::nanoseconds>(time1
    // - time0).count();
    return key;
  }
  Slice user_key() const override {
    printf("6 该函数尚未实现！\n");
    return Slice();
  }
  Slice value() const override {
    assert(Valid());
    rocksdb::Slice key = rocksdb::GetLengthPrefixedSlice(
        (const char*)(Node_iter_ + sizeof(uintptr_t)));
    rocksdb::Slice value =
        rocksdb::GetLengthPrefixedSlice(key.data() + key.size());
    // rocksdb::GetLengthPrefixedSlice(&key_and_vallue, &key);
    // rocksdb::GetLengthPrefixedSlice(&key_and_vallue, &value);
    return value;
  }
  Status status() const override { return status_; }
  bool PrepareValue() override {
    printf("7 该函数尚未实现！\n");
    return true;
  }

  // Keys return from this iterator can be smaller than iterate_lower_bound.
  bool MayBeOutOfLowerBound() override {
    printf("8 该函数尚未实现！\n");
    return true;
  }
  IterBoundCheck UpperBoundCheckResult() override {
    printf("9 该函数尚未实现！\n");
    return IterBoundCheck::kUnknown;
  }
  void SetPinnedItersMgr(
      PinnedIteratorsManager* /*pinned_iters_mgr*/) override {
    printf("10 该函数尚未实现！\n");
  }
  bool IsKeyPinned() const override {
    printf("11 该函数尚未实现！\n");
    return false;
  }
  bool IsValuePinned() const override {
    printf("12 该函数尚未实现！\n");
    return false;
  }
  Status GetProperty(std::string /*prop_name*/,
                     std::string* /*prop*/) override {
    printf("13 该函数尚未实现！\n");
    return Status::NotSupported("");
  }
  void GetReadaheadState(ReadaheadFileInfo* /*readahead_file_info*/) override {
    printf("14 该函数尚未实现！\n");
  }
  void SetReadaheadState(ReadaheadFileInfo* /*readahead_file_info*/) override {
    printf("15 该函数尚未实现！\n");
  }
  bool IsDeleteRangeSentinelKey() const override { return false; }

  uint64_t Node_head() override { return Node_iter_ - offset_; }
};
InternalIterator* NewIterator(NewMemTable* mt, const ReadOptions& read_options,
                              Arena* arena, InternalKeyComparator& c) {
  assert(arena != nullptr);
  auto mem = arena->AllocateAligned(sizeof(NewMemTableIterator));
  return new (mem) NewMemTableIterator(*mt, read_options, c, arena);
}

void iter_parallel(uint64_t index, TableBuilder* builder, FileMetaData* meta,
                   CompactionIterator* c_iter, std::atomic<uint64_t>& counter) {
  auto a_point = std::chrono::high_resolution_clock::now();
  for (c_iter->SeekToFirst(); c_iter->Valid(); c_iter->Next()) {
    const Slice& key = c_iter->key();
    const Slice& value = c_iter->value();
    const ParsedInternalKey& ikey = c_iter->ikey();

    builder->Add_parallel(key, value, index);

    meta->UpdateBoundaries(key, value, ikey.sequence, ikey.type);
  }
  auto b_point = std::chrono::high_resolution_clock::now();
  // Flush最后一部分没有成为整个 datablock 的 kv
  builder->Flush_parallel(index);

  auto c_point = std::chrono::high_resolution_clock::now();
  while (counter.load(std::memory_order_relaxed) != index) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  auto d_point = std::chrono::high_resolution_clock::now();
  if (index > 0) builder->EmitRemainOpts(index);
  if (index == parallel_threads - 1) {
    const Slice& key = c_iter->key();
    const Slice& value = c_iter->value();
    const ParsedInternalKey& ikey = c_iter->ikey();
    meta->UpdateBoundaries(key, value, ikey.sequence, ikey.type);
  }
  counter.store(index + 1, std::memory_order_relaxed);
  uint64_t iter_time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(b_point - a_point)
          .count();
  uint64_t flush_other_time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(c_point - b_point)
          .count();
  uint64_t wait_time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(d_point - c_point)
          .count();
  printf("index:%lu, iter_time:%lu, flush_other_time:%lu, wait_time:%lu\n",
         index, iter_time, flush_other_time, wait_time);
}

void BuildTable_new(uint64_t offset, MetaReq* req, MetaResult* result,
                    bool use_optimized) {
  // 1. initialize options for building table
  ImmutableDBOptions db_options;
  Env::IOPriority io_priority;
  std::vector<SequenceNumber> snapshots;
  FileOptions file_options;
  ReadOptions ro;
  ro.total_order_seek = true;
  ro.io_activity = Env::IOActivity::kFlush;
  MutableCFOptions tboptions_moptions;  // = tboptions->moptions;
  ImmutableOptions ioptions;            //= tboptions->ioptions;
  ioptions.cf_paths = req->tboptions_ioptions_cf_paths;
  InternalKeyComparator internal_comparatortboptions(
      TestComparator(req->timestamp_size)
          .cmp_without_ts_);                // tboptions->internal_comparator
  uint32_t tboptions_column_family_id = 0;  //=tboptions->column_family_id;
  const std::string
      tboptions_column_family_name;  //= tboptions->column_family_name;
  TableFileCreationReason tboptions_reason =
      TableFileCreationReason::kFlush;              // tboptions->reason;
  std::string tboptions_db_id = "tboptions_db_id";  // tboptions->db_id
  std::string tboptions_db_session_id =
      "tboptions->db_session_id";  // tboptions->db_session_id
  CompressionOptions compression_opts;
  compression_opts.parallel_threads = parallel_threads;
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
      int_tbl_prop_collector_factories;
  TableBuilderOptions tboptions(
      ioptions, tboptions_moptions, internal_comparatortboptions,
      &int_tbl_prop_collector_factories, kSnappyCompression, compression_opts,
      tboptions_column_family_id, tboptions_column_family_name, 0, false,
      TableFileCreationReason::kMisc, 0, 0, "", "", 0, 0);
  InternalKeyComparator cfd_internal_comparator;
  uint64_t old_versions_NewFileNumber;
  EventLogger event_logger(db_options.info_log.get());
  IOStatus io_status;
  Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;
  FileDescriptor DPU_fd = FileDescriptor(req->new_versions_NewFileNumber, 0, 0);

  // 2. create a new memtable and iterator via copied memory
  Arena arena;
  NewMemTable new_m(req->Node_head, offset, offset, cfd_internal_comparator);
  InternalIterator* iter =
      NewIterator(&new_m, ro, &arena, cfd_internal_comparator);

  // 3. build table via new iterator
  Status s;
  iter->SeekToFirst();
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&internal_comparatortboptions, snapshots,
                                       nullptr));
  std::string fname =
      TableFileName(ioptions.cf_paths, DPU_fd.GetNumber(), DPU_fd.GetPathId());
  std::vector<std::string> blob_file_paths;
  std::string file_checksum = kUnknownFileChecksum;
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;
  EventHelpers::NotifyTableFileCreationStarted(ioptions.listeners, req->dbname,
                                               tboptions_column_family_name,
                                               fname, req->job_id,
                                               tboptions_reason);  // new event
  Env* env = db_options.env;
  assert(env);
  FileSystem* fs = db_options.fs.get();
  assert(fs);
  TableProperties tp;
  bool table_file_created = false;
  if (iter->Valid()) {
    TableBuilder* builder;
    std::unique_ptr<WritableFileWriter> file_writer;
    // 3.1 new writable file and file writer
    {
      std::unique_ptr<FSWritableFile> file;
      IOStatus io_s = NewWritableFile(fs, fname, &file, file_options);
      assert(s.ok());
      s = io_s;
      if (io_status.ok()) {
        io_status = io_s;
      }
      if (!s.ok()) {
        EventHelpers::LogAndNotifyTableFileCreationFinished(
            &event_logger, ioptions.listeners, req->dbname,
            tboptions_column_family_name, fname, req->job_id, DPU_fd,
            kInvalidBlobFileNumber, tp, tboptions_reason, s, file_checksum,
            file_checksum_func_name);
        result->status = s;
        return;
      }
      FileTypeSet tmp_set = ioptions.checksum_handoff_file_types;
      file->SetIOPriority(io_priority);
      file->SetWriteLifeTimeHint(write_hint);
      file_writer.reset(new WritableFileWriter(
          std::move(file), fname, file_options, ioptions.clock, nullptr,
          ioptions.stats, ioptions.listeners,
          ioptions.file_checksum_gen_factory.get(),
          tmp_set.Contains(FileType::kTableFile), false));
      builder =
          ioptions.table_factory->NewTableBuilder(tboptions, file_writer.get());
    }
    auto ucmp = internal_comparatortboptions.user_comparator();
    MergeHelper merge(env, ucmp, ioptions.merge_operator.get(),
                      nullptr,  // compaction_filter.get(),
                      ioptions.logger,
                      true /* internal key corruption is not ok */,
                      snapshots.empty() ? 0 : snapshots.back(), nullptr);

    const std::atomic<bool> kManualCompactionCanceledFalse{false};

    // 3.2 use iterator to build table
    if (!use_optimized) {
      // native build table
      CompactionIterator c_iter(
          iter, ucmp, &merge, kMaxSequenceNumber, &snapshots,
          req->earliest_write_conflict_snapshot, req->job_snapshot, nullptr,
          env, ShouldReportDetailedTime(env, ioptions.stats), true /*
          internal key corruption is not ok */
          ,
          range_del_agg.get(), nullptr,
          // blob_file_builder.get(),
          ioptions.allow_data_in_errors, ioptions.enforce_single_del_contracts,
          /*manual_compaction_canceled=*/kManualCompactionCanceledFalse,
          /*compaction=*/nullptr, nullptr,  // compaction_filter.get(),
          /*shutting_down=*/nullptr, db_options.info_log, nullptr);

      for (c_iter.SeekToFirst(); c_iter.Valid(); c_iter.Next()) {
        const Slice& key = c_iter.key();
        const Slice& value = c_iter.value();
        const ParsedInternalKey& ikey = c_iter.ikey();
        builder->Add(key, value);

        s = req->file_meta.UpdateBoundaries(key, value, ikey.sequence,
                                            ikey.type);
        if (!s.ok()) {
          break;
        }
      }

      if (!s.ok()) {
        c_iter.status().PermitUncheckedError();
      } else if (!c_iter.status().ok()) {
        s = c_iter.status();
      }

      result->num_input_entries = c_iter.num_input_entry_scanned();
      if (result->num_input_entries != req->num_entries) {
        printf("nums error!\n");
      }
    } else {
      // optimized build table
      std::vector<std::thread> flush_thread_pool;
      std::vector<InternalIterator*> iters(parallel_threads);
      std::vector<CompactionIterator*> c_iters;
      std::vector<Arena> arenas(parallel_threads);
      std::vector<NewMemTable*> new_mems(parallel_threads);
      std::atomic<uint64_t> counter(0);
      for (uint64_t i = 0; i < parallel_threads; ++i) {
        new_mems[i] =
            new NewMemTable(req->Node_heads[i], offset + req->Node_heads[i + 1],
                            offset, cfd_internal_comparator);
        iters[i] =
            NewIterator(new_mems[i], ro, &arenas[i], cfd_internal_comparator);
        iters[i]->SeekToFirst();
        c_iters.push_back(new CompactionIterator(
            iters[i], ucmp, &merge, kMaxSequenceNumber, &snapshots,
            req->earliest_write_conflict_snapshot, req->job_snapshot, nullptr,
            env, ShouldReportDetailedTime(env, ioptions.stats),
            true
            /* internal key corruption is not ok */,
            range_del_agg.get(), nullptr,
            // blob_file_builder.get(),
            ioptions.allow_data_in_errors,
            ioptions.enforce_single_del_contracts,
            /*manual_compaction_canceled=*/kManualCompactionCanceledFalse,
            /*compaction=*/nullptr, nullptr,  // compaction_filter.get(),
            /*shutting_down=*/nullptr, db_options.info_log, nullptr));
      }
      for (uint64_t i = 0; i < parallel_threads; i++) {
        flush_thread_pool.emplace_back(iter_parallel, i, builder,
                                       &req->file_meta, c_iters[i],
                                       std::ref(counter));
      }
      for (uint64_t i = 0; i < flush_thread_pool.size(); i++) {
        flush_thread_pool[i].join();
      }
      builder->merge_prop();

      result->num_input_entries = 0;
      for (uint64_t i = 0; i < parallel_threads; ++i) {
        result->num_input_entries += c_iters[i]->num_input_entry_scanned();
      }
      if (result->num_input_entries != req->num_entries) {
        printf("nums error!\n");
      }
      for (uint64_t i = 0; i < parallel_threads; ++i) {
        delete new_mems[i];
        delete c_iters[i];
      }
    }

    // 3.3 sync and close writable file
    const bool empty = builder->IsEmpty();
    if (!s.ok() || empty) {
      builder->Abandon();
    } else {
      std::string seqno_time_mapping_str;
      req->seqno_to_time_mapping.Encode(
          seqno_time_mapping_str, DPU_fd.smallest_seqno, DPU_fd.largest_seqno,
          req->file_meta.file_creation_time);
      builder->SetSeqnoTimeTableProperties(
          seqno_time_mapping_str,
          ioptions.compaction_style == CompactionStyle::kCompactionStyleFIFO
              ? req->file_meta.file_creation_time
              : req->file_meta.oldest_ancester_time);
      s = builder->Finish();
    }
    if (io_status.ok()) {
      io_status = builder->io_status();
    }
    if (s.ok() && !empty) {
      uint64_t file_size = builder->FileSize();
      DPU_fd.file_size = file_size;
      req->file_meta.tail_size = builder->GetTailSize();
      req->file_meta.marked_for_compaction = builder->NeedCompact();
      assert(DPU_fd.GetFileSize() > 0);
    }
    delete builder;
    if (s.ok() && !empty) {
      StopWatch sw(ioptions.clock, ioptions.stats, TABLE_SYNC_MICROS);
      io_status = file_writer->Sync(ioptions.use_fsync);
    }
    if (s.ok() && io_status.ok() && !empty) {
      io_status = file_writer->Close();
    }
    if (s.ok() && io_status.ok() && !empty) {
      // Add the checksum information to file metadata.
      req->file_meta.file_checksum = file_writer->GetFileChecksum();
      req->file_meta.file_checksum_func_name =
          file_writer->GetFileChecksumFuncName();
      DPU_fd.smallest_seqno = req->file_meta.fd.smallest_seqno;
      DPU_fd.largest_seqno = req->file_meta.fd.largest_seqno;
      file_checksum = req->file_meta.file_checksum;
      file_checksum_func_name = req->file_meta.file_checksum_func_name;
      // Set unique_id only if db_id and db_session_id exist
      // if (!tboptions->db_id.empty() && !tboptions->db_session_id.empty()) {
      if (!GetSstInternalUniqueId(tboptions_db_id, tboptions_db_session_id,
                                  DPU_fd.GetNumber(),
                                  &(req->file_meta.unique_id))
               .ok()) {
        // if failed to get unique id, just set it Null
        req->file_meta.unique_id = kNullUniqueId64x2;
      }
      // }
    }
    if (s.ok()) {
      s = io_status;
    }
  } else {
    printf("no valid!\n");
  }

  // 4. set result that will be return to host
  printf("send back to host begin\n");
  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }
  if (!s.ok() || DPU_fd.GetFileSize() == 0) {
    TEST_SYNC_POINT("BuildTable:BeforeDeleteFile");

    constexpr IODebugContext* dbg = nullptr;

    if (table_file_created) {
      Status ignored = fs->DeleteFile(fname, IOOptions(), dbg);
      ignored.PermitUncheckedError();
    }
  }
  Status status_for_listener = s;
  if (DPU_fd.GetFileSize() == 0) {
    fname = "(nil)";
    if (s.ok()) {
      status_for_listener = Status::Aborted("Empty SST file not kept");
    }
  }
  result->status = s;
  result->packed_number_and_path_id = DPU_fd.packed_number_and_path_id;
  result->file_size = DPU_fd.file_size;
  result->smallest_seqno = DPU_fd.smallest_seqno;
  result->largest_seqno = DPU_fd.largest_seqno;
  result->file_meta = req->file_meta;
  return;
}
}  // namespace ROCKSDB_NAMESPACE

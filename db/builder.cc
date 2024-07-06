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
#include "db/event_helpers.h"
#include "db/internal_stats.h"
#include "db/memtable.h"
#include "db/merge_helper.h"
#include "db/output_validator.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/job_context.h"
#include "db/version_edit.h"
#include "db/db_with_timestamp_test_util.h"
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
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/stop_watch.h"
#include "table/merging_iterator.h"
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

      const std::atomic<bool> kManualCompactionCanceledFalse{ false };
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
      }
      else if (!c_iter.status().ok()) {
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
      }
      else {
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
          }
          else {
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
        }
        else {
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

  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  // /**********************************************************************************************************************/
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

    NewMemTable(uintptr_t Node_head, uintptr_t End_iter, uint64_t offset, InternalKeyComparator& c) : comparator_(c) {
      Node_head_ = Node_head;
      End_iter_ = End_iter;
      offset_ = offset;
      is_range_del_table_empty_ = true;
    }

    ~NewMemTable() {

    }

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
    ~NewMemTableIterator()override {};

    bool Valid() const override { return (Node_iter_ != End_iter_) && (Node_iter_ != offset_) && status_.ok(); }
    void Seek(const Slice& k) override { printf("1 该函数尚未实现！\n"); }
    void SeekForPrev(const Slice& k)override { printf("2 该函数尚未实现！\n"); }
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
      Node_iter_ = *(uintptr_t*)(Node_iter_)+offset_;

      // if (Node_iter_ == offset_) {
      //   valid_ = false;
      // }
      // auto time1 = std::chrono::high_resolution_clock::now();
      // nexttime += std::chrono::duration<double>(time1 - time0).count();
      // printf("dram time:%lu\n", std::chrono::duration_cast<std::chrono::nanoseconds>(time1 - time0).count());
    }
    bool NextAndGetResult(IterateResult* result) override { printf("4 该函数尚未实现！\n"); return true; }
    void Prev() override { printf("5 该函数尚未实现！\n"); return; }
    Slice key() const override {
      assert(Valid());
      rocksdb::Slice key = rocksdb::GetLengthPrefixedSlice(
          (const char*)(Node_iter_ + sizeof(uintptr_t)));
      // rocksdb::Slice key_and_vallue = rocksdb::GetLengthPrefixedSlice(
      //     (const char*)(Node_iter_ + sizeof(uintptr_t)));
      // rocksdb::GetLengthPrefixedSlice(&key_and_vallue, &key);
      // auto time1 = std::chrono::high_resolution_clock::now();
      // getkeytime += std::chrono::duration_cast<std::chrono::nanoseconds>(time1 - time0).count();
      return key;
    }
    Slice user_key() const override { printf("6 该函数尚未实现！\n");return  Slice(); }
    Slice value() const override {
      assert(Valid());
      rocksdb::Slice key = rocksdb::GetLengthPrefixedSlice(
          (const char*)(Node_iter_ + sizeof(uintptr_t)));
      rocksdb::Slice value = rocksdb::GetLengthPrefixedSlice(
          key.data() + key.size());
      // rocksdb::GetLengthPrefixedSlice(&key_and_vallue, &key);
      // rocksdb::GetLengthPrefixedSlice(&key_and_vallue, &value);
      return value;
    }
    Status status() const override { return status_; }
    bool PrepareValue() override { printf("7 该函数尚未实现！\n");return true; }

    // Keys return from this iterator can be smaller than iterate_lower_bound.
    bool MayBeOutOfLowerBound() override { printf("8 该函数尚未实现！\n");return true; }
    IterBoundCheck UpperBoundCheckResult() override { printf("9 该函数尚未实现！\n");return IterBoundCheck::kUnknown; }
    void SetPinnedItersMgr(PinnedIteratorsManager* /*pinned_iters_mgr*/) override { printf("10 该函数尚未实现！\n"); }
    bool IsKeyPinned() const override { printf("11 该函数尚未实现！\n");return false; }
    bool IsValuePinned() const override { printf("12 该函数尚未实现！\n");return false; }
    Status GetProperty(std::string /*prop_name*/, std::string* /*prop*/) override {
      printf("13 该函数尚未实现！\n");
      return Status::NotSupported("");
    }
    void GetReadaheadState(ReadaheadFileInfo* /*readahead_file_info*/) override { printf("14 该函数尚未实现！\n"); }
    void SetReadaheadState(ReadaheadFileInfo* /*readahead_file_info*/) override { printf("15 该函数尚未实现！\n"); }
    bool IsDeleteRangeSentinelKey() const override { return false; }

    uint64_t Node_head() override {
      return Node_iter_ - offset_;
    }

  };
  InternalIterator* NewIterator(NewMemTable* mt, const ReadOptions& read_options, Arena* arena, InternalKeyComparator& c) {
    assert(arena != nullptr);
    auto mem = arena->AllocateAligned(sizeof(NewMemTableIterator));
    return new (mem) NewMemTableIterator(*mt, read_options, c, arena);
  }


  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  /**********************************************************************************************************************/
  // /**********************************************************************************************************************/


  void iter_parallel(uint64_t index, TableBuilder* builder, FileMetaData* meta,
                     std::mutex& meta_mtx, CompactionIterator* c_iter,
                     uint64_t skip_nums, uint64_t need_nums, std::atomic<uint64_t>& counter) {
    // fprintf(stderr, "index:%lu, gethere, %d\n", index, __LINE__);
    // fprintf(stderr, "Node_head:%lx\n", c_iter->Node_head());
    // fflush(stderr);
    // uint64_t nums = 0;
    // fprintf(stderr, "index:%lu, skip_nums:%lu, need_nums:%lu\n", index, skip_nums, need_nums);
    auto a_point = std::chrono::high_resolution_clock::now();
    //nums < need_nums&&
    // std::this_thread::sleep_for(std::chrono::milliseconds(20 * index));
    for (c_iter->SeekToFirst(); c_iter->Valid(); c_iter->Next()) {
      // if (nums < skip_nums) {
      //   nums++;
      //   continue;
      // }
      // if (index) {
      //   printf("index:%lu nums:%lu, gethere, %d\n", index, nums, __LINE__);
      // }
      // fflush(stderr);
      const Slice& key = c_iter->key();
      const Slice& value = c_iter->value();
      const ParsedInternalKey& ikey = c_iter->ikey();
      // if (index) {
      //   printf("index:%lu nums:%lu, gethere, %d\n", index, nums, __LINE__);
      // }


      builder->Add_parallel(key, value, index);
      // fprintf(stderr, "index:%lu nums:%lu, gethere, %d\n", index, nums, __LINE__);
      // fflush(stderr);
      // if (index) {
      //   printf("index:%lu nums:%lu, gethere, %d\n", index, nums, __LINE__);
      // }
      // 这里点进去看代码可以知道和顺序没关系，只需要保证不同时访问就行 
      // meta_mtx.lock();
      meta->UpdateBoundaries(key, value, ikey.sequence, ikey.type);
      // meta_mtx.unlock();
      // if (nums == 0) {
      //   nums++;
      //   printf("index:%lu, fisrt key:%s\n", index, key.ToString().c_str());
      // }
    }
    auto b_point = std::chrono::high_resolution_clock::now();
    // fprintf(stderr, "gethere, %d\n", __LINE__);
    // Flush最后一部分没有成为整个 datablock 的 kv
    // printf("index:%lu, last key:%s\n", index, c_iter->key().ToString().c_str());
    builder->Flush_parallel(index);
    // builder->Flush_parallel(index, Slice{});
    auto c_point = std::chrono::high_resolution_clock::now();
    while (counter.load(std::memory_order_relaxed) != index) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto d_point = std::chrono::high_resolution_clock::now();
    if (index > 0) builder->EmitRemainOpts(index);
    if (index == 2) {
      const Slice& key = c_iter->key();
      const Slice& value = c_iter->value();
      const ParsedInternalKey& ikey = c_iter->ikey();
      meta->UpdateBoundaries(key, value, ikey.sequence, ikey.type);
    }
    counter.store(index + 1, std::memory_order_relaxed);
    uint64_t iter_time = std::chrono::duration_cast<std::chrono::nanoseconds>(b_point - a_point).count();
    uint64_t flush_other_time = std::chrono::duration_cast<std::chrono::nanoseconds>(c_point - b_point).count();
    uint64_t wait_time = std::chrono::duration_cast<std::chrono::nanoseconds>(d_point - c_point).count();
    printf("index:%lu, iter_time:%lu, flush_other_time:%lu, wait_time:%lu\n", index, iter_time, flush_other_time, wait_time);
  }


  void BuildTable_new(
      uintptr_t Node_head, uint64_t offset, uint64_t num_entries,
      FileMetaData* meta,
      uint64_t new_versions_NewFileNumber,

      SeqnoToTimeMapping seqno_to_time_mapping,
      uint32_t kUnknownColumnFamily,
      bool paranoid_file_checks,
      int job_id,
      SequenceNumber earliest_write_conflict_snapshot,
      SequenceNumber job_snapshot,
      size_t timestamp_size,
      std::vector<DbPath> tboptions_ioptions_cf_paths,
      std::string cfd_GetName,
      std::string dbname,

      /* Ouput List */
      Status* return_status,
      uint64_t* num_input_entries,  // output 在程序中定义，局部变量
      uint64_t* memtable_payload_bytes,  // output 在程序中定义，局部变量
      uint64_t* memtable_garbage_bytes,  // output 在程序中定义，局部变量
      uint64_t* packed_number_and_path_id,
      uint64_t* file_size,
      SequenceNumber* smallest_seqno,  // The smallest seqno in this file
      SequenceNumber* largest_seqno

  ) {
    ImmutableDBOptions db_options;
    Env::IOPriority io_priority;
    std::vector<SequenceNumber> snapshots;
    FileOptions file_options;
    ReadOptions ro;
    ro.total_order_seek = true;
    ro.io_activity = Env::IOActivity::kFlush;
    // tboptions
    MutableCFOptions tboptions_moptions;  // = tboptions->moptions;
    ImmutableOptions ioptions;            //= tboptions->ioptions;
    ioptions.cf_paths = tboptions_ioptions_cf_paths;
    InternalKeyComparator internal_comparatortboptions(TestComparator(timestamp_size).cmp_without_ts_);//tboptions->internal_comparator
    uint32_t tboptions_column_family_id = 0;//=tboptions->column_family_id;
    const std::string tboptions_column_family_name;//= tboptions->column_family_name;
    TableFileCreationReason tboptions_reason = TableFileCreationReason::kFlush;//tboptions->reason;
    std::string tboptions_db_id = "tboptions_db_id";//tboptions->db_id
    std::string tboptions_db_session_id =
      "tboptions->db_session_id";  // tboptions->db_session_id
    CompressionOptions compression_opts;
    compression_opts.parallel_threads = 10;
    std::vector<std::unique_ptr<IntTblPropCollectorFactory>> int_tbl_prop_collector_factories;
    TableBuilderOptions tboptions(ioptions, tboptions_moptions,
                                  internal_comparatortboptions, &int_tbl_prop_collector_factories,
                                  kSnappyCompression, compression_opts, tboptions_column_family_id, tboptions_column_family_name, 0, false,
                                  TableFileCreationReason::kMisc, 0, 0, "", "", 0, 0);
    //kSnappyCompression
    InternalKeyComparator cfd_internal_comparator;
    uint64_t old_versions_NewFileNumber;
    EventLogger event_logger(db_options.info_log.get());//event_logger_   事件记录器
    IOStatus io_status;                                 //&io_s  IO操作的状态
    Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

    FileDescriptor DPU_fd = FileDescriptor(new_versions_NewFileNumber, 0, 0);

    Arena arena;

    NewMemTable new_m(Node_head, offset, offset, cfd_internal_comparator);
    InternalIterator* iter = NewIterator(&new_m, ro, &arena, cfd_internal_comparator);
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
    /* test */

    /*请求处理过程*/
    // assert((tboptions_column_family_id == kUnknownColumnFamily) == tboptions_column_family_name.empty());

    // Reports the IOStats for flush for every following bytes.
    // const size_t kReportFlushIOStatsEvery = 1048576;  // 每隔一定字节数报告刷新IO统计信息

    // OutputValidator类用于验证插入到SST文件中的键值对是否符合规范。
    // 通过调用OutputValidator::Add()方法传入文件的每个键值对，可以验证键的顺序，并可选择计算键和值的哈希值。
    // OutputValidator output_validator(
    //     internal_comparatortboptions,
    //     tboptions_moptions.check_flush_compaction_key_order,
    //     paranoid_file_checks);

    Status s;
    iter->SeekToFirst();
    std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
          new CompactionRangeDelAggregator(&internal_comparatortboptions,
            snapshots, nullptr));
    std::string fname = TableFileName(ioptions.cf_paths, DPU_fd.GetNumber(),
                                      DPU_fd.GetPathId());
    // fprintf(stderr, "largest_seqno:%lx, smallest_seqno:%lx line:%d\n", DPU_fd.largest_seqno, DPU_fd.smallest_seqno, __LINE__);
    // std::cout << fname << std::endl;
    std::vector<std::string> blob_file_paths;
    std::string file_checksum = kUnknownFileChecksum;
    std::string file_checksum_func_name = kUnknownFileChecksumFuncName;
    EventHelpers::NotifyTableFileCreationStarted(
        ioptions.listeners, dbname, tboptions_column_family_name, fname, job_id,
        tboptions_reason);  // new event
    Env* env = db_options.env;
    assert(env);
    FileSystem* fs = db_options.fs.get();
    assert(fs);
    TableProperties tp;
    bool table_file_created = false;
    // printf("line %u iter->Valid():%d\n", __LINE__, iter->Valid());
    if (iter->Valid()) {
      TableBuilder* builder;
      std::unique_ptr<WritableFileWriter> file_writer;
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
              &event_logger, ioptions.listeners, dbname,
              tboptions_column_family_name, fname, job_id, DPU_fd,
              kInvalidBlobFileNumber, tp, tboptions_reason, s, file_checksum,
              file_checksum_func_name);
          *return_status = s;
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
        builder = ioptions.table_factory->NewTableBuilder(tboptions, file_writer.get());
      }
      auto ucmp = internal_comparatortboptions.user_comparator();
      MergeHelper merge(
          env, ucmp, ioptions.merge_operator.get(),
          nullptr,  // compaction_filter.get(),
          ioptions.logger, true /* internal key corruption is not ok */,
          snapshots.empty() ? 0 : snapshots.back(), nullptr);

      const std::atomic<bool> kManualCompactionCanceledFalse{ false };

      // CompactionIterator c_iter(
      //     iter, ucmp, &merge, kMaxSequenceNumber, &snapshots,
      //     earliest_write_conflict_snapshot, job_snapshot, nullptr, env,
      //     ShouldReportDetailedTime(env, ioptions.stats),
      //     true /* internal key corruption is not ok */,
      //     range_del_agg.get(),
      //     nullptr,
      //     // blob_file_builder.get(),
      //     ioptions.allow_data_in_errors, ioptions.enforce_single_del_contracts,
      //     /*manual_compaction_canceled=*/kManualCompactionCanceledFalse,
      //     /*compaction=*/nullptr, nullptr,  // compaction_filter.get(),
      //     /*shutting_down=*/nullptr, db_options.info_log, nullptr);
      // printf("iter Node_head:%lx, c_iter Node_head:%lx\n", (uint64_t)iter->Current(), (uint64_t)c_iter.Node_head());
      // c_iter.SeekToFirst();
      // const Slice key = c_iter.key();
      // printf("c_iter keyaddr:%lx\n", key.data());
      // if (!c_iter.Valid()) {
      //   printf("error!\n");
      // }

      // parameters needed for parallel iter
      auto a_point = std::chrono::high_resolution_clock::now();
      uint64_t parallel_threads = 3;
      std::vector<uint64_t> Node_heads;
      Node_heads.push_back(Node_head);
      uint64_t iter_nums = 0;
      uint64_t every_thread_entries = num_entries / parallel_threads;
      uint64_t times = parallel_threads - 1;
      for (;iter->Valid();iter->Next()) {
        if (iter_nums == every_thread_entries) {
          // printf("before iter->Node_head:%lx\n", iter->Node_head());
          // c_iter.Next();
          // c_iter.Next();
          // printf("after iter->Node_head:%lx\n", iter->Node_head());
          Node_heads.push_back(iter->Node_head());
          iter_nums = 0;
          times--;
        }
        if (times == 0) break;
        iter_nums++;
      }
      printf("Node_heads.size:%lu, every_thread_entries:%lu, times:%lu, iter_nums:%lu\n", Node_heads.size(), every_thread_entries, times, iter_nums);
      if (Node_heads.size() == 1) {
        for (iter->SeekToFirst();iter->Valid();iter->Next()) {
          printf("iter->Node_head:%lx\n", iter->Node_head());
        }
        uint64_t* ptr = (uint64_t*)(Node_head + offset);
        for (int i = 0;i < 10;i++) {
          printf("i:%d, data:%lu\n", i, *(ptr + i));
        }
      }
      Node_heads.push_back(0);




      std::mutex meta_mtx;
      std::vector<std::thread> flush_thread_pool;
      std::vector<InternalIterator*> iters(parallel_threads);
      std::vector<CompactionIterator*> c_iters;
      std::vector<Arena> arenas(parallel_threads);
      std::vector<NewMemTable*> new_mems(parallel_threads);
      std::atomic<uint64_t> counter(0);
      // fprintf(stderr, "line:%d, Node_head:%lx\n", __LINE__, Node_head);
      for (int i = 0;i < parallel_threads;++i) {
        new_mems[i] = new NewMemTable(Node_heads[i], offset + Node_heads[i + 1], offset, cfd_internal_comparator);
        // printf("memsNode_head:%lx\n", new_mems[i]->Node_head_);
        iters[i] = NewIterator(new_mems[i], ro, &arenas[i], cfd_internal_comparator);
        iters[i]->SeekToFirst();
        // printf("iters Node_head:%lx\n", iters[i]->Node_head());
        c_iters.push_back(new CompactionIterator(iters[i], ucmp, &merge, kMaxSequenceNumber, &snapshots,
          earliest_write_conflict_snapshot, job_snapshot, nullptr, env,
          ShouldReportDetailedTime(env, ioptions.stats),
          true /* internal key corruption is not ok */,
          range_del_agg.get(),
          nullptr,
          // blob_file_builder.get(),
          ioptions.allow_data_in_errors, ioptions.enforce_single_del_contracts,
          /*manual_compaction_canceled=*/kManualCompactionCanceledFalse,
          /*compaction=*/nullptr, nullptr,  // compaction_filter.get(),
          /*shutting_down=*/nullptr, db_options.info_log, nullptr));
        // printf("c_iters Node_head:%lx\n", c_iters[i]->Node_head());
      }
      auto c_point = std::chrono::high_resolution_clock::now();

      uint64_t piece_nums = 300000;
      for (uint64_t i = 0;i < parallel_threads;i++) {
        // uint64_t index = i;
        // fprintf(stderr, "index:%lu, Node_head:%lx\n", index,
        // c_iters[i]->Node_head()); flush_thread_pool.emplace_back([i,
        // &builder, &meta, &meta_mtx, &token_pools, &c_iters, &arenas]
        // {iter_parallel(0, builder, meta, std::ref(meta_mtx), c_iters[0],
        // arenas[0], token_pools[0], token_pools[1],
        //   0, 10000000ul);});
        // if (i != 0) {
        //   flush_thread_pool[i - 1].join();
        // }
        flush_thread_pool.emplace_back(iter_parallel, i, builder, meta,
          std::ref(meta_mtx), c_iters[i], piece_nums * i, i + 1 == parallel_threads ? 10000000ul : piece_nums * (i + 1), std::ref(counter));
        //piece_nums * index, piece_nums * (index + 1));});
      }
      // flush_thread_pool[parallel_threads - 1].join();

      // uint64_t nums = 0;
      // int c_iter_num = 0;
      // auto begin = std::chrono::steady_clock::now();
      // for (c_iter.SeekToFirst(); c_iter.Valid(); c_iter.Next()) {
      //   // c_iter_num++;
      //   // auto time0 = std::chrono::steady_clock::now();
      //   const Slice& key = c_iter.key();
      //   const Slice& value = c_iter.value();
      //   const ParsedInternalKey& ikey = c_iter.ikey();
      //   // 这一段好像没什么用，注释掉也可以跑
      //   // s = output_validator.Add(key, value);// 添加一个键到键值序列中，并返回键是否符合规范，例如键是否按顺序排列。
      //   // if (!s.ok()) {
      //   //   printf("从这里退出循环%lu\n", __LINE__);
      //   //   break;
      //   // }
      //   // auto time1 = std::chrono::steady_clock::now();
      //   builder->Add(key, value);
      //   // printf("meta->UpdateBoundaries begin\n");
      //   // printf("key.size: %lu key:%ld\n",key.size(),*(uint64_t*)key.data());
      //   // printf("value.size: %lu value:%ld\n", value.size(),*(uint64_t*)value.data());
      //   // auto start = std::chrono::steady_clock::now();
      //   s = meta->UpdateBoundaries(key, value, ikey.sequence, ikey.type);
      //   if (!s.ok()) {
      //     break;
      //   }
        // auto end2 = std::chrono::steady_clock::now();
        // updatetime += std::chrono::duration<double>(end2 - time0).count();
        // addtime += std::chrono::duration<double>(start - time1).count();
        // getkvtime += std::chrono::duration<double>(time1 - time0).count();
        // TODO(noetzli): Update stats after flush, too.
        // 这个分支好像根本没有进去过，所以我注释掉了
        // if (io_priority == Env::IO_HIGH &&
        //     IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
        //   printf("line:%lu\n", __LINE__);
        //   ThreadStatusUtil::SetThreadOperationProperty(
        //       ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
        // }
      // }
      // auto end = std::chrono::steady_clock::now();
      // double c_iter_time = std::chrono::duration<double>(end - begin).count();
      // printf("nums:%lu, c_iter_num %d, c_iter_time:%lf\n", nums, c_iter_num, c_iter_time);
      // printf("nums:%lu, blocktime:%lf, compresstime:%lf, iotime:%lf, nexttime:%lf, c_iter_nexttime:%lu\n", nums, blocktime, compresstime, iotime, nexttime, c_iter_nexttime);
      // printf("nums:%lu, getkeytime:%lu\n", nums++, getkeytime);


      // if (!s.ok()) {
      //   c_iter.status().PermitUncheckedError();
      // }
      // else if (!c_iter.status().ok()) {
      //   s = c_iter.status();
      // }
      // fprintf(stderr, "before token_pools line:%d\n", __LINE__);
      // fflush(stderr);
      try {
        for (int i = 0; i < flush_thread_pool.size(); i++) {
          flush_thread_pool[i].join();
        }
      }
      catch (const std::system_error& error) {
        printf("join error!\n");
      }
      auto b_point = std::chrono::high_resolution_clock::now();
      uint64_t iter_time = std::chrono::duration_cast<std::chrono::nanoseconds>(b_point - a_point).count();
      uint64_t init_time = std::chrono::duration_cast<std::chrono::milliseconds>(c_point - a_point).count();
      printf("iter time:%lu, init_time:%lu\n", iter_time / 1000 / 1000, init_time);


      builder->merge_prop();


      TEST_SYNC_POINT("BuildTable:BeforeFinishBuildTable");
      const bool empty = builder->IsEmpty();
      // fprintf(stderr, "gethere:%d, empty:%d\n", __LINE__, empty);
      // fflush(stderr);
      // *num_input_entries = c_iters[parallel_threads - 1]->num_input_entry_scanned();
      *num_input_entries = 0;
      for (int i = 0;i < parallel_threads;++i) {
        *num_input_entries += c_iters[i]->num_input_entry_scanned();
      }
      if (*num_input_entries != num_entries) {
        printf("nums error!\n");
      }
      // fprintf(stderr, "gethere:%d\n", __LINE__);
      // fflush(stderr);
      // *num_input_entries = c_iter.num_input_entry_scanned();
      if (!s.ok() || empty) {
        // fprintf(stderr, "gethere:%d\n", __LINE__);
        // fflush(stderr);
        builder->Abandon();
      }
      else {
        // fprintf(stderr, "gethere:%d\n", __LINE__);
        // fflush(stderr);
        // 这部分好像也是没什么用的，主要是一些统计信息
        std::string seqno_time_mapping_str;
        // fprintf(stderr, "largest_seqno:%lx, smallest_seqno:%lx line:%d\n", DPU_fd.largest_seqno, DPU_fd.smallest_seqno, __LINE__);
        seqno_to_time_mapping.Encode(
            seqno_time_mapping_str, DPU_fd.smallest_seqno, DPU_fd.largest_seqno,
            meta->file_creation_time);
        // fprintf(stderr, "largest_seqno:%lx, smallest_seqno:%lx line:%d\n", DPU_fd.largest_seqno, DPU_fd.smallest_seqno, __LINE__);
        builder->SetSeqnoTimeTableProperties(
            seqno_time_mapping_str,
        ioptions.compaction_style == CompactionStyle::kCompactionStyleFIFO
                ? meta->file_creation_time
                : meta->oldest_ancester_time);
        // fprintf(stderr, "before Finish line:%d\n", __LINE__);
        // fflush(stderr);
        s = builder->Finish();
        // s = builder->Finish_parallel();
      }
      if (io_status.ok()) {
        io_status = builder->io_status();
      }
      // fprintf(stderr, "gethere:%d\n", __LINE__);
      // fflush(stderr);
      if (s.ok() && !empty) {
        uint64_t file_size = builder->FileSize();
        DPU_fd.file_size = file_size;
        meta->tail_size = builder->GetTailSize();
        meta->marked_for_compaction = builder->NeedCompact();
        assert(DPU_fd.GetFileSize() > 0);
        // tp = builder->GetTableProperties();
        // const CompactionIterationStats& ci_stats = c_iters[0]->iter_stats();
        // const CompactionIterationStats& ci_stats = c_iter.iter_stats();
        // uint64_t total_payload_bytes =
        //   ci_stats.total_input_raw_key_bytes +
        //   ci_stats.total_input_raw_value_bytes;
        // uint64_t total_payload_bytes_written =
        //   (tp.raw_key_size + tp.raw_value_size);
        // if (total_payload_bytes_written <= total_payload_bytes) {
        //   *memtable_payload_bytes = total_payload_bytes;
        //   *memtable_garbage_bytes =
        //     total_payload_bytes - total_payload_bytes_written;
        // }
        // else {
        //   memtable_payload_bytes = 0;
        //   memtable_garbage_bytes = 0;
        // }
      }
      // fprintf(stderr, "gethere:%d\n", __LINE__);
      // fflush(stderr);
      delete builder;
      // fprintf(stderr, "gethere:%d\n", __LINE__);
      // fflush(stderr);
      TEST_SYNC_POINT("BuildTable:BeforeSyncTable");
      if (s.ok() && !empty) {
        StopWatch sw(ioptions.clock, ioptions.stats, TABLE_SYNC_MICROS);
        io_status = file_writer->Sync(ioptions.use_fsync);
      }
      // fprintf(stderr, "gethere:%d\n", __LINE__);
      // fflush(stderr);
      TEST_SYNC_POINT("BuildTable:BeforeCloseTableFile");
      if (s.ok() && io_status.ok() && !empty) {
        io_status = file_writer->Close();
      }
      // fprintf(stderr, "gethere:%d\n", __LINE__);
      // fflush(stderr);
      if (s.ok() && io_status.ok() && !empty) {
        // Add the checksum information to file metadata.
        // fprintf(stderr, "gethere : %d\n", __LINE__);
        // fflush(stderr);
        meta->file_checksum = file_writer->GetFileChecksum();
        meta->file_checksum_func_name = file_writer->GetFileChecksumFuncName();
        DPU_fd.smallest_seqno = meta->fd.smallest_seqno;
        DPU_fd.largest_seqno = meta->fd.largest_seqno;
        file_checksum = meta->file_checksum;
        file_checksum_func_name = meta->file_checksum_func_name;
        // Set unique_id only if db_id and db_session_id exist
        // if (!tboptions->db_id.empty() && !tboptions->db_session_id.empty()) {
        if (!GetSstInternalUniqueId(tboptions_db_id, tboptions_db_session_id,
          DPU_fd.GetNumber(), &(meta->unique_id)).ok()) {
          // if failed to get unique id, just set it Null
          meta->unique_id = kNullUniqueId64x2;
        }
        // fprintf(stderr, "gethere:%d\n", __LINE__);
        // fflush(stderr);
        // }
      }
      // fprintf(stderr, "gethere:%d\n", __LINE__);
      // fflush(stderr);
      if (s.ok()) {
        s = io_status;
      }

      // for (int i = 0; i < parallel_threads; ++i) {
      //   delete new_mems[i];
      //   // delete iters[i];
      //   delete c_iters[i];
      // }
    }
    else {
      printf("no valid!\n");
    }
    // 回传 host
    printf("send back to host begin\n");
    // Check for input iterator errors
    if (!iter->status().ok()) {
      s = iter->status();
    }
    // fprintf(stderr, "gethere:%d\n", __LINE__);
    // fflush(stderr);
    if (!s.ok() || DPU_fd.GetFileSize() == 0) {
      TEST_SYNC_POINT("BuildTable:BeforeDeleteFile");

      constexpr IODebugContext* dbg = nullptr;

      if (table_file_created) {
        Status ignored = fs->DeleteFile(fname, IOOptions(), dbg);
        ignored.PermitUncheckedError();
      }
    }
    Status status_for_listener = s;
    fprintf(stderr, "gethere:%d\n", __LINE__);
    fflush(stderr);
    if (DPU_fd.GetFileSize() == 0) {
      fname = "(nil)";
      if (s.ok()) {
        status_for_listener = Status::Aborted("Empty SST file not kept");
      }
    }
    fprintf(stderr, "gethere:%d\n", __LINE__);
    fflush(stderr);
    *return_status = s;
    *packed_number_and_path_id = DPU_fd.packed_number_and_path_id;
    *file_size = DPU_fd.file_size;
    fprintf(stderr, "largest_seqno:%lx, smallest_seqno:%lx line:%d\n", DPU_fd.largest_seqno, DPU_fd.smallest_seqno, __LINE__);
    *smallest_seqno = DPU_fd.smallest_seqno;
    *largest_seqno = DPU_fd.largest_seqno;
    return;
  }
}  // namespace ROCKSDB_NAMESPACE

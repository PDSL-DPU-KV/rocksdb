//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include <string>
#include <vector>

#include "db/range_tombstone_fragmenter.h"
#include "db/seqno_to_time_mapping.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "logging/event_logger.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

struct FileMetaData;

class VersionSet;
class BlobFileAddition;
class SnapshotChecker;
class TableCache;
class TableBuilder;
class WritableFileWriter;
class InternalStats;
class BlobFileCompletionCallback;

struct DbPath_struct {
  char path[128];
  uint64_t target_size;
};

typedef struct {
  region_t dst;
  region_t src;
  uint64_t piece_size;
  uint64_t region_size;
  uint64_t copy_size;
  uint64_t copy_n;
  uint64_t memcpy_mode;
} params_memcpy_t;

struct MetaReq {
  bool use_dpa;
  uint64_t tp_num;
  uintptr_t TrisectionPoint[200];
  uint64_t num_entries;
  uintptr_t Node_head;
  std::vector<uint64_t> Node_heads;
  uintptr_t mt_buf;
  rocksdb::FileMetaData file_meta;
  uint64_t new_versions_NewFileNumber;
  rocksdb::SeqnoToTimeMapping seqno_to_time_mapping;
  uint32_t kUnknownColumnFamily;
  std::string str_full_history_ts_low;
  rocksdb::Env::IOPriority* io_priority;
  bool paranoid_file_checks;
  int job_id;
  std::vector<rocksdb::SequenceNumber> snapshots;
  rocksdb::SequenceNumber earliest_write_conflict_snapshot;
  rocksdb::SequenceNumber job_snapshot;
  size_t timestamp_size;
  std::vector<rocksdb::DbPath> tboptions_ioptions_cf_paths;
  rocksdb::SequenceNumber smallest_seqno;
  rocksdb::SequenceNumber largest_seqno;
  std::string cfd_GetName;
  std::string dbname;
  std::string mmap_desc;
  params_memcpy_t params;
};

struct MetaResult {
  rocksdb::Status status;
  uint64_t num_input_entries;
  uint64_t memtable_payload_bytes;
  uint64_t memtable_garbage_bytes;
  uint64_t packed_number_and_path_id;
  uint64_t file_size;
  rocksdb::SequenceNumber smallest_seqno;
  rocksdb::SequenceNumber largest_seqno;
  rocksdb::FileMetaData file_meta;
};

// Convenience function for NewTableBuilder on the embedded table_factory.
TableBuilder* NewTableBuilder(const TableBuilderOptions& tboptions,
                              WritableFileWriter* file);

// Build a Table file from the contents of *iter.  The generated file
// will be named according to number specified in meta. On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
//
// @param column_family_name Name of the column family that is also identified
//    by column_family_id, or empty string if unknown.
extern Status BuildTable(
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
    const SeqnoToTimeMapping& seqno_to_time_mapping,
    EventLogger* event_logger = nullptr, int job_id = 0,
    const Env::IOPriority io_priority = Env::IO_HIGH,
    TableProperties* table_properties = nullptr,
    Env::WriteLifeTimeHint write_hint = Env::WLTH_NOT_SET,
    const std::string* full_history_ts_low = nullptr,
    BlobFileCompletionCallback* blob_callback = nullptr,
    Version* version = nullptr, uint64_t* num_input_entries = nullptr,
    uint64_t* memtable_payload_bytes = nullptr,
    uint64_t* memtable_garbage_bytes = nullptr);

void BuildTable_new(uint64_t offset, MetaReq* req, MetaResult* result,
                    bool use_optimized = false, bool use_dpaflush = false);
}  // namespace ROCKSDB_NAMESPACE
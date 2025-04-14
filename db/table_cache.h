// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include "db/db_impl.h"
#include "db/dbformat.h"
#include <cstdint>
#include <string>

#include "leveldb/cache.h"
#include "leveldb/slice.h"
#include "leveldb/table.h"

#include "port/port.h"
#include "util/interval_tree.h"

namespace leveldb {

class Env;

class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);

  TableCache(const TableCache&) = delete;
  TableCache& operator=(const TableCache&) = delete;

  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-null, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or to nullptr if no Table object
  // underlies the returned iterator.  The returned "*tableptr" object is owned
  // by the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, Table** tableptr = nullptr);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, void* arg,
             bool (*saver)(void*, const Slice&, const Slice&,
                           std::string sec_key, int top_k_output, DBImpl* db),
             std::string secondary_key, int top_k_output, DBImpl* db);

  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, const Slice& blockkey,
             void* arg,
             bool (*saver)(void*, const Slice&, const Slice&,
                           std::string sec_key, int top_k_output, DBImpl* db),
             std::string secondary_key, int top_k_output, DBImpl* db);

  Status RangeGet(const ReadOptions& options, uint64_t file_number,
                  uint64_t file_size, const Slice& blockkey, void* arg,
                  bool (*saver)(void*, const Slice&, const Slice&,
                                std::string secondary_key, int top_k_output,
                                DBImpl* db),
                  std::string secondary_key, int top_k_output, DBImpl* db);

  Status RangeGet(const ReadOptions& options, uint64_t file_number,
                  uint64_t file_size, const Slice& start_key,
                  const Slice& end_key, void* arg,
                  bool (*saver)(void*, const Slice&, const Slice&,
                                std::string secondary_key, int top_k_output,
                                DBImpl* db),
                  std::string secondary_key, int top_k_output, DBImpl* db);

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);
  Interval2DTreeWithTopK* GetIntervalTree() { return interval_tree_; }

 private:
  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);

  Env* const env_;
  const std::string dbname_;
  const Options& options_;
  Cache* cache_;
  Interval2DTreeWithTopK* interval_tree_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_

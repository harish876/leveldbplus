// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "db/db_impl.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"

#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
    delete interval_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  FilterBlockReader* secondary_filter;
  const char* secondary_filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
  Block* interval_block;
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  bool isinterval = options.interval_tree_file_name.empty();
  int footerlength;
  if (isinterval) {
    footerlength = Footer::kEncodedLength + BlockHandle::kMaxEncodedLength;
  } else {
    footerlength = Footer::kEncodedLength;
  }
  *table = nullptr;
  if (size < footerlength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[footerlength];
  Slice footer_input;
  Status s = file->Read(size - footerlength, footerlength, &footer_input,
                        footer_space);
  if (!s.ok()) return s;

  ReadOptions roptions;
  roptions.type = ReadType::Meta;

  Footer footer;
  s = footer.DecodeFrom(&footer_input, isinterval);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  opt.type = ReadType::Meta;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  Block* interval_block = NULL;
  if (isinterval) {
    BlockContents interval_block_contents;
    if (s.ok()) {
      s = ReadBlock(file, roptions, footer.interval_handle(),
                    &interval_block_contents);
      if (s.ok()) {
        interval_block = new Block(interval_block_contents);
      }
    }
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->interval_block = interval_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    rep->secondary_filter_data = nullptr;
    rep->secondary_filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  std::string skey = "secondaryfilter.";
  skey.append(rep_->options.filter_policy->Name());
  iter->Seek(skey);
  if (iter->Valid() && iter->key() == Slice(skey)) {
    ReadSecondaryFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  opt.type = ReadType::Meta;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

void Table::ReadSecondaryFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  opt.type = ReadType::Meta;
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->secondary_filter_data =
        block.data.data();  // Will need to delete later
  }
  rep_->secondary_filter =
      new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          bool (*saver)(void*, const Slice&, const Slice&,
                                        std::string& sec_key, int& top_k_output,
                                        DBImpl* db),
                          std::string& sec_key, int& top_k_output, DBImpl* db) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->SeekToFirst();

  int p = 1;
  while (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->secondary_filter;
    BlockHandle handle;
    if (filter != NULL && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->SeekToFirst();
      while (block_iter->Valid()) {
        bool f = (*saver)(arg, block_iter->key(), block_iter->value(), sec_key,
                          top_k_output, db);
        block_iter->Next();
      }
      s = block_iter->status();
      delete block_iter;
    }

    iiter->Next();
  }

  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          bool (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

Status Table::InternalGet(const ReadOptions& options, const Slice& blockkey,
                          const Slice& pointkey, void* arg,
                          bool (*saver)(void*, const Slice&, const Slice&,
                                        std::string& secKey, int& topKOutput,
                                        DBImpl* db),
                          std::string& secKey, int& topKOutput, DBImpl* db) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(blockkey);

  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->secondary_filter;
    BlockHandle handle;
    if (filter != NULL && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), pointkey)) {
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->SeekToFirst();
      while (block_iter->Valid()) {
        bool f = (*saver)(arg, block_iter->key(), block_iter->value(), secKey,
                          topKOutput, db);
        block_iter->Next();
      }
      s = block_iter->status();
      delete block_iter;
    }

    if (s.ok()) {
      s = iiter->status();
    }

  } else
    s.IOError("");

  delete iiter;
  return s;
}

Status Table::RangeInternalGet(const ReadOptions& options, const Slice& k,
                               void* arg,
                               bool (*saver)(void*, const Slice&, const Slice&,
                                             std::string& secondary_key,
                                             int& top_k_output, DBImpl* db),
                               std::string& secondary_key, int& top_k_output,
                               DBImpl* db) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != NULL && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->SeekToFirst();
      while (block_iter->Valid()) {
        bool f = (*saver)(arg, block_iter->key(), block_iter->value(),
                          secondary_key, top_k_output, db);
        block_iter->Next();
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

Status Table::RangeInternalGetWithInterval(
    const ReadOptions& options, const Slice& startk, const Slice& endk,
    void* arg,
    bool (*saver)(void*, const Slice&, const Slice&, std::string& secKey,
                  int& topKOutput, DBImpl* db),
    std::string& secKey, int& topKOutput, DBImpl* db) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->SeekToFirst();

  Iterator* iterInterval =
      rep_->interval_block->NewIterator(rep_->options.comparator);
  iterInterval->SeekToFirst();

  while (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->secondary_filter;
    BlockHandle handle;
    Slice key, value;
    if (iterInterval->Valid()) {
      key = iterInterval->key();
      value = iterInterval->value();
    }
    if (startk.compare(value) > 0 || endk.compare(key) < 0) {
    }

    else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->SeekToFirst();
      while (block_iter->Valid()) {
        bool f = (*saver)(arg, block_iter->key(), block_iter->value(), secKey,
                          topKOutput, db);
        block_iter->Next();
      }
      s = block_iter->status();
      delete block_iter;
    }

    iiter->Next();
    iterInterval->Next();
  }

  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  delete iterInterval;
  return s;
}

Status Table::InternalGetWithInterval(
    const ReadOptions& options, const Slice& k, void* arg,
    bool (*saver)(void*, const Slice&, const Slice&, std::string& secKey,
                  int& topKOutput, DBImpl* db),
    std::string& secKey, int& topKOutput, DBImpl* db) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->SeekToFirst();

  Iterator* iterInterval =
      rep_->interval_block->NewIterator(rep_->options.comparator);
  iterInterval->SeekToFirst();

  const char* ks = k.data();
  Slice sk = Slice(ks, k.size() - 8);

  while (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->secondary_filter;
    BlockHandle handle;
    Slice key, value;
    if (iterInterval->Valid()) {
      key = iterInterval->key();
      value = iterInterval->value();
    }
    if (sk.compare(key) < 0 || sk.compare(value) > 0) {
    } else if (filter != NULL && handle.DecodeFrom(&handle_value).ok() &&
               !filter->KeyMayMatch(handle.offset(), k)) {
    }

    else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->SeekToFirst();
      while (block_iter->Valid()) {
        bool f = (*saver)(arg, block_iter->key(), block_iter->value(), secKey,
                          topKOutput, db);

        block_iter->Next();
      }

      s = block_iter->status();
      delete block_iter;
    }

    iiter->Next();
    iterInterval->Next();
  }

  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  delete iterInterval;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb

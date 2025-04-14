// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include "db/dbformat.h"
#include <cassert>
#include <sstream>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"

#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/interval_tree.h"
#include "util/json_utils.h"

#define SSTR(x)                                                             \
  static_cast<std::ostringstream&>((std::ostringstream() << std::dec << x)) \
      .str()

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        interval_block(&interval_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        secondary_filter_block(
            opt.filter_policy == nullptr
                ? nullptr
                : new FilterBlockBuilder(opt.filter_policy)),  // revisit
        pending_index_entry(false),
        min_sec_value(""),
        max_sec_value(""),
        max_sec_seq_number(0) {
    index_block_options.block_restart_interval = 1;
    interval_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  Options interval_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  BlockBuilder interval_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;
  FilterBlockBuilder* secondary_filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;

  // Add Min Max and MaxTimestamp secondary attribute value of a Block for
  // Interval Tree
  std::string max_sec_value;
  std::string min_sec_value;
  uint64_t max_sec_seq_number;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file,
                           Interval2DTreeWithTopK* interval_tree,
                           uint64_t file_number, std::string* minsec,
                           std::string* maxsec)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }

  if (rep_->secondary_filter_block != nullptr) {
    rep_->secondary_filter_block->StartBlock(0);
  }

  interval_tree_ = interval_tree;
  file_number_ = file_number;
  smallest_sec_ = minsec;
  largest_sec_ = maxsec;
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_->secondary_filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  rep_->interval_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  if (r->pending_index_entry) {
    if (!r->options.interval_tree_file_name.empty()) {
      interval_tree_->insertInterval(
          SSTR(file_number_) + "+" +
              r->last_key.substr(0, r->last_key.size() - 8),
          r->min_sec_value, r->max_sec_value, r->max_sec_seq_number);
    } else {
      Slice value(r->max_sec_value);
      Slice inKey(r->min_sec_value);
      r->interval_block.Add(inKey, value);
    }

    if (smallest_sec_->empty()) {
      smallest_sec_->assign(r->min_sec_value);
      largest_sec_->assign(r->max_sec_value);
    } else {
      if (this->smallest_sec_->compare(r->min_sec_value) > 0) {
        this->smallest_sec_->assign(r->min_sec_value);
      } else if (this->largest_sec_->compare(rep_->max_sec_value) < 0) {
        this->largest_sec_->assign(r->max_sec_value);
      }
    }

    r->min_sec_value.clear();
    r->max_sec_value.clear();
    r->max_sec_seq_number = 0;

    assert(r->data_block.empty());
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  if (r->secondary_filter_block != nullptr) {
    if (r->options.secondary_key.empty()) {
      return;
    }
    std::string secondary_key_attr;
    Status s = ExtractKeyFromJSON(value, r->options.secondary_key,
                                  &secondary_key_attr);

    if (!s.ok()) {
      r->last_key.assign(key.data(), key.size());
      r->num_entries++;
      r->data_block.Add(key, value);
      const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
      if (estimated_block_size >= r->options.block_size) {
        Flush();
      }
      return;
    }
    std::string tag = key.ToString().substr(key.size() - 8);
    std::string sec_keys = secondary_key_attr;
    Slice Key = sec_keys + tag;

    r->secondary_filter_block->AddKey(Key);
    // range logic
    if (rep_->max_sec_value == "" ||
        rep_->max_sec_value.compare(sec_keys) < 0) {
      rep_->max_sec_value.assign(sec_keys);
    }
    if (rep_->min_sec_value == "" ||
        rep_->min_sec_value.compare(sec_keys) > 0) {
      rep_->min_sec_value.assign(sec_keys);
    }
    // ParsedInternalKey parsed_key;
    // why negation here
    // if (!ParseInternalKey(key, &parsed_key)) {
    //   if (rep_->max_sec_seq_number < parsed_key.sequence) {
    //     rep_->max_sec_seq_number = parsed_key.sequence;
    //   }
    // }
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  const size_t n = key.size();
  if (n >= 8) {
    uint64_t num = DecodeFixed64(key.data() + n - 8);
    SequenceNumber seq = num >> 8;
    if (rep_->max_sec_seq_number < seq) {
      rep_->max_sec_seq_number = seq;
    }
  }

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
  if (r->secondary_filter_block != nullptr) {
    r->secondary_filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }

    case kZstdCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Zstd_Compress(r->options.zstd_compression_level, raw.data(),
                              raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Zstd not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, secondary_filter_block_handle,
      metaindex_block_handle, index_block_handle, interval_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }
  // Write Secondary Filer Block
  if (ok() && r->secondary_filter_block != NULL) {
    WriteRawBlock(r->secondary_filter_block->Finish(), kNoCompression,
                  &secondary_filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }
    if (r->secondary_filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "secondaryfilter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      secondary_filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      if (!r->options.interval_tree_file_name.empty() && interval_tree_) {
        interval_tree_->insertInterval(
            SSTR(file_number_) + "+" +
                r->last_key.substr(0, r->last_key.size() - 8),
            r->min_sec_value, r->max_sec_value, r->max_sec_seq_number);
        interval_tree_->sync();
      } else {
        Slice value(r->max_sec_value);
        Slice inKey(r->min_sec_value);
        r->interval_block.Add(inKey, value);
      }

      if (smallest_sec_->empty()) {
        smallest_sec_->assign(r->min_sec_value);
        largest_sec_->assign(r->max_sec_value);
      } else {
        if (this->smallest_sec_->compare(r->min_sec_value) > 0) {
          this->smallest_sec_->assign(r->min_sec_value);
        } else if (this->largest_sec_->compare(rep_->max_sec_value) < 0) {
          this->largest_sec_->assign(r->max_sec_value);
        }
      }

      r->min_sec_value.clear();
      r->max_sec_value.clear();
      r->max_sec_seq_number = 0;

      // r->options.comparator->FindShortSuccessor(&r->last_key); //?
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    if (r->options.interval_tree_file_name.empty()) {
      WriteBlock(&r->interval_block, &interval_block_handle);
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    bool isinterval = r->options.interval_tree_file_name.empty();
    if (isinterval) {
      footer.set_interval_handle(interval_block_handle);
    }
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding, isinterval);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
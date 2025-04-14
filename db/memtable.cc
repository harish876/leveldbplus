// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"

#include "db/db_impl.h"
#include "db/dbformat.h"
#include <cstdint>
#include <unordered_set>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/slice.h"

#include "util/coding.h"
#include "util/json_utils.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& comparator,
                   std::string secondary_key)
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {
  secondary_attribute_ = secondary_key;
}

MemTable::~MemTable() {
  SecMemTable::const_iterator lookup = secondary_table_.begin();
  for (; lookup != secondary_table_.end(); lookup.increment()) {
    std::pair<std::string, std::vector<std::string>*> pr = *lookup;
    delete pr.second;
  }
  secondary_table_.clear();
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  tag          : uint64((sequence << 8) | type)
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  table_.Insert(buf);

  // SECONDARY MEMTABLE
  // Ex: { id: 1, age: 30} we add this record with key age=30

  /*
    secTable_ = {
        "30": ["1", "2"],
        "25": ["3"]
      }
  */
  if (type == kTypeDeletion) {
    return;
  }
  std::string extracted_secondary_key;
  Status st = ExtractKeyFromJSON(value.ToString().c_str(), secondary_attribute_,
                                 &extracted_secondary_key);
  if (!st.ok()) {
    return;
  }
  SecMemTable::const_iterator lookup =
      secondary_table_.find(extracted_secondary_key);
  if (lookup == secondary_table_.end()) {
    std::vector<std::string>* invertedList = new std::vector<std::string>();
    invertedList->push_back(key.ToString());
    secondary_table_.insert(
        std::make_pair(extracted_secondary_key, invertedList));
  } else {
    std::pair<std::string, std::vector<std::string>*> pr = *lookup;
    pr.second->push_back(key.ToString());
  }
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s,
                   uint64_t* tag) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      *tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(*tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

void MemTable::Get(const Slice& skey, SequenceNumber snapshot,
                   std::vector<SecondaryKeyReturnVal>* acc, Status* s,
                   std::unordered_set<std::string>* result_set,
                   int top_k_output) {
  auto lookup = secondary_table_.find(skey.ToString());
  if (lookup == secondary_table_.end()) {
    return;
  }
  std::pair<std::string, std::vector<std::string>*> pr = *lookup;
  for (int i = pr.second->size() - 1; i >= 0; i--) {
    if (acc->size() >= top_k_output) return;

    Slice pkey = pr.second->at(i);
    LookupKey lkey(pkey, snapshot);
    std::string secKeyVal;
    std::string svalue;
    Status s;
    uint64_t tag;
    if (!this->Get(lkey, &svalue, &s, &tag)) return;
    if (s.IsNotFound()) return;

    Status st = ExtractKeyFromJSON(svalue, secondary_attribute_, &secKeyVal);
    if (!st.ok()) return;
    if (comparator_.comparator.user_comparator()->Compare(secKeyVal, skey) ==
        0) {
      struct SecondaryKeyReturnVal newVal;
      newVal.key = pr.second->at(i);
      std::string temp;

      if (result_set->find(newVal.key) == result_set->end()) {
        newVal.value = svalue;
        newVal.sequence_number = tag;

        if (acc->size() < top_k_output) {
          newVal.Push(acc, newVal);
          result_set->insert(newVal.key);

        } else if (newVal.sequence_number > acc->front().sequence_number) {
          newVal.Pop(acc);
          newVal.Push(acc, newVal);
          result_set->insert(newVal.key);
          result_set->erase(result_set->find(acc->front().key));
        }
      }
    }
  }
}

void MemTable::RangeGet(const Slice& start_skey, const Slice& end_skey,
                        SequenceNumber snapshot,
                        std::vector<SecondaryKeyReturnVal>* acc, Status* s,
                        std::unordered_set<std::string>* result_set,
                        int top_k_output) {
  auto lookuplb = secondary_table_.lower_bound(start_skey.ToString());
  auto lookupub = secondary_table_.upper_bound(end_skey.ToString());

  for (; lookuplb != lookupub; lookuplb++) {
    std::pair<std::string, std::vector<std::string>*> pr = *lookuplb;
    for (int i = pr.second->size() - 1; i >= 0; i--) {
      if (acc->size() >= top_k_output) {
        continue;
      }
      Slice pkey = pr.second->at(i);
      LookupKey lkey(pkey, snapshot);
      std::string secKeyVal;
      std::string svalue;
      Status s;
      uint64_t tag;
      if (!this->Get(lkey, &svalue, &s, &tag)) return;
      if (s.IsNotFound()) return;

      Status st = ExtractKeyFromJSON(svalue, secondary_attribute_, &secKeyVal);
      if (!st.ok()) return;
      if (comparator_.comparator.user_comparator()->Compare(secKeyVal,
                                                            pr.first) == 0) {
        struct SecondaryKeyReturnVal newVal;
        newVal.key = pr.second->at(i);
        std::string temp;

        if (result_set->find(newVal.key) == result_set->end()) {
          newVal.value = svalue;
          newVal.sequence_number = tag;

          if (acc->size() < top_k_output) {
            newVal.Push(acc, newVal);
            result_set->insert(newVal.key);

          } else if (newVal.sequence_number > acc->front().sequence_number) {
            newVal.Pop(acc);
            newVal.Push(acc, newVal);
            result_set->insert(newVal.key);
            result_set->erase(result_set->find(acc->front().key));
          }
        }
      }
    }
  }
}

}  // namespace leveldb

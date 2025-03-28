#ifndef LEVELDB_UTIL_JSON_UTILS_H_
#define LEVELDB_UTIL_JSON_UTILS_H_

#include <string>

#include "leveldb/slice.h"
#include "leveldb/status.h"

#include "rapidjson/document.h"

namespace leveldb {

// Extracts any key from a JSON document. Helper function used to get primary
// key and secondary key attribute from document
Status ExtractKeyFromJSON(const Slice& json_data, const std::string& key,
                          std::string* key_attr);

}  // namespace leveldb

#endif  // LEVELDB_UTIL_JSON_UTILS_H_
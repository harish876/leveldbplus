#include "util/json_utils.h"

#include <sstream>

namespace leveldb {

Status ExtractKeyFromJSON(const Slice& json_data, const std::string& key,
                          std::string* key_attr) {
  if (key.empty()) {
    return Status::InvalidArgument("primary key not set");
  }

  rapidjson::Document doc;
  doc.Parse<0>(json_data.ToString().c_str());

  if (!doc.IsObject() || !doc.HasMember(key.c_str()) ||
      doc[key.c_str()].IsNull()) {
    return Status::InvalidArgument(
        "primary key attribute not dound in the document.");
  }

  std::ostringstream key_stream;
  const auto& key_value = doc[key.c_str()];

  if (key_value.IsNumber()) {
    if (key_value.IsUint64()) {
      key_stream << key_value.GetUint64();
    } else if (key_value.IsInt64()) {
      key_stream << key_value.GetInt64();
    } else if (key_value.IsDouble()) {
      key_stream << key_value.GetDouble();
    } else if (key_value.IsUint()) {
      key_stream << key_value.GetUint();
    } else if (key_value.IsInt()) {
      key_stream << key_value.GetInt();
    }
  } else if (key_value.IsString()) {
    key_stream << key_value.GetString();
  } else if (key_value.IsBool()) {
    key_stream << key_value.GetBool();
  } else {
    return Status::InvalidArgument("Unsupported primary key type");
  }

  *key_attr = key_stream.str();
  return Status::OK();
}

}  // namespace leveldb
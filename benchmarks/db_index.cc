#include <cassert>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>
#include <rapidjson/document.h>
#include <sstream>  // for stringstream
#include <stdlib.h>

#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

using namespace std;

int main() {
  leveldb::DB* db;
  leveldb::Options options;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);
  options.primary_key = "id";
  options.secondary_key = "age";
  options.create_if_missing = true;
  leveldb::Status status =
      leveldb::DB::Open(options, "/opt/leveldbplus/test_level_db_idx", &db);
  assert(status.ok());
  leveldb::ReadOptions roptions;
  leveldb::WriteOptions woptions;

  std::cout << "Starting Index test..." << std::endl;

  // Add 10,000 key-value pairs
  for (int i = 0; i < 10000; ++i) {
    std::stringstream ss;
    ss << "{\n \"id\": " << i << ",\n \"age\": " << (i % 50 + 10)
       << ",\n \"name\": \"User" << i << "\"\n}";
    std::string json_string = ss.str();
    leveldb::Status put_status = db->Put(woptions, json_string);
    if (!put_status.ok()) {
      std::cerr << "Error putting key " << i << ": " << put_status.ToString()
                << std::endl;
    }
  }

  /*
        Using Secondary Index
   */

  vector<leveldb::SKeyReturnVal> values;
  leveldb::Status s =
      db->Get(roptions, leveldb::Slice(std::to_string(30)), &values, 10000);

  if (!s.ok()) {
    std::cout << "Error calling new get method " << s.ToString() << std::endl;
  }
  std::cout << "Found " << values.size()
            << " records with age 30 using secondary index" << std::endl;
  std::cout << "------------------------------------------------\n";

  /*
    How this would without secondary index
  */
  rapidjson::Document doc;
  leveldb::Iterator* it = db->NewIterator(roptions);
  int count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    leveldb::Slice key = it->key();
    leveldb::Slice value = it->value();
    std::string json_string = value.ToString();

    // Parse JSON to extract age
    rapidjson::Document doc;
    doc.Parse<0>(json_string.c_str());

    if (doc.HasParseError()) {
      std::cerr << "Error parsing JSON: " << doc.GetParseError() << '\n';
      continue;  // Skip to the next record
    }

    if (doc.HasMember("age") && doc["age"].IsInt()) {
      int age = doc["age"].GetInt();
      if (age == 30) {
        // std::cout << "Key: " << key.ToString() << ", Value: " << json_string
        //           << std::endl;
        count++;
      }
    }
  }
  assert(it->status().ok());  // Check for any errors found during the scan
  std::cout << "Found " << count
            << " records with age 30 without using secondary index"
            << std::endl;

  delete db;
  delete options.filter_policy;
  return 0;
}
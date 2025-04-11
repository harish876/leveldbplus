#include <cassert>
#include <chrono>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>
#include <rapidjson/document.h>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

using namespace std;
using namespace std::chrono;

void printUsage() {
  std::cout
      << "Usage: db_index [OPTIONS]\n"
      << "Options:\n"
      << "  --insert             Run only data insertion phase\n"
      << "  --query              Run only query phase (must have existing "
         "data)\n"
      << "  --run-all            Run both insertion and query phases "
         "(default)\n"
      << "  --use-index          Run only secondary index benchmark\n"
      << "  --no-index           Run only full scan benchmark\n"
      << "  --records N          Number of records to insert (default: 10000)\n"
      << "  --target-age N       Age value to search for (default: 30)\n"
      << "  --db-path PATH       Database path (default: "
         "/opt/leveldbplus/test_level_db_idx)\n"
      << "  --help               Print this help message\n";
}

// Check if directory exists
bool directoryExists(const std::string& path) {
  struct stat info;
  return stat(path.c_str(), &info) == 0 && (info.st_mode & S_IFDIR);
}

// Insert data into the database
void insertData(leveldb::DB* db, int numRecords,
                const leveldb::WriteOptions& woptions) {
  std::cout << "==========================================\n";
  std::cout << "INSERTING DATA\n";
  std::cout << "==========================================\n";
  std::cout << "Inserting " << numRecords << " records...\n";

  auto startInsert = high_resolution_clock::now();

  for (int i = 0; i < numRecords; ++i) {
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

  auto endInsert = high_resolution_clock::now();
  auto insertDuration = duration_cast<milliseconds>(endInsert - startInsert);
  std::cout << "Insertion took " << insertDuration.count() << " ms\n\n";
}

// Run queries with secondary index
void queryWithIndex(leveldb::DB* db, int targetAge, int numRecords,
                    const leveldb::ReadOptions& roptions) {
  std::cout << "==========================================\n";
  std::cout << "USING SECONDARY INDEX\n";
  std::cout << "==========================================\n";

  auto startWithIndex = high_resolution_clock::now();

  vector<leveldb::SKeyReturnVal> values;
  leveldb::Status s = db->Get(
      roptions, leveldb::Slice(std::to_string(targetAge)), &values, numRecords);

  auto endWithIndex = high_resolution_clock::now();
  auto withIndexDuration =
      duration_cast<microseconds>(endWithIndex - startWithIndex);

  if (!s.ok()) {
    std::cout << "Error calling new get method: " << s.ToString() << std::endl;
  }

  std::cout << "Found " << values.size() << " records with age " << targetAge
            << " using secondary index\n";
  std::cout << "Query took " << withIndexDuration.count()
            << " microseconds\n\n";
}

// Run queries without secondary index (full scan)
void queryWithoutIndex(leveldb::DB* db, int targetAge,
                       const leveldb::ReadOptions& roptions) {
  std::cout << "==========================================\n";
  std::cout << "WITHOUT SECONDARY INDEX (FULL SCAN)\n";
  std::cout << "==========================================\n";

  auto startWithoutIndex = high_resolution_clock::now();

  leveldb::Iterator* it = db->NewIterator(roptions);
  int count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
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
      if (age == targetAge) {
        count++;
      }
    }
  }

  auto endWithoutIndex = high_resolution_clock::now();
  auto withoutIndexDuration =
      duration_cast<microseconds>(endWithoutIndex - startWithoutIndex);

  assert(it->status().ok());  // Check for any errors found during the scan
  std::cout << "Found " << count << " records with age " << targetAge
            << " without using secondary index\n";
  std::cout << "Query took " << withoutIndexDuration.count()
            << " microseconds\n\n";

  delete it;
}

// Run performance comparison
void runComparison(leveldb::DB* db, int targetAge, int numRecords,
                   const leveldb::ReadOptions& roptions) {
  std::cout << "==========================================\n";
  std::cout << "PERFORMANCE COMPARISON\n";
  std::cout << "==========================================\n";

  vector<leveldb::SKeyReturnVal> values;
  auto startWithIndex = high_resolution_clock::now();
  db->Get(roptions, leveldb::Slice(std::to_string(targetAge)), &values,
          numRecords);
  auto endWithIndex = high_resolution_clock::now();
  auto withIndexDuration =
      duration_cast<microseconds>(endWithIndex - startWithIndex);

  leveldb::Iterator* it = db->NewIterator(roptions);
  int count = 0;
  auto startWithoutIndex = high_resolution_clock::now();
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    rapidjson::Document doc;
    doc.Parse<0>(it->value().ToString().c_str());
    if (!doc.HasParseError() && doc.HasMember("age") && doc["age"].IsInt()) {
      if (doc["age"].GetInt() == targetAge) {
        count++;
      }
    }
  }
  auto endWithoutIndex = high_resolution_clock::now();
  auto withoutIndexDuration =
      duration_cast<microseconds>(endWithoutIndex - startWithoutIndex);
  delete it;

  std::cout << "With Index: " << withIndexDuration.count() << " microseconds\n";
  std::cout << "Without Index: " << withoutIndexDuration.count()
            << " microseconds\n";
  double speedup =
      (double)withoutIndexDuration.count() / withIndexDuration.count();
  std::cout << "Speedup: " << speedup << "x\n";
}

int main(int argc, char* argv[]) {
  // Default settings
  bool runInsert = true;
  bool runQuery = true;
  bool runWithIndex = true;
  bool runWithoutIndex = true;
  int numRecords = 10000;
  int targetAge = 30;
  string dbPath = "/opt/leveldbplus/test_level_db_idx";

  // Parse command line arguments
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--insert") == 0) {
      runInsert = true;
      runQuery = false;
    } else if (strcmp(argv[i], "--query") == 0) {
      runInsert = false;
      runQuery = true;
    } else if (strcmp(argv[i], "--run-all") == 0) {
      runInsert = true;
      runQuery = true;
    } else if (strcmp(argv[i], "--use-index") == 0) {
      runWithIndex = true;
      runWithoutIndex = false;
    } else if (strcmp(argv[i], "--no-index") == 0) {
      runWithIndex = false;
      runWithoutIndex = true;
    } else if (strcmp(argv[i], "--records") == 0 && i + 1 < argc) {
      numRecords = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--target-age") == 0 && i + 1 < argc) {
      targetAge = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--db-path") == 0 && i + 1 < argc) {
      dbPath = argv[++i];
    } else if (strcmp(argv[i], "--help") == 0) {
      printUsage();
      return 0;
    } else {
      std::cerr << "Unknown option: " << argv[i] << std::endl;
      printUsage();
      return 1;
    }
  }

  // Check if we're just querying but the database doesn't exist
  if (!runInsert && runQuery && !directoryExists(dbPath)) {
    std::cerr << "Error: Cannot run query phase without existing database at "
              << dbPath << std::endl;
    std::cerr << "Run with --insert first or provide valid --db-path"
              << std::endl;
    return 1;
  }

  // Setup database
  leveldb::DB* db;
  leveldb::Options options;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);
  options.primary_key = "id";
  options.secondary_key = "age";
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, dbPath, &db);

  if (!status.ok()) {
    std::cerr << "Error opening database: " << status.ToString() << std::endl;
    return 1;
  }

  leveldb::ReadOptions roptions;
  leveldb::WriteOptions woptions;

  std::cout << "==========================================\n";
  std::cout << "LevelDB Secondary Index Benchmark\n";
  std::cout << "==========================================\n";
  std::cout << "Records: " << numRecords << "\n";
  std::cout << "Target Age: " << targetAge << "\n";
  std::cout << "DB Path: " << dbPath << "\n";
  std::cout << "==========================================\n\n";

  // Insertion phase
  if (runInsert) {
    insertData(db, numRecords, woptions);
  }

  // Query phase
  if (runQuery) {
    if (runWithIndex) {
      queryWithIndex(db, targetAge, numRecords, roptions);
    }

    if (runWithoutIndex) {
      queryWithoutIndex(db, targetAge, roptions);
    }

    if (runWithIndex && runWithoutIndex) {
      runComparison(db, targetAge, numRecords, roptions);
    }
  }

  delete db;
  delete options.filter_policy;
  return 0;
}
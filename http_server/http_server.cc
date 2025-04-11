#include <crow.h>
#include <iostream>
#include <string>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

#include "rapidjson/document.h"

int main(int argc, char* argv[]) {
  // Process command line arguments
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <database_path> [port]" << std::endl;
    return 1;
  }

  std::string db_path = argv[1];
  int port = (argc > 2) ? std::stoi(argv[2]) : 8080;

  // Open the database
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  options.primary_key = "id";
  options.secondary_key = "age";

  leveldb::Status status = leveldb::DB::Open(options, db_path, &db);
  if (!status.ok()) {
    std::cerr << "Unable to open/create database: " << status.ToString()
              << std::endl;
    return 1;
  }

  crow::SimpleApp app;

  CROW_ROUTE(app, "/db/get/<string>")
      .methods("GET"_method)([db](const crow::request& req,
                                  std::string primary_key) {
        // Get value by primary key
        std::string value;
        leveldb::Status s =
            db->Get(leveldb::ReadOptions(), primary_key, &value);

        if (s.ok()) {
          // Return the JSON value
          return crow::response(200, value);
        } else if (s.IsNotFound()) {
          return crow::response(404, "{\"error\": \"Key not found\"}");
        } else {
          return crow::response(500, "{\"error\": \"" + s.ToString() + "\"}");
        }
      });

  // Define route for secondary key operations
  CROW_ROUTE(app, "/db/query")
      .methods("GET"_method)([db](const crow::request& req) {
        // Get query parameters
        auto secondary_key = req.url_params.get("key");
        auto limit_param = req.url_params.get("limit");
        auto use_secondary = req.url_params.get("use_secondary");
        int limit = 10;

        if (limit_param) {
          try {
            limit = std::stoi(limit_param);
          } catch (...) {
            return crow::response(400,
                                  "{\"error\": \"Invalid 'limit' parameter\"}");
          }
        }

        if (!use_secondary) {
          std::unique_ptr<leveldb::Iterator> it(
              db->NewIterator(leveldb::ReadOptions()));
          std::vector<std::string> results;
          int target_key_value = std::stoi(secondary_key);

          for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string value = it->value().ToString();

            std::string sec_key_str;
            rapidjson::Document doc;
            if (doc.Parse<0>(value.c_str()).HasParseError() ||
                !doc.HasMember("age")) {
              continue;
            }
            if (doc.HasMember("age") && doc["age"].IsInt()) {
              int age = doc["age"].GetInt();
              if (age == target_key_value) {
                results.push_back(value);
              }
            }
          }

          if (!it->status().ok()) {
            return crow::response(500, "{\"error\": \"Iterator error: " +
                                           it->status().ToString() + "\"}");
          }

          std::string json_results = "[";
          for (size_t i = 0; i < results.size() && i < limit; i++) {
            if (i > 0) json_results += ",";
            json_results += results[i];
          }
          json_results += "]";

          if (results.empty()) {
            return crow::response(200, "application/json", "[]");
          } else {
            return crow::response(200, "application/json", json_results);
          }
        }

        if (!secondary_key) {
          return crow::response(400,
                                "{\"error\": \"Missing 'key' parameter\"}");
        }

        std::vector<leveldb::SKeyReturnVal> results;
        leveldb::Status s =
            db->Get(leveldb::ReadOptions(), leveldb::Slice(secondary_key),
                    &results, limit);

        if (s.ok()) {
          std::string json_results = "[";
          for (size_t i = 0; i < results.size(); i++) {
            if (i > 0) json_results += ",";
            json_results += results[i].value;
          }
          json_results += "]";
          return crow::response(200, "application/json", json_results);
        } else if (s.IsNotFound()) {
          return crow::response(404, "{\"error\": \"No records found\"}");
        } else {
          return crow::response(500, "{\"error\": \"" + s.ToString() + "\"}");
        }
      });

  CROW_ROUTE(app, "/db/put")
      .methods("POST"_method)([db](const crow::request& req) {
        auto body = req.body;
        if (body.empty()) {
          return crow::response(400, "{\"error\": \"Empty request body\"}");
        }

        leveldb::Status s = db->Put(leveldb::WriteOptions(), body);
        if (s.ok()) {
          return crow::response(200, "{\"status\": \"success\"}");
        } else {
          return crow::response(500, "{\"error\": \"" + s.ToString() + "\"}");
        }
      });

  // Start the server
  std::cout << "Starting LevelDB HTTP server on port " << port << std::endl;
  std::cout << "Database path: " << db_path << std::endl;
  app.port(port).multithreaded().run();

  delete db;
  return 0;
}
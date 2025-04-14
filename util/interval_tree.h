#ifndef TWOD_IT_W_TOPK_H
#define TWOD_IT_W_TOPK_H

#include <algorithm>
#include <inttypes.h>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

class Interval2DTreeNode;
class TopKIterator;

// 1d-interval in interval_dimension-time space
class Interval2DTree {
 public:
  Interval2DTree() {};
  Interval2DTree(const std::string& id, const std::string& low,
                 const std::string& high, const uint64_t& timestamp)
      : _id(id), _low(low), _high(high), _timestamp(timestamp) {};

  std::string GetId() const { return _id; };
  std::string GetLowPoint() const { return _low; };
  std::string GetHighPoint() const { return _high; };
  uint64_t GetTimeStamp() const { return _timestamp; };

  bool operator==(const Interval2DTree& otherInterval) const {
    return (_id == otherInterval._id);
  }
  bool operator>(const Interval2DTree& otherInterval) const {
    return (_timestamp > otherInterval._timestamp);
  }

  // overlap operator
  bool operator*(const Interval2DTree& otherInterval) const {
    // point intersections are considered intersections
    if (_low < otherInterval._low) return (_high >= otherInterval._low);
    return (otherInterval._high >= _low);
  }

 protected:
  std::string _id;
  std::string _low;
  std::string _high;
  uint64_t _timestamp;
};

// Interval tree node
class Interval2DTreeNode {
 public:
  Interval2DTreeNode() : is_red(false) {};

  Interval2DTree interval;
  bool is_red;
  std::string max_high;
  uint64_t max_timestamp;
  Interval2DTreeNode *left, *right, *parent;
};

// Storage and index for intervals
class Interval2DTreeWithTopK {
 public:
  Interval2DTreeWithTopK();
  Interval2DTreeWithTopK(const std::string& filename,
                         const bool& sync_from_file);
  ~Interval2DTreeWithTopK();

  void insertInterval(const std::string& id, const std::string& minKey,
                      const std::string& maxKey, const uint64_t& maxTimestamp);

  void deleteInterval(const std::string& id);
  void deleteAllIntervals(const std::string& id_prefix);

  void getInterval(Interval2DTree& ret_interval, const std::string& id) const;
  void topK(std::vector<Interval2DTree>& ret_value, const std::string& minKey,
            const std::string& maxKey);

  void sync() const;

  void setSyncFile(const std::string& filename);
  void getSyncFile(std::string& filename) const;
  void setSyncThreshold(const uint32_t& threshold);
  void getSyncThreshold(uint32_t& threshold) const;

  void setIdDelimiter(const char& delim);
  void getIdDelimiter(char& delim) const;

  void storagePrint() const;
  void treePrintLevelOrder() const;
  void treePrintInOrder() const;
  int treeHeight() const;

 private:
  void setDefaults();

  void treePrintInOrderRecursive(Interval2DTreeNode* x, const int& depth) const;
  int treeHeightRecursive(Interval2DTreeNode* x) const;
  bool treeIntervalSearch(const Interval2DTree& test_interval,
                          std::unordered_set<Interval2DTreeNode*>& found,
                          Interval2DTreeNode*& x) const;
  void treeIntervalSearch(const Interval2DTree& test_interval,
                          std::vector<Interval2DTree>& ret_value,
                          Interval2DTreeNode* x) const;

  void treeInsert(Interval2DTreeNode* z);
  void treeInsertFixup(Interval2DTreeNode* z);
  void treeDelete(Interval2DTreeNode* z);
  void treeDeleteFixup(Interval2DTreeNode* x);
  Interval2DTreeNode* treeMinimum(Interval2DTreeNode* x) const;
  Interval2DTreeNode* treeSuccessor(Interval2DTreeNode* x) const;
  void treeLeftRotate(Interval2DTreeNode* x);
  void treeRightRotate(Interval2DTreeNode* x);
  void treeTransplant(Interval2DTreeNode* u, Interval2DTreeNode* v);
  void treeMaxFieldsFixup(Interval2DTreeNode* x);
  void treeSetMaxFields(Interval2DTreeNode* x);
  void treeDestroy(Interval2DTreeNode* x);

  Interval2DTreeNode *root, nil;
  std::unordered_map<std::string, Interval2DTreeNode*> storage;

  std::unordered_map<std::string, std::unordered_set<std::string>> ids;
  char id_delim;

  std::string sync_file;
  uint32_t sync_threshold;
  mutable uint32_t sync_counter;

  bool iterator_in_use;
  TopKIterator* iterator;

  friend class TopKIterator;
};

class TopKIterator {
 public:
  TopKIterator(Interval2DTreeWithTopK& it, Interval2DTree& ret_int,
               const std::string& min, const std::string& max);
  ~TopKIterator();

  bool next();
  void restart(const std::string& min, const std::string& max);
  void stop(const bool& release = true);

 private:
  bool start(const std::string& min, const std::string& max);

  Interval2DTreeWithTopK* _it;
  Interval2DTree *_ret_int, search_int;

  bool iterator_in_use;
  std::vector<std::pair<Interval2DTreeNode*, uint64_t>> nodes;
  std::unordered_set<Interval2DTreeNode*> explored;
};

#endif
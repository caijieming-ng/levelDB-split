#ifndef STORAGE_LEVELDB_DB_BigTable_H_
#define STORAGE_LEVELDB_DB_BigTable_H_

#include "db/db_impl.h"

#include <pthread.h>
#include <assert.h>

#include <string>

namespace leveldb {

namespace port {

class RWLock {
 public:
  RWLock() {
    assert(pthread_rwlock_init(&rwlock, NULL) == 0);
  }
  
  ~RWLock() {
    assert(pthread_rwlock_destroy(&rwlock) == 0);
  }
  
  void ReadLock() {
    assert(pthread_rwlock_rdlock(&rwlock) == 0);
  }

  void WriteLock() {
    assert(pthread_rwlock_wrlock(&rwlock) == 0);
  }

  void Unlock() {
    assert(pthread_rwlock_unlock(&rwlock) == 0);
    //PthreadCall("unlock", pthread_rwlock_unlock(&rwlock));
  }

 private:
  pthread_rwlock_t rwlock;
}; // end rwlock class

} // end port namespace


//InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
//InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
//LookupKey kMaxKey("maxkey", kMaxSequenceNumber);
//std::string kMaxKeyStr("CJM-max-key");
//InternalKey kMaxKey(kMaxKeyStr, kMaxSequenceNumber, 0, static_cast<ValueType>(0));
#define kMaxKey ("CJM-max-key")

// abstraction of tablet's range
struct Tablet {
 public:

  Tablet(uint64_t tid) : db(NULL), tabletId(tid) {}
  
  // return like memcmp 
  static int SortByLargestKey(const Slice& a, const Slice& b) {
    if (memcmp(a.data(), kMaxKey, strlen(kMaxKey)) == 0) {
      return 1;
    }
    if (memcmp(b.data(), kMaxKey, strlen(kMaxKey)) == 0) {
      return -1;
    }
    return a.compare(b);
  }
  
  // if l < r, true
  static bool TabletCompare(const Tablet* left, const Tablet* right)
  {
    Slice a(left->maxkey);
    Slice b(right->maxkey);
    int res = SortByLargestKey(a, b);
    return res < 0;
  }

 private:
  friend class BigTable;
  std::string maxkey; // max key in this tablet, value in metaTablet
  uint64_t tabletId; // tablets' id, dataTablet-xxx is key in metaTablet
  port::RWLock rwlock; // split use write lock, put/get/delete use read lock
  DB* db; // leveldb handler
};

class BigTable {
 public:
  BigTable(const Options& options, const std::string& dir);
  
  ~BigTable();

  static Status Open(const Options& options, const std::string& dbname,
                BigTable** dbptr);
  
  Status Put(const WriteOptions& opt, const Slice& key, 
                const Slice& value);

  Status Delete(const WriteOptions& opt, const Slice& key);
  
  Status Get(const ReadOptions& options, const Slice& key, 
                std::string *value);
  
  // choose a tablet, which apply split
  Status SplitTablet(uint64_t tablet_num);

 
  // if a < b , return true
  //std::sort(tabletQueue.begin(), tabletQueue.end(), SortByLargeKey);

 private:
  // tablet name = /tmp/BigTable-0/dataTablet-xxx
  // std::string MakeTabletName(const std::string& BigTableName, uint64_t id);
  // meta tablet name = /tmp/BigTable-0/metaTablet-0 
  std::string GetMetaTabletName(const std::string& BigTableName);
  
  Tablet* OpenTablet(uint64_t tid, Slice lkey);

  // return first tablet, whose maxkey >= key
  Tablet* LookupTablet(const Slice& key);
  
  // alloc a new tablet num
  uint64_t GetNewTabletNum() { return tablet_num_++;}
  
 private: 
  const std::string BigTable_name; // big table name, default = /tmp/BigTable-0/
  port::Mutex lock; // protect tabletQueue;
  uint64_t tablet_num_; 
  std::vector<Tablet*> tabletQueue;
  DB *meta_tablet;
  
  std::string kMaxKeyStr;
};
 
}
#endif


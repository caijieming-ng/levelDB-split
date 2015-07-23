
#include "db/bigtable.h"

#include "leveldb/status.h"
#include <errno.h>
#include <sys/stat.h>
#include <stdio.h>
#include <dirent.h>

#include <vector>
#include <string>
#include <algorithm>

namespace leveldb {
#define BUG_ON(cond)                                          \
    do {                                                      \
      if ((cond) == true) {                                   \
        BUG();                                                \
      }                                                       \
    } while (0)                                         

#define BUG()                                                 \
    do {                                                      \
      printf("%s: LINE %d, BUG...\n", __func__, __LINE__);    \
      assert(0);                                              \
    } while(0) 

Status createBigTableDir(const std::string& dbname)
{
  Status s;
  errno = 0;
  if (mkdir(dbname.c_str(), 0755) && (errno != EEXIST)) {
    BUG();
  }
  return s;
}

std::string BigTable::GetMetaTabletName(const std::string& dbname)
{
  std::string res = dbname + "/metaTablet-0";
  return res;
}

Status GetChildren(const std::string& dir,
                   std::vector<std::string>* result) 
{
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == NULL) {
      BUG();
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL) {
      if (memcmp(entry->d_name, ".", 1)) { // donot use "." and ".."
        result->push_back(entry->d_name);
      }
    }
    closedir(d);
    return Status::OK();
}

BigTable::BigTable(const Options& options, const std::string& dir)
      : BigTable_name(dir),
        tablet_num_(1),
        meta_tablet(NULL)
{
  std::string metaTabletname = GetMetaTabletName(dir);
  Options mopt;
  mopt.create_if_missing = true;
  Status s = DB::Open(mopt, metaTabletname, &meta_tablet);
  BUG_ON(!s.ok());
}

BigTable::~BigTable()
{
  this->lock.Lock();
  while (!tabletQueue.empty()) {
    Tablet* t = tabletQueue[0];
    tabletQueue.erase(tabletQueue.begin());
    delete t->db;
    delete t;
  }
  this->lock.Unlock();
  delete meta_tablet;
}

uint64_t GetTabletID(const std::string& DataTabletName)
{
  Slice slice(DataTabletName);
  slice.remove_prefix(strlen("dataTablet-"));

  uint64_t num;
  if (!ConsumeDecimalNumber(&slice, &num)) {
    BUG();
  }
  return num;
}

std::string GetTabletName(uint64_t tid)
{
  char buf[100];
  snprintf(buf, sizeof(buf), "dataTablet-%06llu",
           static_cast<unsigned long long>(tid));
  std::string name(buf);
  return name;
}

Tablet* BigTable::OpenTablet(uint64_t tid, Slice lkey)
{
  Tablet *t = new Tablet(tid);
  BUG_ON(t == NULL);
  std::string tbname = this->BigTable_name + "/" + GetTabletName(tid);
  Status s;

  Options options;
  options.create_if_missing = true;
  s = DB::Open(options, tbname, &t->db);
  BUG_ON(!s.ok());
  
  t->maxkey = lkey.ToString(); 
  
  this->lock.Lock();
  this->tabletQueue.push_back(t);
  std::sort(this->tabletQueue.begin(), this->tabletQueue.end(), 
            Tablet::TabletCompare); 
  this->lock.Unlock();
  
  return t;
}

Status BigTable::Open(const Options& options, const std::string& dbname,
                BigTable** dbptr)
{
  Status s;
  BigTable *bt = NULL;
  s = createBigTableDir(dbname);
  if (!s.ok()) {
    BUG();
  }
    
  bt = new BigTable(options, dbname);
  BUG_ON(bt == NULL); 
  bt->kMaxKeyStr = std::string(kMaxKey);
  
  bool FirstCreateDB = true;
  std::vector<std::string> filenames;
  GetChildren(dbname, &filenames); 
  for (int i = 0; i < filenames.size(); i++) {
    Slice slice(filenames[i]);
    if (slice.starts_with("dataTablet-")) {
      ReadOptions Ropt;
      std::string dataTablet = slice.ToString();
      std::string value;
      uint64_t tid = GetTabletID(dataTablet);
      bt->tablet_num_ = ((bt->tablet_num_ > tid) ? bt->tablet_num_ : (tid + 1));
      
      if (bt->meta_tablet->Get(Ropt, dataTablet, &value).ok()) {
        Tablet *t = bt->OpenTablet(tid, value);
        BUG_ON(t == NULL);
        //bt->tabletQueue.push_back(t);
        //std::sort(bt->tabletQueue.begin(), bt->tabletQueue.end(), SortByLargeKey);
      } else {
        // delete dataTablet-xxx
        BUG();
      }
      FirstCreateDB = false;
    }
  }
  if (FirstCreateDB) {
    uint64_t tid = bt->GetNewTabletNum();
    Tablet *t = bt->OpenTablet(tid, bt->kMaxKeyStr);
    std::string tname = GetTabletName(tid);
    WriteOptions wopt;
    s = bt->meta_tablet->Put(wopt, tname, bt->kMaxKeyStr);
    BUG_ON(!s.ok());
  }

  *dbptr = NULL;
  if (s.ok()) {
    *dbptr = bt;
  }

  return s;
}

Tablet* BigTable::LookupTablet(const Slice& key)
{
  Tablet* t = NULL;
  this->lock.Lock();
  for (int i = 0; i < this->tabletQueue.size(); i++) {
    t = this->tabletQueue[i];
    // if key <= t->maxkey, then return tablet
    if (t->SortByLargestKey(key, Slice(t->maxkey)) <= 0) {
      //BUG_ON(i == 0);
      this->lock.Unlock();
      return t; 
    }
  }
  this->lock.Unlock();
  BUG();
}

Status BigTable::Put(const WriteOptions& opt, const Slice& key, 
                     const Slice& value)
{
  Tablet* t = LookupTablet(key);
  while (true) {
    Tablet* t2 = NULL;
    t->rwlock.ReadLock();
    t2 = LookupTablet(key);
    if (t == t2)
      break;
    t->rwlock.Unlock();
  }
  
  Status s = t->db->Put(opt, key, value);
  t->rwlock.Unlock();
  return s;
}

Status BigTable::Delete(const WriteOptions& opt, const Slice& key)
{
  Tablet* t = LookupTablet(key);
  while (true) {
    Tablet* t2 = NULL;
    t->rwlock.ReadLock();
    t2 = LookupTablet(key);
    if (t == t2)
      break;
    t->rwlock.Unlock();
  }
  
  Status s = t->db->Delete(opt, key);
  t->rwlock.Unlock();
  return s;
}

Status BigTable::Get(const ReadOptions& opt, const Slice& key, 
           std::string *value)
{
  Tablet* t = LookupTablet(key);
  while (true) {
    Tablet* t2 = NULL;
    t->rwlock.ReadLock();
    t2 = LookupTablet(key);
    if (t == t2)
      break;
    t->rwlock.Unlock();
  }
  
  Status s = t->db->Get(opt, key, value);
  t->rwlock.Unlock();
  return s;
}

Status BigTable::SplitTablet(uint64_t tablet_num)
{
  Tablet *t = NULL;
  Status s;
  this->lock.Lock();
  for (int i = 0; i < this->tabletQueue.size(); i++) {
    t = this->tabletQueue[i];
    if (t->tabletId == tablet_num) {
      break;
    }
  }
  this->lock.Unlock();
  if (t == NULL) {
    return s;
  }

  DBImpl* db_impl = reinterpret_cast<DBImpl*>(t->db);
  // trigger memtable compact
  db_impl->TEST_CompactMemTable();
  t->rwlock.WriteLock();
  // trigger memtable compact
  db_impl->TEST_CompactMemTable();
  
  uint64_t newId = GetNewTabletNum();
  std::string newKey;
  std::string newTB = GetTabletName(newId);
  std::string newName = this->BigTable_name + "/"+ newTB;
  s = db_impl->SplitTablet(newName, &newKey);
  BUG_ON(!s.ok());
  
  WriteOptions wopt;
  s = this->meta_tablet->Put(wopt, newTB, newKey);
  BUG_ON(!s.ok()); 
   
  Tablet *newTablet = OpenTablet(newId, newKey);
  BUG_ON(newTablet == NULL); 
  t->rwlock.Unlock();
  return s;
}

}



// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {
}

namespace {

// LRU cache implementation
// 该文件希望能实现一个通用的lrucache

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
// lru cache中的1项，使用双向链表链接
struct LRUHandle {
  void* value; // value对象
  void (*deleter)(const Slice&, void* value);// value对象析构器
  LRUHandle* next_hash; // 用于hash管理
  LRUHandle* next; // 用于lru管理
  LRUHandle* prev; // 用于lru管理
  size_t charge;      // 表述该项的权重，一般为1。TODO(opt): Only allow uint32_t?
  size_t key_length; // 记录的key的长度
  uint32_t refs; // cache的引用计数
  uint32_t hash;      // 元素的hash值，Hash of key(); used for fast sharding and comparisons
  char key_data[1];   // 元数的key，Beginning of key, key的空间可变，使用key_length方能知道entry项的大小
  
  // 返回通过cache元素的key
  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
// lruhandle元素的hash表的管理，导出插入，删除，重平衡的接口
// hash 表
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; }
  
  // 能找到，返回lruhandle，否则返回NULL
  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }
  
  // 若重复插入，则插入新的，返回旧的lruhandle
  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);// ptr是key待插位置的前一个元素LRUHandle->next_next的指针地址
    LRUHandle* old = *ptr;
    // 设置新插入元素的下一个hash指针值
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;
    if (old == NULL) {
      // 若old为空，则表示key在插入前不在hash中
      ++elems_; //hash表的元数个数增加1
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        // 一旦hash表出现不平衡，则执行重设置空间大小
        Resize();
      }
    }
    return old;
  }
  
  // 若找到，则删除，并返回旧的lruhandle
  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;// 设置前一个lruhandle->next_hash指向待删除元素的下一个lruhandle
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_; // hash表头长度大小
  uint32_t elems_; // hash表内的元素个数
  LRUHandle** list_; // hash表头指针数组,之所以使用单向链表，为了在同一大小的hash表中，能索引更多的元素

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  // 比较核心的内部函数
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    //根据hash值，定位链表头的位置，list_[x], 遍历链表，比较hash和key是否都一直；
    //若是，则返回指向lruhandle指针的指针地址（即前一个lruhandle->next_hash字段地址），
    //否则返回最后一个元素的next_hash指针的地址
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  // 将hash表重新一致
  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    // 二重循环，遍历hash表中每个元数, 进行hash重计算
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != NULL) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;

        // 遍历下一个元素
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
// lru cache的管理结构
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  // 设置lru cache的空间大小
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  void Unref(LRUHandle* e);

  // Initialized before use.
  size_t capacity_; // 总的lru cache 大小

  // mutex_ protects the following state.
  port::Mutex mutex_; // 对象包含锁
  size_t usage_; // lru cache的空间使用量

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  LRUHandle lru_; // lru链表头

  HandleTable table_; // hash表，用于查询
};

LRUCache::LRUCache()
    : usage_(0) {
      // 初始化双向链表
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

LRUCache::~LRUCache() {
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->refs == 1);  // Error if caller has an unreleased handle
    Unref(e); // 每个元素进行解引用
    e = next;
  }
}

// 减引用计数，若为0，则将lruhandle和其中的对象释放空间
void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs <= 0) {
    usage_ -= e->charge;
    // 将lru cache中的对象e->value删除
    (*e->deleter)(e->key(), e->value);
    free(e);
  }
}

// 将lruhandle从lru cache中删除
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

// 将lruhandle放入lru cache尾部
void LRUCache::LRU_Append(LRUHandle* e) {
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

// 首先通过hash table 查找key，hash对应的元素是否存在；
// 若找到，lruhandle引用计数+1，更新lru 链
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != NULL) {
    e->refs++;
    LRU_Remove(e);
    LRU_Append(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

// 该lruhandle的释放
void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}
 
Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);
  
  // 分配lruhandle的空间
  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  // lruhandle的初始引用引用计数是2的原因是：对象创建时是1，并且插入后返回给用户操作，因此需要+1
  e->refs = 2;  // One from LRUCache, one for the returned handle
  memcpy(e->key_data, key.data(), key.size());
  LRU_Append(e); // 插入到lru链上
  usage_ += charge;
  
  // 产入到hash表内
  LRUHandle* old = table_.Insert(e);
  if (old != NULL) {
    LRU_Remove(old);
    Unref(old);
  }
  
  // 一旦lrucache的使用量过大，则开始替换
  // 从lru上选择最老的lruhandle，提出lru链和hash链
  while (usage_ > capacity_ && lru_.next != &lru_) {
    // 之所以不调用LRUCache::Erase接口的原因是该接口获得mutex_锁，而insert函数
    // 也是获得该锁，所有没有调用删除接口
    LRUHandle* old = lru_.next;
    LRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    Unref(old);
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// 删除lruhandle：从hash表删除，从lru list上删除
void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Remove(key, hash);
  if (e != NULL) {
    LRU_Remove(e);
    Unref(e); // 此时通过减少引用计数，保证最后一个引用者释放存储空间
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

// lrucache的切片管理
class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];// 默认有16个lrucache
  port::Mutex id_mutex_;
  uint64_t last_id_;
  
  // 根据s，计算hash值
  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }
  
  // 将hash值进行取摸运算
  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  // 将容量等分到每个lrucache的分片处
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache() { }
  
  //首先计算key对应的hash值，再将hash值对应到唯一的切片处 
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key); // 计算key的切片hash值
    // hash取摸插入
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  
  // key算hash，取模查找
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
};

}  // end anonymous namespace

// 对外导出的待切片的lrucache
Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

}  // namespace leveldb

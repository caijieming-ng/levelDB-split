// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

// 根据输入流，构建id为meta->number的sst表,及table cache
// 构建meta的最大key和最小key
Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();// 从遍历器获得最小的key
  
  // 产生tablefile文件名 dnname+number+ldb
  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    // 已可写方式打开该文件
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }
  
    // sst表构造类，将memtable的kv流入sst表：block builder，index builder
    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key()); // 获得最小值
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key); // 获得最大值
      builder->Add(key, iter->value());//将kv填入数据块，并构造index块，下刷数据块
    }

    // Finish and check for builder errors
    if (s.ok()) {
      s = builder->Finish();// kv的块构建完成
      // 查看是否完成
      if (s.ok()) {
        meta->file_size = builder->FileSize(); // 更新sst文件大小
        assert(meta->file_size > 0);
      }
    } else {
      builder->Abandon(); // 若写入过程中出错，则丢弃
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();// 成功，则文件下刷
    }
    if (s.ok()) {
      s = file->Close(); // 关闭文件
    }
    delete file;
    file = NULL;

    if (s.ok()) {
      // Verify that the table is usable
      // 对于sst构建table cache
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    // 构建文件失败，删除文件
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb

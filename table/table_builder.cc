// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Options options;
  Options index_block_options;
  WritableFile* file; // sst文件
  uint64_t offset; // sst文件的大小, 不依赖与文件系统的stat接口获得文件大小
  Status status;
  BlockBuilder data_block; // key的前缀压缩数据块
  BlockBuilder index_block; // 索引数据块的索引块
  std::string last_key; // 数据块的最后一个key
  int64_t num_entries; // kv数量
  bool closed;          // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block; // 构造bloom filter的数据块

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry; // 记录是否有未完成的index block，因为data block每次下刷都会设置该字段
  BlockHandle pending_handle;  // Handle to add to index block, 编码数据块在sst文件内的偏移和大小

  std::string compressed_output;

  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == NULL ? NULL
                     : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != NULL) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

// 将kv流入sst，添加数据块
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) { // 非有序，产生崩溃
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }
  
  // 若数据块被下刷过，那么需要更新index block
  if (r->pending_index_entry) { // 先写完数据块，才将索引块构建
    assert(r->data_block.empty());
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding); // 数据块在文件的偏移和大小进行编码
    r->index_block.Add(r->last_key, Slice(handle_encoding));// 索引块的key=数据块的最后key，value=数据块的偏移和大小
    r->pending_index_entry = false;
  }
  
  // 若存在bloom filter过滤器，则将key添加进入过滤器
  if (r->filter_block != NULL) {
    r->filter_block->AddKey(key);
  }
  
  // 记录上一次的k
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;// kv数量
  r->data_block.Add(key, value); // 添加到key前缀压缩的数据块中

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) { // 默认4K大小
    // 若数据块的大小到达上限，执行数据块flush操作
    Flush(); // 将数据块进行下刷
  }
}

// 将数据块下刷进入sst
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    // 执行数据下刷
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != NULL) {
    r->filter_block->StartBlock(r->offset);
  }
}

// 数据块（可能带压缩）+tailer(压缩类型+crc)
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish(); // 将data block的格式构找完成,关闭本次数据块写

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset(); // 清空数据块
}

// 根据压缩类型，将压缩过的数据块下刷
// 数据内容+压缩类型+crc
// 通过handle返回本次写的block的文件内偏移和大小
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset); // 数据块在文件的offset
  handle->set_size(block_contents.size()); // 数据块在文件的经过压缩的大小
  r->status = r->file->Append(block_contents); //执行pwrite
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];//block的tailer数据区
    trailer[0] = type; // 压缩类型
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize)); //将tailer写到文件
    if (r->status.ok()) {
      // 更新文件大小
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const {
  return rep_->status;
}

// 执行最后的sst文件的构造，下刷索引块
// sst表的格式（必须包含的部分）：
//    数据块（多个） + 
//    过滤块（1个）+ 
//    过滤索引块（1个） + 
//    数据索引块（1个） + 
//    footer: 过滤索引块（偏移，大小）+数据索引块（偏移，大小）
Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush(); // 将剩余数据块进行下刷
  assert(!r->closed);
  r->closed = true; // 关闭sst表构造类

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != NULL) {
    // 块内带crc
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != NULL) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);// 元数据索引块，一个
  }

  // Write index block, 将索引块下刷
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding); //将最后一个数据块的偏移+size记录到块索引中
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle); // 下刷数据索引块, 一个sst只有一个数据块索引 
  }

  // Write footer, 设置footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);// 设置元数据索引的文件内[偏移+大小]
    footer.set_index_handle(index_block_handle); // 设置数据块索引的文件内[偏移+大小]
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);// 只要能从文件尾部解析出footer，就能获得数据块索引，进而获得kv数据块；
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

// 设置closed字段，关闭表构造类
void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

// 准确的kv数量
uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

// sst文件的准确大小
uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace leveldb

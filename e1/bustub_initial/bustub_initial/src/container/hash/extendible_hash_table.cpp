//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  // 初始化全局深度为0
  global_depth_ = 0;
  // 初始化桶数量为1
  num_buckets_ = 1;
  // 创建第一个桶
  auto first_bucket = std::make_shared<Bucket>(bucket_size_, 0);
  // 初始化目录，大小为2^0 = 1
  dir_.push_back(first_bucket);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_index = IndexOf(key);
  return dir_[dir_index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_index = IndexOf(key);
  return dir_[dir_index]->Remove(key);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  // 增加桶的局部深度
  bucket->IncrementDepth();
  
  // 创建新桶
  auto new_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  
  // 获取当前桶的局部深度
  int local_depth = bucket->GetDepth();
  
  // 计算分裂掩码
  int split_mask = 1 << (local_depth - 1);
  
  // 收集需要重新分配的键值对
  std::vector<std::pair<K, V>> items_to_redistribute;
  for (auto &item : bucket->GetItems()) {
    items_to_redistribute.push_back(item);
  }
  
  // 清空原桶
  bucket->GetItems().clear();
  
  // 重新分配键值对
  for (auto &item : items_to_redistribute) {
    size_t hash_value = std::hash<K>()(item.first);
    if (hash_value & split_mask) {
      // 应该分配到新桶
      new_bucket->GetItems().push_back(item);
    } else {
      // 应该保留在原桶
      bucket->GetItems().push_back(item);
    }
  }
  
  // 更新目录指针
  for (size_t i = 0; i < dir_.size(); i++) {
    if (dir_[i] == bucket) {
      size_t hash_value = i;
      if (hash_value & split_mask) {
        dir_[i] = new_bucket;
      }
    }
  }
  
  // 增加桶数量
  num_buckets_++;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  
  while (true) {
    size_t dir_index = IndexOf(key);
    auto bucket = dir_[dir_index];
    
    // 尝试插入到桶中
    if (bucket->Insert(key, value)) {
      return; // 插入成功
    }
    
    // 桶已满且键不存在，需要分裂
    // 1. 如果桶的局部深度等于全局深度，增加全局深度并扩展目录
    if (bucket->GetDepth() == global_depth_) {
      global_depth_++;
      // 扩展目录大小
      size_t old_size = dir_.size();
      dir_.resize(old_size * 2);
      // 复制目录指针
      for (size_t i = 0; i < old_size; i++) {
        dir_[i + old_size] = dir_[i];
      }
    }
    
    // 2. 分裂桶
    RedistributeBucket(bucket);
    
    // 3. 重新尝试插入
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto &item : list_) {
    if (item.first == key) {
      value = item.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // 首先检查键是否已存在，如果存在则更新值
  for (auto &item : list_) {
    if (item.first == key) {
      item.second = value;
      return true;
    }
  }
  
  // 如果桶已满，返回false
  if (IsFull()) {
    return false;
  }
  
  // 插入新的键值对
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

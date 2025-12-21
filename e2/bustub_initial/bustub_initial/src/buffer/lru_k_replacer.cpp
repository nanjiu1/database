//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  current_timestamp_ = 0;
  curr_size_ = 0;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  
  // 如果没有可淘汰的帧，返回false
  if (curr_size_ == 0) {
    return false;
  }
  
  frame_id_t candidate_frame = -1;
  size_t max_k_distance = 0;
  bool has_inf = false;
  size_t earliest_timestamp = std::numeric_limits<size_t>::max();
  
  // 遍历所有帧，找到具有最大后退k-距离的可淘汰帧
  for (const auto &entry : history_) {
    frame_id_t fid = entry.first;
    
    // 只考虑可淘汰的帧
    if (evictable_.find(fid) == evictable_.end() || !evictable_[fid]) {
      continue;
    }
    
    const auto &history = entry.second;
    size_t k_distance;
    bool is_inf = false;
    
    // 计算后退k-距离
    if (history.size() < k_) {
      // 访问历史少于k次，后退k-距离为+inf
      is_inf = true;
      k_distance = std::numeric_limits<size_t>::max();
    } else {
      // 后退k-距离 = 当前时间戳 - 第k次访问的时间戳
      // 由于我们只保留最近的k个访问，最后一个就是第k次访问
      size_t kth_timestamp = history.front();  // 最旧的访问时间戳
      k_distance = current_timestamp_ - kth_timestamp;
    }
    
    // 选择具有最大后退k-距离的帧
    if (is_inf) {
      // 对于+inf的帧，使用最早访问时间戳作为选择标准
      if (!has_inf) {
        // 第一次遇到+inf帧，直接选择
        has_inf = true;
        candidate_frame = fid;
        earliest_timestamp = history.front();
      } else {
        // 已经有+inf帧，选择最早访问时间戳的
        if (history.front() < earliest_timestamp) {
          candidate_frame = fid;
          earliest_timestamp = history.front();
        }
      }
    } else {
      // 普通帧，比较后退k-距离
      if (!has_inf) {
        if (candidate_frame == -1 || k_distance > max_k_distance) {
          max_k_distance = k_distance;
          candidate_frame = fid;
        } else if (k_distance == max_k_distance) {
          // 如果后退k-距离相同，选择最早访问时间戳的（LRU）
          if (history.front() < history_.at(candidate_frame).front()) {
            candidate_frame = fid;
          }
        }
      }
      // 如果已经有+inf帧，不处理普通帧
    }
  }
  
  // 如果没有找到候选帧，返回false
  if (candidate_frame == -1) {
    return false;
  }
  
  // 清除选中帧的访问历史
  history_.erase(candidate_frame);
  
  // 清除可淘汰标志
  evictable_.erase(candidate_frame);
  
  // 减少大小
  curr_size_--;
  
  // 返回被淘汰的帧ID
  *frame_id = candidate_frame;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  
  // 检查frame_id是否有效
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id");
  
  // 记录当前时间戳的访问
  history_[frame_id].push_back(current_timestamp_);
  
  // 如果访问历史超过k个，只保留最近的k个
  if (history_[frame_id].size() > k_) {
    history_[frame_id].pop_front();
  }
  
  // 增加时间戳
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  
  // 检查frame_id是否有效
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id");
  
  // 如果帧不在历史记录中，直接返回（可能是还没有访问过的帧）
  if (history_.find(frame_id) == history_.end()) {
    return;
  }
  
  // 获取当前的可淘汰状态
  bool was_evictable = evictable_.find(frame_id) != evictable_.end() && evictable_[frame_id];
  
  // 如果状态没有改变，直接返回
  if (was_evictable == set_evictable) {
    return;
  }
  
  // 更新可淘汰状态
  evictable_[frame_id] = set_evictable;
  
  // 更新大小
  if (set_evictable) {
    // 从不可淘汰变为可淘汰，增加大小
    curr_size_++;
  } else {
    // 从可淘汰变为不可淘汰，减少大小
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  
  // 检查frame_id是否有效
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id");
  
  // 如果帧不存在，直接返回
  if (history_.find(frame_id) == history_.end()) {
    return;
  }
  
  // 如果帧不是可淘汰的，抛出异常
  if (evictable_.find(frame_id) == evictable_.end() || !evictable_[frame_id]) {
    BUSTUB_ASSERT(false, "Cannot remove non-evictable frame");
  }
  
  // 清除访问历史
  history_.erase(frame_id);
  
  // 清除可淘汰标志
  evictable_.erase(frame_id);
  
  // 减少大小
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub

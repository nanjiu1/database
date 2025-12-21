//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> guard(latch_);

  frame_id_t frame_id;
  // 1) 先尝试从空闲列表获取帧
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // 2) 空闲列表为空，从替换器选择可淘汰帧
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    // 3) 如该帧上有旧页，必要时写回并从页表移除
    Page &victim = pages_[frame_id];
    if (victim.page_id_ != INVALID_PAGE_ID) {
      if (victim.is_dirty_) {
        disk_manager_->WritePage(victim.page_id_, victim.data_);
        victim.is_dirty_ = false;
      }
      page_table_->Remove(victim.page_id_);
    }
  }

  // 4) 分配新页号
  *page_id = AllocatePage();

  // 5) 初始化帧中的Page
  Page &page = pages_[frame_id];
  page.ResetMemory();
  page.page_id_ = *page_id;
  page.pin_count_ = 1;
  page.is_dirty_ = false;

  // 6) 更新页表
  page_table_->Insert(*page_id, frame_id);

  // 7) 在替换器中记录访问，并禁止淘汰
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> guard(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  // 1) 在页表中查找
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    Page &page = pages_[frame_id];
    page.pin_count_ += 1;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &page;
  }

  // 2) 选择一个帧
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    // 回收旧页
    Page &victim = pages_[frame_id];
    if (victim.page_id_ != INVALID_PAGE_ID) {
      if (victim.is_dirty_) {
        disk_manager_->WritePage(victim.page_id_, victim.data_);
        victim.is_dirty_ = false;
      }
      page_table_->Remove(victim.page_id_);
    }
  }

  // 3) 将目标页从磁盘读入该帧
  Page &page = pages_[frame_id];
  page.ResetMemory();
  disk_manager_->ReadPage(page_id, page.data_);
  page.page_id_ = page_id;
  page.pin_count_ = 1;
  page.is_dirty_ = false;

  // 4) 更新页表与替换器
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> guard(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page &page = pages_[frame_id];
  if (page.pin_count_ <= 0) {
    return false;
  }

  page.pin_count_ -= 1;
  if (is_dirty) {
    page.is_dirty_ = true;
  }

  if (page.pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> guard(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page &page = pages_[frame_id];
  disk_manager_->WritePage(page_id, page.data_);
  page.is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> guard(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    Page &page = pages_[i];
    if (page.page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page.page_id_, page.data_);
      page.is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> guard(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    // 不在缓冲池，视为删除成功
    DeallocatePage(page_id);
    return true;
  }

  Page &page = pages_[frame_id];
  if (page.pin_count_ > 0) {
    return false;
  }

  // 从替换器移除并回收帧
  replacer_->SetEvictable(frame_id, true);
  replacer_->Remove(frame_id);

  page_table_->Remove(page_id);

  // 重置页并放回空闲列表
  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;
  free_list_.push_back(frame_id);

  // 模拟磁盘释放
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub

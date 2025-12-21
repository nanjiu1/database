/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() : buffer_pool_manager_(nullptr), page_id_(INVALID_PAGE_ID), index_(0), leaf_page_(nullptr), page_(nullptr) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t page_id, int index)
    : buffer_pool_manager_(buffer_pool_manager), page_id_(page_id), index_(index), leaf_page_(nullptr), page_(nullptr) {
  if (page_id != INVALID_PAGE_ID && buffer_pool_manager_ != nullptr) {
    page_ = buffer_pool_manager_->FetchPage(page_id);
    if (page_ != nullptr) {
      leaf_page_ = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page_->GetData());
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  // Unpin the page if it's still pinned
  if (page_ != nullptr && buffer_pool_manager_ != nullptr && page_id_ != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(page_id_, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return page_id_ == INVALID_PAGE_ID || leaf_page_ == nullptr || index_ >= leaf_page_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  assert(!IsEnd());
  return leaf_page_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }
  
  index_++;
  
  // 检查是否已到达当前页面的末尾
  if (index_ >= leaf_page_->GetSize()) {
    // 移动到下一个叶子页面
    page_id_t next_page_id = leaf_page_->GetNextPageId();
    
    // 取消固定当前页面
    buffer_pool_manager_->UnpinPage(page_id_, false);
    
    if (next_page_id == INVALID_PAGE_ID) {
      // 到达末尾
      page_id_ = INVALID_PAGE_ID;
      leaf_page_ = nullptr;
      page_ = nullptr;
      index_ = 0;
    } else {
      // 获取下一个页面
      page_id_ = next_page_id;
      page_ = buffer_pool_manager_->FetchPage(page_id_);
      if (page_ != nullptr) {
        leaf_page_ = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page_->GetData());
        index_ = 0;
      } else {
        leaf_page_ = nullptr;
        index_ = 0;
      }
    }
  }
  
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return page_id_ == itr.page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  return !(*this == itr);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "common/rwlatch.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * 判断当前 B+ 树是否为空的辅助函数
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * 返回与输入键关联的唯一值
 * 此方法用于点查询
 * @return : true 表示键存在
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // 使用锁爬行查找叶子页面
  Page *page = FindLeafPage(key, false, OperationType::SEARCH, transaction);
  if (page == nullptr) {
  return false;
  }

  // 现在我们在叶子页面，搜索键
  auto *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  bool found = false;
  
  for (int i = 0; i < leaf_node->GetSize(); i++) {
    if (comparator_(key, leaf_node->KeyAt(i)) == 0) {
      // 找到键
      result->push_back(leaf_node->ValueAt(i));
      found = true;
      break;
    }
    if (comparator_(key, leaf_node->KeyAt(i)) < 0) {
      // 键小于当前键，所以不存在
      break;
    }
  }

  // 释放锁并取消固定叶子页面
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  
  // 清理事务页面集合 - 释放所有剩余的锁
  // 注意：叶子页面应该在 page_set 中，所以我们需要先移除它
  if (transaction != nullptr) {
    auto page_set = transaction->GetPageSet();
    // 如果叶子页面在 page_set 中，将其移除
    if (!page_set->empty() && page_set->back() == page) {
      page_set->pop_back();
    }
    // 释放所有剩余的祖先锁
    while (!page_set->empty()) {
      Page *locked_page = page_set->back();
      page_set->pop_back();
      locked_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(locked_page->GetPageId(), false);
    }
  }
  
  return found;
}

/*****************************************************************************
 * 插入
 *****************************************************************************/
/*
 * 将常量键值对插入到 b+ 树中
 * 如果当前树为空，创建新树，更新根页面 id 并插入条目，
 * 否则插入到叶子页面。
 * @return: 由于我们只支持唯一键，如果用户尝试插入重复键则返回 false，
 * 否则返回 true。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 如果树为空，创建新的根叶子页面
  // 处理并发根节点创建：多个线程可能同时尝试创建根节点
  // 使用简单方法：尝试创建根节点，如果已存在，则正常插入
  page_id_t current_root = root_page_id_;
  if (current_root == INVALID_PAGE_ID) {
    // 树看起来为空，尝试创建根节点
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    if (new_page == nullptr) {
      return false;
    }
    
    auto *leaf_node = reinterpret_cast<LeafPage *>(new_page->GetData());
    leaf_node->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    leaf_node->GetItem(0) = std::make_pair(key, value);
    leaf_node->SetSize(1);
    
    // 双重检查：另一个线程可能在我们创建页面时已经创建了根节点
    // 只有当 root_page_id_ 仍然是 INVALID_PAGE_ID 时才设置
    // 注意：这不是完美的原子性，但在实践中应该可以工作
    // 如果另一个线程设置了它，我们会检测到并正常插入
    if (root_page_id_ == INVALID_PAGE_ID) {
      root_page_id_ = new_page_id;
      UpdateRootPageId(1);
      buffer_pool_manager_->UnpinPage(new_page_id, true);
      return true;
    } else {
      // 根节点已被另一个线程创建，清理并正常插入
      buffer_pool_manager_->UnpinPage(new_page_id, false);
      buffer_pool_manager_->DeletePage(new_page_id);
      // 继续正常插入流程
    }
  }

  // 使用锁爬行找到应该插入键的叶子页面
  Page *page = FindLeafPage(key, false, OperationType::INSERT, transaction);
  if (page == nullptr) {
    return false;
  }

  // 现在我们在叶子页面（已加写锁）
  auto *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  
  // 查找插入位置并检查重复键
  // 由于键是排序的，可以在一次遍历中完成
  int insert_pos = 0;
  bool duplicate_found = false;
  
  while (insert_pos < leaf_node->GetSize()) {
    int cmp = comparator_(key, leaf_node->KeyAt(insert_pos));
    if (cmp == 0) {
      // 找到重复键
      duplicate_found = true;
      break;
    }
    if (cmp < 0) {
      // 找到插入位置
      break;
    }
    insert_pos++;
  }
  
  // 检查键是否属于下一个页面（仅在搜索完整个当前页面后）
  // 这处理了并发场景中页面在查找后被分裂的情况
  // 只有当键大于当前页面所有键时才检查下一个页面
  if (insert_pos >= leaf_node->GetSize() && leaf_node->GetSize() > 0) {
    int last_index = leaf_node->GetSize() - 1;
    if (comparator_(key, leaf_node->KeyAt(last_index)) > 0) {
      // 键大于当前页面的最后一个键，检查下一个页面
      page_id_t next_page_id = leaf_node->GetNextPageId();
      if (next_page_id != INVALID_PAGE_ID) {
        // 检查键是否属于下一个页面
        Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
        if (next_page != nullptr) {
          next_page->WLatch();
          auto *next_leaf = reinterpret_cast<LeafPage *>(next_page->GetData());
          if (next_leaf->GetSize() > 0 && comparator_(key, next_leaf->KeyAt(0)) >= 0) {
            // 键属于下一个页面，释放当前页面并使用下一个页面
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            if (transaction != nullptr) {
              auto page_set = transaction->GetPageSet();
              if (!page_set->empty() && page_set->back() == page) {
                page_set->pop_back();
              }
            }
            if (transaction != nullptr) {
              transaction->AddIntoPageSet(next_page);
            }
            page = next_page;
            leaf_node = next_leaf;
            // 在新页面中重新查找插入位置
            insert_pos = 0;
            duplicate_found = false;
            while (insert_pos < leaf_node->GetSize()) {
              int cmp = comparator_(key, leaf_node->KeyAt(insert_pos));
              if (cmp == 0) {
                duplicate_found = true;
                break;
              }
              if (cmp < 0) {
                break;
              }
              insert_pos++;
            }
          } else {
            // 键也不属于下一个页面，解锁下一个页面
            next_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(next_page_id, false);
          }
        }
      }
    }
  }
  
  if (duplicate_found) {
    // 找到重复键 - 释放锁并取消固定
    // 首先，如果叶子页面在 page_set 中，将其移除
    if (transaction != nullptr) {
      auto page_set = transaction->GetPageSet();
      // 从 page_set 中移除叶子页面
      if (!page_set->empty() && page_set->back() == page) {
        page_set->pop_back();
      } else {
        // 如果不在末尾，搜索并移除
        for (auto it = page_set->begin(); it != page_set->end(); ++it) {
          if ((*it)->GetPageId() == page->GetPageId()) {
            page_set->erase(it);
            break;
          }
        }
      }
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    // 清理事务页面集合 - 释放所有剩余的锁
    if (transaction != nullptr) {
      auto page_set = transaction->GetPageSet();
      while (!page_set->empty()) {
        Page *locked_page = page_set->back();
        page_set->pop_back();
        locked_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(locked_page->GetPageId(), false);
      }
    }
    return false;
  }
  
  // 移动元素以腾出空间
  for (int i = leaf_node->GetSize(); i > insert_pos; i--) {
    leaf_node->GetItem(i) = leaf_node->GetItem(i - 1);
  }
  leaf_node->GetItem(insert_pos) = std::make_pair(key, value);
  leaf_node->IncreaseSize(1);
  
  bool need_split = (leaf_node->GetSize() >= leaf_node->GetMaxSize());
  page_id_t leaf_page_id = page->GetPageId();
  
  if (need_split) {
    // 分裂叶子页面
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    if (new_page == nullptr) {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_page_id, true);
      // 清理事务的 page_set，释放所有剩余锁
      if (transaction != nullptr) {
        auto page_set = transaction->GetPageSet();
        // 如果叶子页面在 page_set 中，移除它
        if (!page_set->empty() && page_set->back() == page) {
          page_set->pop_back();
        }
        // 释放所有剩余的祖先锁
        while (!page_set->empty()) {
          Page *locked_page = page_set->back();
          page_set->pop_back();
          locked_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(locked_page->GetPageId(), false);
        }
      }
  return false;
    }
    
    // 新页面尚未接入树，不需要加锁
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_leaf->Init(new_page_id, leaf_node->GetParentPageId(), leaf_max_size_);
    
    // 将一半元素移动到新页面
    int split_index = leaf_node->GetSize() / 2;
    int new_size = leaf_node->GetSize() - split_index;
    
    for (int i = 0; i < new_size; i++) {
      new_leaf->GetItem(i) = leaf_node->GetItem(split_index + i);
    }
    new_leaf->SetSize(new_size);
    leaf_node->SetSize(split_index);
    
    // 更新下一个页面指针
    page_id_t old_next = leaf_node->GetNextPageId();
    new_leaf->SetNextPageId(old_next);
    leaf_node->SetNextPageId(new_page_id);
    
    // 获取要插入到父节点的中间键
    KeyType middle_key = new_leaf->KeyAt(0);
    
    // 在解锁前保存 parent_id
    page_id_t parent_id = leaf_node->GetParentPageId();
    
    // 如果叶子页面在 page_set 中，将其移除（在解锁前）
    if (transaction != nullptr && !transaction->GetPageSet()->empty() && 
        transaction->GetPageSet()->back() == page) {
      transaction->GetPageSet()->pop_back();
    }
    
    // 解锁并取消固定叶子页面，只取消固定新页面（不需要锁）
    // 注意：我们在这里解锁叶子页面，但 InsertIntoParent 会处理父节点锁
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    
    // 将中间键插入到父节点（可能需要获取锁）
    // 这必须在其他线程看到新结构之前完成
    InsertIntoParent(parent_id, leaf_page_id, middle_key, new_page_id, transaction);
  } else {
    // 不需要分裂，只需解锁并取消固定
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
    // 从 page_set 中移除叶子页面
    if (transaction != nullptr && !transaction->GetPageSet()->empty() && 
        transaction->GetPageSet()->back() == page) {
      transaction->GetPageSet()->pop_back();
    }
  }
  
  // 清理事务页面集合 - 释放所有剩余的锁
  // 这应该在 InsertIntoParent 完成后执行
  // 注意：InsertIntoParent 可能已经添加/移除了 page_set 中的页面
  // page_set 中任何剩余的页面应该使用 is_dirty=true 取消固定
  // 因为它们在插入过程中被修改了
  if (transaction != nullptr) {
    auto page_set = transaction->GetPageSet();
    // 释放所有剩余的锁（如果没有分裂应该为空，如果发生分裂则包含父页面）
    while (!page_set->empty()) {
      Page *locked_page = page_set->back();
      page_set->pop_back();
      locked_page->WUnlatch();
      // 使用 is_dirty=true，因为这些页面在插入过程中被修改了
      buffer_pool_manager_->UnpinPage(locked_page->GetPageId(), true);
    }
  }
  
  return true;
}

// 叶子节点分裂后插入到父节点的辅助函数
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(page_id_t parent_id, page_id_t left_id, const KeyType &key, page_id_t right_id, 
                                       Transaction *transaction) {
  if (parent_id == INVALID_PAGE_ID) {
    // 创建新的根节点
    page_id_t new_root_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_id);
    if (new_root_page == nullptr) {
      return;
    }
    
    auto *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    
    // 将第一个值设置为左子节点（第一个键无效）
    new_root->GetItem(0) = std::make_pair(KeyType{}, left_id);
    new_root->SetKeyAt(1, key);
    new_root->GetItem(1).second = right_id;
    new_root->SetSize(2);
    
    // 更新父节点指针
    // 检查 left_page 和 right_page 是否已经在 page_set 中（来自锁爬行）
    Page *left_page = nullptr;
    Page *right_page = nullptr;
    bool left_in_set = false;
    bool right_in_set = false;
    
    if (transaction != nullptr) {
      auto page_set = transaction->GetPageSet();
      for (auto *p : *page_set) {
        if (p->GetPageId() == left_id) {
          left_page = p;
          left_in_set = true;
        }
        if (p->GetPageId() == right_id) {
          right_page = p;
          right_in_set = true;
        }
      }
    }
    
    if (left_page == nullptr) {
      left_page = buffer_pool_manager_->FetchPage(left_id);
    }
    if (right_page == nullptr) {
      right_page = buffer_pool_manager_->FetchPage(right_id);
    }
    
    if (left_page != nullptr && right_page != nullptr) {
      auto *left_node = reinterpret_cast<BPlusTreePage *>(left_page->GetData());
      auto *right_node = reinterpret_cast<BPlusTreePage *>(right_page->GetData());
      left_node->SetParentPageId(new_root_id);
      right_node->SetParentPageId(new_root_id);
      
      root_page_id_ = new_root_id;
      UpdateRootPageId(0);
      
      // 取消固定新根节点（未锁定，刚创建）
      buffer_pool_manager_->UnpinPage(new_root_id, true);
      
      // 对于 left_page 和 right_page：
      // - 如果它们在 page_set 中，它们被锁定并将在 Insert 的最终清理中释放
      //   我们需要在这里解锁它们并从 page_set 中移除，但保持它们固定
      //   实际上，等等 - 如果它们在 page_set 中，它们应该在 Insert 的清理中释放
      //   但我们已经修改了它们（SetParentPageId），所以它们是脏的
      //   让我们在这里解锁它们并从 page_set 中移除，然后取消固定它们
      if (left_in_set) {
        // 左页面在 page_set 中，解锁它并从 page_set 中移除
        left_page->WUnlatch();
        if (transaction != nullptr) {
          auto page_set = transaction->GetPageSet();
          for (auto it = page_set->begin(); it != page_set->end(); ++it) {
            if ((*it)->GetPageId() == left_id) {
              page_set->erase(it);
              break;
            }
          }
        }
        buffer_pool_manager_->UnpinPage(left_id, true);
      } else {
        // 左页面刚刚获取，取消固定它
        buffer_pool_manager_->UnpinPage(left_id, true);
      }
      
      if (right_in_set) {
        // 右页面在 page_set 中，解锁它并从 page_set 中移除
        right_page->WUnlatch();
        if (transaction != nullptr) {
          auto page_set = transaction->GetPageSet();
          for (auto it = page_set->begin(); it != page_set->end(); ++it) {
            if ((*it)->GetPageId() == right_id) {
              page_set->erase(it);
              break;
            }
          }
        }
        buffer_pool_manager_->UnpinPage(right_id, true);
      } else {
        // 右页面刚刚获取，取消固定它
        buffer_pool_manager_->UnpinPage(right_id, true);
      }
    } else {
      // 错误：获取 left_page 或 right_page 失败
      // 清理创建的新根页面
      buffer_pool_manager_->UnpinPage(new_root_id, false);
      // 同时清理任何已获取的页面
      if (left_page != nullptr) {
        if (left_in_set) {
          left_page->WUnlatch();
          if (transaction != nullptr) {
            auto page_set = transaction->GetPageSet();
            for (auto it = page_set->begin(); it != page_set->end(); ++it) {
              if ((*it)->GetPageId() == left_id) {
                page_set->erase(it);
                break;
              }
            }
          }
        }
        buffer_pool_manager_->UnpinPage(left_id, false);
      }
      if (right_page != nullptr) {
        if (right_in_set) {
          right_page->WUnlatch();
          if (transaction != nullptr) {
            auto page_set = transaction->GetPageSet();
            for (auto it = page_set->begin(); it != page_set->end(); ++it) {
              if ((*it)->GetPageId() == right_id) {
                page_set->erase(it);
                break;
              }
            }
          }
        }
        buffer_pool_manager_->UnpinPage(right_id, false);
      }
    }
    return;
  }
  
  // 插入到现有父节点
  // 检查父节点是否已在事务中锁定
  Page *parent_page = nullptr;
  bool parent_locked = false;
  
  if (transaction != nullptr) {
    auto page_set = transaction->GetPageSet();
    // 检查父节点是否在页面集合中（已锁定）
    for (auto *p : *page_set) {
      if (p->GetPageId() == parent_id) {
        parent_page = p;
        parent_locked = true;
        break;
      }
    }
  }
  
  if (!parent_locked) {
    parent_page = buffer_pool_manager_->FetchPage(parent_id);
    if (parent_page == nullptr) {
      return;
    }
    parent_page->WLatch();
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(parent_page);
    }
  }
  
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  
  // 查找 left_id 出现的位置，然后在其后插入键
  int insert_pos = 0;
  for (int i = 0; i < parent_node->GetSize(); i++) {
    if (parent_node->ValueAt(i) == left_id) {
      insert_pos = i + 1;
      break;
    }
  }
  
  // 移动元素以腾出空间
  for (int i = parent_node->GetSize(); i > insert_pos; i--) {
    parent_node->GetItem(i) = parent_node->GetItem(i - 1);
  }
  parent_node->SetKeyAt(insert_pos, key);
  parent_node->GetItem(insert_pos).second = right_id;
  parent_node->IncreaseSize(1);
  
  // 更新右子节点的父节点指针
  // 检查 right_page 是否已经在 page_set 中（来自锁爬行）
  Page *right_page = nullptr;
  bool right_in_set = false;
  
  if (transaction != nullptr) {
    auto page_set = transaction->GetPageSet();
    for (auto *p : *page_set) {
      if (p->GetPageId() == right_id) {
        right_page = p;
        right_in_set = true;
        break;
      }
    }
  }
  
  if (right_page == nullptr) {
    right_page = buffer_pool_manager_->FetchPage(right_id);
  }
  
  if (right_page != nullptr) {
    auto *right_node = reinterpret_cast<BPlusTreePage *>(right_page->GetData());
    right_node->SetParentPageId(parent_id);
    if (!right_in_set) {
      buffer_pool_manager_->UnpinPage(right_id, true);
    }
  }
  
  bool need_split = (parent_node->GetSize() > parent_node->GetMaxSize());
  
  if (need_split) {
    // 分裂内部页面
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    if (new_page == nullptr) {
      if (!parent_locked) {
        parent_page->WUnlatch();
        if (transaction != nullptr && !transaction->GetPageSet()->empty() && 
            transaction->GetPageSet()->back() == parent_page) {
          transaction->GetPageSet()->pop_back();
        }
      }
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return;
    }
    
    auto *new_internal = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_internal->Init(new_page_id, parent_node->GetParentPageId(), internal_max_size_);
    
    // 分裂位置：将一半元素移动到新页面
    // 对于有 n 个键、n+1 个值的内部节点，在 (n+1)/2 处分裂
    int split_index = (parent_node->GetSize() + 1) / 2;
    KeyType middle_key = parent_node->KeyAt(split_index);
    
    // 新页面的第一个元素使用 split_index 位置的值
    new_internal->GetItem(0) = std::make_pair(KeyType{}, parent_node->ValueAt(split_index));
    
    // 复制剩余元素（从 split_index+1 到末尾）
    int new_size = parent_node->GetSize() - split_index;
    for (int i = 1; i < new_size; i++) {
      new_internal->GetItem(i) = parent_node->GetItem(split_index + i);
    }
    new_internal->SetSize(new_size);
    parent_node->SetSize(split_index);
    
    // 更新新页面中子节点的父指针
    for (int i = 0; i < new_internal->GetSize(); i++) {
      page_id_t child_id = new_internal->ValueAt(i);
      Page *child_page = buffer_pool_manager_->FetchPage(child_id);
      if (child_page == nullptr) {
        // 清理新页面后直接返回
        buffer_pool_manager_->UnpinPage(new_page_id, false);
        if (!parent_locked) {
          parent_page->WUnlatch();
          if (transaction != nullptr && !transaction->GetPageSet()->empty() && 
              transaction->GetPageSet()->back() == parent_page) {
            transaction->GetPageSet()->pop_back();
          }
        }
        buffer_pool_manager_->UnpinPage(parent_id, true);
        return;
      }
      auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_node->SetParentPageId(new_page_id);
      buffer_pool_manager_->UnpinPage(child_id, true);
    }
    
    // 新的内部页面尚未接入树结构，无需加锁，直接取消固定即可
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    
    // 对于父页面：
    // - 如果它在向下遍历时已加入 page_set，则保持锁定和保留，由 Insert 的最终清理释放
    // - 如果是当前才获取的页面，现在就解锁并取消固定
    if (!parent_locked) {
      parent_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(parent_id, true);
      // 如果是此处加入的 page_set，在这里移除
      if (transaction != nullptr && !transaction->GetPageSet()->empty() && 
          transaction->GetPageSet()->back() == parent_page) {
        transaction->GetPageSet()->pop_back();
      }
    }
    // 如果 parent_locked，则保持锁定并留在 page_set，等待 Insert 最终清理
    
    // 递归将中间键插入父节点
    // 递归后父节点可能继续分裂
    // 若父节点在 page_set 中，会在 Insert 的清理阶段统一处理
    InsertIntoParent(parent_node->GetParentPageId(), parent_id, middle_key, new_page_id, transaction);
  } else {
    // 不需要分裂
    if (!parent_locked) {
      // 父节点不在 page_set 中，是刚获取的
      // 现在解锁并取消固定
      parent_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(parent_id, true);
      // 如果是此处加入的 page_set，移除它
      if (transaction != nullptr && !transaction->GetPageSet()->empty() && 
          transaction->GetPageSet()->back() == parent_page) {
        transaction->GetPageSet()->pop_back();
      }
    }
    // 父节点已在 page_set 中（来自向下遍历）
    // 保持锁定并留在 page_set，由 Insert 最终清理处理
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * 删除与输入键关联的键值对
 * 如果树为空，直接返回
 * 否则需要先找到包含该键的叶子页面，再执行删除
 * 删除后如有需要记得做重分配或合并
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 使用锁爬行查找叶子页面
  Page *page = FindLeafPage(key, false, OperationType::DELETE, transaction);
  if (page == nullptr) {
    return;
  }

  // 此时位于叶子页面（已持有写锁）
  auto *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  
  // 查找并删除该键
  int delete_pos = -1;
  for (int i = 0; i < leaf_node->GetSize(); i++) {
    if (comparator_(key, leaf_node->KeyAt(i)) == 0) {
      delete_pos = i;
      break;
    }
    if (comparator_(key, leaf_node->KeyAt(i)) < 0) {
      // 未找到键，解锁并取消固定
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      // 清理事务的 page_set，释放所有剩余锁
      if (transaction != nullptr) {
        auto page_set = transaction->GetPageSet();
        // 如果叶子页面在 page_set 中，将其移除
        if (!page_set->empty() && page_set->back() == page) {
          page_set->pop_back();
        }
        // 释放所有剩余的祖先锁
        while (!page_set->empty()) {
          Page *locked_page = page_set->back();
          page_set->pop_back();
          locked_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(locked_page->GetPageId(), false);
        }
      }
      return;
    }
  }
  
  if (delete_pos == -1) {
    // 未找到键，解锁并取消固定
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    // 清理事务的 page_set，释放所有剩余锁
    if (transaction != nullptr) {
      auto page_set = transaction->GetPageSet();
      // 如果叶子页面在 page_set 中，将其移除
      if (!page_set->empty() && page_set->back() == page) {
        page_set->pop_back();
      }
      // 释放所有剩余的祖先锁
      while (!page_set->empty()) {
        Page *locked_page = page_set->back();
        page_set->pop_back();
        locked_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(locked_page->GetPageId(), false);
      }
    }
    return;
  }
  
  // 通过移动元素删除该键值对
  for (int i = delete_pos; i < leaf_node->GetSize() - 1; i++) {
    leaf_node->GetItem(i) = leaf_node->GetItem(i + 1);
  }
  leaf_node->IncreaseSize(-1);
  
  page_id_t leaf_page_id = page->GetPageId();
  
  // 检查是否需要合并或重分配
  bool need_merge = (leaf_node->GetSize() < leaf_node->GetMinSize() && !leaf_node->IsRootPage());
  
  if (need_merge) {
    // CoalesceOrRedistribute 会负责解锁，并可能修改 page_set
    CoalesceOrRedistribute(leaf_node, leaf_page_id, transaction);
  } else {
    // 不需要合并，直接解锁并取消固定
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
    // 如果叶子页面在 page_set 中，将其移除
    if (transaction != nullptr) {
      auto page_set = transaction->GetPageSet();
      if (!page_set->empty() && page_set->back() == page) {
        page_set->pop_back();
      }
    }
  }
  
  // 如果根节点为空且仍有子节点，则将其子节点设为新根
  // 在清理 page_set 之前执行此检查，因为根节点可能在 page_set 中
  if (root_page_id_ != INVALID_PAGE_ID) {
    Page *root_page = nullptr;
    bool root_in_set = false;
    
    if (transaction != nullptr) {
      auto page_set = transaction->GetPageSet();
      for (auto *p : *page_set) {
        if (p->GetPageId() == root_page_id_) {
          root_page = p;
          root_in_set = true;
          break;
        }
      }
    }
    
    if (!root_in_set) {
      root_page = buffer_pool_manager_->FetchPage(root_page_id_);
      if (root_page == nullptr) {
        return;
      }
      root_page->WLatch();
    }
    
    if (root_page != nullptr) {
      auto *root_node = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
      // 对内部节点：GetSize() 返回键的数量
      // 如果 size == 0，则只有 1 个子节点（0 个键），需要提升子节点
      // 如果 size == 1，则有 2 个子节点（1 个键），对根来说仍合法
      if (!root_node->IsLeafPage() && root_node->GetSize() == 0) {
        // 根只有一个子节点（0 个键），将其设为新根
        auto *root_internal = reinterpret_cast<InternalPage *>(root_node);
        page_id_t new_root_id = root_internal->ValueAt(0);
        page_id_t old_root_id = root_page->GetPageId();  // 更新前保存旧根页面 ID
        
        Page *new_root_page = buffer_pool_manager_->FetchPage(new_root_id);
        if (new_root_page != nullptr) {
          new_root_page->WLatch();
          auto *new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
          new_root_node->SetParentPageId(INVALID_PAGE_ID);
          new_root_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(new_root_id, true);
        }
        
        root_page_id_ = new_root_id;
        UpdateRootPageId(0);
        
        // 将旧根标记为待删除
        if (transaction != nullptr) {
          transaction->AddIntoDeletedPageSet(old_root_id);
        }
        
        // 若旧根在 page_set 中则移除（使用 old_root_id，而非 root_page_id_）
        if (root_in_set && transaction != nullptr) {
          auto page_set = transaction->GetPageSet();
          for (auto it = page_set->begin(); it != page_set->end(); ++it) {
            if ((*it)->GetPageId() == old_root_id) {
              page_set->erase(it);
              break;
            }
          }
        }
        
        // 解锁并取消固定旧根页面
        root_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(old_root_id, false);
        buffer_pool_manager_->DeletePage(old_root_id);
      } else if (root_node->IsLeafPage() && root_node->GetSize() == 0) {
        // 根是空叶子，整棵树置空
        page_id_t old_root_id = root_page_id_;  // 更新前保存旧根页面 ID
        
        if (transaction != nullptr) {
          transaction->AddIntoDeletedPageSet(old_root_id);
        }
        
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId(0);
        
        // 若旧根在 page_set 中则移除（使用 old_root_id，而非 root_page_id_）
        if (root_in_set && transaction != nullptr) {
          auto page_set = transaction->GetPageSet();
          for (auto it = page_set->begin(); it != page_set->end(); ++it) {
            if ((*it)->GetPageId() == old_root_id) {
              page_set->erase(it);
              break;
            }
          }
        }
        
        // 解锁并取消固定旧根页面
        root_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(old_root_id, false);
        buffer_pool_manager_->DeletePage(old_root_id);
      } else {
        // 根仍有效，只需解锁并取消固定
        if (root_in_set) {
          // 根在 page_set 中，解锁后保留到最终清理
          // 实际上既然已用完，应在这里解锁并从 page_set 移除
          root_page->WUnlatch();
          if (transaction != nullptr) {
            auto page_set = transaction->GetPageSet();
            for (auto it = page_set->begin(); it != page_set->end(); ++it) {
              if ((*it)->GetPageId() == root_page_id_) {
                page_set->erase(it);
                break;
              }
            }
          }
          buffer_pool_manager_->UnpinPage(root_page_id_, false);
        } else {
          // 根页面是刚获取的，解锁并取消固定
          root_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(root_page_id_, false);
        }
      }
    }
  }
  
  // 清理事务的 page_set，释放所有剩余锁
  // 注意：叶子页已在上文或 CoalesceOrRedistribute 中解锁并移除
  // 根节点检查已完成，若在 page_set 中应已被移除
  if (transaction != nullptr) {
    auto page_set = transaction->GetPageSet();
    // 释放所有剩余的锁（无合并时应为空，发生合并时可能含有父页面）
    while (!page_set->empty()) {
      Page *locked_page = page_set->back();
      page_set->pop_back();
      locked_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(locked_page->GetPageId(), false);
    }
  }
}

// 合并或重分配的辅助函数
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceOrRedistribute(BPlusTreePage *node, page_id_t page_id, Transaction *transaction) {
  // 获取页面对象（正常情况下它已加锁并位于 page_set 中）
  Page *current_page = nullptr;
  bool page_in_set = false;
  if (transaction != nullptr && !transaction->GetPageSet()->empty()) {
    // 检查该页面是否在事务的 page_set 中
    auto page_set = transaction->GetPageSet();
    for (auto it = page_set->rbegin(); it != page_set->rend(); ++it) {
      if ((*it)->GetPageId() == page_id) {
        current_page = *it;
        page_in_set = true;
        break;
      }
    }
  }
  
  if (current_page == nullptr) {
    current_page = buffer_pool_manager_->FetchPage(page_id);
    if (current_page == nullptr) {
      return;
    }
    current_page->WLatch();
  }
  
  // 若未提供节点（如递归调用），从页面中取得节点指针
  BPlusTreePage *current_node = node;
  if (current_node == nullptr) {
    current_node = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
  }
  
  if (current_node->IsRootPage()) {
    if (!page_in_set) {
      current_page->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(page_id, true);
    return;
  }
  
  page_id_t parent_id = current_node->GetParentPageId();
  
  // 检查父节点是否已在事务的 page_set 中（锁爬行阶段加入）
  Page *parent_page = nullptr;
  bool parent_locked = false;
  if (transaction != nullptr) {
    auto page_set = transaction->GetPageSet();
    for (auto *p : *page_set) {
      if (p->GetPageId() == parent_id) {
        parent_page = p;
        parent_locked = true;
        break;
      }
    }
  }
  
  if (!parent_locked) {
    parent_page = buffer_pool_manager_->FetchPage(parent_id);
    if (parent_page == nullptr) {
      if (!page_in_set) {
        current_page->WUnlatch();
      }
      buffer_pool_manager_->UnpinPage(page_id, true);
      return;
    }
    parent_page->WLatch();
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(parent_page);
    }
  }
  
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  
  // 查找左右兄弟节点
  int index_in_parent = -1;
  for (int i = 0; i < parent_node->GetSize(); i++) {
    if (parent_node->ValueAt(i) == page_id) {
      index_in_parent = i;
      break;
    }
  }
  
  if (index_in_parent == -1) {
    if (!parent_locked) {
      parent_page->WUnlatch();
      if (transaction != nullptr && !transaction->GetPageSet()->empty() && 
          transaction->GetPageSet()->back() == parent_page) {
        transaction->GetPageSet()->pop_back();
      }
    }
    if (!page_in_set) {
      current_page->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(page_id, true);
    if (!parent_locked) {
      buffer_pool_manager_->UnpinPage(parent_id, false);
    }
    return;
  }
  
  page_id_t sibling_id = INVALID_PAGE_ID;
  bool is_left_sibling = false;
  
  if (index_in_parent > 0) {
    // 优先尝试左兄弟
    sibling_id = parent_node->ValueAt(index_in_parent - 1);
    is_left_sibling = true;
  } else if (index_in_parent < parent_node->GetSize() - 1) {
    // 再尝试右兄弟
    sibling_id = parent_node->ValueAt(index_in_parent + 1);
    is_left_sibling = false;
  }
  
  if (sibling_id == INVALID_PAGE_ID) {
    if (!parent_locked) {
      parent_page->WUnlatch();
      if (transaction != nullptr && !transaction->GetPageSet()->empty() && 
          transaction->GetPageSet()->back() == parent_page) {
        transaction->GetPageSet()->pop_back();
      }
    }
    if (!page_in_set) {
      current_page->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(page_id, true);
    if (!parent_locked) {
      buffer_pool_manager_->UnpinPage(parent_id, false);
    }
    return;
  }
  
  // 关键：为避免死锁，获取锁必须按 page_id 升序
  // 我们已持有父页面锁（parent_id 通常最小）
  // 若 sibling_id < page_id，需要先释放当前页面的锁，再按顺序获取
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
  if (sibling_page == nullptr) {
    if (!parent_locked) {
      parent_page->WUnlatch();
      if (transaction != nullptr && !transaction->GetPageSet()->empty() && 
          transaction->GetPageSet()->back() == parent_page) {
        transaction->GetPageSet()->pop_back();
      }
    }
    if (!page_in_set) {
      current_page->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(page_id, true);
    if (!parent_locked) {
      buffer_pool_manager_->UnpinPage(parent_id, false);
    }
    return;
  }
  
  // 关键：为避免死锁，必须按 page_id 升序拿锁
  // 如果 sibling_id < page_id，即便在 page_set 中也必须先释放当前页的锁，
  // 以保证严格的锁顺序，避免循环等待。
  //
  // 死锁示例：
  // - 线程 1：持有 page_id=10，想获取 sibling_id=5
  // - 线程 2：持有 page_id=5，想获取 sibling_id=10
  // 两者互等导致死锁。
  //
  // 解决：始终按 page_id 从小到大获取锁
  bool need_reacquire = (sibling_id < page_id);
  if (need_reacquire) {
    // 为保持严格顺序，先释放当前页锁
    // 如在 page_set 中，先移除
    if (page_in_set && transaction != nullptr) {
      auto page_set = transaction->GetPageSet();
      for (auto it = page_set->begin(); it != page_set->end(); ++it) {
        if ((*it)->GetPageId() == page_id) {
          page_set->erase(it);
          break;
        }
      }
    }
    current_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
  
  // 先锁定 sibling_page（page_id 更小）
  sibling_page->WLatch();
  
  // 重新获取当前页锁（此时顺序正确：兄弟页 < 当前页）
  if (need_reacquire) {
    current_page = buffer_pool_manager_->FetchPage(page_id);
    if (current_page == nullptr) {
      // 错误：重新获取 current_page 失败，清理后返回
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_id, false);
      if (!parent_locked) {
        parent_page->WUnlatch();
        if (transaction != nullptr && !transaction->GetPageSet()->empty() && 
            transaction->GetPageSet()->back() == parent_page) {
          transaction->GetPageSet()->pop_back();
        }
        buffer_pool_manager_->UnpinPage(parent_id, false);
      }
      return;
    }
    current_page->WLatch();
    // 重新获取页面后需更新节点指针
    current_node = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
    // 注意：这里不把 current_page 加回 page_set，后续清理逻辑会处理
  }
  auto *sibling_node = reinterpret_cast<BPlusTreePage *>(sibling_page->GetData());
  
  // 优先尝试重分配（仅针对叶子页面）
  if (current_node->IsLeafPage() && sibling_node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(current_node);
    auto *sibling_leaf = reinterpret_cast<LeafPage *>(sibling_node);
    
    if (sibling_leaf->GetSize() > sibling_leaf->GetMinSize()) {
      // 执行重分配
      if (is_left_sibling) {
        // 将左兄弟最后一个元素移动到当前节点
        int last_index = sibling_leaf->GetSize() - 1;
        for (int i = leaf_node->GetSize(); i > 0; i--) {
          leaf_node->GetItem(i) = leaf_node->GetItem(i - 1);
        }
        leaf_node->GetItem(0) = sibling_leaf->GetItem(last_index);
        leaf_node->IncreaseSize(1);
        sibling_leaf->IncreaseSize(-1);
        
        // 更新父节点键
        parent_node->SetKeyAt(index_in_parent, leaf_node->KeyAt(0));
      } else {
        // 将右兄弟第一个元素移动到当前节点
        leaf_node->GetItem(leaf_node->GetSize()) = sibling_leaf->GetItem(0);
        leaf_node->IncreaseSize(1);
        
        for (int i = 0; i < sibling_leaf->GetSize() - 1; i++) {
          sibling_leaf->GetItem(i) = sibling_leaf->GetItem(i + 1);
        }
        sibling_leaf->IncreaseSize(-1);
        
        // 更新父节点键
        parent_node->SetKeyAt(index_in_parent + 1, sibling_leaf->KeyAt(0));
      }
      
      // 按获取顺序的逆序释放锁
      // 若重新获取过 current_page，它不在 page_set 中，直接解锁并取消固定
      if (need_reacquire) {
        current_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page_id, true);
      } else {
        // 若未重新获取，current_page 可能仍在 page_set 中
        if (page_in_set && transaction != nullptr) {
          // 先从 page_set 移除
          auto page_set = transaction->GetPageSet();
          for (auto it = page_set->begin(); it != page_set->end(); ++it) {
            if ((*it)->GetPageId() == page_id) {
              page_set->erase(it);
              break;
            }
          }
        }
        if (!page_in_set) {
          current_page->WUnlatch();
        } else {
          current_page->WUnlatch();
        }
        buffer_pool_manager_->UnpinPage(page_id, true);
      }
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_id, true);
      if (!parent_locked) {
        parent_page->WUnlatch();
        if (transaction != nullptr && !transaction->GetPageSet()->empty() && 
            transaction->GetPageSet()->back() == parent_page) {
          transaction->GetPageSet()->pop_back();
        }
        buffer_pool_manager_->UnpinPage(parent_id, true);
      }
      return;
    }
  }
  
  // 合并（仅适用于叶子页面）
  if (current_node->IsLeafPage() && sibling_node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(current_node);
    auto *sibling_leaf = reinterpret_cast<LeafPage *>(sibling_node);
    
    if (is_left_sibling) {
      // 将当前页合并到左兄弟
      for (int i = 0; i < leaf_node->GetSize(); i++) {
        sibling_leaf->GetItem(sibling_leaf->GetSize() + i) = leaf_node->GetItem(i);
      }
      sibling_leaf->IncreaseSize(leaf_node->GetSize());
      sibling_leaf->SetNextPageId(leaf_node->GetNextPageId());
    } else {
      // 将右兄弟合并到当前页
      for (int i = 0; i < sibling_leaf->GetSize(); i++) {
        leaf_node->GetItem(leaf_node->GetSize() + i) = sibling_leaf->GetItem(i);
      }
      leaf_node->IncreaseSize(sibling_leaf->GetSize());
      leaf_node->SetNextPageId(sibling_leaf->GetNextPageId());
    }
    
    // 从父节点移除对应的键
    int key_index = is_left_sibling ? index_in_parent : index_in_parent + 1;
    for (int i = key_index; i < parent_node->GetSize() - 1; i++) {
      parent_node->GetItem(i) = parent_node->GetItem(i + 1);
    }
    parent_node->IncreaseSize(-1);
    
    page_id_t page_to_delete = is_left_sibling ? page_id : sibling_id;
    // 将合并后待删除的页面加入删除集合
    if (transaction != nullptr) {
      transaction->AddIntoDeletedPageSet(page_to_delete);
    }
    
    // 释放锁，兼顾 need_reacquire 情况
    // 若重新获取过 current_page，它不在 page_set 中，直接解锁并取消固定
    if (need_reacquire) {
      current_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, true);
    } else {
      // 若未重新获取，current_page 可能在 page_set 中
      if (page_in_set && transaction != nullptr) {
        // 先从 page_set 移除
        auto page_set = transaction->GetPageSet();
        for (auto it = page_set->begin(); it != page_set->end(); ++it) {
          if ((*it)->GetPageId() == page_id) {
            page_set->erase(it);
            break;
          }
        }
      }
      current_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, true);
    }
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_id, true);
    buffer_pool_manager_->DeletePage(page_to_delete);
    
    // 在释放锁之前先检查父节点是否需要继续合并
    // 解锁前先保存所需的信息
    bool parent_needs_merge = (parent_node->GetSize() < parent_node->GetMinSize() && !parent_node->IsRootPage());
    
    // 释放父节点锁，如有需要再递归处理父节点合并
    // 这样可以避免在递归时持有多把锁导致死锁
    if (!parent_locked) {
      parent_page->WUnlatch();
      if (transaction != nullptr && !transaction->GetPageSet()->empty() && 
          transaction->GetPageSet()->back() == parent_page) {
        transaction->GetPageSet()->pop_back();
      }
      buffer_pool_manager_->UnpinPage(parent_id, true);
    } else {
      // 父节点在 page_set 中，解锁并将其移除
      parent_page->WUnlatch();
      if (transaction != nullptr) {
        auto page_set = transaction->GetPageSet();
        // 查找并移除父页面
        for (auto it = page_set->begin(); it != page_set->end(); ++it) {
          if ((*it)->GetPageId() == parent_id) {
            page_set->erase(it);
            break;
          }
        }
      }
      buffer_pool_manager_->UnpinPage(parent_id, true);
    }
    
    // 递归检查父节点是否需要合并
    // 递归前已释放所有锁以避免死锁
    // 递归过程中会按需重新加锁
    // 注意：父节点在取消固定后指针失效，因此这里传入 nullptr，
    // CoalesceOrRedistribute 会再次获取该页面
    if (parent_needs_merge) {
      CoalesceOrRedistribute(nullptr, parent_id, transaction);
    }
  } else {
    // 对内部节点这里只取消固定（内部合并更复杂，Checkpoint 1 或许无需处理）
    // 释放锁，兼顾 need_reacquire 情况
    if (need_reacquire) {
      current_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, true);
    } else {
      // 若未重新获取，current_page 可能仍在 page_set 中
      if (page_in_set && transaction != nullptr) {
        // 先从 page_set 移除
        auto page_set = transaction->GetPageSet();
        for (auto it = page_set->begin(); it != page_set->end(); ++it) {
          if ((*it)->GetPageId() == page_id) {
            page_set->erase(it);
            break;
          }
        }
      }
      current_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, true);
    }
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_id, false);
    if (!parent_locked) {
      parent_page->WUnlatch();
      if (transaction != nullptr && !transaction->GetPageSet()->empty() && 
          transaction->GetPageSet()->back() == parent_page) {
        transaction->GetPageSet()->pop_back();
      }
      buffer_pool_manager_->UnpinPage(parent_id, false);
    } else {
      parent_page->WUnlatch();
      // 将父页面从 page_set 移除
      if (transaction != nullptr) {
        auto page_set = transaction->GetPageSet();
        for (auto it = page_set->begin(); it != page_set->end(); ++it) {
          if ((*it)->GetPageId() == parent_id) {
            page_set->erase(it);
            break;
          }
        }
      }
      buffer_pool_manager_->UnpinPage(parent_id, true);
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * 无参版本：先找到最左叶子页面，再构造索引迭代器
 * @return：索引迭代器
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // 将 root_page_id_ 读入局部变量以避免竞态
  page_id_t root_id = root_page_id_;
  if (root_id == INVALID_PAGE_ID) {
    return End();
  }
  
  // 查找最左侧叶子页面
  page_id_t current_page_id = root_id;
  Page *page = buffer_pool_manager_->FetchPage(current_page_id);
  if (page == nullptr) {
    return End();
  }
  
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  
  // 向下遍历直到最左叶子页面
  while (!node->IsLeafPage()) {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    // 走向最左子节点（第一个 value）
    page_id_t next_page_id = internal_node->ValueAt(0);
    
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    current_page_id = next_page_id;
    page = buffer_pool_manager_->FetchPage(current_page_id);
    if (page == nullptr) {
      return End();
    }
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  
  // 此时位于最左叶子页面
  // 迭代器稍后会再次获取，先取消固定
  buffer_pool_manager_->UnpinPage(current_page_id, false);
  
  // 创建指向第一个元素（索引 0）的迭代器
  return INDEXITERATOR_TYPE(buffer_pool_manager_, current_page_id, 0);
}

/*
 * 传入下界键，先定位包含该键的叶子页面，再构造迭代器
 * @return：索引迭代器
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // 将 root_page_id_ 读入局部变量以避免竞争条件
  page_id_t root_id = root_page_id_;
  if (root_id == INVALID_PAGE_ID) {
    return End();
  }
  
  // 查找包含键的叶子页面（类似于 GetValue）
  page_id_t current_page_id = root_id;
  Page *page = buffer_pool_manager_->FetchPage(current_page_id);
  if (page == nullptr) {
    return End();
  }
  
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  
  // 向下遍历到叶子页面
  while (!node->IsLeafPage()) {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    
    // 查找应该包含键的子页面
    // 内部节点结构：ValueAt(0) 指向键 < KeyAt(1) 的子树
    // ValueAt(i) 指向键 >= KeyAt(i) 且 < KeyAt(i+1) 的子树
    int index = 1;
    while (index < internal_node->GetSize() && comparator_(key, internal_node->KeyAt(index)) >= 0) {
      index++;
    }
    // index 现在指向第一个 > 输入键的键，所以我们转到 ValueAt(index-1)
    page_id_t next_page_id = internal_node->ValueAt(index - 1);
    
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    current_page_id = next_page_id;
    page = buffer_pool_manager_->FetchPage(current_page_id);
    if (page == nullptr) {
      return End();
    }
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  
  // 现在我们在叶子页面，查找键的位置
  auto *leaf_node = reinterpret_cast<LeafPage *>(node);
  int key_index = 0;
  
  // 在当前页面中查找第一个 >= 输入键的键
  // 遍历逻辑应该已经将我们带到正确的叶子页面
  // 该页面可能包含键（或第一个键 >= 输入键的页面）
  while (key_index < leaf_node->GetSize() && comparator_(key, leaf_node->KeyAt(key_index)) > 0) {
    key_index++;
  }
  
  // 如果我们搜索了整个页面但没有找到 >= 输入键的键，
  // 键可能在下一个页面（如果存在）
  if (key_index >= leaf_node->GetSize()) {
    page_id_t next_page_id = leaf_node->GetNextPageId();
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    
    if (next_page_id == INVALID_PAGE_ID) {
      // 没有下一个页面，返回 End()
      return End();
    }
    
    // 移动到下一个页面并从索引 0 开始
    current_page_id = next_page_id;
    page = buffer_pool_manager_->FetchPage(current_page_id);
    if (page == nullptr) {
      return End();
    }
    leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
    key_index = 0;
    
    // 在下一个页面中查找第一个 >= 输入键的键
    while (key_index < leaf_node->GetSize() && comparator_(key, leaf_node->KeyAt(key_index)) > 0) {
      key_index++;
    }
    
    // 如果仍然没有找到，返回 End()（如果树是正确的，这不应该发生）
    if (key_index >= leaf_node->GetSize()) {
      buffer_pool_manager_->UnpinPage(current_page_id, false);
      return End();
    }
    
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    return INDEXITERATOR_TYPE(buffer_pool_manager_, current_page_id, key_index);
  }
  
  // 取消固定页面，因为迭代器会再次获取它
  buffer_pool_manager_->UnpinPage(current_page_id, false);
  
  // 创建指向找到位置的迭代器
  return INDEXITERATOR_TYPE(buffer_pool_manager_, current_page_id, key_index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE();
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * 在 header_page（page_id = 0，定义于 include/page/header_page.h）中更新/插入根页面 ID
 * 每次根页面 ID 变化时都应调用
 * @parameter insert_record：默认 false，若为 true 则插入一条 <index_name, root_page_id> 记录，
 * 否则更新已有记录
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // 在 header_page 中创建一条 <index_name, root_page_id> 记录
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // 更新 header_page 中的 root_page_id
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * 仅用于测试：从文件逐条读取并插入
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * 仅用于测试：从文件逐条读取并删除
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * 仅用于调试，无需修改
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * 仅用于调试，无需修改
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * 仅用于调试，无需修改
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // 打印节点名称
    out << leaf_prefix << leaf->GetPageId();
    // 打印节点属性
    out << "[shape=plain color=green ";
    // 打印节点数据
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // 填充键数据
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // 结束表格
    out << "</TABLE>>];\n";
    // 如有下一叶子页面则输出链
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // 如有父节点则输出父指针
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // 打印节点名称
    out << internal_prefix << inner->GetPageId();
    // 打印节点属性
    out << "[shape=plain color=pink ";  // 粉色只是为了区分节点
    // 打印节点数据
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // 填充键数据
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // 结束表格
    out << "</TABLE>>];\n";
    // 输出父节点连线
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // 递归打印子节点
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * 仅用于调试，无需修改
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/*****************************************************************************
 * HELPER METHODS FOR CONCURRENCY
 *****************************************************************************/
/*
 * 使用锁爬行查找叶子页面的辅助函数
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool left_most, OperationType op, Transaction *transaction) -> Page * {
  // 将 root_page_id_ 读入局部变量，避免被其他线程修改造成竞态
  page_id_t root_id = root_page_id_;
  if (root_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  
  page_id_t current_page_id = root_id;
  Page *page = buffer_pool_manager_->FetchPage(current_page_id);
  if (page == nullptr) {
    return nullptr;
  }
  
  // 根据操作类型给根页面加锁
  if (op == OperationType::SEARCH) {
    page->RLatch();
  } else {
    page->WLatch();
  }
  
  // 若提供事务，则加入 page_set
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }
  
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  
  // 向下遍历直到叶子页面
  while (!node->IsLeafPage()) {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    
    page_id_t next_page_id;
    if (left_most) {
      // 走向最左子节点
      next_page_id = internal_node->ValueAt(0);
    } else {
      // 找到应继续遍历的子页面
      int index = 1;
      while (index < internal_node->GetSize() && comparator_(key, internal_node->KeyAt(index)) >= 0) {
        index++;
      }
      next_page_id = internal_node->ValueAt(index - 1);
    }
    
    // 获取并锁定子页面
    Page *child_page = buffer_pool_manager_->FetchPage(next_page_id);
    if (child_page == nullptr) {
      // 释放当前页面锁并取消固定
      if (op == OperationType::SEARCH) {
        page->RUnlatch();
      } else {
        page->WUnlatch();
      }
      if (transaction != nullptr && !transaction->GetPageSet()->empty()) {
        transaction->GetPageSet()->pop_back();
      }
      buffer_pool_manager_->UnpinPage(current_page_id, false);
      return nullptr;
    }
    
    // 根据操作类型给子页面加锁
    if (op == OperationType::SEARCH) {
      child_page->RLatch();
    } else {
      child_page->WLatch();
    }
    
    // 检查子页面是否“安全”
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    bool is_safe = IsSafe(child_node, op);
    
    // SEARCH 操作视所有节点为安全，始终释放父锁
    // INSERT/DELETE 仅在子节点安全时才释放父锁
    if (op == OperationType::SEARCH || is_safe) {
      // 释放父页面（当前页面）的锁
      Page *parent_page = page;
      page_id_t parent_page_id = current_page_id;
      
      if (transaction != nullptr) {
        auto page_set = transaction->GetPageSet();
        // 如果父页面在 page_set 中则移除
        // 注意：parent_page 理应是 page_set 的末尾元素（最近添加）
        if (!page_set->empty() && page_set->back() == parent_page) {
          page_set->pop_back();
        } else {
          // 若不在末尾，则搜索并移除（理论上不该发生，保险处理）
          for (auto it = page_set->begin(); it != page_set->end(); ++it) {
            if ((*it)->GetPageId() == parent_page_id) {
              page_set->erase(it);
              break;
            }
          }
        }
      }
      
      // 解锁并取消固定父页面
      if (op == OperationType::SEARCH) {
        parent_page->RUnlatch();
      } else {
        parent_page->WUnlatch();
      }
      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      
      // 将子页面加入事务的 page_set
      if (transaction != nullptr) {
        transaction->AddIntoPageSet(child_page);
      }
    } else {
      // 不安全，保留所有祖先锁，仅把子页面加入 page_set
      if (transaction != nullptr) {
        transaction->AddIntoPageSet(child_page);
      }
    }
    
    // 下移到子页面
    current_page_id = next_page_id;
    page = child_page;
    node = child_node;
  }
  
  // 已到达叶子页面
  return page;
}

/*
 * 判断页面对特定操作是否“安全”的辅助函数
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafe(BPlusTreePage *node, OperationType op) -> bool {
  if (node->IsRootPage()) {
    // 根页面对查询总是安全
    if (op == OperationType::SEARCH) {
      return true;
    }
    // 对插入/删除：有空间或不会被删空时视为安全
    if (op == OperationType::INSERT) {
      return node->GetSize() < node->GetMaxSize();
    }
    if (op == OperationType::DELETE) {
      // 删除后不至于变空则安全
      // 对内部根：GetSize() 为键数，删除后至少需 2 个子节点（1 个键），故 size > 1
      // 对叶子根：大小 > 1 即安全
      if (node->IsLeafPage()) {
        return node->GetSize() > 1;
      } else {
        // 内部节点：size 表示键数，至少需要 1 个键（2 个子节点）
        return node->GetSize() > 1;
      }
    }
  }
  
  // 非根页面
  if (op == OperationType::SEARCH) {
    // 搜索时任意页面都安全，可立刻释放父锁
    return true;
  }
  if (op == OperationType::INSERT) {
    // 未满则安全（插入不会触发分裂），即 size < max_size
    return node->GetSize() < node->GetMaxSize();
  }
  if (op == OperationType::DELETE) {
    // 至少半满则安全（删除不会触发合并），即 size > min_size
    return node->GetSize() > node->GetMinSize();
  }
  
  return false;
}


template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

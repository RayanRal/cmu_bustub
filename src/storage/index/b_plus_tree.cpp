//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "buffer/traced_buffer_pool_manager.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}

/**
 * @return Page id of the root of this tree
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  page_id_t root_id = header_page->root_page_id_;
  header_guard.Drop(); // Release header

  if (root_id == INVALID_PAGE_ID) {
    return false;
  }
  
  // Start traversing
  ReadPageGuard guard = bpm_->ReadPage(root_id);
  auto page = guard.As<BPlusTreePage>();
  
  while (!page->IsLeafPage()) {
    auto internal = guard.As<InternalPage>();
    page_id_t child_id = internal->Lookup(key, comparator_);
    
    // Crabbing: Get child, release parent
    ReadPageGuard child_guard = bpm_->ReadPage(child_id);
    guard = std::move(child_guard);
    page = guard.As<BPlusTreePage>();
  }
  
  // At leaf
  auto leaf = guard.As<LeafPage>();
  int index = leaf->Lookup(key, comparator_);
  if (index != -1) {
    // Check tombstone
    for (int i = 0; i < leaf->GetTombstoneCount(); ++i) {
        if (leaf->GetTombstoneAt(i) == index) {
            return false;
        }
    }
    result->push_back(leaf->ValueAt(index));
    return true;
  }
  
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry; otherwise, insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false; otherwise, return true.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // 1. Check Empty
  {
      ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
      auto header = header_guard.As<BPlusTreeHeaderPage>();
      if (header->root_page_id_ == INVALID_PAGE_ID) {
          header_guard.Drop(); // Upgrade requires drop
          
          WritePageGuard header_write_guard = bpm_->WritePage(header_page_id_);
          auto header_mut = header_write_guard.AsMut<BPlusTreeHeaderPage>();
          if (header_mut->root_page_id_ == INVALID_PAGE_ID) {
              // Create root
              page_id_t new_page_id = bpm_->NewPage();
              if (new_page_id == INVALID_PAGE_ID) return false;
              
              WritePageGuard root_guard = bpm_->WritePage(new_page_id);
              auto leaf = root_guard.AsMut<LeafPage>();
              leaf->Init(leaf_max_size_);
              leaf->Insert(key, value, comparator_);
              header_mut->root_page_id_ = new_page_id;
              return true;
          }
      }
  }

  // 2. Optimistic Insertion
  {
      ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
      auto header = header_guard.As<BPlusTreeHeaderPage>();
      page_id_t root_id = header->root_page_id_;
      header_guard.Drop();
      
      if (root_id != INVALID_PAGE_ID) {
          ReadPageGuard guard = bpm_->ReadPage(root_id);
          auto page = guard.As<BPlusTreePage>();
          
          // If root is leaf, we need Write lock to insert. Fallback to pessimistic.
          if (page->IsLeafPage()) {
             // We can't upgrade. Release and retry.
             guard.Drop();
             goto pessimistic;
          }
          
          while (!page->IsLeafPage()) {
             auto internal = guard.As<InternalPage>();
             page_id_t child_id = internal->Lookup(key, comparator_);
             
             // Peek Child to see if it is leaf?
             // No, just fetch it with Read.
             ReadPageGuard child_guard = bpm_->ReadPage(child_id);
             auto child_page = child_guard.As<BPlusTreePage>();
             
             if (child_page->IsLeafPage()) {
                 // Child is leaf. We hold Parent Read (guard) and Child Read (child_guard).
                 // We want to write to Child.
                 // Release Child Read.
                 child_guard.Drop();
                 
                 // Acquire Child Write.
                 WritePageGuard leaf_guard = bpm_->WritePage(child_id);
                 auto leaf = leaf_guard.AsMut<LeafPage>();
                 
                 // Release Parent Read (safe to do now, as we hold leaf write)
                 guard.Drop();
                 
                 // Check if safe (Size < MaxSize - 1)
                 // NOTE: LeafPage::Insert fails if Size >= MaxSize. 
                 // So we need Size < MaxSize to insert.
                 // The test expects optimistic success if "Size + 1 < MaxSize".
                 if (leaf->GetSize() < leaf->GetMaxSize() - 1) {
                     bool res = leaf->Insert(key, value, comparator_);
                     return res;
                 } else {
                     // Unsafe, fallback
                     leaf_guard.Drop();
                     goto pessimistic;
                 }
             }
             
             // Child is internal. Crab.
             guard = std::move(child_guard);
             page = guard.As<BPlusTreePage>();
          }
      }
  }

pessimistic:
  // 3. Pessimistic Insertion (Crabbing with Write Latches)
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
      // Should handle empty tree again in case it became empty (race condition)
      page_id_t new_page_id = bpm_->NewPage();
      WritePageGuard new_page_guard = bpm_->WritePage(new_page_id);
      auto leaf = new_page_guard.AsMut<LeafPage>();
      leaf->Init(leaf_max_size_);
      leaf->Insert(key, value, comparator_);
      header_page->root_page_id_ = new_page_id;
      return true;
  }
  
  ctx.root_page_id_ = header_page->root_page_id_;
  
  WritePageGuard guard = bpm_->WritePage(ctx.root_page_id_);
  auto page = guard.AsMut<BPlusTreePage>();
  ctx.write_set_.push_back(std::move(guard));
  
  // Safe Check: Size < MaxSize
  auto is_safe = [&](const BPlusTreePage *p) {
    return p->GetSize() < p->GetMaxSize();
  };
  
  if (is_safe(page)) {
    ctx.header_page_ = std::nullopt;
  }
  
  while (!page->IsLeafPage()) {
    auto internal = reinterpret_cast<const InternalPage *>(page);
    page_id_t child_id = internal->Lookup(key, comparator_);
    
    WritePageGuard child_guard = bpm_->WritePage(child_id);
    auto child_page = child_guard.AsMut<BPlusTreePage>();
    
    if (is_safe(child_page)) {
      if (ctx.header_page_ != std::nullopt) ctx.header_page_ = std::nullopt;
      ctx.write_set_.clear(); 
    }
    
    ctx.write_set_.push_back(std::move(child_guard));
    page = child_page;
  }
  
  auto leaf = reinterpret_cast<LeafPage *>(page);
  
  // Handle Split if Full
  if (leaf->GetSize() == leaf->GetMaxSize()) {
      page_id_t new_leaf_id = bpm_->NewPage();
      if (new_leaf_id == INVALID_PAGE_ID) return false;
      WritePageGuard new_leaf_guard = bpm_->WritePage(new_leaf_id);
      auto new_leaf = new_leaf_guard.AsMut<LeafPage>();
      new_leaf->Init(leaf_max_size_);
      
      leaf->MoveHalfTo(new_leaf);
      leaf->SetNextPageId(new_leaf_id);
      
      // Determine where to insert
      bool success;
      if (comparator_(key, new_leaf->KeyAt(0)) >= 0) {
          success = new_leaf->Insert(key, value, comparator_);
      } else {
          success = leaf->Insert(key, value, comparator_);
      }
      
      if (!success) return false; // Duplicate
      
      // Insert Pivot into Parent
      KeyType middle_key = new_leaf->KeyAt(0);
      new_leaf_guard.Drop();
      InsertIntoParent(middle_key, new_leaf_id, ctx.write_set_.back().GetPageId(), ctx);
      return true;
  }
  
  return leaf->Insert(key, value, comparator_);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(const KeyType &key, page_id_t value, page_id_t old_value, Context &ctx) {
  if (ctx.write_set_.empty()) {
      // Should not happen if root split
      return;
  }
  
  if (ctx.write_set_.size() == 1) {
    auto &root_guard = ctx.write_set_.back();
    // Create new root
    page_id_t new_root_id = bpm_->NewPage();
    if (new_root_id == INVALID_PAGE_ID) return;
    
    WritePageGuard new_root_guard = bpm_->WritePage(new_root_id);
    auto new_root = new_root_guard.AsMut<InternalPage>();
    new_root->Init(internal_max_size_);
    
    new_root->PopulateNewRoot(root_guard.GetPageId(), key, value);
    
    auto header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header->root_page_id_ = new_root_id;
    return;
  }
  
  ctx.write_set_.pop_back(); // Pop the child (leaf or internal)
  
  auto &p_guard = ctx.write_set_.back();
  auto p = p_guard.AsMut<InternalPage>();
  
  p->InsertNodeAfter(old_value, key, value); 
  
  if (p->GetSize() > p->GetMaxSize()) {
    // Split parent
    page_id_t new_p_id = bpm_->NewPage();
    if (new_p_id == INVALID_PAGE_ID) return;
    
    WritePageGuard new_p_guard = bpm_->WritePage(new_p_id);
    auto new_p = new_p_guard.AsMut<InternalPage>();
    new_p->Init(internal_max_size_);
    
    p->MoveHalfTo(new_p);
    
    KeyType new_key = new_p->KeyAt(0);
    new_p_guard.Drop();
    
    page_id_t old_p_id = p_guard.GetPageId();
    InsertIntoParent(new_key, new_p_id, old_p_id, ctx);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Optimistic Remove
  {
      ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
      auto header = header_guard.As<BPlusTreeHeaderPage>();
      page_id_t root_id = header->root_page_id_;
      header_guard.Drop();
      
      if (root_id != INVALID_PAGE_ID) {
          ReadPageGuard guard = bpm_->ReadPage(root_id);
          auto page = guard.As<BPlusTreePage>();
          
          if (page->IsLeafPage()) {
              guard.Drop();
              goto pessimistic_remove;
          }
          
          while (!page->IsLeafPage()) {
             auto internal = guard.As<InternalPage>();
             page_id_t child_id = internal->Lookup(key, comparator_);
             ReadPageGuard child_guard = bpm_->ReadPage(child_id);
             auto child_page = child_guard.As<BPlusTreePage>();
             
             if (child_page->IsLeafPage()) {
                 child_guard.Drop();
                 WritePageGuard leaf_guard = bpm_->WritePage(child_id);
                 auto leaf = leaf_guard.AsMut<LeafPage>();
                 guard.Drop();
                 
                 // Check Safe (Size > MinSize)
                 // Note: Remove deletes 1 key.
                 // Safe if (Size - 1) >= MinSize.
                 if (leaf->GetSize() > leaf->GetMinSize()) {
                     leaf->Remove(key, comparator_);
                     return;
                 } else {
                     leaf_guard.Drop();
                     goto pessimistic_remove;
                 }
             }
             
             guard = std::move(child_guard);
             page = guard.As<BPlusTreePage>();
          }
      } else {
          return;
      }
  }

pessimistic_remove:
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  
  if (header_page->root_page_id_ == INVALID_PAGE_ID) return;
  
  ctx.root_page_id_ = header_page->root_page_id_;
  WritePageGuard guard = bpm_->WritePage(ctx.root_page_id_);
  auto page = guard.AsMut<BPlusTreePage>();
  ctx.write_set_.push_back(std::move(guard));
  
  auto is_safe = [&](const BPlusTreePage *p) {
    auto min_size = p->GetMinSize();
    if (!p->IsLeafPage()) {
      min_size = min_size < 2 ? 2 : min_size;
    }
    return p->GetSize() > min_size;
  };
  
  if (is_safe(page)) {
    ctx.header_page_ = std::nullopt;
  }
  
  while (!page->IsLeafPage()) {
    auto internal = reinterpret_cast<const InternalPage *>(page);
    page_id_t child_id = internal->Lookup(key, comparator_);
    
    WritePageGuard child_guard = bpm_->WritePage(child_id);
    auto child_page = child_guard.AsMut<BPlusTreePage>();
    
    if (is_safe(child_page)) {
        if (ctx.header_page_ != std::nullopt) ctx.header_page_ = std::nullopt;
        ctx.write_set_.clear();
    }
    
    ctx.write_set_.push_back(std::move(child_guard));
    page = child_page;
  }
  
  auto leaf = reinterpret_cast<LeafPage *>(page);
  bool res = leaf->Remove(key, comparator_);
  if (!res) {
     return;
  }
  
  if (leaf->GetSize() < leaf->GetMinSize()) {
     // Underflow Handling
     while (true) {
         if (ctx.write_set_.size() == 1) {
             // Root
             auto &root_g = ctx.write_set_.back();
             auto root_p = root_g.AsMut<BPlusTreePage>();
             if (root_p->IsLeafPage()) {
                 if (root_p->GetSize() == 0) {
                     auto header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
                     header->root_page_id_ = INVALID_PAGE_ID;
                 }
             } else {
                 if (root_p->GetSize() == 1) {
                     auto internal_root = reinterpret_cast<InternalPage *>(root_p);
                     page_id_t child_id = internal_root->ValueAt(0);
                     auto header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
                     header->root_page_id_ = child_id;
                 }
             }
             return;
         }
         
         auto &node_guard = ctx.write_set_.back();
         auto node = node_guard.AsMut<BPlusTreePage>();
         
         // Fix for loop condition: internal nodes need at least 2 entries (unless Root)
         int min_size = node->GetMinSize();
         if (!node->IsLeafPage()) {
             min_size = min_size < 2 ? 2 : min_size;
         }
         
         if (node->GetSize() >= min_size) break;
         
         auto &parent_guard = ctx.write_set_[ctx.write_set_.size() - 2];
         auto parent = parent_guard.AsMut<InternalPage>();
         
         page_id_t node_id = node_guard.GetPageId();
         int idx = parent->ValueIndex(node_id);
         
         int sibling_idx = (idx == 0) ? 1 : idx - 1;
         page_id_t sibling_id = parent->ValueAt(sibling_idx);
         
         WritePageGuard sibling_guard = bpm_->WritePage(sibling_id);
         auto sibling = sibling_guard.AsMut<BPlusTreePage>();
         
         // Merge/Redistribute Logic
         bool is_merge = false;
         if (node->IsLeafPage()) {
             if (node->GetSize() + sibling->GetSize() <= node->GetMaxSize()) is_merge = true;
         } else {
             if (node->GetSize() + sibling->GetSize() + 1 <= node->GetMaxSize()) is_merge = true;
         }
         
         if (!is_merge) {
             // Redistribute (Borrow)
             if (idx == 0) { // Sibling is Right
                 KeyType middle_key = parent->KeyAt(1);
                 if (node->IsLeafPage()) {
                     auto l_node = reinterpret_cast<LeafPage *>(node);
                     auto l_sibling = reinterpret_cast<LeafPage *>(sibling);
                     l_sibling->MoveFirstToEndOf(l_node);
                     parent->SetKeyAt(1, l_sibling->KeyAt(0));
                 } else {
                     auto i_node = reinterpret_cast<InternalPage *>(node);
                     auto i_sibling = reinterpret_cast<InternalPage *>(sibling);
                     i_sibling->MoveFirstToEndOf(i_node, middle_key);
                     parent->SetKeyAt(1, i_sibling->KeyAt(0));
                 }
             } else { // Sibling is Left
                 KeyType middle_key = parent->KeyAt(idx);
                 if (node->IsLeafPage()) {
                     auto l_node = reinterpret_cast<LeafPage *>(node);
                     auto l_sibling = reinterpret_cast<LeafPage *>(sibling);
                     l_sibling->MoveLastToFrontOf(l_node);
                     parent->SetKeyAt(idx, l_node->KeyAt(0));
                 } else {
                     auto i_node = reinterpret_cast<InternalPage *>(node);
                     auto i_sibling = reinterpret_cast<InternalPage *>(sibling);
                     i_sibling->MoveLastToFrontOf(i_node, middle_key);
                     parent->SetKeyAt(idx, i_sibling->KeyAt(i_sibling->GetSize())); 
                 }
             }
             break; // Done
         }
         
         // MERGE
         BPlusTreePage *left;
         BPlusTreePage *right;
         
         if (idx == 0) { // Node is Left, Sibling is Right
             left = node;
             right = sibling;
         } else { // Sibling is Left, Node is Right
             left = sibling;
             right = node;
         }
         
         if (left->IsLeafPage()) {
             auto l = reinterpret_cast<LeafPage *>(left);
             auto r = reinterpret_cast<LeafPage *>(right);
             r->MoveAllTo(l);
         } else {
             auto l = reinterpret_cast<InternalPage *>(left);
             auto r = reinterpret_cast<InternalPage *>(right);
             int key_idx = (idx == 0) ? 1 : idx;
             KeyType middle_key = parent->KeyAt(key_idx);
             r->MoveAllTo(l, middle_key);
         }
         
         // Update Parent
         int remove_idx = (idx == 0) ? 1 : idx;
         // Remove key at remove_idx from parent
         for (int i = remove_idx; i < parent->GetSize() - 1; ++i) {
             parent->SetKeyAt(i, parent->KeyAt(i+1));
             parent->SetValueAt(i, parent->ValueAt(i+1));
         }
         parent->ChangeSizeBy(-1);
         
         // Done with current node.
         ctx.write_set_.pop_back(); 
         // Loop again.
     }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
    ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
    auto header_page = header_guard.As<BPlusTreeHeaderPage>();
    page_id_t root_id = header_page->root_page_id_;
    
    if (root_id == INVALID_PAGE_ID) {
        return INDEXITERATOR_TYPE(bpm_.get(), ReadPageGuard(), 0, INVALID_PAGE_ID);
    }
    
    ReadPageGuard guard = bpm_->ReadPage(root_id);
    auto page = guard.As<BPlusTreePage>();
    
    while (!page->IsLeafPage()) {
        auto internal = guard.As<InternalPage>();
        page_id_t child_id = internal->ValueAt(0);
        guard = bpm_->ReadPage(child_id);
        page = guard.As<BPlusTreePage>();
    }
    
    page_id_t pid = guard.GetPageId();
    return INDEXITERATOR_TYPE(bpm_.get(), std::move(guard), 0, pid);
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
    ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
    auto header_page = header_guard.As<BPlusTreeHeaderPage>();
    page_id_t root_id = header_page->root_page_id_;
    
    if (root_id == INVALID_PAGE_ID) {
        return INDEXITERATOR_TYPE(bpm_.get(), ReadPageGuard(), 0, INVALID_PAGE_ID);
    }
    
    ReadPageGuard guard = bpm_->ReadPage(root_id);
    auto page = guard.As<BPlusTreePage>();
    
    while (!page->IsLeafPage()) {
        auto internal = guard.As<InternalPage>();
        page_id_t child_id = internal->Lookup(key, comparator_);
        guard = bpm_->ReadPage(child_id);
        page = guard.As<BPlusTreePage>();
    }
    
    auto leaf = guard.As<LeafPage>();
    int idx = leaf->Lookup(key, comparator_);
    if (idx == -1) {
        int l = 0; 
        int r = leaf->GetSize() - 1;
        int ans = leaf->GetSize();
        while (l <= r) {
            int mid = l + (r - l) / 2;
            if (comparator_(leaf->KeyAt(mid), key) >= 0) {
                ans = mid;
                r = mid - 1;
            } else {
                l = mid + 1;
            }
        }
        idx = ans;
    }
    
    page_id_t pid = guard.GetPageId();
    return INDEXITERATOR_TYPE(bpm_.get(), std::move(guard), idx, pid);
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
    return INDEXITERATOR_TYPE(bpm_.get(), ReadPageGuard(), 0, INVALID_PAGE_ID);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
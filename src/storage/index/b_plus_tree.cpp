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

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafeInsert(const BPlusTreePage *page) -> bool {
  return page->GetSize() < page->GetMaxSize();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafeRemove(const BPlusTreePage *page) -> bool {
  int min_size = page->GetMinSize();
  if (!page->IsLeafPage()) {
    min_size = min_size < 2 ? 2 : min_size;
  }
  return page->GetSize() > min_size;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Operation op, Context &ctx, bool leftMost) -> const LeafPage * {
  // 1. Get Root ID
  if (ctx.header_page_.has_value()) {
    ctx.root_page_id_ = ctx.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;
  } else {
    ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
    ctx.root_page_id_ = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  }

  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return nullptr;
  }

  // 2. Traverse
  page_id_t next_id = ctx.root_page_id_;
  
  while (true) {
    BPlusTreePage *page;
    if (op == Operation::SEARCH) {
      ReadPageGuard guard = bpm_->ReadPage(next_id);
      page = const_cast<BPlusTreePage *>(guard.As<BPlusTreePage>());
      ctx.read_set_.push_back(std::move(guard));
    } else {
      WritePageGuard guard = bpm_->WritePage(next_id);
      page = guard.AsMut<BPlusTreePage>();
      ctx.write_set_.push_back(std::move(guard));
    }

    if (page->IsLeafPage()) {
      return reinterpret_cast<const LeafPage *>(page);
    }

    // Crabbing logic for Search: release parent
    if (op == Operation::SEARCH) {
       if (ctx.read_set_.size() > 1) ctx.read_set_.pop_front();
    } else {
       // Crabbing logic for Insert/Delete: release ancestors if safe
       bool safe = (op == Operation::INSERT) ? IsSafeInsert(page) : IsSafeRemove(page);
       if (safe) {
           ctx.header_page_ = std::nullopt;
           while (ctx.write_set_.size() > 1) ctx.write_set_.pop_front();
       }
    }

    auto internal = reinterpret_cast<const InternalPage *>(page);
    next_id = leftMost ? internal->ValueAt(0) : internal->Lookup(key, comparator_);
  }
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
  Context ctx;
  auto leaf = FindLeafPage(key, Operation::SEARCH, ctx);
  if (leaf == nullptr) {
    return false;
  }
  
  int index = leaf->Lookup(key, comparator_);
  if (index != -1 && !leaf->IsTombstone(index)) {
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
  // 1. Optimistic
  {
    Context ctx;
    auto leaf = FindLeafPage(key, Operation::SEARCH, ctx);
    if (leaf != nullptr) {
      page_id_t leaf_id = ctx.read_set_.back().GetPageId();
      ctx.read_set_.clear();
      
      auto leaf_guard = bpm_->WritePage(leaf_id);
      auto leaf_mut = leaf_guard.AsMut<LeafPage>();
      if (IsSafeInsert(leaf_mut)) {
        return leaf_mut->Insert(key, value, comparator_);
      }
    }
  }

  // 2. Pessimistic
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto leaf = FindLeafPage(key, Operation::INSERT, ctx);
  
  if (leaf == nullptr) {
    // Empty tree
    page_id_t new_page_id = bpm_->NewPage();
    if (new_page_id == INVALID_PAGE_ID) return false;
    
    WritePageGuard root_guard = bpm_->WritePage(new_page_id);
    auto leaf_mut = root_guard.AsMut<LeafPage>();
    leaf_mut->Init(leaf_max_size_);
    leaf_mut->Insert(key, value, comparator_);
    
    auto header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header->root_page_id_ = new_page_id;
    return true;
  }

  auto leaf_mut = const_cast<LeafPage *>(leaf);
  if (leaf_mut->GetSize() == leaf_mut->GetMaxSize()) {
    page_id_t new_leaf_id = bpm_->NewPage();
    if (new_leaf_id == INVALID_PAGE_ID) return false;
    WritePageGuard new_leaf_guard = bpm_->WritePage(new_leaf_id);
    auto new_leaf = new_leaf_guard.AsMut<LeafPage>();
    new_leaf->Init(leaf_max_size_);
    
    leaf_mut->MoveHalfTo(new_leaf);
    leaf_mut->SetNextPageId(new_leaf_id);
    
    bool success;
    if (comparator_(key, new_leaf->KeyAt(0)) >= 0) {
      success = new_leaf->Insert(key, value, comparator_);
    } else {
      success = leaf_mut->Insert(key, value, comparator_);
    }
    
    if (!success) return false;
    
    KeyType up_key = new_leaf->KeyAt(0);
    InsertIntoParent(up_key, new_leaf_id, ctx.write_set_.back().GetPageId(), ctx);
    return true;
  }

  return leaf_mut->Insert(key, value, comparator_);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(const KeyType &key, page_id_t value, page_id_t old_value, Context &ctx) {
  ctx.write_set_.pop_back(); // Pop the child (leaf or internal)

  if (ctx.write_set_.empty()) {
    // New root
    page_id_t new_root_id = bpm_->NewPage();
    if (new_root_id == INVALID_PAGE_ID) return;
    WritePageGuard new_root_guard = bpm_->WritePage(new_root_id);
    auto new_root = new_root_guard.AsMut<InternalPage>();
    new_root->Init(internal_max_size_);
    new_root->PopulateNewRoot(old_value, key, value);
    auto header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header->root_page_id_ = new_root_id;
    return;
  }

  auto p_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto p = p_guard.AsMut<InternalPage>();

  if (p->GetSize() == p->GetMaxSize()) {
    // Split before insert
    page_id_t new_p_id = bpm_->NewPage();
    if (new_p_id == INVALID_PAGE_ID) return;
    WritePageGuard new_p_guard = bpm_->WritePage(new_p_id);
    auto new_p = new_p_guard.AsMut<InternalPage>();
    new_p->Init(internal_max_size_);

    p->MoveHalfTo(new_p);

    if (p->ValueIndex(old_value) != -1) {
      p->InsertNodeAfter(old_value, key, value);
    } else {
      new_p->InsertNodeAfter(old_value, key, value);
    }

    KeyType up_key = new_p->KeyAt(0);
    page_id_t p_id = p_guard.GetPageId();
    ctx.write_set_.push_back(std::move(p_guard));
    InsertIntoParent(up_key, new_p_id, p_id, ctx);
  } else {
    p->InsertNodeAfter(old_value, key, value);
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
  // 1. Optimistic
  {
    Context ctx;
    auto leaf = FindLeafPage(key, Operation::SEARCH, ctx);
    if (leaf != nullptr) {
      page_id_t leaf_id = ctx.read_set_.back().GetPageId();
      ctx.read_set_.clear();
      
      auto leaf_guard = bpm_->WritePage(leaf_id);
      auto leaf_mut = leaf_guard.AsMut<LeafPage>();
      if (IsSafeRemove(leaf_mut)) {
        leaf_mut->Remove(key, comparator_);
        return;
      }
    }
  }

  // 2. Pessimistic
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto leaf = FindLeafPage(key, Operation::DELETE, ctx);
  if (leaf == nullptr) return;

  auto leaf_mut = const_cast<LeafPage *>(leaf);
  bool res = leaf_mut->Remove(key, comparator_);
  if (!res) return;

  if (leaf_mut->GetSize() < leaf_mut->GetMinSize()) {
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
         
         auto node_guard = std::move(ctx.write_set_.back());
         ctx.write_set_.pop_back();
         auto node = node_guard.AsMut<BPlusTreePage>();
         
         if (IsSafeRemove(node)) break;
         
         auto &parent_guard = ctx.write_set_.back();
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
                 if (node->IsLeafPage()) {
                     auto l_node = reinterpret_cast<LeafPage *>(node);
                     auto l_sibling = reinterpret_cast<LeafPage *>(sibling);
                     l_sibling->MoveFirstToEndOf(l_node);
                     parent->SetKeyAt(1, l_sibling->KeyAt(0));
                 } else {
                     KeyType middle_key = parent->KeyAt(1);
                     auto i_node = reinterpret_cast<InternalPage *>(node);
                     auto i_sibling = reinterpret_cast<InternalPage *>(sibling);
                     KeyType up_key = i_sibling->KeyAt(1);
                     i_sibling->MoveFirstToEndOf(i_node, middle_key);
                     parent->SetKeyAt(1, up_key);
                 }
             } else { // Sibling is Left
                 if (node->IsLeafPage()) {
                     auto l_node = reinterpret_cast<LeafPage *>(node);
                     auto l_sibling = reinterpret_cast<LeafPage *>(sibling);
                     l_sibling->MoveLastToFrontOf(l_node);
                     parent->SetKeyAt(idx, l_node->KeyAt(0));
                 } else {
                     KeyType middle_key = parent->KeyAt(idx);
                     auto i_node = reinterpret_cast<InternalPage *>(node);
                     auto i_sibling = reinterpret_cast<InternalPage *>(sibling);
                     KeyType up_key = i_sibling->KeyAt(i_sibling->GetSize() - 1);
                     i_sibling->MoveLastToFrontOf(i_node, middle_key);
                     parent->SetKeyAt(idx, up_key);
                 }
             }
             break; // Done
         }
         
         // MERGE
         BPlusTreePage *left;
         BPlusTreePage *right;
         int remove_idx;
         
         if (idx == 0) { // Node is Left, Sibling is Right
             left = node;
             right = sibling;
             remove_idx = 1;
         } else { // Sibling is Left, Node is Right
             left = sibling;
             right = node;
             remove_idx = idx;
         }
         
         if (left->IsLeafPage()) {
             auto l = reinterpret_cast<LeafPage *>(left);
             auto r = reinterpret_cast<LeafPage *>(right);
             r->MoveAllTo(l);
         } else {
             auto l = reinterpret_cast<InternalPage *>(left);
             auto r = reinterpret_cast<InternalPage *>(right);
             KeyType middle_key = parent->KeyAt(remove_idx);
             r->MoveAllTo(l, middle_key);
         }
         
         // Update Parent
         for (int i = remove_idx; i < parent->GetSize() - 1; ++i) {
             parent->SetKeyAt(i, parent->KeyAt(i+1));
             parent->SetValueAt(i, parent->ValueAt(i+1));
         }
         parent->ChangeSizeBy(-1);
         
         // Loop again for parent underflow
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
    Context ctx;
    auto leaf = FindLeafPage(KeyType(), Operation::SEARCH, ctx, true);
    if (leaf == nullptr) {
        return INDEXITERATOR_TYPE(bpm_.get(), ReadPageGuard(), 0, INVALID_PAGE_ID);
    }
    
    page_id_t pid = ctx.read_set_.back().GetPageId();
    return INDEXITERATOR_TYPE(bpm_.get(), std::move(ctx.read_set_.back()), 0, pid);
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
    Context ctx;
    auto leaf = FindLeafPage(key, Operation::SEARCH, ctx);
    if (leaf == nullptr) {
        return INDEXITERATOR_TYPE(bpm_.get(), ReadPageGuard(), 0, INVALID_PAGE_ID);
    }
    
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
    
    page_id_t pid = ctx.read_set_.back().GetPageId();
    return INDEXITERATOR_TYPE(bpm_.get(), std::move(ctx.read_set_.back()), idx, pid);
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
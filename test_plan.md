# Test Plan for B+Tree Implementation

This document outlines the comprehensive test plan for the B+Tree implementation, ensuring coverage of public methods in header files and critical internal logic.

## 1. Page Level Tests
**Target Files:** `src/include/storage/page/b_plus_tree_page.h`, `src/include/storage/page/b_plus_tree_internal_page.h`, `src/include/storage/page/b_plus_tree_leaf_page.h`

These tests verify the correctness of the data structures underlying the B+Tree, independent of the tree traversal logic.

### 1.1. BPlusTreeLeafPage
*   **Initialization:**
    *   Verify `Init` correctly sets `page_type_` to `LEAF_PAGE`.
    *   Verify `size_` is 0, `max_size_` is set correctly.
    *   Verify `next_page_id_` is `INVALID_PAGE_ID`.
*   **Data Management:**
    *   **Insert:** verify `Insert` adds keys in sorted order and increments size.
    *   **Lookup:** verify `Lookup` finds existing keys and returns correct index/found status.
    *   **Remove:** verify `Remove` deletes key, shifts elements, and decrements size.
    *   **Tombstones:** verify `SetTombstoneAt`, `GetTombstones` (list retrieval), and correct `num_tombstones_` accounting.
*   **Redistribution Helpers:**
    *   **MoveHalfTo:** Verify splitting a full leaf page moves top half to recipient, updates counts, and handles tombstones if strictly associated with moved keys.
    *   **MoveAllTo:** Verify merging: all keys moved to recipient, source becomes empty.
    *   **MoveFirstToEndOf:** Verify moving first element of `this` to end of `recipient`.
    *   **MoveLastToFrontOf:** Verify moving last element of `this` to front of `recipient`.

### 1.2. BPlusTreeInternalPage
*   **Initialization:**
    *   Verify `Init` sets `page_type_` to `INTERNAL_PAGE`.
*   **Data Management:**
    *   **Key/Value Access:** Verify `SetKeyAt`, `KeyAt`, `SetValueAt`, `ValueAt`. Ensure `KeyAt(0)` is treated as invalid/not used for comparison.
    *   **ValueIndex:** Verify retrieval of index for a given `page_id` (value).
    *   **Lookup:** Verify `Lookup(key, comparator)` returns the correct child `page_id` that should contain the key (logic: find last key <= search key).
*   **Root Operations:**
    *   **PopulateNewRoot:** Verify initializing a new root from two child pages and a middle key.
*   **Redistribution Helpers:**
    *   **MoveHalfTo:** Verify moving top half of keys/children to recipient.
    *   **MoveAllTo:** Verify merging all keys/children to recipient.
    *   **MoveFirstToEndOf / MoveLastToFrontOf:** Verify borrowing operations between siblings.

## 2. B+Tree Core Functionality
**Target File:** `src/include/storage/index/b_plus_tree.h`

These tests cover the main API of the B+Tree.

### 2.1. Basic Operations
*   **IsEmpty:**
    *   `true` for new tree.
    *   `false` after first insert.
    *   `true` after removing all elements.
*   **GetValue:**
    *   Found: Return correct value for existing key.
    *   Not Found: Return failure for non-existing key.
    *   Updated: Verify value update if supported (or insert overwrite behavior).
*   **GetRootPageId:**
    *   Verify valid ID after insert.
    *   Verify change if root splits (height increases).

### 2.2. Insertion Strategy
*   **Single Insert:** Insert one key, verify `GetValue`.
*   **Sequential Insert:** Insert 1..N. Verify structure (mostly right-leaning or balanced depending on split logic).
*   **Reverse Insert:** Insert N..1. Verify structure.
*   **Duplicate Insert:** Verify insertion fails or returns false for duplicate keys (since `unique key` is required).
*   **Leaf Split:** Insert enough keys to fill a leaf (`leaf_max_size`). Verify split occurs (can be inferred by successfully inserting `max + 1` keys and checking `GetValue` for all).
*   **Internal Split:** Insert enough keys to fill internal nodes. Verify tree height growth.

### 2.3. Deletion Strategy
*   **Simple Delete:** Remove key from leaf with spare capacity. Verify `GetValue` fails.
*   **Leaf Coalesce (Merge):** Remove key causing leaf to drop below min size. Verify merge with sibling (check page count or `next_page_id` links).
*   **Leaf Redistribute (Borrow):** Remove key causing underflow, where sibling has spare keys. Verify borrowing occurs instead of merge.
*   **Internal Coalesce/Redistribute:** Remove keys to cause internal node underflow. Verify merge/borrow propagation up the tree.
*   **Root Modification:**
    *   Delete keys until root has only 1 child -> Root should become that child (height decrease).
    *   Delete all keys -> Tree becomes empty.

### 2.4. Iterator Interface
*   **Begin():** Verify points to smallest key.
*   **Begin(key):**
    *   If key exists: Points to that key.
    *   If key missing: Points to first key > `key`.
    *   If key > max: Points to `End()`.
*   **End():** Verify standard end iterator properties.
*   **Iteration:**
    *   `++`: Verify traversing from `Begin()` reaches `End()` visiting all keys in sorted order.
    *   Verify traversing across leaf page boundaries (using `next_page_id`).

## 3. BPlusTreeIndex Wrapper
**Target File:** `src/include/storage/index/b_plus_tree_index.h`

*   **Integration:** Verify `InsertEntry`, `DeleteEntry`, and `ScanKey` correctly map to `BPlusTree` methods.
*   **Metadata:** Verify index metadata is correctly utilized.

## 4. Edge Cases & Robustness
*   **Empty Tree:** Calling Remove/GetValue on empty tree.
*   **Single Node:** Insert/Delete when tree has only root.
*   **Full Node:** Insert into full node (trigger split).
*   **Min-Filled Node:** Delete from min-filled node (trigger merge/borrow).
*   **Tombstone Interaction:** If tombstones are used for delayed cleanup or MVCC-like features (implied by leaf page header), verify they are skipped by iterators and cleaned up during splits/merges.

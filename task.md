Execute refactoring of the following files:
[b_plus_tree.cpp](src/storage/index/b_plus_tree.cpp)
[b_plus_tree_index.cpp](src/storage/index/b_plus_tree_index.cpp)
[b_plus_tree_internal_page.cpp](src/storage/page/b_plus_tree_internal_page.cpp)
[b_plus_tree_leaf_page.cpp](src/storage/page/b_plus_tree_leaf_page.cpp)
[b_plus_tree_page.cpp](src/storage/page/b_plus_tree_page.cpp)

Your refactoring plan is in plan.md. Do changes one by one, after each change make sure that following tests still pass:

 - All tests in [b_plus_tree_tests](test/storage/b_plus_tree_tests)
 - [b_plus_tree_delete_test.cpp](test/storage/b_plus_tree_delete_test.cpp)
 - [b_plus_tree_insert_test.cpp](test/storage/b_plus_tree_insert_test.cpp)
 - [b_plus_tree_page_test.cpp](test/storage/b_plus_tree_page_test.cpp)
 - [b_plus_tree_sequential_scale_test.cpp](test/storage/b_plus_tree_sequential_scale_test.cpp)
 - [b_plus_tree_tombstone_test.cpp](test/storage/b_plus_tree_tombstone_test.cpp)

Do not change any code functionality, just improve it's readability.
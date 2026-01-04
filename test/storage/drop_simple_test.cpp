#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(DropSimpleTest, SimpleWrite) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(10, disk_manager.get());

  const auto pid = bpm->NewPage();
  auto guard = bpm->WritePage(pid);
  
  ASSERT_EQ(1, bpm->GetPinCount(pid).value());
  
  guard.Drop();
  ASSERT_EQ(0, bpm->GetPinCount(pid).value());
}

TEST(DropSimpleTest, DoubleDrop) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(10, disk_manager.get());

  const auto pid = bpm->NewPage();
  auto guard = bpm->WritePage(pid);
  
  ASSERT_EQ(1, bpm->GetPinCount(pid).value());
  
  guard.Drop();
  ASSERT_EQ(0, bpm->GetPinCount(pid).value());
  
  guard.Drop();
  ASSERT_EQ(0, bpm->GetPinCount(pid).value());
}

TEST(DropSimpleTest, DestructorDrop) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(10, disk_manager.get());

  const auto pid = bpm->NewPage();
  {
    auto guard = bpm->WritePage(pid);
    ASSERT_EQ(1, bpm->GetPinCount(pid).value());
  }
  ASSERT_EQ(0, bpm->GetPinCount(pid).value());
}

TEST(DropSimpleTest, VectorOfGuards) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(10, disk_manager.get());

  std::vector<page_id_t> pids;
  {
    std::vector<WritePageGuard> guards;
    for (int i = 0; i < 5; i++) {
      auto pid = bpm->NewPage();
      guards.push_back(bpm->WritePage(pid));
      pids.push_back(pid);
      ASSERT_EQ(1, bpm->GetPinCount(pid).value());
    }
  }
  
  // All guards should be destroyed and unpinned
  for (auto pid : pids) {
    ASSERT_EQ(0, bpm->GetPinCount(pid).value());
  }
}

TEST(DropSimpleTest, FillBufferPoolThenEvict) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(3, disk_manager.get());

  std::vector<page_id_t> pids;
  {
    std::vector<WritePageGuard> guards;
    for (int i = 0; i < 3; i++) {
      auto pid = bpm->NewPage();
      guards.push_back(bpm->WritePage(pid));
      pids.push_back(pid);
      ASSERT_EQ(1, bpm->GetPinCount(pid).value());
    }
  }
  
  // All 3 guards should be destroyed and unpinned
  for (auto pid : pids) {
    ASSERT_EQ(0, bpm->GetPinCount(pid).value());
  }
  
  // Now try to create a 4th page - should evict one
  auto pid4 = bpm->NewPage();
  auto guard4 = bpm->WritePage(pid4);
  ASSERT_EQ(1, bpm->GetPinCount(pid4).value());
  guard4.Drop();
  ASSERT_EQ(0, bpm->GetPinCount(pid4).value());
}

TEST(DropSimpleTest, ReusePageAfterDrop) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(2, disk_manager.get());

  // Create page 0 in frame 0
  auto pid0 = bpm->NewPage();
  {
    auto guard = bpm->WritePage(pid0);
    ASSERT_EQ(1, bpm->GetPinCount(pid0).value());
  }
  // Guard destroyed, page 0 unpinned
  ASSERT_EQ(0, bpm->GetPinCount(pid0).value());
  
  // Create page 1 in frame 1
  auto pid1 = bpm->NewPage();
  {
    auto guard = bpm->WritePage(pid1);
    ASSERT_EQ(1, bpm->GetPinCount(pid1).value());
  }
  // Guard destroyed, page 1 unpinned
  ASSERT_EQ(0, bpm->GetPinCount(pid1).value());
  
  // Both frames are full with unpinned pages
  // Now try to access page 0 again (should find it in buffer)
  {
    auto guard = bpm->ReadPage(pid0);
    ASSERT_EQ(1, bpm->GetPinCount(pid0).value());
  }
  ASSERT_EQ(0, bpm->GetPinCount(pid0).value());
  
  // Create page 2 - should evict one of the pages
  auto pid2 = bpm->NewPage();
  auto guard2 = bpm->WritePage(pid2);
  ASSERT_EQ(1, bpm->GetPinCount(pid2).value());
}

TEST(DropSimpleTest, MultiAccessSamePage) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(2, disk_manager.get());

  auto pid0 = bpm->NewPage();
  
  // First access: pin page 0
  {
    auto guard1 = bpm->WritePage(pid0);
    ASSERT_EQ(1, bpm->GetPinCount(pid0).value());
    
    // While pinned, pin it again
    {
      auto guard2 = bpm->WritePage(pid0);
      ASSERT_EQ(2, bpm->GetPinCount(pid0).value());
    }
    // guard2 destroyed, pin count should go down
    ASSERT_EQ(1, bpm->GetPinCount(pid0).value());
  }
  // guard1 destroyed, pin count should go to 0
  ASSERT_EQ(0, bpm->GetPinCount(pid0).value());
}

TEST(DropSimpleTest, AccessAfterDrop) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(3, disk_manager.get());

  // Create and pin page 0
  auto pid0 = bpm->NewPage();
  auto guard1 = bpm->WritePage(pid0);
  ASSERT_EQ(1, bpm->GetPinCount(pid0).value());
  
  // Drop it explicitly
  guard1.Drop();
  ASSERT_EQ(0, bpm->GetPinCount(pid0).value());
  
  // Access it again - should find in buffer
  auto guard2 = bpm->WritePage(pid0);
  ASSERT_EQ(1, bpm->GetPinCount(pid0).value());
  
  // Drop again
  guard2.Drop();
  ASSERT_EQ(0, bpm->GetPinCount(pid0).value());
  
  // The destructor will be called when guard1 and guard2 go out of scope
  // since they were invalidated by explicit Drop(), the destructors should be no-ops
}

TEST(DropSimpleTest, SequentialPagesMultipleTimes) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(3, disk_manager.get());

  auto pid1 = bpm->NewPage();
  auto pid2 = bpm->NewPage();
  
  // First round: access pages
  {
    auto g1 = bpm->WritePage(pid1);
    auto g2 = bpm->WritePage(pid2);
    ASSERT_EQ(1, bpm->GetPinCount(pid1).value());
    ASSERT_EQ(1, bpm->GetPinCount(pid2).value());
  }
  ASSERT_EQ(0, bpm->GetPinCount(pid1).value());
  ASSERT_EQ(0, bpm->GetPinCount(pid2).value());
  
  // Second round: access pages again
  {
    auto g1 = bpm->WritePage(pid1);
    auto g2 = bpm->WritePage(pid2);
    ASSERT_EQ(1, bpm->GetPinCount(pid1).value());
    ASSERT_EQ(1, bpm->GetPinCount(pid2).value());
  }
  ASSERT_EQ(0, bpm->GetPinCount(pid1).value());
  ASSERT_EQ(0, bpm->GetPinCount(pid2).value());
  
  // Try to create a new page - should not evict from earlier accesses
  auto pid3 = bpm->NewPage();
  auto g3 = bpm->WritePage(pid3);
  ASSERT_EQ(1, bpm->GetPinCount(pid3).value());
}

TEST(DropSimpleTest, ExactDropTestScenario) {
  const size_t FRAMES = 3;
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  // Part 1: Create pages 0-2 and drop them
  const auto pid0 = bpm->NewPage();
  fmt::println(stderr, "\nDEBUG Part 1: Created pid0={}", pid0);
  auto page0 = bpm->WritePage(pid0);
  fmt::println(stderr, "  pin_count={}", bpm->GetPinCount(pid0).value_or(999));
  ASSERT_EQ(1, bpm->GetPinCount(pid0).value());
  page0.Drop();
  fmt::println(stderr, "  After drop: pin_count={}", bpm->GetPinCount(pid0).value_or(999));
  ASSERT_EQ(0, bpm->GetPinCount(pid0).value());

  const auto pid1 = bpm->NewPage();
  const auto pid2 = bpm->NewPage();
  fmt::println(stderr, "  Created pid1={}, pid2={}", pid1, pid2);

  // Part 2: Pin pages 1-2, then drop them
  fmt::println(stderr, "\nDEBUG Part 2: Starting");
  {
    auto read_guarded_page = bpm->ReadPage(pid1);
    auto write_guarded_page = bpm->WritePage(pid2);
    fmt::println(stderr, "  pin_count pid1={}, pid2={}", bpm->GetPinCount(pid1).value_or(999), bpm->GetPinCount(pid2).value_or(999));
    ASSERT_EQ(1, bpm->GetPinCount(pid1).value());
    ASSERT_EQ(1, bpm->GetPinCount(pid2).value());
    read_guarded_page.Drop();
    write_guarded_page.Drop();
    fmt::println(stderr, "  After drop: pin_count pid1={}, pid2={}", bpm->GetPinCount(pid1).value_or(999), bpm->GetPinCount(pid2).value_or(999));
    ASSERT_EQ(0, bpm->GetPinCount(pid1).value());
    ASSERT_EQ(0, bpm->GetPinCount(pid2).value());
  }
  fmt::println(stderr, "  After scope: pin_count pid1={}, pid2={}", bpm->GetPinCount(pid1).value_or(999), bpm->GetPinCount(pid2).value_or(999));

  // Part 3: Pin pages 1-2 again in temporary guards
  fmt::println(stderr, "\nDEBUG Part 3: Starting");
  {
    const auto write_test1 = bpm->WritePage(pid1);
    const auto write_test2 = bpm->WritePage(pid2);
    fmt::println(stderr, "  pin_count pid1={}, pid2={}", bpm->GetPinCount(pid1).value_or(999), bpm->GetPinCount(pid2).value_or(999));
  }
  fmt::println(stderr, "  After scope: pin_count pid1={}, pid2={}", bpm->GetPinCount(pid1).value_or(999), bpm->GetPinCount(pid2).value_or(999));
  // Destructors should have been called and pages unpinned
  ASSERT_EQ(0, bpm->GetPinCount(pid1).value());
  ASSERT_EQ(0, bpm->GetPinCount(pid2).value());

  // Part 4: Fill buffer with pages 3-5, then try to add page 6
  fmt::println(stderr, "\nDEBUG: Before Part 4:");
  fmt::println(stderr, "  pid0 pin count: {}", bpm->GetPinCount(pid0).value_or(999));
  fmt::println(stderr, "  pid1 pin count: {}", bpm->GetPinCount(pid1).value_or(999));
  fmt::println(stderr, "  pid2 pin count: {}", bpm->GetPinCount(pid2).value_or(999));
  
  std::vector<page_id_t> page_ids;
  {
    std::vector<WritePageGuard> guards;
    for (size_t i = 0; i < FRAMES; i++) {
      fmt::println(stderr, "DEBUG: Part 4 iteration {}", i);
      const auto new_pid = bpm->NewPage();
      fmt::println(stderr, "  Created page {}", new_pid);
      guards.push_back(bpm->WritePage(new_pid));
      fmt::println(stderr, "  Got guard for page {}", new_pid);
      ASSERT_EQ(1, bpm->GetPinCount(new_pid).value());
      page_ids.push_back(new_pid);
    }
  }
  // All guards destroyed, all pages unpinned
  for (auto pid : page_ids) {
    ASSERT_EQ(0, bpm->GetPinCount(pid).value());
  }

  // Part 5: Try to add a new page - this is where the original test failed
  const auto mutable_page_id = bpm->NewPage();
  auto mutable_guard = bpm->WritePage(mutable_page_id);
  ASSERT_EQ(1, bpm->GetPinCount(mutable_page_id).value());
  strcpy(mutable_guard.GetDataMut(), "data");
  mutable_guard.Drop();
  ASSERT_EQ(0, bpm->GetPinCount(mutable_page_id).value());
}

}  // namespace bustub

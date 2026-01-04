#include <iostream>
#include <memory>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"

int main() {
  const size_t FRAMES = 3;
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  std::cout << "=== Start of vector scope ===" << std::endl;
  {
    std::vector<WritePageGuard> guards;
    for (size_t i = 0; i < 3; i++) {
      auto pid = bpm->NewPage();
      std::cout << "Created page " << pid << std::endl;
      guards.push_back(bpm->WritePage(pid));
      std::cout << "Loaded page " << pid << ", vector size = " << guards.size() << std::endl;
    }
    std::cout << "=== End of loop, vector has " << guards.size() << " guards ===" << std::endl;
  }
  std::cout << "=== End of vector scope ===" << std::endl;

  return 0;
}

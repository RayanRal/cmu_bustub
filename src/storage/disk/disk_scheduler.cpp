//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <vector>
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param requests The requests to be scheduled.
 */
void DiskScheduler::Schedule(std::vector<DiskRequest> &requests) {
  for (auto &request : requests) {
    request_queue_.Put(std::move(request));
  }
}

/**
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto request = request_queue_.Get();

    // Check for the termination signal (std::nullopt)
    if (!request.has_value()) {
      break;
    }

    auto &disk_request = request.value();
    if (disk_request.is_write_) {
      disk_manager_->WritePage(disk_request.page_id_, disk_request.data_);
    } else {
      disk_manager_->ReadPage(disk_request.page_id_, disk_request.data_);
    }

    // Signal completion via the promise
    disk_request.callback_.set_value(true);
  }
}

}  // namespace bustub

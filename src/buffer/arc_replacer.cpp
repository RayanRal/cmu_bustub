// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <optional>
#include "common/config.h"

namespace bustub {

/**
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : mru_target_size_(0), replacer_size_(num_frames) {}

/**
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  // If MRU size is >= target, evict from MRU
  if (mru_.size() >= mru_target_size_) {
    // Find first evictable frame from back of MRU
    for (auto it = mru_.rbegin(); it != mru_.rend(); ++it) {
      frame_id_t frame_id = *it;
      auto frame_status = alive_map_[frame_id];

      if (frame_status->evictable_) {
        // Move to MRU ghost using iterator for O(1) erase
        if (frame_status->alive_iter_) {
          mru_.erase(*frame_status->alive_iter_);
        }
        mru_ghost_.push_front(frame_status->page_id_);
        frame_status->ghost_iter_ = mru_ghost_.begin();

        // Update maps
        alive_map_.erase(frame_id);
        ghost_map_[frame_status->page_id_] = frame_status;
        frame_status->arc_status_ = ArcStatus::MRU_GHOST;

        curr_size_--;
        return frame_id;
      }
    }

    // Fallback: If all entries in MRU are pinned, try MFU
    for (auto it = mfu_.rbegin(); it != mfu_.rend(); ++it) {
      frame_id_t frame_id = *it;
      auto frame_status = alive_map_[frame_id];

      if (frame_status->evictable_) {
        // Move to MFU ghost using iterator for O(1) erase
        if (frame_status->alive_iter_) {
          mfu_.erase(*frame_status->alive_iter_);
        }
        mfu_ghost_.push_front(frame_status->page_id_);
        frame_status->ghost_iter_ = mfu_ghost_.begin();

        // Update maps
        alive_map_.erase(frame_id);
        ghost_map_[frame_status->page_id_] = frame_status;
        frame_status->arc_status_ = ArcStatus::MFU_GHOST;

        curr_size_--;
        return frame_id;
      }
    }
  } else {
    // Evict from MFU
    for (auto it = mfu_.rbegin(); it != mfu_.rend(); ++it) {
      frame_id_t frame_id = *it;
      auto frame_status = alive_map_[frame_id];

      if (frame_status->evictable_) {
        // Move to MFU ghost using iterator for O(1) erase
        if (frame_status->alive_iter_) {
          mfu_.erase(*frame_status->alive_iter_);
        }
        mfu_ghost_.push_front(frame_status->page_id_);
        frame_status->ghost_iter_ = mfu_ghost_.begin();

        // Update maps
        alive_map_.erase(frame_id);
        ghost_map_[frame_status->page_id_] = frame_status;
        frame_status->arc_status_ = ArcStatus::MFU_GHOST;

        curr_size_--;
        return frame_id;
      }
    }

    // If all entries in MFU are pinned, try MRU
    for (auto it = mru_.rbegin(); it != mru_.rend(); ++it) {
      frame_id_t frame_id = *it;
      auto frame_status = alive_map_[frame_id];

      if (frame_status->evictable_) {
        // Move to MRU ghost using iterator for O(1) erase
        if (frame_status->alive_iter_) {
          mru_.erase(*frame_status->alive_iter_);
        }
        mru_ghost_.push_front(frame_status->page_id_);
        frame_status->ghost_iter_ = mru_ghost_.begin();

        // Update maps
        alive_map_.erase(frame_id);
        ghost_map_[frame_status->page_id_] = frame_status;
        frame_status->arc_status_ = ArcStatus::MRU_GHOST;

        curr_size_--;
        return frame_id;
      }
    }
  }

  return std::nullopt;
}

/**
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preparation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(const frame_id_t frame_id, const page_id_t page_id,
                               [[maybe_unused]] AccessType access_type) {
  if (const auto it = alive_map_.find(frame_id); it != alive_map_.end()) {
    HandleCacheHit(frame_id);
    return;
  }

  const auto ghost_it = ghost_map_.find(page_id);
  if (ghost_it != ghost_map_.end() && ghost_it->second->arc_status_ == ArcStatus::MRU_GHOST) {
    HandleMruGhostHit(frame_id, page_id);
    return;
  }

  if (ghost_it != ghost_map_.end() && ghost_it->second->arc_status_ == ArcStatus::MFU_GHOST) {
    HandleMfuGhostHit(frame_id, page_id);
    return;
  }

  HandleCacheMiss(frame_id, page_id);
}

/*
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  auto it = alive_map_.find(frame_id);

  // If frame not found, throw exception (invalid frame)
  if (it == alive_map_.end()) {
    throw std::runtime_error("Invalid frame id");
  }

  auto frame_status = it->second;

  // If already in the desired state, return without modifying
  if (frame_status->evictable_ == set_evictable) {
    return;
  }

  // Update evictable status and size accordingly
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }

  frame_status->evictable_ = set_evictable;
}

/**
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  auto it = alive_map_.find(frame_id);

  if (it == alive_map_.end()) {
    return;
  }

  auto frame_status = it->second;

  // If frame is not evictable, throw exception
  if (!frame_status->evictable_) {
    throw std::runtime_error("Cannot remove non-evictable frame");
  }

  // Remove from appropriate list (MRU or MFU) using iterator for O(1) erase
  if (frame_status->arc_status_ == ArcStatus::MRU && frame_status->alive_iter_) {
    mru_.erase(*frame_status->alive_iter_);
  } else if (frame_status->arc_status_ == ArcStatus::MFU && frame_status->alive_iter_) {
    mfu_.erase(*frame_status->alive_iter_);
  }

  // Remove from alive_map_ and decrement size
  alive_map_.erase(frame_id);
  curr_size_--;
}

/**
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t { return curr_size_; }

void ArcReplacer::HandleCacheHit(const frame_id_t frame_id) {
  const auto frame_status = alive_map_[frame_id];

  // Remove from current list (MRU or MFU) using iterator
  if (frame_status->arc_status_ == ArcStatus::MRU && frame_status->alive_iter_) {
    mru_.erase(*frame_status->alive_iter_);
  } else if (frame_status->arc_status_ == ArcStatus::MFU && frame_status->alive_iter_) {
    mfu_.erase(*frame_status->alive_iter_);
  }

  // Add to front of MFU and store iterator
  mfu_.push_front(frame_id);
  frame_status->alive_iter_ = mfu_.begin();
  frame_status->arc_status_ = ArcStatus::MFU;
}

void ArcReplacer::HandleMruGhostHit(frame_id_t frame_id, page_id_t page_id) {
  auto ghost_status = ghost_map_[page_id];

  // Adapt target size based on ghost list sizes
  const size_t mru_ghost_size = mru_ghost_.size();

  if (const size_t mfu_ghost_size = mfu_ghost_.size(); mru_ghost_size >= mfu_ghost_size) {
    mru_target_size_ = std::min(mru_target_size_ + 1, replacer_size_);
  } else {
    mru_target_size_ = std::min(mru_target_size_ + (mfu_ghost_size / mru_ghost_size), replacer_size_);
  }

  // Remove from MRU ghost list using iterator
  if (ghost_status->ghost_iter_) {
    mru_ghost_.erase(*ghost_status->ghost_iter_);
  }
  ghost_map_.erase(page_id);

  // Create new entry in alive_map_ and add to front of MFU
  const auto new_frame_status = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MFU);
  mfu_.push_front(frame_id);
  new_frame_status->alive_iter_ = mfu_.begin();
  alive_map_[frame_id] = new_frame_status;
  curr_size_++;
}

void ArcReplacer::HandleMfuGhostHit(frame_id_t frame_id, page_id_t page_id) {
  auto ghost_status = ghost_map_[page_id];

  // Adapt target size based on ghost list sizes
  const size_t mru_ghost_size = mru_ghost_.size();

  if (const size_t mfu_ghost_size = mfu_ghost_.size(); mfu_ghost_size >= mru_ghost_size) {
    mru_target_size_ = (mru_target_size_ > 0) ? mru_target_size_ - 1 : 0;
  } else {
    const size_t decrease = mru_ghost_size / mfu_ghost_size;
    mru_target_size_ = (mru_target_size_ >= decrease) ? mru_target_size_ - decrease : 0;
  }

  // Remove from MFU ghost list using iterator
  if (ghost_status->ghost_iter_) {
    mfu_ghost_.erase(*ghost_status->ghost_iter_);
  }
  ghost_map_.erase(page_id);

  // Create new entry in alive_map_ and add to front of MFU
  const auto new_frame_status = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MFU);
  mfu_.push_front(frame_id);
  new_frame_status->alive_iter_ = mfu_.begin();
  alive_map_[frame_id] = new_frame_status;
  curr_size_++;
}

void ArcReplacer::HandleCacheMiss(frame_id_t frame_id, page_id_t page_id) {
  const size_t mru_size = mru_.size();
  const size_t mru_ghost_size = mru_ghost_.size();
  const size_t mfu_size = mfu_.size();
  const size_t mfu_ghost_size = mfu_ghost_.size();

  // If MRU size + MRU ghost size = replacer size, evict last from MRU ghost
  if (mru_size + mru_ghost_size == replacer_size_) {
    if (!mru_ghost_.empty()) {
      auto ghost_status = ghost_map_[mru_ghost_.back()];
      ghost_map_.erase(mru_ghost_.back());
      mru_ghost_.pop_back();
    }
  }
  // Else if total size = 2 * replacer size, evict last from MFU ghost
  else if (mru_size + mru_ghost_size + mfu_size + mfu_ghost_size == 2 * replacer_size_) {
    if (!mfu_ghost_.empty()) {
      auto ghost_status = ghost_map_[mfu_ghost_.back()];
      ghost_map_.erase(mfu_ghost_.back());
      mfu_ghost_.pop_back();
    }
  }
  // Else just add to front of MRU (no eviction needed)

  // Create new entry in alive_map_ and add to front of MRU
  const auto new_frame_status = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MRU);
  mru_.push_front(frame_id);
  new_frame_status->alive_iter_ = mru_.begin();
  alive_map_[frame_id] = new_frame_status;
  curr_size_++;
}

}  // namespace bustub

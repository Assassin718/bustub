//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <mutex>
#include "common/exception.h"

#include <ctime>

namespace bustub {

LRUKNode::LRUKNode(size_t k, frame_id_t fid, bool is_evictable) 
 : k_(k), fid_(fid), is_evictable_(is_evictable) {}

void LRUKNode::AddHistory(size_t timestamp) {
  if (history_.size() >= k_) {
    history_.pop_back();
  }
  history_.push_front(timestamp);
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  curr_size_ = 0;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
  // TODO(assassin): finish
  {
    LockGuard lock_guard(latch_);
    if (curr_size_ <= 0) {
      return false;
    }
    // 在k次以下链表中寻找
    if (!node_list_less_k_.empty()) {
      auto evict_iter = node_list_less_k_.rbegin();
      while (!evict_iter->GetEvictable()) { ++evict_iter; }
      auto fid = evict_iter->GetFrameId();
      *frame_id = fid;
      node_store_.erase(fid);
      node_list_less_k_.erase((++evict_iter).base());
      --curr_size_;
      return true;
    }
    // 如果k次以下链表不为空，在k次以上的链表中寻找
    if (!node_list_more_k_.empty()) {
      auto evict_iter = node_list_more_k_.begin();
      for (auto iter = node_list_more_k_.begin(); iter != node_list_more_k_.end(); ++iter) {
        if (iter->GetEvictable() && 
            (iter->GetKHistory() < evict_iter->GetKHistory() || evict_iter->GetEvictable() == false)) 
        {
          evict_iter = iter;
        }
      }
      if (evict_iter->GetEvictable()) {
        auto fid = evict_iter->GetFrameId();
        *frame_id = fid;
        node_store_.erase(fid);
        node_list_more_k_.erase(evict_iter);
        --curr_size_;
        return true;
      }
    }
    // 如果都没有找到，返回false
    return false; 
  }
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "invalid frame_id in RecordAccess()\n");
  {
    LockGuard lock_guard(latch_);
    // 如果是新的frame_id，则新建一个entry
    if (node_store_.find(frame_id) == node_store_.end()) {
      node_list_less_k_.emplace_front(k_, frame_id, true);
      auto iter = node_list_less_k_.begin();
      iter->AddHistory(current_timestamp_++);
      node_store_[frame_id] = iter;
      ++curr_size_;
    // 否则更新access time
    } else {
      auto iter = node_store_[frame_id];
      if (iter->GetHistorySize() < k_ - 1) {
        node_list_less_k_.splice(node_list_less_k_.begin(), node_list_less_k_, iter);
      } else if (iter->GetHistorySize() == k_ - 1) {
        node_list_more_k_.splice(node_list_more_k_.end(), node_list_less_k_, iter); 
      } 
      iter->AddHistory(current_timestamp_++);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  {
    LockGuard lock_guard(latch_);
    BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "invalid frame_id in SetEvicetable()");
    auto iter = node_store_[frame_id];
    bool evictable = iter->GetEvictable();
    iter->SetEvictable(set_evictable);
    if (evictable ^ set_evictable) {
      curr_size_ = set_evictable ? curr_size_ + 1 : curr_size_ - 1;
    }
  }
}

void LRUKReplacer::RecordAccessAndSetEvictable(frame_id_t frame_id, bool set_evictable, [[maybe_unused]] AccessType access_type) {
  {
    LockGuard lock_guard(latch_);
    // 如果是新的frame_id，则新建一个entry
    if (node_store_.find(frame_id) == node_store_.end()) {
      node_list_less_k_.emplace_front(k_, frame_id, true);
      auto iter = node_list_less_k_.begin();
      iter->AddHistory(current_timestamp_++);
      node_store_[frame_id] = iter;
      ++curr_size_;
      iter->SetEvictable(set_evictable);
    // 否则更新access time
    } else {
      auto iter = node_store_[frame_id];
      if (iter->GetHistorySize() < k_ - 1) {
        node_list_less_k_.splice(node_list_less_k_.begin(), node_list_less_k_, iter);
      } else if (iter->GetHistorySize() == k_ - 1) {
        node_list_more_k_.splice(node_list_more_k_.end(), node_list_less_k_, iter); 
      } 
      iter->AddHistory(current_timestamp_++);
      bool evictable = iter->GetEvictable();
      iter->SetEvictable(set_evictable);
      if (evictable ^ set_evictable) {
        curr_size_ = set_evictable ? curr_size_ + 1 : curr_size_ - 1;
      }
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  {
    LockGuard lock_guard(latch_);
    BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "invalid frame_id in SetEvicetable()");
    auto found = node_store_.find(frame_id);
    if (found == node_store_.end()) {
      return;
    }
    auto iter = found->second;
    bool evictable = iter->GetEvictable();
    BUSTUB_ASSERT(evictable, "frame is not evictable in Remove()");
    if (iter->GetHistorySize() >= k_) {
      node_list_more_k_.erase(iter);
    } else {
      node_list_less_k_.erase(iter);
    }
    --curr_size_;
  }
}

auto LRUKReplacer::Size() -> size_t {
  {
    LockGuard lock_guard(latch_);
    return curr_size_;
  }
}

}  // namespace bustub



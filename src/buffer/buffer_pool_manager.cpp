//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * { 
  // 1.get a frame (no other thread can access this frame)
  // 2.get new page id 
  frame_id_t fid = -1;
  page_id_t pid = -1;
  bool success = false;
  {
    LockGuard guard(latch_);
    success = GetFreeFrameLF(&fid);
    if (success) {
      pid = AllocatePage();
    }
  }
  if (!success || pid == -1) {
    page_id = nullptr;
    return nullptr; 
  }
  BUSTUB_ASSERT(fid != -1, "error, allocated frame id is -1");
  BUSTUB_ASSERT(pid != -1, "error, allocated page id is -1");

  // write back dirty page, reset memory and metadata
  Page* alloc_page = pages_ + fid;
  if (alloc_page->IsDirty()) {
    disk_manager_->WritePage(alloc_page->GetPageId(), alloc_page->GetData());
  }
  alloc_page->ResetMemory();
  // set alloc_page and build map
  alloc_page->page_id_ = pid;
  alloc_page->pin_count_ = 1;
  *page_id = pid;
  {
    LockGuard guard(latch_);
    page_table_.insert({pid, fid});
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
  }
  return alloc_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // find in page_table_
  frame_id_t fid = -1;
  bool success = false;
  {
    LockGuard guard(latch_);
    auto found = page_table_.find(page_id);
    if (found != page_table_.end()) {
      return &pages_[found->second];
    }
    success = GetFreeFrameLF(&fid);
  }
  // if not in page_table_, get a free frame to replace
  if (!success) {
    return nullptr;
  }
  // write back dirty page, read page_id page
  Page* alloc_page = pages_ + fid;
  if (alloc_page->IsDirty()) {
    disk_manager_->WritePage(alloc_page->GetPageId(), alloc_page->GetData());
  }
  disk_manager_->ReadPage(page_id, alloc_page->GetData());
  // set alloc_page and build map
  alloc_page->page_id_ = page_id;
  ++alloc_page->pin_count_;
  {
    LockGuard guard(latch_);
    page_table_.insert({page_id, fid});
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
  }
  return alloc_page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  {
    LockGuard guard(latch_);
    auto found = page_table_.find(page_id);
    if (found == page_table_.end()) {
      return false;
    }
    frame_id_t fid = found->second;
    Page* page = &pages_[fid];
    if (page->GetPinCount() == 0) {
      return false;
    }
    page->pin_count_ -= 1;
    if (page->GetPinCount() == 0) {
      replacer_->SetEvictable(fid, true);
    }
    if (is_dirty) {
      page->is_dirty_ = true;
    }
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  {
    LockGuard guard(latch_);
    auto found = page_table_.find(page_id);
    if (found == page_table_.end()) {
      return false;
    }
    disk_manager_->WritePage(page_id, pages_[found->second].GetData());
    pages_[found->second].is_dirty_ = false;
    return true; 
  }
}

void BufferPoolManager::FlushAllPages() {
  {
    LockGuard guard(latch_);
    for (size_t i = 0; i < pool_size_; ++i) {
      Page* page = pages_ + i;
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { 
  {
    LockGuard guard(latch_);
    auto found = page_table_.find(page_id);
    if (found == page_table_.end()) {
      return true;
    }
    frame_id_t fid = found->second;
    if (pages_[fid].GetPinCount() > 0) {
      return false;
    }
    page_table_.erase(page_id);
    replacer_->Remove(fid);
    free_list_.push_back(fid);
    pages_[fid].ResetMemory();
    pages_[fid].pin_count_ = 0;
    pages_[fid].is_dirty_ = false;
  }
  return false; 
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::GetFreeFrameLF(frame_id_t* frame_id) -> bool {
  bool success = false;
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    success = true;
  } else {
    success = replacer_->Evict(frame_id);
    if (success) {
      Page* alloc_page = pages_ + *frame_id;
      page_table_.erase(alloc_page->GetPageId());
    }
  }   
  return success;
}

}  // namespace bustub

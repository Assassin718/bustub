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
  frame_id_t frame_id = -1;
  page_id_t page_id_new = -1;
  bool success = false;
  {
    LockGuard guard(latch_);
    success = GetFreeFrameLF(&frame_id);
  }
  if (success) {
    page_id_new = AllocatePage();
  }
  if (!success || page_id_new == -1) {
    *page_id = -1;
    return nullptr; 
  }
  BUSTUB_ASSERT(frame_id != -1, "error, allocated frame id is -1");
  BUSTUB_ASSERT(page_id_new != -1, "error, allocated page id is -1");

  // write back dirty page, reset memory and metadata
  Page* alloc_page = pages_ + frame_id;
  BUSTUB_ASSERT(alloc_page->GetPinCount() == 0, "error, allocated page pin count is not 0");
  if (alloc_page->IsDirty()) {
    disk_manager_->WritePage(alloc_page->GetPageId(), alloc_page->GetData());
  }
  alloc_page->ResetMemory();
  // set alloc_page and build map
  alloc_page->page_id_ = page_id_new;
  alloc_page->pin_count_ = 1;
  alloc_page->is_dirty_ = false;

  *page_id = page_id_new;
  replacer_->RecordAccessAndSetEvictable(frame_id, false);
  {
    LockGuard guard(latch_);
    page_table_.insert({page_id_new, frame_id});
  }
  return alloc_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // find in page_table_
  frame_id_t frame_id = -1;
  bool success = false;
  Page* alloc_page = nullptr;
  {
    LockGuard guard(latch_);
// if page_id is already in page_table_, just add pin count and return
    auto found = page_table_.find(page_id);
    if (found != page_table_.end()) {
      frame_id = found->second;
      alloc_page = pages_ + frame_id; 
      ++alloc_page->pin_count_;
      replacer_->RecordAccessAndSetEvictable(frame_id, false);
      return alloc_page;
    }

// if page_id is not in page_table_, find a free frame to replace
    success = GetFreeFrameLF(&frame_id);
  }
  // if no free frame, return nullptr
  if (!success) {
    return alloc_page;
  }
  // write back dirty page, read page_id page
  alloc_page = pages_ + frame_id;
  if (alloc_page->IsDirty()) {
    disk_manager_->WritePage(alloc_page->GetPageId(), alloc_page->GetData());
  }
  disk_manager_->ReadPage(page_id, alloc_page->GetData());
  // set alloc_page and build map
  alloc_page->page_id_ = page_id;
  alloc_page->pin_count_ = 1;
  alloc_page->is_dirty_ = false;
  replacer_->RecordAccessAndSetEvictable(frame_id, false);
  {
    LockGuard guard(latch_);
    page_table_.insert({page_id, frame_id});
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
    frame_id_t frame_id = found->second;
    Page* page = pages_ + frame_id;
    if (page->GetPinCount() == 0) {
      return false;
    }
    page->pin_count_ -= 1;
    if (page->GetPinCount() == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    page->is_dirty_ = is_dirty;
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
    frame_id_t frame_id = found->second;
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
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
    frame_id_t frame_id = found->second;
    Page* page = pages_ + frame_id;
    if (page->GetPinCount() > 0) {
      return false;
    }
    if (page->IsDirty()) {
      disk_manager_->WritePage(page_id, page->GetData());
    }
    page_table_.erase(page_id);
    replacer_->Remove(frame_id);
    free_list_.push_back(frame_id);
    page->ResetMemory();
    page->pin_count_ = 0;
    page->is_dirty_ = false;
  }
  return true; 
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { 
  Page* page = FetchPage(page_id);
  BasicPageGuard page_guard(this, page);
  return page_guard; 
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { 
  Page* page = FetchPage(page_id);
  ReadPageGuard page_guard(this, page);
  page->RLatch();
  return {this, page}; 
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { 
  Page* page = FetchPage(page_id);
  WritePageGuard page_guard(this, page);
  page->WLatch();
  return {this, page}; 
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { 
  Page* page = NewPage(page_id);
  BasicPageGuard page_guard(this, page);
  return {this, page}; 
}

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

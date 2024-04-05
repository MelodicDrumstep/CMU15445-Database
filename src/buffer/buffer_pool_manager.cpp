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

#include <mutex>
#include <sstream>
#include <string>
#include <utility>

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "common/macros.h"
#include "include/common/config.h"
#include "include/common/logger.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/page_guard.h"

namespace bustub
{

BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     DiskManager* disk_manager,
                                     size_t replacer_k, LogManager* log_manager)
    : pool_size_(pool_size),
      disk_manager_(disk_manager),
      log_manager_(log_manager)
{
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i)
  {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t* page_id) -> Page*
{
  frame_id_t frame_to_set;
  page_id_t dirty_page_id;
  bool dirty_flag{false};

  std::lock_guard<std::mutex> lock(latch_);
  if (!free_list_.empty())
  {  //  If I can get a free frame, use the free list first
    frame_to_set = this->free_list_.front();
    free_list_.pop_front();
  }
  else
  {
    if (replacer_->Evict(&frame_to_set))
    {  // Now I have to evict
      dirty_page_id = pages_[frame_to_set].page_id_;
      page_table_.erase(dirty_page_id);
      if (pages_[frame_to_set].is_dirty_)
      {
        dirty_flag = true;
      }
    }
    else
    {
      return nullptr;
    }
  }

  *page_id = AllocatePage();
  replacer_->RecordAccess(frame_to_set);
  replacer_->SetEvictable(frame_to_set, false);
  auto new_page = &pages_[frame_to_set];
  new_page->is_dirty_ = false;
  new_page->page_id_ = *page_id;
  new_page->pin_count_ = 1;
  page_table_.insert(std::make_pair(*page_id, frame_to_set));

  if (dirty_flag)
  {
    disk_manager_->WritePage(dirty_page_id, pages_[frame_to_set].data_);
  }
  return new_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id,
                                  [[maybe_unused]] AccessType access_type)
    -> Page*
{
  frame_id_t frame_to_fetch;
  bool dirty_flag{false};
  page_id_t dirty_page_id;

  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) != page_table_.end())
  {  // buffer pool hit! Then I do not need disk IO
     // Just fetch it from the buffer pool
    frame_to_fetch = page_table_.at(page_id);
    replacer_->RecordAccess(frame_to_fetch);  // record an access
    replacer_->SetEvictable(frame_to_fetch, false);
    pages_[frame_to_fetch].pin_count_++;
    auto fetch_page = &pages_[frame_to_fetch];
    return fetch_page;
  }
  if (!free_list_.empty())
  {  // use the free list
    frame_to_fetch = free_list_.front();
    free_list_.pop_front();
  }
  else
  {
    if (replacer_->Evict(&frame_to_fetch))
    {
      dirty_page_id = pages_[frame_to_fetch].page_id_;
      page_table_.erase(dirty_page_id);
      if (pages_[frame_to_fetch].is_dirty_)
      {
        dirty_flag = true;
      }
    }
    else
    {
      return nullptr;
    }
  }
  replacer_->RecordAccess(frame_to_fetch);
  replacer_->SetEvictable(frame_to_fetch, false);
  auto fetch_page = &pages_[frame_to_fetch];
  fetch_page->is_dirty_ = false;
  fetch_page->page_id_ = page_id;
  fetch_page->pin_count_ = 1;
  page_table_.insert(std::make_pair(page_id, frame_to_fetch));

  if (dirty_flag)
  {
    disk_manager_->WritePage(dirty_page_id, pages_[frame_to_fetch].data_);
  }
  disk_manager_->ReadPage(page_id, pages_[frame_to_fetch].data_);
  return fetch_page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty,
                                  [[maybe_unused]] AccessType access_type)
    -> bool
{
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end())
  {
    return false;
  }
  auto frame_to_unpin = page_table_.at(page_id);
  if (pages_[frame_to_unpin].pin_count_ <= 0)
  {
    return false;
  }
  pages_[frame_to_unpin].pin_count_--;
  if (pages_[frame_to_unpin].pin_count_ <= 0)
  {
    replacer_->SetEvictable(frame_to_unpin, true);
  }
  if (is_dirty)
  {
    pages_[frame_to_unpin].is_dirty_ = true;
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool
{
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end())
  {
    return false;
  }

  auto frame_to_flush = page_table_.at(page_id);
  disk_manager_->WritePage(page_id, pages_[frame_to_flush].data_);
  pages_[frame_to_flush].is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages()
{
  std::lock_guard<std::mutex> lock(latch_);
  for (auto it : page_table_)
  {
    FlushPage(it.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool
{
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end())
  {
    return true;
  }
  auto frame_to_delete = page_table_.at(page_id);
  if (pages_[frame_to_delete].pin_count_ != 0)
  {
    return false;
  }
  // write back to disk
  if (pages_[frame_to_delete].is_dirty_)
  {
    disk_manager_->WritePage(pages_[frame_to_delete].page_id_,
                             pages_[frame_to_delete].data_);
  }
  page_table_.erase(page_id);
  replacer_->Remove(frame_to_delete);
  free_list_.push_back(frame_to_delete);
  pages_[frame_to_delete].page_id_ = INVALID_PAGE_ID;
  pages_[frame_to_delete].is_dirty_ = false;
  pages_[frame_to_delete].pin_count_ = 0;

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

// PageGuard
auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard
{
  auto fetch_page = FetchPage(page_id);
  return {this, fetch_page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard
{
  auto fetch_page = FetchPage(page_id);
  return {this, fetch_page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard
{
  auto fetch_page = FetchPage(page_id);
  return {this, fetch_page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t* page_id,
                                       AccessType access_type) -> BasicPageGuard
{
  auto fetch_page = NewPage(page_id);
  return {this, fetch_page};
}

}  // namespace bustub
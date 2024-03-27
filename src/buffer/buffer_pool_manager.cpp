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

void BufferPoolManager::Write_back_if_Dirty(frame_id_t frame_id)
{
  //Now I want to evict this frame from the pool
  //If the page relating to it is dirty
  //I will write back the page to disk
    page_id_t old_page_id = pages_[frame_id].page_id_;
    //get the old page id for the argument of the WritePage function
    
    if(pages_[frame_id].IsDirty())
    {
      disk_manager_ -> WritePage(old_page_id, pages_[frame_id].data_);
      pages_[frame_id].is_dirty_ = false;
    }
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page *
{
  std::lock_guard<std::mutex> guard(latch_);

  frame_id_t frame_id; 
  //Determine the frame_id this page should reside

  if(!free_list_.empty())
  { //I can choose a frame from the free list
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  else
  {
    //I must choose a frame from the replacer
    if(replacer_ -> Evict(&frame_id) == false)
    {
      page_id = nullptr;
      return nullptr;
    }

    //What I have to do in this funciton:
    //Write_back if Dirty
    
    Write_back_if_Dirty(frame_id);
  }

  replacer_ -> RecordAccess(frame_id);
  replacer_ -> SetEvictable(frame_id, false);
  //access the frame and set it to non-evictable

  page_id_t new_page_id_ = AllocatePage();
  //std::cout << "new_page_id_ = " << new_page_id_ << std::endl;

  pages_[frame_id].ResetMemory();
  //reset the memory for this new page
  pages_[frame_id].page_id_ = new_page_id_;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  *page_id = new_page_id_;
  page_table_[new_page_id_] = frame_id;
  //Set the page_table

  return &pages_[frame_id];

}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * 
{
  if(page_table_.find(page_id) != page_table_.end())
  //I can find the page id in the buffer pool
  {
    frame_id_t frame_id = page_table_[page_id];
    return &pages_[frame_id];
  }

  else // I cannot find the page id
  {
    std::lock_guard<std::mutex> guard(latch_);

    frame_id_t frame_id = -1;
    //Determine the frame_id this page should reside

    if(!free_list_.empty())
    { //I can choose a frame from the free list
      frame_id = free_list_.front();
      free_list_.pop_front();
    }
    else
    {
      //I must choose a frame from the replacer
      if(replacer_ -> Evict(&frame_id) == false)
      {
        return nullptr;
      }

      //What I have to do in this funciton:
      //Write_back if Dirty
      Write_back_if_Dirty(frame_id);
    }

    replacer_ -> RecordAccess(frame_id);
    replacer_ -> SetEvictable(frame_id, false);
    //access the frame and set it to non-evictable

    char * data = nullptr;

    disk_manager_ -> ReadPage(page_id, data);

    pages_[frame_id].ResetMemory();
    //reset the memory for this new page

    pages_[frame_id].data_ = data;
    page_table_[page_id] = frame_id;
    
    //Set the page_table

    return &pages_[frame_id];
  }

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool 
{
  frame_id_t frame_id = page_table_[page_id];
  // Now I have to unpin page_id
  //Firstly I get the frame_id

  Page * page = &pages_[frame_id];
  //And I get the page for information

  if(page == nullptr)
  {
    return false;
  }

  page -> WLatch(); //lock
  if(page -> pin_count_ <= 0)
  {
    return false;
  }
  page -> pin_count_--;
  if(page -> is_dirty_ == 0)
  {
    page -> is_dirty_ = is_dirty;
  }

  if(page -> pin_count_ == 0)
  {
    replacer_ -> SetEvictable(frame_id, true);
  }

  replacer_ -> RecordAccess(frame_id); //access it

  page -> WUnlatch(); //unlock
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool
{
  if(page_table_.find(page_id) == page_table_.end())
  {//Can not find it in the page_table
    return false;
  }
  else
  {
    disk_manager_ -> WritePage(page_id, pages_[page_table_[page_id]].data_);
    frame_id_t frame_id = page_table_[page_id];
    pages_[frame_id].is_dirty_ = false;

  }
  return true;
}

void BufferPoolManager::FlushAllPages() 
{
  //Just use FlushPage to implement it
  std::lock_guard<std::mutex> guard(latch_);
  for(auto page_id : page_table_)
  {
    FlushPage(page_id.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool
{
  //Delete the page and 
  //erase all the footprint it has in 
  //replacer or page_table
  //remember to add the frame to free_list
  std::lock_guard<std::mutex> guard(latch_);
  auto it = page_table_.find(page_id);
  if(it == page_table_.end())
  {
    return true;
  }
  frame_id_t frame_id = (*it).second;
  Page * page = &pages_[frame_id];
  if(page -> pin_count_ != 0)
  {
    return false;
  }
  Write_back_if_Dirty(frame_id);

  page_table_.erase(page_id);
  replacer_ -> Remove(frame_id);
  free_list_.push_back(frame_id);
  page -> page_id_ = INVALID_PAGE_ID;
  page -> is_dirty_ = false;
  page -> pin_count_ = 0;

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub

/*
class BufferPoolManager {
 public:
  
   * @brief Creates a new BufferPoolManager.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param replacer_k the LookBack constant k for the LRU-K replacer
   * @param log_manager the log manager (for testing only: nullptr = disable logging). Please ignore this for P1.
   
  BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k = LRUK_REPLACER_K,
                    LogManager *log_manager = nullptr);

  
   * @brief Destroy an existing BufferPoolManager.
   
  ~BufferPoolManager();

   @brief Return the size (number of frames) of the buffer pool. 
  auto GetPoolSize() -> size_t { return pool_size_; }

   @brief Return the pointer to all the pages in the buffer pool. 
  auto GetPages() -> Page * { return pages_; }

  
   * TODO(P1): Add implementation
   *
   * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
   * are currently in use and not evictable (in another word, pinned).
   *
   * You should pick the replacement frame from either the free list or the replacer (always find from the free list
   * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
   * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
   *
   * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
   * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
   * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
   *
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   
  auto NewPage(page_id_t *page_id) -> Page *;

  
   * TODO(P1): Add implementation
   *
   * @brief PageGuard wrapper for NewPage
   *
   * Functionality should be the same as NewPage, except that
   * instead of returning a pointer to a page, you return a
   * BasicPageGuard structure.
   *
   * @param[out] page_id, the id of the new page
   * @return BasicPageGuard holding a new page
   
  auto NewPageGuarded(page_id_t *page_id) -> BasicPageGuard;

  
   * TODO(P1): Add implementation
   *
   * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
   * but all frames are currently in use and not evictable (in another word, pinned).
   *
   * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
   * the replacer (always find from the free list first), read the page from disk by scheduling a read DiskRequest with
   * disk_scheduler_->Schedule(), and replace the old page in the frame. Similar to NewPage(), if the old page is dirty,
   * you need to write it back to disk and update the metadata of the new page
   *
   * In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
   *
   * @param page_id id of page to be fetched
   * @param access_type type of access to the page, only needed for leaderboard tests.
   * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
   
  auto FetchPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> Page *;

  
   * TODO(P1): Add implementation
   *
   * @brief PageGuard wrappers for FetchPage
   *
   * Functionality should be the same as FetchPage, except
   * that, depending on the function called, a guard is returned.
   * If FetchPageRead or FetchPageWrite is called, it is expected that
   * the returned page already has a read or write latch held, respectively.
   *
   * @param page_id, the id of the page to fetch
   * @return PageGuard holding the fetched page
   
  auto FetchPageBasic(page_id_t page_id) -> BasicPageGuard;
  auto FetchPageRead(page_id_t page_id) -> ReadPageGuard;
  auto FetchPageWrite(page_id_t page_id) -> WritePageGuard;

  
   * TODO(P1): Add implementation
   *
   * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
   * 0, return false.
   *
   * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
   * Also, set the dirty flag on the page to indicate if the page was modified.
   *
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @param access_type type of access to the page, only needed for leaderboard tests.
   * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
   
  auto UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type = AccessType::Unknown) -> bool;

  
   * TODO(P1): Add implementation
   *
   * @brief Flush the target page to disk.
   *
   * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
   * Unset the dirty flag of the page after flushing.
   *
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   
  auto FlushPage(page_id_t page_id) -> bool;

  
   * TODO(P1): Add implementation
   *
   * @brief Flush all the pages in the buffer pool to disk.
   
  void FlushAllPages();

  
   * TODO(P1): Add implementation
   *
   * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
   * page is pinned and cannot be deleted, return false immediately.
   *
   * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
   * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
   * imitate freeing the page on the disk.
   *
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   
  auto DeletePage(page_id_t page_id) -> bool;

 private:
   Number of pages in the buffer pool. 
  const size_t pool_size_;
   The next page id to be allocated  
  std::atomic<page_id_t> next_page_id_ = 0;

   Array of buffer pool pages. 
  Page *pages_;
   Pointer to the disk sheduler. 
  std::unique_ptr<DiskScheduler> disk_scheduler_ __attribute__((__unused__));
   Pointer to the log manager. Please ignore this for P1. 
  LogManager *log_manager_ __attribute__((__unused__));
   Page table for keeping track of buffer pool pages. 
  std::unordered_map<page_id_t, frame_id_t> page_table_;
   Replacer to find unpinned pages for replacement. 
  std::unique_ptr<LRUKReplacer> replacer_;
   List of free frames that don't have any pages on them. 
  std::list<frame_id_t> free_list_;
   This latch protects shared data structures. We recommend updating this comment to describe what it protects. 
  std::mutex latch_;
   This buffer is for the leaderboard task. You may want to use it to optimize the write requests. 
  WriteBackCache write_back_cache_ __attribute__((__unused__));

  
   * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
   * @return the id of the allocated page
   
  auto AllocatePage() -> page_id_t;

  
   * @brief Deallocate a page on disk. Caller should acquire the latch before calling this function.
   * @param page_id id of the page to deallocate
   
  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track deallocated pages
  }

  // TODO(student): You may add additional private members and helper functions
};
*/
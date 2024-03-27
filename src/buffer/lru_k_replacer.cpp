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
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(frame_id_t frame_id, size_t cur, size_t k) : fid_(frame_id), k_(k)
{
    history_.push_back(cur);
}

//lhs < rhs means lhs should lie in the left of rhs
bool operator<(const std::shared_ptr<LRUKNode> &lhs, const std::shared_ptr<LRUKNode> &rhs)
{
    size_t k_ = lhs -> k_;
    if(lhs -> history_.size() < k_ && rhs -> history_.size() < k_)
    {
        return lhs -> history_.back() < rhs -> history_.back();
    }

    else if(lhs -> history_.size() < k_)
    {
        return true;
    }

    else if(rhs -> history_.size() < k_)
    {
        return false;
    }
    else
    {
        return lhs -> history_.front() < rhs -> history_.front();
    }

}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : max_replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool 
{
    current_timestamp_++;
    std::lock_guard<std::mutex> guard(latch_);
    if(replacer_size_ == 0 || replacer_list.empty())
    {
        return false;
    }
    
    auto it = replacer_list.begin();

    *frame_id = (*it) -> fid_;
    frame2node_map_.erase(*frame_id);
    
    replacer_list.erase(it);
    replacer_size_--;
    return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) 
{
    current_timestamp_++;
    //BUSTUB_ASSERT(frame_id <= replacer_size_, "invalid");
    std::lock_guard<std::mutex> guard(latch_);    

    auto it = frame2node_map_.find(frame_id);
    //check if it has been accessed

    if(it == frame2node_map_.end())
    {
        //it's new, let's insert it into the map
        //And it should be non-evictable
        std::shared_ptr<LRUKNode> new_node = std::make_shared<LRUKNode>(frame_id, current_timestamp_, k_);
        frame2node_map_[frame_id] = new_node;
    }
    else
    {
        auto current_node = frame2node_map_[frame_id];
        //it's existing
        if(current_node -> history_.size() == k_)
        {
            current_node -> history_.pop_front();
        }
        current_node -> history_.push_back(current_timestamp_);
        //update the timestamp of the exsiting node

        if(current_node -> is_evictable_)
        {
            //if it's evictable, we need to update the replacer_list
            auto it2 = frame2node_map_.find(frame_id);
            BUSTUB_ASSERT(it2 != frame2node_map_.end(), "invalid");
            //it's evictable, so it should be in the replacer_list

            for(auto it3 = replacer_list.begin(); it3 != replacer_list.end(); it3++)
            {
                if((*it3) -> fid_ == frame_id)
                {
                    replacer_list.erase(it3);
                    break;
                }
            }

            // < is overloaded that means the less one should lie in the left
            //of the list
            auto it3 = replacer_list.begin();
            while(it3 != replacer_list.end() && *it3 < current_node)
            {
                it3++;
            }
            //This is the correct position to insert the current_node
            replacer_list.insert(it3, current_node);
        }
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable)
{
  current_timestamp_++;
 // BUSTUB_ASSERT(frame_id <= replacer_size_, "invalid");
  auto it = frame2node_map_.find(frame_id);
  BUSTUB_ASSERT(it != frame2node_map_.end(), "invalid");

  std::lock_guard<std::mutex> guard(latch_);

  if(set_evictable)
  {
    if(it -> second -> is_evictable_)
    {
      return;
    }
    else
    {
      //it's set to evictable and I have to add it to the 
      //replacer_list
      it -> second -> is_evictable_ = true;
      replacer_size_++;
      BUSTUB_ASSERT(replacer_size_ <= max_replacer_size_, "invalid");
      auto it2 = replacer_list.begin();
      while(it2 != replacer_list.end() && *it2 < it -> second)
      {
        it2++;
      }
      std::shared_ptr<LRUKNode> new_node = it -> second;
      replacer_list.insert(it2, new_node);
    }
  }
  else
  {
    //it's set to non-evictable
    //I have to remove it from the replacer_list
    if(it -> second -> is_evictable_)
    {
      it -> second -> is_evictable_ = false;
      replacer_size_--;
      auto it2 = frame2node_map_.find(frame_id);
      BUSTUB_ASSERT(it2 != frame2node_map_.end(), "invalid");

      //erase it from the replacer_list
      for(auto it3 = replacer_list.begin(); it3 != replacer_list.end(); it3++)
      {
        if((*it3) -> fid_ == frame_id)
        {
          replacer_list.erase(it3);
          break;
        }
      }
    }
    else
    {
      return;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) 
{
  current_timestamp_++;
  auto it = frame2node_map_.find(frame_id);
  BUSTUB_ASSERT(it != frame2node_map_.end() && it -> second -> is_evictable_, "invalid");

  std::lock_guard<std::mutex> guard(latch_);

  for(auto it2 = replacer_list.begin(); it2 != replacer_list.end(); it2++)
  {
    if((*it2) -> fid_ == frame_id)
    {
      replacer_list.erase(it2);
      break;
    }
  }

  frame2node_map_.erase(frame_id);

  replacer_size_--;
}

auto LRUKReplacer::Size() -> size_t
{
    return replacer_size_;
}

}  // namespace bustub

/*

class LRUKNode {
public:
  LRUKNode() = default;
  * History of last seen K timestamps of this page. Least recent timestamp stored in front. 
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.
  LRUKNode(frame_id_t frame_id, size_t cur);
  std::queue<size_t> history_;
  frame_id_t fid_;
  bool is_evictable_{false};
};

*
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 
class LRUKReplacer {
 public:
  *
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   
  ~LRUKReplacer() = default;

  
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   
  auto Evict(frame_id_t *frame_id) -> bool;

  
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  
   * TODO(P1): Add implementation
   *
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
   
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   
  void Remove(frame_id_t frame_id);

  
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.

  std::unordered_map<frame_id_t, std::shared_ptr<LRUKNode>> frame2node_map_;
  //map frame to pointer to LRUKNode

  std::unordered_map<frame_id_t, std::optional<typename std::list<LRUKNode>::iterator>> frame2iter_map_;
  //map frame to iterator in replacer_list

  std::list<std::shared_ptr<LRUKNode>> replacer_list;
  //list of pointers to LRUKNode in order of lru_k

  size_t current_timestamp_{0};
  
  size_t replacer_size_;
  size_t k_;
  
  std::mutex latch_;
};

*/
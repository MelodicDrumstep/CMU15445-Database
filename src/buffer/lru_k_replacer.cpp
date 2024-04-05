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

namespace bustub
{

LRUKNode::LRUKNode(frame_id_t frame_id, size_t cur, size_t k)
    : fid_(frame_id), k_(k)
{
  history_.push_back(cur);
}

// lhs < rhs means lhs should lie in the left of rhs
/*
根据Lru_k的规则写这个比较函数，用于evict list。
就是如果一个node访问次数<k， 另外一个>=k ,那一定是小于k的优先被evict.
如果两个都小于k， 用普通lru来判断优先级。
如果都>=k， 就看距离目前最近的第k次访问哪个时间戳更靠前就优先evict谁。
*/
bool operator<(const std::shared_ptr<LRUKNode>& lhs,
               const std::shared_ptr<LRUKNode>& rhs)
{
  size_t k_ = lhs->k_;
  if (lhs->history_.size() < k_ && rhs->history_.size() < k_)
  {
    return lhs->history_.back() < rhs->history_.back();
  }

  else if (lhs->history_.size() < k_)
  {
    return true;
  }

  else if (rhs->history_.size() < k_)
  {
    return false;
  }
  else
  {
    return lhs->history_.front() < rhs->history_.front();
  }
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : max_replacer_size_(num_frames), k_(k)
{
}

auto LRUKReplacer::Evict(frame_id_t* frame_id) -> bool
{
  current_timestamp_++;
  std::lock_guard<std::mutex> guard(latch_);
  if (replacer_size_ == 0 || replacer_list.empty())
  {
    return false;
  }

  auto it = replacer_list.begin();

  *frame_id = (*it)->fid_;
  frame2node_map_.erase(*frame_id);

  replacer_list.erase(it);
  replacer_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id,
                                [[maybe_unused]] AccessType access_type)
{
  // BUSTUB_ASSERT(frame_id <= replacer_size_, "invalid");
  std::lock_guard<std::mutex> guard(latch_);
  current_timestamp_++;

  auto it = frame2node_map_.find(frame_id);
  // check if it has been accessed

  if (it == frame2node_map_.end())
  {
    // it's new, let's insert it into the map
    // And it should be non-evictable
    std::shared_ptr<LRUKNode> new_node =
        std::make_shared<LRUKNode>(frame_id, current_timestamp_, k_);
    frame2node_map_[frame_id] = new_node;
  }
  else
  {
    auto current_node = frame2node_map_[frame_id];
    // it's existing
    if (current_node->history_.size() == k_)
    {
      current_node->history_.pop_front();
    }
    current_node->history_.push_back(current_timestamp_);
    // update the timestamp of the exsiting node

    if (current_node->is_evictable_)
    {
      // if it's evictable, we need to update the replacer_list
      auto it2 = frame2node_map_.find(frame_id);
      BUSTUB_ASSERT(it2 != frame2node_map_.end(), "invalid");
      // it's evictable, so it should be in the replacer_list

      for (auto it3 = replacer_list.begin(); it3 != replacer_list.end(); it3++)
      {
        if ((*it3)->fid_ == frame_id)
        {
          replacer_list.erase(it3);
          break;
        }
      }

      // < is overloaded that means the less one should lie in the left
      // of the list
      auto it3 = replacer_list.begin();
      while (it3 != replacer_list.end() && *it3 < current_node)
      {
        it3++;
      }
      // This is the correct position to insert the current_node
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

  if (set_evictable)
  {
    if (it->second->is_evictable_)
    {
      return;
    }
    else
    {
      // it's set to evictable and I have to add it to the
      // replacer_list
      it->second->is_evictable_ = true;
      replacer_size_++;
      BUSTUB_ASSERT(replacer_size_ <= max_replacer_size_, "invalid");
      auto it2 = replacer_list.begin();
      while (it2 != replacer_list.end() && *it2 < it->second)
      {
        it2++;
      }
      std::shared_ptr<LRUKNode> new_node = it->second;
      replacer_list.insert(it2, new_node);
    }
  }
  else
  {
    // it's set to non-evictable
    // I have to remove it from the replacer_list
    if (it->second->is_evictable_)
    {
      it->second->is_evictable_ = false;
      replacer_size_--;
      auto it2 = frame2node_map_.find(frame_id);
      BUSTUB_ASSERT(it2 != frame2node_map_.end(), "invalid");

      // erase it from the replacer_list
      for (auto it3 = replacer_list.begin(); it3 != replacer_list.end(); it3++)
      {
        if ((*it3)->fid_ == frame_id)
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
  std::lock_guard<std::mutex> guard(latch_);
  auto it = frame2node_map_.find(frame_id);
  BUSTUB_ASSERT(it != frame2node_map_.end() && it->second->is_evictable_,
                "invalid");

  for (auto it2 = replacer_list.begin(); it2 != replacer_list.end(); it2++)
  {
    if ((*it2)->fid_ == frame_id)
    {
      replacer_list.erase(it2);
      break;
    }
  }

  frame2node_map_.erase(frame_id);

  replacer_size_--;
}

auto LRUKReplacer::Size() -> size_t { return replacer_size_; }

}  // namespace bustub

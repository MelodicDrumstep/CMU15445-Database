#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

// Put a new key-value pair into the trie. If the key already exists, overwrite the value.
  // Returns the new trie.

  template <class T>
  auto Trie::Get(std::string_view key) const -> const T * 
  {
  //throw NotImplementedException("Trie::Get is not implemented.");

    if(key.size() == 0)
    {
        return nullptr;
    }

    size_t i = 0;
    std::shared_ptr<const TrieNode> cur = root_;
    
    while(i < key.size() && cur -> children_.find(key[i]) != cur -> children_.end()) 
    //This means that the current node has a child with the key[i]
    {
        cur = cur -> children_.at(key[i]);
        i++;
    }
    if(i == key.size())
    {
        const TrieNodeWithValue<T>* node = dynamic_cast<const TrieNodeWithValue<T>*>(cur.get());
        if(node != nullptr) 
        {
        return ((node -> value_).get());
        } 
        else 
        {
        return nullptr;
        }
    }
    else
    {
        return nullptr;
    }
  }

  
  template <class T>
  auto Trie::Put(std::string_view key, T value) const -> Trie 
  {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //throw NotImplementedException("Trie::Put is not implemented.");


    size_t i = 0;
    std::shared_ptr<const TrieNode> old_cur = root_;
    
    std::shared_ptr<TrieNode> new_root;

    if(root_ == nullptr)
    {
        ////std::cout << "root is null" << std::endl;
        new_root = std::make_shared<TrieNode>();
        old_cur = new_root;
    }
    else
    {
        new_root = root_ -> Clone();
    }

    ////std::cout << "! " << std::endl;

    std::shared_ptr<TrieNode> new_cur = new_root;

    //std::cout << "key size : " << key.size() << std::endl;
    
    //std::cout << " find : " << (old_cur -> children_.find(key[i]) != old_cur -> children_.end()) << std::endl;

    while(i < key.size() && old_cur -> children_.find(key[i]) != old_cur -> children_.end())
    {
        //std::cout << "!!!" << std::endl;
        if(i == key.size() - 1)
        {
        std::shared_ptr<T> ptr2value = std::make_shared<T>(std::move(value));
        std::shared_ptr<TrieNodeWithValue<T>> temp = std::make_shared<TrieNodeWithValue<T>>(ptr2value);
        new_cur -> children_[key[i]] = temp;
        }
        else
        {
        std::shared_ptr<TrieNode> temp = old_cur -> children_.at(key[i]) -> Clone();
        new_cur -> children_[key[i]] = temp;
        new_cur = temp;
        old_cur = old_cur -> children_.at(key[i]);
        }
        i++;
    }
    

    while(i < key.size())
    {
        if(i == key.size() - 1)
        {
        std::shared_ptr<T> ptr2value = std::make_shared<T>(std::move(value));
        std::shared_ptr<TrieNodeWithValue<T>> temp = std::make_shared<TrieNodeWithValue<T>>(ptr2value);
        new_cur -> children_[key[i]] = temp;
        }
        else
        {
        std::shared_ptr<TrieNode> temp = std::make_shared<TrieNode>();
        new_cur -> children_[key[i]] = temp;
        new_cur = temp;
        }
        i++;
    }
    
    return Trie(new_root);

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

  // Remove the key from the trie. If the key does not exist, return the original trie.
  // Otherwise, returns the new trie.
  auto Trie::Remove(std::string_view key) const -> Trie
  {
    
    if(key.size() == 0)
    {
        return *this;
    }

    size_t i = 0;
    std::shared_ptr<const TrieNode> cur = root_;
    std::shared_ptr<TrieNode> new_root = root_ -> Clone();
    std::shared_ptr<TrieNode> new_cur = new_root;

    while(i < key.size() && cur -> children_.find(key[i]) != cur -> children_.end())
    {
        if(i == key.size() - 1)
        {
            if(cur -> children_.at(key[i]) -> is_value_node_)
            {
                if(cur -> children_.at(key[i]) -> children_.size() == 0)
                {
                    new_cur -> children_.erase(key[i]);
                }
                else
                {
                   std::shared_ptr<TrieNode> temp = std::make_shared<TrieNode>(std::move(cur -> children_.at(key[i]) -> children_));
                     new_cur -> children_[key[i]] = temp;
                }
            }
        }
        else
        {
        std::shared_ptr<TrieNode> temp = cur -> children_.at(key[i]) -> Clone();
        new_cur -> children_[key[i]] = temp;
        new_cur = temp;
        cur = cur -> children_.at(key[i]);
        }
        i++;
    }
    return Trie(new_root);
  }

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub

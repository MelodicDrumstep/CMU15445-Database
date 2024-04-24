#include "primer/trie.h"

#include <string_view>

#include "common/exception.h"

#include <stack>

//#define DEBUG

namespace bustub
{

// Put a new key-value pair into the trie. If the key already exists, overwrite
// the value. Returns the new trie.

template <class T>
auto Trie::Get(std::string_view key) const -> const T*
{
	auto temp_node = root_;
  //Now this assignment operator is "move constructor"
  //It will just let temp_node be a shared_ptr pointing to the root node

  if(!temp_node)
  {
    return nullptr;
  }

  for(char c : key)
  {
    if(!temp_node || temp_node -> children_.find(c) == temp_node -> children_.end())
    {
      //This means we can not find the key
      return nullptr;
    }
    //go down. this is move assignment 
    temp_node = temp_node -> children_.at(c);
  }

  if(temp_node -> is_value_node_)
  {
    //Notice: 1.We can only use dynamic_cast to origin pointer or reference type
    //Cannot use it to the std::shared_ptr
    //Now I have to dynamic_cast the ptr to "TrieNodeWithValue * ". 
    //So as to interprete the node as "TrieNodeWithValue" and use its "value_" field.
    auto value_node_ptr = dynamic_cast<const TrieNodeWithValue<T> *>(temp_node.get());
    if(!value_node_ptr)
    {
      return nullptr;
    }
    return value_node_ptr -> value_.get();
    //value_ is std::shared_ptr<T>
  }

  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie
{
  // Note that `T` might be a non-copyable type. Always use `std::move` when
  // creating `shared_ptr` on that value. throw
  // NotImplementedException("Trie::Put is not implemented.");

  std::shared_ptr<TrieNode> temp_node;

  if(!root_)
  {
    temp_node = std::make_shared<TrieNode>();
    //The trie is empty. Create a new node
  }
  else
  {
    temp_node = std::shared_ptr<TrieNode>(std::move(root_ -> Clone()));
  }
  //otherwise, Clone the root
  //Clone() just std::make_unique to DEEP COPY a node. 
  //I can use std::shared_ptr<TrieNode>(std::move(root_ -> Clone()))
  //to convert it into std::shared_ptr

  std::stack<std::shared_ptr<TrieNode>> stk;
  //Use a stack to build the new trie from the bottom to the top.

  for(auto c : key)
  {
    stk.push(temp_node);
    if(!temp_node || temp_node -> children_.find(c) == temp_node -> children_.end())
    {
      temp_node = std::make_shared<TrieNode>();
    }
    else
    {
      temp_node = std::shared_ptr<TrieNode>(std::move(temp_node -> children_.at(c) -> Clone()));
    }
  }

/*
  Warning:
  dynamic_pointer_cast can only convert Derived to Base.
  It cannot convert Base to Derived.
  When I try to convert a shared_ptr<TrieNodeWithValue<T>> to shared_ptr<TrieNode>
  it's always ok
  However, when I try to convert a shared_ptr<TrieNode> to shared_ptr<TrieNodeWithValue<T>>
  It only works when the original pointer is pointing to a TrieNodeWithValue<T> object
  Otherwise, it will return nullptr
  So the following code will not work
*/

  // auto new_temp_node = std::dynamic_pointer_cast<TrieNodeWithValue<T>>(temp_node);
  // new_temp_node -> is_value_node_ = 1;
  // new_temp_node -> value_ = std::make_shared<T>(std::move(value));
  // temp_node = std::dynamic_pointer_cast<TrieNode>(new_temp_node);


  // This is the right way to do it:
  // When I have to convert a Base object to a Derived object
  // I have to CREATE a new Derived object using the Base object
  // rather than cast the pointer
  // Below shows how to do it: std::make_shared<TrieNodeWithValue<T>>(...) will CREATE a new TrieNodeWithValue object
  //Then I command temp_node points to it
  // (Base pointer can point to a Derived object, but it cannot access the Derived object's member)
  temp_node = std::make_shared<TrieNodeWithValue<T>>(temp_node -> children_, std::make_shared<T>(std::move(value)));

  for(int k = key.size() - 1; k >= 0; k--)
  {
    char c = key[k];
    stk.top() -> children_[c] = temp_node;
    temp_node = stk.top();
    stk.pop();
  }

  return Trie(temp_node);
  // You should walk through the trie and create new nodes if necessary. If the
  // node corresponding to the key already exists, you should create a new
  // `TrieNodeWithValue`.
}

// Remove the key from the trie. If the key does not exist, return the original
// trie. Otherwise, returns the new trie.
auto Trie::Remove(std::string_view key) const -> Trie
{
  if(!root_)
  {
    return Trie();
  }

	#ifdef DEBUG
	std::cout << "Remove() is called!" << std::endl;
	std::cout << "key is " << key << std::endl;
	std::cout << "key size is " << key.size() << std::endl;
	std::cout << "And root has value? " << root_ -> is_value_node_ << std::endl;
	#endif

	bool temp_bool = root_ -> is_value_node_;
  std::shared_ptr<TrieNode> temp_node = std::shared_ptr<TrieNode>(std::move(root_ -> Clone()));
	temp_node -> is_value_node_ = temp_bool;

	#ifdef DEBUG
	std::cout << "temp_node has value? " << temp_node -> is_value_node_ << std::endl;
	#endif

  if(key.size() == 0)
  {
    #ifdef DEBUG
    std::cout << "the keysize is 0! " << std::endl << "And the is_value_node : " << temp_node -> is_value_node_ << std::endl; 
    #endif

    if(!temp_node -> is_value_node_)
    {
      return *this;
    }
    else
    {
      temp_node = std::make_shared<TrieNode>(temp_node -> children_);
      return Trie(temp_node);
    }
  }

  std::stack<std::shared_ptr<TrieNode>> stk;
  //Use a stack to build the new trie from the bottom to the top.

  for(auto c : key)
  {
    stk.push(temp_node);
    if(!temp_node || temp_node -> children_.find(c) == temp_node -> children_.end())
    {
      temp_node = nullptr;
      break;
    }
    else
    {
      temp_node = std::shared_ptr<TrieNode>(std::move(temp_node -> children_.at(c) -> Clone()));
    }
  }

  if(!temp_node || !temp_node -> is_value_node_)
  {
    return Trie(root_);
  }
  else
  {
    // if(temp_node -> children_.empty())
    // {
    //   temp_node = nullptr;
    // }
    //else
    //{
      //temp_node -> is_value_node_ = 0; 
      temp_node = std::make_shared<TrieNode>(temp_node -> children_);
    //}
  }

  for(int k = key.size() - 1; k >= 0; k--)
  {
    char c = key[k];
    stk.top() -> children_[c] = temp_node;
    temp_node = stk.top();
    stk.pop();
  }

  return Trie(temp_node);
}

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t*;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t*;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string*;

// If your solution cannot compile for non-copy tests, you can remove the below
// lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer*;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked*;

}  // namespace bustub

#include "storage/index/b_plus_tree.h"

#include <sstream>
#include <string>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub
{

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id,
                          BufferPoolManager* buffer_pool_manager,
                          const KeyComparator& comparator, int leaf_max_size,
                          int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id)
{
  WritePageGuard guard = bpm_ -> FetchPageWrite(header_page_id_);
  // In the original bpt, I fetch the header page
  // thus there's at least one page now
  auto root_header_page = guard.template AsMut<BPlusTreeHeaderPage>();
  // reinterprete the data of the page into "HeaderPage"
  root_header_page -> root_page_id_ = INVALID_PAGE_ID;
  // set the root_id to INVALID
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const  ->  bool
{
  ReadPageGuard guard = bpm_ -> FetchPageRead(header_page_id_);
  auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
  bool is_empty = root_header_page -> root_page_id_ == INVALID_PAGE_ID;
  // Just check if the root_page_id is INVALID
  // usage to fetch a page:
  // fetch the page guard   ->   call the "As" function of the page guard
  // to reinterprete the data of the page as "BPlusTreePage"
  return is_empty;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType& key,
                              std::vector<ValueType>* result, Transaction* txn)
     ->  bool
{
  // Find the leaf node
  ReadPageGuard head_guard = bpm_ -> FetchPageRead(header_page_id_);
  if (head_guard.template As<BPlusTreeHeaderPage>() -> root_page_id_ ==
      INVALID_PAGE_ID)
  {
    return false;
  }
  ReadPageGuard guard =
      bpm_ -> FetchPageRead(head_guard.As<BPlusTreeHeaderPage>() -> root_page_id_);
  head_guard.Drop();
  auto tmp_page = guard.template As<BPlusTreePage>();
  while (!tmp_page -> IsLeafPage())
  {
    int slot_num = BS(key, tmp_page, true);  // binary search
    if (slot_num == -1)
    {
      return false;
    }
    guard = bpm_ -> FetchPageRead(
        reinterpret_cast<const InternalPage*>(tmp_page) -> ValueAt(slot_num));
    tmp_page = guard.template As<BPlusTreePage>();
  }
  auto* leaf_page = reinterpret_cast<const LeafPage*>(tmp_page);

  // check the key
  int slot_num = BS(key, leaf_page, false);
  if (slot_num != -1)
  {
    result -> push_back(leaf_page -> ValueAt(slot_num));
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BS(const KeyType& key, const BPlusTreePage* bp_page,
                        bool internaltype)  ->  int
{
  if (!internaltype)
  {  // case 1 : leaf page
    auto leaf_page = reinterpret_cast<const LeafPage*>(bp_page);
    int start = 0;
    int end = leaf_page -> GetSize() - 1;
    while (start <= end)
    {
      int slot_num = (start + end) / 2;
      int val = comparator_(key, leaf_page -> KeyAt(slot_num));
      if (val == 0)
      {
        return slot_num;
      }
      if (val > 0)
      {
        start = slot_num + 1;
      }
      else
      {
        end = slot_num - 1;
      }
    }
    return -1;
  }
  else
  {  // internal page
    auto internal_page = reinterpret_cast<const InternalPage*>(bp_page);
    int start = 1;
    int end = internal_page -> GetSize() - 1;
    while (start <= end)
    {
      int slot_num = (start + end) / 2;
      if (comparator_(key, internal_page -> KeyAt(slot_num)) < 0)
      {
        if (slot_num == start)
        {
          return start - 1;
        }
        if (comparator_(key, internal_page -> KeyAt(slot_num - 1)) >= 0)
        {
          return slot_num - 1;
        }
        end = slot_num - 1;
      }
      else
      {
        if (slot_num == end)
        {
          return end;
        }
        if (comparator_(key, internal_page -> KeyAt(slot_num + 1)) < 0)
        {
          return slot_num;
        }
        start = slot_num + 1;
      }
    }
  }
  return -1;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

/*
------------------------- Improved_Crabbing_Insertion function --------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
*/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Improved_Crabbing_Insertion(const KeyType& key, const ValueType& value,
                           int & insert_slot, MappingType & inserted_pair, MappingType & tmp, Transaction* txn)  ->  bool
{
  ReadPageGuard head_read_guard = bpm_ -> FetchPageRead(header_page_id_);

  auto head_read_page = head_read_guard.As<BPlusTreeHeaderPage>();
  if (head_read_page -> root_page_id_ != INVALID_PAGE_ID)
  {
    ReadPageGuard read_guard = bpm_ -> FetchPageRead(head_read_page -> root_page_id_);
    WritePageGuard leaf_crab_guard;
    auto curr_read_page = read_guard.template As<InternalPage>();
    // get the next write guard
    if (curr_read_page -> IsLeafPage())
    {
      read_guard.Drop();
      leaf_crab_guard = bpm_ -> FetchPageWrite(head_read_page -> root_page_id_);
      head_read_guard.Drop();
    }
    else
    {
      head_read_guard.Drop();
      KeyType vice_key;
      vice_key.SetFromInteger(1);
      bool already_found{false};
      while (!already_found)
      {
        int slot_num = BS(key, curr_read_page, true);
        if (comparator_(curr_read_page -> KeyAt(0), vice_key) != 0)
        {
          read_guard = bpm_ -> FetchPageRead(curr_read_page -> ValueAt(slot_num));
          curr_read_page = read_guard.template As<InternalPage>();
        }
        else
        {
          leaf_crab_guard =
              bpm_ -> FetchPageWrite(curr_read_page -> ValueAt(slot_num));
          read_guard.Drop();
          already_found = true;
        }
      }
    }

    auto leaf_crab_page = leaf_crab_guard.AsMut<LeafPage>();
    if (BS(key, leaf_crab_page, false) != -1)
    {
      return false;
    }

    if (leaf_crab_page -> GetSize() < leaf_crab_page -> GetMaxSize())
    {  // This is the case that no split is happening, I can insert without any
       // hesitation
      for (int i = 0; i < leaf_crab_page -> GetSize(); i++)
      {
        if (comparator_(key, leaf_crab_page -> KeyAt(i)) < 0 && insert_slot == -1)
        {
          insert_slot = i;
        }
        if (insert_slot != -1)
        {
          tmp = {leaf_crab_page -> KeyAt(i), leaf_crab_page -> ValueAt(i)};
          leaf_crab_page -> SetAt(i, inserted_pair.first, inserted_pair.second);
          inserted_pair = tmp;
        }
      }
      leaf_crab_page -> IncreaseSize(1);
      leaf_crab_page -> SetAt(leaf_crab_page -> GetSize() - 1, inserted_pair.first,
                            inserted_pair.second);
      return true;
    }
    leaf_crab_guard.Drop();
  }
  else
  {
    head_read_guard.Drop();
  }
  return false;
}



/*
------------------------- Split function --------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
*/


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(LeafPage * leaf_page, const KeyType& key, const ValueType& value,
                            int & insert_slot, Context & my_context, page_id_t & leaf_page_id, 
                            MappingType & inserted_pair, MappingType & tmp, 
                            WritePageGuard && leaf_guard, Transaction* txn) -> bool
{
  // Now I have to split
  KeyType split_key;        // Which key I insert into the splitting page
  page_id_t split_page_id;  // this is the new page for the split

  BasicPageGuard new_page_guard = bpm_ -> NewPageGuarded(&split_page_id);
  // get a new page and fetch it (get the write latch)
  auto split_guard = bpm_ -> FetchPageWrite(split_page_id);
  new_page_guard.Drop();

  auto split_leaf_page = split_guard.template AsMut<LeafPage>();
  split_leaf_page -> Init(leaf_max_size_);
  split_leaf_page -> SetSize((leaf_page -> GetMaxSize() + 1) -
                           (leaf_page -> GetMaxSize() + 1) / 2);

  for (int i = 0; i < leaf_page -> GetMaxSize(); i++)
  {
    //find the place that all these n element should be inserted
    //When this loop is finished, the page split is done
    //every element will reside in LHS(split_leaf_page) or RHS(leaf_page)
    if (comparator_(key, leaf_page -> KeyAt(i)) < 0 && insert_slot == -1)
    {
      insert_slot = i;
      //This will find the first element that is larger than the inserting key
    }
    if (insert_slot != -1 || i >= (leaf_page -> GetMaxSize() + 1) / 2)
    {
      if (i < (leaf_page -> GetMaxSize() + 1) / 2)
      { 
        tmp = {leaf_page -> KeyAt(i), leaf_page -> ValueAt(i)};
        leaf_page -> SetAt(i, inserted_pair.first, inserted_pair.second);
        inserted_pair = tmp;
      }
      else
      { 
        if (insert_slot != -1)
        {
          tmp = {leaf_page -> KeyAt(i), leaf_page -> ValueAt(i)};
          split_leaf_page -> SetAt(i - (leaf_page -> GetMaxSize() + 1) / 2,
                                 inserted_pair.first, inserted_pair.second);
          inserted_pair = tmp;
        }
        else
        {
          split_leaf_page -> SetAt(i - (leaf_page -> GetMaxSize() + 1) / 2,
                                 leaf_page -> KeyAt(i), leaf_page -> ValueAt(i));
        }
      }
    }
  }

  //Now I finish the splitting, I have to change the pointers
  leaf_page -> SetSize((leaf_page -> GetMaxSize() + 1) / 2);
  split_leaf_page -> SetAt(split_leaf_page -> GetSize() - 1, inserted_pair.first, inserted_pair.second);
  split_key = split_leaf_page -> KeyAt(0);
  split_leaf_page -> SetNextPageId(leaf_page -> GetNextPageId());
  leaf_page -> SetNextPageId(split_page_id);
  leaf_guard.Drop();
  split_guard.Drop();

  // Now I have to recursively upload the parents
  page_id_t new_split_page_id;
  bool corner_case = true;
  while (my_context.write_set_.size() > 1)
  {
    BasicPageGuard new_page_guard = bpm_ -> NewPageGuarded(&new_split_page_id);
    WritePageGuard split_guard = bpm_ -> FetchPageWrite(new_split_page_id); 
    new_page_guard.Drop();
    WritePageGuard parent_guard = std::move(my_context.write_set_.back());
    my_context.write_set_.pop_back();
    leaf_page_id = parent_guard.PageId();
    auto parent_page = parent_guard.template AsMut<InternalPage>();
    auto split_page = split_guard.template AsMut<InternalPage>();
    split_page -> Init(internal_max_size_);
    split_page -> SetSize((parent_page -> GetMaxSize() + 1) -
                        (parent_page -> GetMaxSize() / 2 + 1) - 1 + 1);
    int insert_slot = -1;
    KeyType tmp_key;
    page_id_t tmp_page_id;
    KeyType inserted_key = split_key;
    page_id_t inserted_page_id = split_page_id;
    KeyType new_split_key = split_key;
    for (int i = 1; i < parent_page -> GetMaxSize(); i++)
    {
      if (comparator_(split_key, parent_page -> KeyAt(i)) < 0 &&
          insert_slot == -1)
      { 
        insert_slot = i;
      }
      if (insert_slot != -1 || i >= (parent_page -> GetMaxSize() / 2 + 1))
      { 
        if (i < ((parent_page -> GetMaxSize()) / 2 + 1))
        {
          tmp_key = parent_page -> KeyAt(i);
          tmp_page_id = parent_page -> ValueAt(i);
          parent_page -> SetKeyAt(i, inserted_key);
          parent_page -> SetValueAt(i, inserted_page_id);
          inserted_key = tmp_key;
          inserted_page_id = tmp_page_id;
        }
        else if (i == (parent_page -> GetMaxSize() / 2 + 1))
        {
          if (insert_slot != -1)
          {
            new_split_key = inserted_key;
            split_page -> SetValueAt(0, inserted_page_id);
            inserted_key = parent_page -> KeyAt(i);
            inserted_page_id = parent_page -> ValueAt(i);
          }
          else
          {
            new_split_key = parent_page -> KeyAt(i);
            split_page -> SetValueAt(0, parent_page -> ValueAt(i));
          }
        }
        else
        {
          if (insert_slot != -1)
          {
            split_page -> SetKeyAt(i - (parent_page -> GetMaxSize() / 2 + 1), inserted_key);
            split_page -> SetValueAt(i - (parent_page -> GetMaxSize() / 2 + 1), inserted_page_id);
            inserted_key = parent_page -> KeyAt(i);
            inserted_page_id = parent_page -> ValueAt(i);
          }
          else
          {
            split_page -> SetKeyAt(i - (parent_page -> GetMaxSize() / 2 + 1),
                                 parent_page -> KeyAt(i));
            split_page -> SetValueAt(i - (parent_page -> GetMaxSize() / 2 + 1),
                                   parent_page -> ValueAt(i));
          }
        }
      }
    }
    parent_page -> SetSize(parent_page -> GetMaxSize() / 2 + 1);
    split_page -> SetKeyAt(split_page -> GetSize() - 1, inserted_key);
    split_page -> SetValueAt(split_page -> GetSize() - 1, inserted_page_id);

    //Fix a small corner case :
    //The first splitting internal page have its [0] element unset
    if (corner_case)
    {
      KeyType vice_key;
      vice_key.SetFromInteger(1);
      split_page -> SetKeyAt(0, vice_key);
      corner_case = false;
    }

    split_page_id = new_split_page_id;
    split_key = new_split_key;
    parent_guard.Drop();
    split_guard.Drop();
  }

  if (my_context.write_set_.front().PageId() == header_page_id_)
  { //This means we arrive at the header page : we have to reconstruct a root
    //This happens when the root also splits
    //because if the root does not split, the header page will not be in the context
    auto root_header_page = my_context.write_set_.front().template AsMut<BPlusTreeHeaderPage>();
    page_id_t root_page_id;
    BasicPageGuard new_page_guard = bpm_ -> NewPageGuarded(&root_page_id);  //
    root_header_page -> root_page_id_ = root_page_id;
    WritePageGuard guard = bpm_ -> FetchPageWrite(root_page_id); 

    new_page_guard.Drop(); 
    my_context.write_set_.front().Drop();
    auto root_page = guard.template AsMut<InternalPage>();
    root_page -> Init(internal_max_size_);
    root_page -> IncreaseSize(1);
    root_page -> SetKeyAt(1, split_key);
    root_page -> SetValueAt(1, split_page_id);
    root_page -> SetValueAt(0, leaf_page_id);

    //also fix it here: we only have the root and the root is full
    if (corner_case)
    {
      KeyType vice_key;
      vice_key.SetFromInteger(1);
      root_page -> SetKeyAt(0, vice_key);
      corner_case = false;
    }
    guard.Drop();
    return true;
  }

  //For the last one, specialize it (maybe root splits already happen)
  //By testing, If I do not specialize this I will get wrong answer
  WritePageGuard parent_guard = std::move(my_context.write_set_.back());
  auto parent_page = parent_guard.template AsMut<InternalPage>();
  my_context.write_set_.pop_back();
  KeyType inserted_key = split_key;
  page_id_t inserted_page_id = split_page_id;
  insert_slot = BS(split_key, parent_page, true) + 1;
  parent_page -> IncreaseSize(1);
  for (int i = parent_page -> GetSize() - 1; i > insert_slot; i--)
  {
    parent_page -> SetKeyAt(i, parent_page -> KeyAt(i - 1));
    parent_page -> SetValueAt(i, parent_page -> ValueAt(i - 1));
  }
  parent_page -> SetKeyAt(insert_slot, inserted_key);
  parent_page -> SetValueAt(insert_slot, inserted_page_id);
  parent_guard.Drop();
  return true;
}

/*
------------------------- Crabbing_Insertion function -----------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
*/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Crabbing_Insertion(const KeyType& key, const ValueType& value,
                            int & insert_slot, Context & my_context, 
                            MappingType & inserted_pair, MappingType & tmp, Transaction* txn)  ->  bool
{
  WritePageGuard head_write_guard = bpm_ -> FetchPageWrite(header_page_id_);

  auto root_header_page = head_write_guard.template As<BPlusTreeHeaderPage>();
  if (root_header_page -> root_page_id_ == INVALID_PAGE_ID)
  {
    page_id_t root_page_id;
    BasicPageGuard new_page_guard = bpm_ -> NewPageGuarded(&root_page_id);
    WritePageGuard root_guard = bpm_ -> FetchPageWrite(root_page_id);
    new_page_guard.Drop();
    auto root_header_page = head_write_guard.template AsMut<BPlusTreeHeaderPage>();
    root_header_page -> root_page_id_ = root_page_id;
    head_write_guard.Drop();
    auto root_page = root_guard.template AsMut<LeafPage>();
    root_page -> Init(leaf_max_size_);
    root_page -> IncreaseSize(1);
    root_page -> SetAt(0, key, value);
    root_guard.Drop();
    return true;
  }
  
  //Now I have to get the write latch 
  WritePageGuard root_guard = bpm_ -> FetchPageWrite(root_header_page -> root_page_id_);
  my_context.write_set_.push_back(std::move(head_write_guard));
  auto tmp_page = root_guard.template As<InternalPage>();
  if (tmp_page -> GetSize() < tmp_page -> GetMaxSize())
  {
    my_context.write_set_.clear();
  }
  my_context.write_set_.push_back(std::move(root_guard));
  while (!tmp_page -> IsLeafPage())
  {
    WritePageGuard guard;
    int slot_num = BS(key, tmp_page, true);
    guard = bpm_ -> FetchPageWrite(tmp_page -> ValueAt(slot_num));
    tmp_page = guard.template As<InternalPage>();
    if (tmp_page -> GetSize() < tmp_page -> GetMaxSize())
    {
      my_context.write_set_.clear();
    }
    my_context.write_set_.push_back(std::move(guard));
  }
  auto leaf_guard = std::move(my_context.write_set_.back());
  auto leaf_page_id = leaf_guard.PageId();
  auto leaf_page_not_mut = leaf_guard.template As<LeafPage>();
  my_context.write_set_.pop_back();

  if (BS(key, leaf_page_not_mut, false) != -1)
  {
    return false;
  }
  auto leaf_page = leaf_guard.template AsMut<LeafPage>();
  if (leaf_page -> GetSize() < leaf_page -> GetMaxSize())  // check if it split
  {                                                    // if not, just insert
    for (int i = 0; i < leaf_page -> GetSize(); i++)
    {
      if (comparator_(key, leaf_page -> KeyAt(i)) < 0 && insert_slot == -1)
      {
        insert_slot = i;
      }
      if (insert_slot != -1)
      {
        tmp = {leaf_page -> KeyAt(i), leaf_page -> ValueAt(i)};
        leaf_page -> SetAt(i, inserted_pair.first, inserted_pair.second);
        inserted_pair = tmp;
      }
    }
    leaf_page -> IncreaseSize(1);
    leaf_page -> SetAt(leaf_page -> GetSize() - 1, inserted_pair.first, inserted_pair.second);

    return true;
  }

  return Split(leaf_page, key, value, insert_slot, 
              my_context, leaf_page_id, inserted_pair, 
              tmp, std::move(leaf_guard), txn);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType& key, const ValueType& value,
                            Transaction* txn)  ->  bool
{
  Context my_context;
  my_context.write_set_.clear();
  MappingType tmp;
  MappingType inserted_pair = {key, value};
  int insert_slot = -1;

  // Firstly, use "improved Crabbing_Insertion protocol", only fetch the read latch

  if(Improved_Crabbing_Insertion(key, value, insert_slot, inserted_pair, tmp, txn))
  {
    return true;
  }

  // If the program reaches here, the improved Crabbing_Insertion protocol lose
  // I have to resort to the normal Crabbing_Insertion protocol
  return Crabbing_Insertion(key, value, insert_slot, my_context, inserted_pair, tmp, txn);
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType& key, Transaction* txn)
{
  //Use basic Crabbing 
  Context my_context;
  my_context.write_set_.clear();
  Crabbing_Deletion(key, my_context, txn);
}

/*
------------------------------------------Recursive_Deletion----------------------------------------
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
*/

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Recursive_Deletion(const KeyType& key, Context & my_context, LeafPage * leaf_page, 
                                  WritePageGuard && leaf_guard, KeyType & key_for_locate, Transaction* txn)
{
  //Recursive deletion from the context
  //(the context store all dangerous pages, 
  // dangerous meaning it's at most half full and may need borrowing or merging)

  auto parent_guard = std::move(my_context.write_set_.back());
  my_context.write_set_.pop_back();
  auto parent_page = parent_guard.template AsMut<InternalPage>();
  int parent_index = -1;

  //Find which index of the parent of the deleting page 
  //should I modify
  for (int i = 1; i < parent_page -> GetSize(); i++)
  {
    if (comparator_(key, parent_page -> KeyAt(i)) < 0)
    {
      parent_index = i - 1;
      break;
    }
  }

    // Now I encounter the LeafPage
  Handle_Underflow_Leaf(key, my_context, leaf_page, std::move(leaf_guard), key_for_locate, parent_index, parent_page, std::move(parent_guard), txn);

    //Handle for the parent : the core of recursive deletion
  Handle_Underflow_Parent(key, my_context, key_for_locate, txn);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Handle_Underflow_Parent(const KeyType& key, Context & my_context, KeyType & key_for_locate, Transaction* txn)
{
  while (my_context.write_set_.size() > 1)
  {  
    WritePageGuard temp_guard = std::move(my_context.write_set_.back());
    my_context.write_set_.pop_back();
    WritePageGuard parent_guard = std::move(my_context.write_set_.back());
    my_context.write_set_.pop_back();

    auto tmp_page = temp_guard.template AsMut<InternalPage>();
    auto parent_page = parent_guard.template AsMut<InternalPage>();

    if (parent_guard.PageId() == header_page_id_)
    {
      auto header_page = reinterpret_cast<BPlusTreeHeaderPage*>(parent_page);
      header_page -> root_page_id_ = tmp_page -> ValueAt(0);
      temp_guard.Drop();
      return;
    }
    int parent_index = -1;
    if (tmp_page -> GetSize() != 1)
    {
      key_for_locate = tmp_page -> KeyAt(1);
    }
    for (int i = 1; i < parent_page -> GetSize(); i++)
    {
      if (comparator_(key_for_locate, parent_page -> KeyAt(i)) < 0)
      {
        parent_index = i - 1;
        break;
      }
    }
    key_for_locate = parent_page -> KeyAt(1);
    WritePageGuard right_sibling_guard;
    WritePageGuard left_sibling_guard;
    if (parent_index == 0)
    {
      // This means that there's no left brother
      right_sibling_guard = bpm_ -> FetchPageWrite(parent_page -> ValueAt(parent_index + 1));
      auto right_sibling_page = right_sibling_guard.template AsMut<InternalPage>();
      if (right_sibling_page -> GetSize() > right_sibling_page -> GetMinSize())
      {
        // If I can do this I will borrow from the right brother first
        tmp_page -> IncreaseSize(1);
        tmp_page -> SetKeyAt(tmp_page -> GetSize() - 1,
                            parent_page -> KeyAt(parent_index + 1));
        tmp_page -> SetValueAt(tmp_page -> GetSize() - 1,
                              right_sibling_page -> ValueAt(0));
        parent_page -> SetKeyAt(parent_index + 1,
                              right_sibling_page -> KeyAt(1));
        for (int i = 1; i < right_sibling_page -> GetSize(); i++)
        {
          if (i != 1)
          {
            right_sibling_page -> SetKeyAt(i - 1, right_sibling_page -> KeyAt(i));
          }
          right_sibling_page -> SetValueAt(i - 1, right_sibling_page -> ValueAt(i));
        }
        right_sibling_page -> IncreaseSize(-1);
        return;
      }
      // otherwise, merge with the right brother
      int i = tmp_page -> GetSize();
      tmp_page -> IncreaseSize(right_sibling_page -> GetSize());
      tmp_page -> SetKeyAt(i, parent_page -> KeyAt(parent_index + 1));
      tmp_page -> SetValueAt(i, right_sibling_page -> ValueAt(0));
      for (int j = 1; j < right_sibling_page -> GetSize(); i++, j++)
      {
        tmp_page -> SetKeyAt(i + 1, right_sibling_page -> KeyAt(j));
        tmp_page -> SetValueAt(i + 1, right_sibling_page -> ValueAt(j));
      }
      right_sibling_guard.Drop();
      for (int i = parent_index + 2; i < parent_page -> GetSize(); i++)
      {
        parent_page -> SetKeyAt(i - 1, parent_page -> KeyAt(i));
        parent_page -> SetValueAt(i - 1, parent_page -> ValueAt(i));
      }
      parent_page -> IncreaseSize(-1);
    }
    else if (parent_index == -1)
    {
      // There's no right brother
      parent_index = parent_page -> GetSize() - 1;
      left_sibling_guard =
          bpm_ -> FetchPageWrite(parent_page -> ValueAt(parent_index - 1));
      auto left_sibling_page = left_sibling_guard.template AsMut<InternalPage>();
      if (left_sibling_page -> GetSize() > left_sibling_page -> GetMinSize())
      {
        // Borrow from the left brother
        tmp_page -> IncreaseSize(1);
        for (int i = tmp_page -> GetSize() - 1; i >= 1; i--)
        {
          if (i != 1)
          {
            tmp_page -> SetKeyAt(i, tmp_page -> KeyAt(i - 1));
          }
          tmp_page -> SetValueAt(i, tmp_page -> ValueAt(i - 1));
        }
        tmp_page -> SetKeyAt(1, parent_page -> KeyAt(parent_index));
        tmp_page -> SetValueAt(
            0, left_sibling_page -> ValueAt(left_sibling_page -> GetSize() - 1));
        parent_page -> SetKeyAt(
            parent_index,
            left_sibling_page -> KeyAt(left_sibling_page -> GetSize() - 1));
        left_sibling_page -> IncreaseSize(-1);
        return;
      }
      // merge with the left brother
      int i = left_sibling_page -> GetSize();
      left_sibling_page -> IncreaseSize(tmp_page -> GetSize());
      left_sibling_page -> SetKeyAt(i, parent_page -> KeyAt(parent_index));
      left_sibling_page -> SetValueAt(i, tmp_page -> ValueAt(0));
      for (int j = 1; j < tmp_page -> GetSize(); i++, j++)
      {
        left_sibling_page -> SetKeyAt(i + 1, tmp_page -> KeyAt(j));
        left_sibling_page -> SetValueAt(i + 1, tmp_page -> ValueAt(j));
      }
      temp_guard.Drop();
      for (int i = parent_index + 1; i < parent_page -> GetSize(); i++)
      {
        parent_page -> SetKeyAt(i - 1, parent_page -> KeyAt(i));
        parent_page -> SetValueAt(i - 1, parent_page -> ValueAt(i));
      }
      parent_page -> IncreaseSize(-1);
    }
    else
    {
      // Both right and left brother resides
      left_sibling_guard = bpm_ -> FetchPageWrite(parent_page -> ValueAt(parent_index - 1));
      right_sibling_guard = bpm_ -> FetchPageWrite(parent_page -> ValueAt(parent_index + 1));
      auto left_sibling_page = left_sibling_guard.template AsMut<InternalPage>();
      auto right_sibling_page = right_sibling_guard.template AsMut<InternalPage>();
      if (right_sibling_page -> GetSize() > right_sibling_page -> GetMinSize())
      {
        // Borrow from the right brother
        tmp_page -> IncreaseSize(1);
        tmp_page -> SetKeyAt(tmp_page -> GetSize() - 1,
                            parent_page -> KeyAt(parent_index + 1));
        tmp_page -> SetValueAt(tmp_page -> GetSize() - 1,
                              right_sibling_page -> ValueAt(0));
        parent_page -> SetKeyAt(parent_index + 1,
                              right_sibling_page -> KeyAt(1));
        for (int i = 1; i < right_sibling_page -> GetSize(); i++)
        {
          if (i != 1)
          {
            right_sibling_page -> SetKeyAt(i - 1, right_sibling_page -> KeyAt(i));
          }
          right_sibling_page -> SetValueAt(i - 1, right_sibling_page -> ValueAt(i));
        }
        right_sibling_page -> IncreaseSize(-1);
        return;
      }
      if (left_sibling_page -> GetSize() > left_sibling_page -> GetMinSize())
      {
        // Borrow from the left brother
        tmp_page -> IncreaseSize(1);
        for (int i = tmp_page -> GetSize() - 1; i >= 1; i--)
        {
          if (i != 1)
          {
            tmp_page -> SetKeyAt(i, tmp_page -> KeyAt(i - 1));
          }
          tmp_page -> SetValueAt(i, tmp_page -> ValueAt(i - 1));
        }
        tmp_page -> SetKeyAt(1, parent_page -> KeyAt(parent_index));
        tmp_page -> SetValueAt(0, left_sibling_page -> ValueAt(left_sibling_page -> GetSize() - 1));
        parent_page -> SetKeyAt(
            parent_index,
            left_sibling_page -> KeyAt(left_sibling_page -> GetSize() - 1));
        left_sibling_page -> IncreaseSize(-1);
        return;
      }
      // Merge with the right brother or left brother
      int i = tmp_page -> GetSize();
      tmp_page -> IncreaseSize(right_sibling_page -> GetSize());
      tmp_page -> SetKeyAt(i, parent_page -> KeyAt(parent_index + 1));
      tmp_page -> SetValueAt(i, right_sibling_page -> ValueAt(0));
      for (int j = 1; j < right_sibling_page -> GetSize(); i++, j++)
      {
        tmp_page -> SetKeyAt(i + 1, right_sibling_page -> KeyAt(j));
        tmp_page -> SetValueAt(i + 1, right_sibling_page -> ValueAt(j));
      }
      right_sibling_guard.Drop();
      for (int i = parent_index + 2; i < parent_page -> GetSize(); i++)
      {
        parent_page -> SetKeyAt(i - 1, parent_page -> KeyAt(i));
        parent_page -> SetValueAt(i - 1, parent_page -> ValueAt(i));
      }
      parent_page -> IncreaseSize(-1);
    }
    my_context.write_set_.push_back(std::move(parent_guard));
  }
  my_context.write_set_.front().Drop();
  //finish the job!
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Handle_Underflow_Leaf(const KeyType& key, Context & my_context, LeafPage * leaf_page, 
                                  WritePageGuard && leaf_guard, KeyType & key_for_locate, int parent_index,
                                  InternalPage * parent_page, WritePageGuard && parent_guard, Transaction* txn)
{
  WritePageGuard right_sibling_guard;
  WritePageGuard left_sibling_guard;
  if (parent_index == 0)
  {
    // This means that there's no left brother
    right_sibling_guard = bpm_ -> FetchPageWrite(parent_page -> ValueAt(parent_index + 1));
    auto right_sibling_page = right_sibling_guard.template AsMut<LeafPage>();
    if (right_sibling_page -> GetSize() > right_sibling_page -> GetMinSize())
    {
      // If I can do this I will borrow from the right brother
      leaf_page -> IncreaseSize(1);
      leaf_page -> SetAt(leaf_page -> GetSize() - 1, right_sibling_page -> KeyAt(0),
                       right_sibling_page -> ValueAt(0));
      parent_page -> SetKeyAt(parent_index + 1, right_sibling_page -> KeyAt(1));
      for (int i = 1; i < right_sibling_page -> GetSize(); i++)
      {
        right_sibling_page -> SetAt(i - 1, right_sibling_page -> KeyAt(i),
                             right_sibling_page -> ValueAt(i));
      }
      right_sibling_page -> IncreaseSize(-1);
      return;
    }
    // merge with the right brother
    leaf_page -> SetNextPageId(right_sibling_page -> GetNextPageId());
    int i = leaf_page -> GetSize();
    leaf_page -> IncreaseSize(right_sibling_page -> GetSize());
    for (int j = 0; j < right_sibling_page -> GetSize(); i++, j++)
    {
      leaf_page -> SetAt(i, right_sibling_page -> KeyAt(j), right_sibling_page -> ValueAt(j));
    }
    right_sibling_guard.Drop();
    for (int i = parent_index + 2; i < parent_page -> GetSize(); i++)
    {
      parent_page -> SetKeyAt(i - 1, parent_page -> KeyAt(i));
      parent_page -> SetValueAt(i - 1, parent_page -> ValueAt(i));
    }
    parent_page -> IncreaseSize(-1);
  }
  else if (parent_index == -1)
  {
    // This means there's no right brother
    parent_index = parent_page -> GetSize() - 1;
    left_sibling_guard =
        bpm_ -> FetchPageWrite(parent_page -> ValueAt(parent_index - 1));
    auto left_sibling_page = left_sibling_guard.template AsMut<LeafPage>();
    if (left_sibling_page -> GetSize() > left_sibling_page -> GetMinSize())
    {
      // If I can do this I will borrow from the left brother
      leaf_page -> IncreaseSize(1);
      for (int i = leaf_page -> GetSize() - 1; i >= 1; i--)
      {
        leaf_page -> SetAt(i, leaf_page -> KeyAt(i - 1), leaf_page -> ValueAt(i - 1));
      }
      leaf_page -> SetAt(0, left_sibling_page -> KeyAt(left_sibling_page -> GetSize() - 1),
                       left_sibling_page -> ValueAt(left_sibling_page -> GetSize() - 1));
      parent_page -> SetKeyAt(parent_index,
                            left_sibling_page -> KeyAt(left_sibling_page -> GetSize() - 1));
      left_sibling_page -> IncreaseSize(-1);
      return;
    }
    // Merge with the left brother
    left_sibling_page -> SetNextPageId(leaf_page -> GetNextPageId());
    int i = left_sibling_page -> GetSize();
    left_sibling_page -> IncreaseSize(leaf_page -> GetSize());
    for (int j = 0; j < leaf_page -> GetSize(); i++, j++)
    {
      left_sibling_page -> SetAt(i, leaf_page -> KeyAt(j), leaf_page -> ValueAt(j));
    }
    leaf_guard.Drop();
    for (int i = parent_index + 1; i < parent_page -> GetSize(); i++)
    {
      parent_page -> SetKeyAt(i - 1, parent_page -> KeyAt(i));
      parent_page -> SetValueAt(i - 1, parent_page -> ValueAt(i));
    }
    parent_page -> IncreaseSize(-1);
  }
  else
  {
    // Now I encounter the case that there exist both the left brother and the
    // right one
    left_sibling_guard = bpm_ -> FetchPageWrite(parent_page -> ValueAt(parent_index - 1));
    right_sibling_guard = bpm_ -> FetchPageWrite(parent_page -> ValueAt(parent_index + 1));
    auto left_sibling_page = left_sibling_guard.template AsMut<LeafPage>();
    auto right_sibling_page = right_sibling_guard.template AsMut<LeafPage>();
    if (right_sibling_page -> GetSize() > right_sibling_page -> GetMinSize())
    {
      // First try to borrow from the right brother
      leaf_page -> IncreaseSize(1);
      leaf_page -> SetAt(leaf_page -> GetSize() - 1, right_sibling_page -> KeyAt(0),
                       right_sibling_page -> ValueAt(0));
      parent_page -> SetKeyAt(parent_index + 1, right_sibling_page -> KeyAt(1));
      for (int i = 1; i < right_sibling_page -> GetSize(); i++)
      {
        right_sibling_page -> SetAt(i - 1, right_sibling_page -> KeyAt(i),
                             right_sibling_page -> ValueAt(i));
      }
      right_sibling_page -> IncreaseSize(-1);
      return;
    }
    if (left_sibling_page -> GetSize() > left_sibling_page -> GetMinSize())
    {
      // If it failed, try to borrow from the left brother
      leaf_page -> IncreaseSize(1);
      for (int i = leaf_page -> GetSize() - 1; i >= 1; i--)
      {
        leaf_page -> SetAt(i, leaf_page -> KeyAt(i - 1), leaf_page -> ValueAt(i - 1));
      }
      leaf_page -> SetAt(0, left_sibling_page -> KeyAt(left_sibling_page -> GetSize() - 1),
                       left_sibling_page -> ValueAt(left_sibling_page -> GetSize() - 1));
      parent_page -> SetKeyAt(parent_index,
                            left_sibling_page -> KeyAt(left_sibling_page -> GetSize() - 1));
      left_sibling_page -> IncreaseSize(-1);
      return;
    }
    // If they failed, try to merge with the right brother or the left brother
    leaf_page -> SetNextPageId(right_sibling_page -> GetNextPageId());
    int i = leaf_page -> GetSize();
    leaf_page -> IncreaseSize(right_sibling_page -> GetSize());
    for (int j = 0; j < right_sibling_page -> GetSize(); i++, j++)
    {
      leaf_page -> SetAt(i, right_sibling_page -> KeyAt(j), right_sibling_page -> ValueAt(j));
    }
    right_sibling_guard.Drop();
    for (int i = parent_index + 2; i < parent_page -> GetSize(); i++)
    {
      parent_page -> SetKeyAt(i - 1, parent_page -> KeyAt(i));
      parent_page -> SetValueAt(i - 1, parent_page -> ValueAt(i));
    }
    parent_page->IncreaseSize(-1);
  }

  my_context.write_set_.push_back(std::move(parent_guard));
}

/*
----------------------------------------Crabbing_Deletion-------------------------------------------
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
*/

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Crabbing_Deletion(const KeyType& key, Context & my_context, Transaction* txn)
{

  //fetch the header page
  WritePageGuard header_guard = bpm_ -> FetchPageWrite(header_page_id_);
  auto header_page = header_guard.template AsMut<BPlusTreeHeaderPage>();
  if (header_page -> root_page_id_ == INVALID_PAGE_ID)
  {
    return;
  }

  // normal search, get the write lock each time
  my_context.root_page_id_ = header_page -> root_page_id_;
  WritePageGuard guard = bpm_ -> FetchPageWrite(my_context.root_page_id_);

  auto tmp_page = guard.template AsMut<InternalPage>();
  my_context.write_set_.push_back(std::move(header_guard));
  if ((tmp_page -> IsLeafPage() && tmp_page -> GetSize() >= 2) ||
      tmp_page -> GetSize() >= 3)
  {
    my_context.write_set_.clear();
  }
  my_context.write_set_.push_back(std::move(guard));
  while (!tmp_page -> IsLeafPage())
  {
    WritePageGuard guard;
    for (int i = 1; i < tmp_page -> GetSize(); i++)
    {
      if (comparator_(key, tmp_page -> KeyAt(i)) < 0)
      {
        guard = bpm_ -> FetchPageWrite(tmp_page -> ValueAt(i - 1));
        break;
      }
      if (i + 1 == tmp_page -> GetSize())
      {
        guard = bpm_ -> FetchPageWrite(tmp_page -> ValueAt(i));
      }
    }
    tmp_page = guard.template AsMut<InternalPage>();
    if (tmp_page -> GetSize() > tmp_page -> GetMinSize())
    {
      my_context.write_set_.clear();
    }

    my_context.write_set_.push_back(std::move(guard));
  }
  auto leaf_guard = std::move(my_context.write_set_.back());
  auto leaf_page = leaf_guard.template AsMut<LeafPage>();
  my_context.write_set_.pop_back();

  //Now I arrive at the leaf node
  KeyType key_for_locate = leaf_page -> KeyAt(0);  

  int delete_index = -1;
  //delete_index is where the element I should delete resides
  for (int i = 0; i < leaf_page -> GetSize(); i++)
  {
    if (comparator_(key, leaf_page -> KeyAt(i)) == 0)
    {
      //Find the key to be deleted
      delete_index = i; 
      break;
    }
  }
  if (delete_index == -1)
  {
    //This means I can not find the key (error occurs)
    return;
  }
  
  // The loop will Delete the key (move elements backward)
  for (int i = delete_index + 1; i < leaf_page -> GetSize(); i++)
  {
    leaf_page -> SetAt(i - 1, leaf_page -> KeyAt(i), leaf_page -> ValueAt(i));
  }

  leaf_page -> IncreaseSize(-1);
  if (leaf_page -> GetSize() >= leaf_page -> GetMinSize() ||
      leaf_guard.PageId() == my_context.root_page_id_)
      //This means no borrowing or merging is needed or it's the root
  {
    if (leaf_page -> GetSize() == 0)
    //This means I delete the only element of the tree : now the root is invalid
    {  
      leaf_guard.Drop();
      my_context.write_set_.front().template AsMut<BPlusTreeHeaderPage>()
                                      -> root_page_id_ = INVALID_PAGE_ID;
      my_context.write_set_.pop_front();
    }
    //If no borrowing or merging is needed, return directly
    return;
  }

  Recursive_Deletion(key, my_context, leaf_page, std::move(leaf_guard), key_for_locate, txn);
}


/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
//BS
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinaryFind(const LeafPage* leaf_page, const KeyType& key)
     ->  int
{
  int l = 0;
  int r = leaf_page -> GetSize() - 1;
  while (l < r)
  {
    int mid = (l + r + 1) >> 1;
    if (comparator_(leaf_page -> KeyAt(mid), key) != 1)
    {
      l = mid;
    }
    else
    {
      r = mid - 1;
    }
  }

  if (r >= 0 && comparator_(leaf_page -> KeyAt(r), key) == 1)
  {
    r = -1;
  }

  return r;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinaryFind(const InternalPage* internal_page,
                                const KeyType& key)  ->  int
{
  int l = 1;
  int r = internal_page -> GetSize() - 1;
  while (l < r)
  {
    int mid = (l + r + 1) >> 1;
    if (comparator_(internal_page -> KeyAt(mid), key) != 1)
    {
      l = mid;
    }
    else
    {
      r = mid - 1;
    }
  }

  if (r == -1 || comparator_(internal_page -> KeyAt(r), key) == 1)
  {
    r = 0;
  }

  return r;
}

/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin()  ->  INDEXITERATOR_TYPE
//Just go left forever
{
  ReadPageGuard head_guard = bpm_ -> FetchPageRead(header_page_id_);
  if (head_guard.template As<BPlusTreeHeaderPage>() -> root_page_id_ ==
      INVALID_PAGE_ID)
  {
    return End();
  }
  ReadPageGuard guard =
      bpm_ -> FetchPageRead(head_guard.As<BPlusTreeHeaderPage>() -> root_page_id_);
  head_guard.Drop();

  auto tmp_page = guard.template As<BPlusTreePage>();
  while (!tmp_page -> IsLeafPage())
  {
    int slot_num = 0;
    guard = bpm_ -> FetchPageRead(
        reinterpret_cast<const InternalPage*>(tmp_page) -> ValueAt(slot_num));
    tmp_page = guard.template As<BPlusTreePage>();
  }
  int slot_num = 0;
  if (slot_num != -1)
  {
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), 0);
  }
  return End();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType& key)  ->  INDEXITERATOR_TYPE
{
  ReadPageGuard head_guard = bpm_ -> FetchPageRead(header_page_id_);

  if (head_guard.template As<BPlusTreeHeaderPage>() -> root_page_id_ ==
      INVALID_PAGE_ID)
  {
    return End();
  }
  ReadPageGuard guard =
      bpm_ -> FetchPageRead(head_guard.As<BPlusTreeHeaderPage>() -> root_page_id_);
  head_guard.Drop();
  auto tmp_page = guard.template As<BPlusTreePage>();
  while (!tmp_page -> IsLeafPage())
  {
    auto internal = reinterpret_cast<const InternalPage*>(tmp_page);
    int slot_num = BinaryFind(internal, key);
    if (slot_num == -1)
    {
      return End();
    }
    guard = bpm_ -> FetchPageRead(
        reinterpret_cast<const InternalPage*>(tmp_page) -> ValueAt(slot_num));
    tmp_page = guard.template As<BPlusTreePage>();
  }
  auto* leaf_page = reinterpret_cast<const LeafPage*>(tmp_page);

  int slot_num = BinaryFind(leaf_page, key);
  if (slot_num != -1)
  {
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), slot_num);
  }
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End()  ->  INDEXITERATOR_TYPE
{
  return INDEXITERATOR_TYPE(bpm_, -1, -1);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId()  ->  page_id_t
{
  ReadPageGuard guard = bpm_ -> FetchPageRead(header_page_id_);
  auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = root_header_page -> root_page_id_;
  guard.Drop();
  return root_page_id;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string& file_name,
                                    Transaction* txn)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key)
  {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string& file_name,
                                    Transaction* txn)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key)
  {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string& file_name,
                                      Transaction* txn)
{
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input)
  {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction)
    {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager* bpm)
{
  auto root_page_id = GetRootPageId();
  auto guard = bpm -> FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage* page)
{
  if (page -> IsLeafPage())
  {
    auto* leaf = reinterpret_cast<const LeafPage*>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf -> GetNextPageId()
              << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf -> GetSize(); i++)
    {
      std::cout << leaf -> KeyAt(i);
      if ((i + 1) < leaf -> GetSize())
      {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
  }
  else
  {
    auto* internal = reinterpret_cast<const InternalPage*>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal -> GetSize(); i++)
    {
      std::cout << internal -> KeyAt(i) << ": " << internal -> ValueAt(i);
      if ((i + 1) < internal -> GetSize())
      {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal -> GetSize(); i++)
    {
      auto guard = bpm_ -> FetchPageBasic(internal -> ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager* bpm, const std::string& outf)
{
  if (IsEmpty())
  {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm -> FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage* page,
                             std::ofstream& out)
{
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page -> IsLeafPage())
  {
    auto* leaf = reinterpret_cast<const LeafPage*>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf -> GetSize() << "\">P=" << page_id
        << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf -> GetSize() << "\">"
        << "max_size=" << leaf -> GetMaxSize()
        << ",min_size=" << leaf -> GetMinSize() << ",size=" << leaf -> GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf -> GetSize(); i++)
    {
      out << "<TD>" << leaf -> KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf -> GetNextPageId() != INVALID_PAGE_ID)
    {
      out << leaf_prefix << page_id << "   ->   " << leaf_prefix
          << leaf -> GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix
          << leaf -> GetNextPageId() << "};\n";
    }
  }
  else
  {
    auto* inner = reinterpret_cast<const InternalPage*>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner -> GetSize() << "\">P=" << page_id
        << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner -> GetSize() << "\">"
        << "max_size=" << inner -> GetMaxSize()
        << ",min_size=" << inner -> GetMinSize() << ",size=" << inner -> GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner -> GetSize(); i++)
    {
      out << "<TD PORT=\"p" << inner -> ValueAt(i) << "\">";
      // if (i > 0) {
      out << inner -> KeyAt(i) << "  " << inner -> ValueAt(i);
      // } else {
      // out << inner  ->  ValueAt(0);
      // }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner -> GetSize(); i++)
    {
      auto child_guard = bpm_ -> FetchPageBasic(inner -> ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0)
      {
        auto sibling_guard = bpm_ -> FetchPageBasic(inner -> ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page -> IsLeafPage() && !child_page -> IsLeafPage())
        {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId()
              << " " << internal_prefix << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId()
          << "   ->   ";
      if (child_page -> IsLeafPage())
      {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      }
      else
      {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree()  ->  std::string
{
  if (IsEmpty())
  {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id)
     ->  PrintableBPlusTree
{
  auto root_page_guard = bpm_ -> FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page -> IsLeafPage())
  {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page -> ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page -> ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page -> GetSize(); i++)
  {
    page_id_t child_id = internal_page -> ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
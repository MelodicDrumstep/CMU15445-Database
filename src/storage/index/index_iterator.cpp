/**
 * index_iterator.cpp
 */
#include "storage/index/index_iterator.h"

#include <cassert>

namespace bustub
{

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager,
// B_PLUS_TREE_LEAF_PAGE_TYPE *cur, int index)
//     : cur_(cur), index_(index), bpm_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager* buffer_pool_manager,
                                  page_id_t cur, int index)
{
  bpm_ = buffer_pool_manager;
  index_ = index;
  cur_ = cur;
  if (cur != -1)  // This means we have to fetch the page so as to get the
  // content : the key and the value stored in this page ("index" tells us which
  // is it)
  {
    auto guard = bpm_->FetchPageRead(cur);
    auto leaf = guard.template As<LeafPage>();
    item_ = {leaf->KeyAt(index), leaf->ValueAt(index)};
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return cur_ == -1; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType& { return item_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE&
{
  index_++;

  auto guard = bpm_->FetchPageRead(cur_);
  auto leaf = guard.template As<LeafPage>();

  if (index_ >= leaf->GetSize())  // out of a page
  {
    auto next_id = leaf->GetNextPageId();
    if (next_id != -1)  // it's not the end
    {
      index_ = 0;
      cur_ = next_id;
      guard = bpm_->FetchPageRead(cur_);
      leaf = guard.template As<LeafPage>();
      item_ = {leaf->KeyAt(index_), leaf->ValueAt(index_)};
    }
    else  // it's the end
    {
      cur_ = -1;
      index_ = -1;
      item_ = {};
    }
  }
  else  // not out of a page
  {
    item_ = {leaf->KeyAt(index_), leaf->ValueAt(index_)};
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
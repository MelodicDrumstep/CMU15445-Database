//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub
{

SeqScanExecutor::SeqScanExecutor(ExecutorContext* exec_ctx,
                                 const SeqScanPlanNode* plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan)
{
}

void SeqScanExecutor::Init()
{ 
    table_info_ = exec_ctx_ -> GetCatalog() -> GetTable(plan_ -> GetTableOid());
    table_iterator_ = std::make_unique<TableIterator>(table_info_ -> table_ -> MakeIterator());
}

auto SeqScanExecutor::Next(Tuple* tuple, RID* rid) -> bool
{
  while(!table_iterator_ -> IsEnd())
  {
    *rid = table_iterator_ -> GetRID();
    auto table_tuple = table_iterator_ -> GetTuple();
    table_iterator_ -> operator++();
    if(!table_tuple.first.is_deleted_)
    {
      //I have to check if the tuple is deleted 
      *tuple = table_tuple.second;
    }
    return true;
  }
  return false;
}

}  // namespace bustub

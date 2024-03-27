#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
{
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.page_ = nullptr;
}

void BasicPageGuard::Drop()
{
    if(page_ == nullptr)
    {
      return;
    }
    bpm_ -> UnpinPage(page_ -> GetPageId(), is_dirty_);
    page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard &
{
    if(this == &that)
    {
      return that;
    }
    Drop();
    //drop the previous one
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.page_ = nullptr;
    return *this;
  }

BasicPageGuard::~BasicPageGuard()
{
    this -> Drop();
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard { return {bpm_, page_}; }

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { return {bpm_, page_}; }

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page)
{
    guard_ = BasicPageGuard(bpm, page);
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default
{
    this -> Drop();
    guard_ = that.guard_;
    that.guard_ = nullptr;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard &
{
  if(this == &that)
  {
    return that;
  }
  this -> Drop();
  guard_ = that.guard_;
  that.guard_ = nullptr;
  return *this;
}

void ReadPageGuard::Drop()
{
    guard_.Drop();
}

ReadPageGuard::~ReadPageGuard()
{
    this -> Drop();
}

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page)
{
    guard_ = BasicPageGuard(bpm, page);
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard &
{
  if(this == &that)
  {
    return that;
  }
  this -> Drop();
  guard_ = that.guard_;
  that.guard_ = nullptr;
  return *this;
}

void WritePageGuard::Drop()
{
    guard_.Drop();
}

WritePageGuard::~WritePageGuard()
{
    this -> Drop();
}

}  // namespace bustub

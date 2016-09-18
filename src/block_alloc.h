// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common.h"
#include "sync.h"
#include "types.h"

#include <map>

namespace cheesebase {

enum class BlockType : char {
  pg = 'P', // 1 page (4k)
  t1 = '1', // 1/2 page (2k)
  t2 = '2', // 1/4 page (1k)
  t3 = '3', // 1/8 page (512)
  t4 = '4'  // 1/16 page (256)
};

inline size_t toBlockSize(BlockType t) {
  switch (t) {
  case BlockType::pg:
    return k_page_size;
  case BlockType::t1:
    return k_page_size / 2;
  case BlockType::t2:
    return k_page_size / 4;
  case BlockType::t3:
    return k_page_size / 8;
  case BlockType::t4:
    return k_page_size / 16;
  default:
    throw ConsistencyError("Invalid block type");
  }
}

class Storage;

using AllocWrite = std::pair<Addr, uint64_t>;

class BlockAllocator {
public:
  void setFirstFree(Addr free) { free_ = free; }
  Addr getFirstFree() const { return free_; }
  void clearCache() { next_cache_.clear(); }

protected:
  BlockAllocator(Storage& store, Addr free) : free_(free), store_(store) {}
  Addr free_{ 0 };
  Storage& store_;
  std::map<Addr, Addr> next_cache_;
};

class PageAllocator : public BlockAllocator {
public:
  PageAllocator(Storage& store, Addr free, Addr eof)
      : BlockAllocator(store, free), eof_(eof) {}
  static constexpr size_t size() noexcept { return k_page_size; }
  static Addr hdrOffset() noexcept {
    return Addr(offsetof(DskDatabaseHdr, free_alloc_pg));
  }
  static constexpr BlockType type() noexcept { return BlockType::pg; }
  std::pair<Block, std::vector<AllocWrite>> allocBlock();
  std::vector<AllocWrite> freeBlock(Addr);

private:
  using Next = DskNext<static_cast<char>(BlockType::pg)>;
  Addr eof_;
};

template <class ParentAlloc, BlockType Type>
class TierAllocator : public BlockAllocator {
public:
  TierAllocator(Storage& store, Addr free, ParentAlloc& parent)
      : BlockAllocator(store, free), parent_alloc_(parent) {}
  static constexpr size_t size() noexcept { return ParentAlloc::size() / 2; }
  static Addr hdrOffset();
  static constexpr BlockType type() noexcept { return Type; }
  std::pair<Block, std::vector<AllocWrite>> allocBlock();
  std::vector<AllocWrite> freeBlock(Addr);

private:
  using Next = DskNext<static_cast<char>(Type)>;
  ParentAlloc& parent_alloc_;
};

using T1Allocator = TierAllocator<PageAllocator, BlockType::t1>;
using T2Allocator = TierAllocator<T1Allocator, BlockType::t2>;
using T3Allocator = TierAllocator<T2Allocator, BlockType::t3>;
using T4Allocator = TierAllocator<T3Allocator, BlockType::t4>;

} // namespace cheesebase

// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "sync.h"
#include "structs.h"
#include <gsl.h>

#include <map>

namespace cheesebase {

struct Block {
  Addr addr;
  size_t size;
};

class Storage;

using AllocWrite = std::pair<Addr, Addr>;

using AllocWrites = std::vector<AllocWrite>;

class BlockAllocator {
public:
  void setFirstFree(Addr free) { free_ = free; }
  Addr getFirstFree() const { return free_; }
  void clearCache() { next_cache_.clear(); }

protected:
  BlockAllocator(Storage& store, Addr free) : store_(store), free_(free) {}
  Addr free_{ 0 };
  Storage& store_;
  std::map<Addr, Addr> next_cache_;
};

class PageAllocator : public BlockAllocator {
public:
  PageAllocator(Storage& store, Addr free, Addr eof)
      : BlockAllocator(store, free), eof_(eof) {}
  static constexpr size_t size() { return k_page_size; };
  static constexpr Addr hdrOffset() {
    return offsetof(DskDatabaseHdr, free_alloc_pg);
  }
  static constexpr BlockType type() { return BlockType::pg; };
  std::pair<Block, AllocWrites> allocBlock();
  AllocWrites freeBlock(Addr);

private:
  Addr eof_;
};

template <class ParentAlloc>
class TierAllocator : public BlockAllocator {
public:
  TierAllocator(Storage& store, Addr free, ParentAlloc& parent)
      : BlockAllocator(store, free), parent_alloc_(parent) {}
  static constexpr size_t size() noexcept { return ParentAlloc::size() / 2; }
  static Addr hdrOffset();
  static BlockType type();
  std::pair<Block, AllocWrites> allocBlock();
  AllocWrites freeBlock(Addr);

private:
  ParentAlloc& parent_alloc_;
};

using T1Allocator = TierAllocator<PageAllocator>;
using T2Allocator = TierAllocator<T1Allocator>;
using T3Allocator = TierAllocator<T2Allocator>;
using T4Allocator = TierAllocator<T3Allocator>;

} // namespace cheesebase

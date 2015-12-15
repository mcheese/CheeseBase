// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common/sync.h"
#include "common/structs.h"
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
  void setFirstFree(Addr free) { m_free = free; }
  Addr getFirstFree() const { return m_free; }
  void clearCache() { m_next_cache.clear(); }

protected:
  BlockAllocator(Storage& store, Addr free) : m_store(store), m_free(free) {}
  Addr m_free{ 0 };
  Storage& m_store;
  std::map<Addr, Addr> m_next_cache;
};

class PageAllocator : public BlockAllocator {
public:
  PageAllocator(Storage& store, Addr free, Addr eof)
      : BlockAllocator(store, free), m_eof(eof) {}
  static constexpr size_t size() { return k_page_size; };
  static constexpr Addr hdrOffset() {
    return offsetof(DskDatabaseHdr, free_alloc_pg);
  }
  static constexpr BlockType type() { return BlockType::pg; };
  std::pair<Block, AllocWrites> allocBlock();
  AllocWrites freeBlock(Addr);

private:
  Addr m_eof;
};

template <class ParentAlloc>
class TierAllocator : public BlockAllocator {
public:
  TierAllocator(Storage& store, Addr free, ParentAlloc& parent)
      : BlockAllocator(store, free), m_parent_alloc(parent) {}
  static constexpr size_t size() noexcept { return ParentAlloc::size() / 2; }
  static constexpr Addr hdrOffset();
  static constexpr BlockType type();
  std::pair<Block, AllocWrites> allocBlock();
  AllocWrites freeBlock(Addr);

private:
  ParentAlloc& m_parent_alloc;
};

using T1Allocator = TierAllocator<PageAllocator>;
using T2Allocator = TierAllocator<T1Allocator>;
using T3Allocator = TierAllocator<T2Allocator>;
using T4Allocator = TierAllocator<T3Allocator>;

constexpr Addr T1Allocator::hdrOffset() {
  return offsetof(DskDatabaseHdr, free_alloc_t1);
}

constexpr Addr T2Allocator::hdrOffset() {
  return offsetof(DskDatabaseHdr, free_alloc_t2);
}

constexpr Addr T3Allocator::hdrOffset() {
  return offsetof(DskDatabaseHdr, free_alloc_t3);
}

constexpr Addr T4Allocator::hdrOffset() {
  return offsetof(DskDatabaseHdr, free_alloc_t4);
}

constexpr BlockType T1Allocator::type() { return BlockType::t1; }

constexpr BlockType T2Allocator::type() { return BlockType::t2; }

constexpr BlockType T3Allocator::type() { return BlockType::t3; }

constexpr BlockType T4Allocator::type() { return BlockType::t4; }

} // namespace cheesebase

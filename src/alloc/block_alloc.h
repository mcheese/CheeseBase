// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common/sync.h"
#include "structs.h"
#include <gsl.h>

#include <map>

namespace cheesebase {

DEF_EXCEPTION(ConsistencyError);

struct Block {
  Addr addr;
  size_t size;
};

class Storage;

using AllocWrite = std::pair<Addr, Addr>;

using AllocWrites = std::vector<AllocWrite>;

class BlockAllocator {
public:
  void set_free(Addr free);
  Addr get_free() const;
  void clear_cache();

protected:
  BlockAllocator(Storage& store, Addr free);
  Addr m_free{ 0 };
  Storage& m_store;
  std::map<Addr, Addr> m_next_cache;
};

class PageAllocator : public BlockAllocator {
public:
  PageAllocator(Storage& store, Addr free, Addr eof);
  static constexpr size_t size();
  static constexpr Addr free_offset();
  static constexpr BlockType type();
  std::pair<Block, AllocWrites> alloc();
  AllocWrites free(Addr);

private:
  Addr m_eof;
};

template <class ParentAlloc>
class TierAllocator : public BlockAllocator {
public:
  TierAllocator(Storage& store, Addr free, ParentAlloc& parent);
  static constexpr size_t size();
  static constexpr Addr free_offset();
  static constexpr BlockType type();
  std::pair<Block, AllocWrites> alloc();
  AllocWrites free(Addr);

private:
  ParentAlloc& m_parent_alloc;
};

using T1Allocator = TierAllocator<PageAllocator>;
using T2Allocator = TierAllocator<T1Allocator>;
using T3Allocator = TierAllocator<T2Allocator>;
using T4Allocator = TierAllocator<T3Allocator>;

} // namespace cheesebase

// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common.h"
#include "sync.h"
#include "types.h"
#include <gsl.h>

#include <map>

namespace cheesebase {

enum class BlockType {
  pg = 'P', // 1 page (4k)
  t1 = '1', // 1/2 page (2k)
  t2 = '2', // 1/4 page (1k)
  t3 = '3', // 1/8 page (512)
  t4 = '4'  // 1/16 page (256)
};

constexpr size_t toBlockSize(BlockType t) {
  return t == BlockType::pg
             ? k_page_size
             : t == BlockType::t1
                   ? k_page_size / 2
                   : t == BlockType::t2
                         ? k_page_size / 4
                         : t == BlockType::t3
                               ? k_page_size / 8
                               : t == BlockType::t4 ? k_page_size / 16
                                                    : throw ConsistencyError(
                                                          "Invalid block type");
}

CB_PACKED(struct DskBlockHdr {
  DskBlockHdr() = default;
  DskBlockHdr(BlockType type, Addr next)
      : data_{ (static_cast<uint64_t>(type) << 56) + next.value } {
    Expects(static_cast<uint64_t>(type) <= 0xff);
    Expects(next.value <= lowerBitmask(56));
  }

  Addr next() const noexcept { return Addr(data_ & lowerBitmask(56)); }

  BlockType type() const noexcept {
    return gsl::narrow_cast<BlockType>(data_ >> 56);
  }

  uint64_t data() const noexcept { return data_; }

  uint64_t data_;
});

class Storage;

using AllocWrite = std::pair<Addr, uint64_t>;

using AllocWrites = std::vector<AllocWrite>;

class BlockAllocator {
public:
  void setFirstFree(Addr free) { free_ = free; }
  Addr getFirstFree() const { return free_; }
  void clearCache() { next_cache_.clear(); }

protected:
  BlockAllocator(Storage& store, Addr free) : free_(free), store_(store) {}
  Addr free_{ 0 };
  Storage& store_;
  std::map<Addr, uint64_t> next_cache_;
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

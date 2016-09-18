// Licensed under the Apache License 2.0 (see LICENSE file).

#include "block_alloc.h"
#include "storage.h"

namespace cheesebase {

template <>
Addr T1Allocator::hdrOffset() {
  return Addr(offsetof(DskDatabaseHdr, free_alloc_t1));
}

template <>
Addr T2Allocator::hdrOffset() {
  return Addr(offsetof(DskDatabaseHdr, free_alloc_t2));
}

template <>
Addr T3Allocator::hdrOffset() {
  return Addr(offsetof(DskDatabaseHdr, free_alloc_t3));
}

template <>
Addr T4Allocator::hdrOffset() {
  return Addr(offsetof(DskDatabaseHdr, free_alloc_t4));
}

////////////////////////////////////////////////////////////////////////////////
// PageAllocator

std::pair<Block, std::vector<AllocWrite>> PageAllocator::allocBlock() {
  if (free_.value != 0) {
    auto page = free_;

    Addr next = [&] {
      auto lookup = next_cache_.find(page);
      if (lookup != next_cache_.end()) {
        return lookup->second;
      }
      auto block = store_.loadBlock<size()>(free_);
      auto& next = bytesAsType<Next>(*block);
      if (next.next().pageOffset() != 0)
        throw ConsistencyError("Invalid header in block of free list");
      return next.next();
    }();

    free_ = next;
    return { { page, size() }, { { hdrOffset(), free_.value } } };
  } else {
    auto page = eof_;
    eof_.value += k_page_size;
    return { { page, size() },
             { { Addr(offsetof(DskDatabaseHdr, end_of_file)), eof_.value } } };
  }
}

std::vector<AllocWrite> PageAllocator::freeBlock(Addr addr) {
  auto next = free_;
  free_ = addr;
  next_cache_[addr] = next;
  return { { hdrOffset(), free_.value }, { addr, Next(next).data() } };
}

////////////////////////////////////////////////////////////////////////////////
// TierAllocator

template <class P, BlockType T>
std::pair<Block, std::vector<AllocWrite>> TierAllocator<P, T>::allocBlock() {
  if (free_.value != 0) {
    auto block = free_;

    Addr next = [&] {
      auto lookup = next_cache_.find(block);
      if (lookup != next_cache_.end()) {
        return lookup->second;
      }

      auto block = store_.loadBlock<size()>(free_);
      auto& next = bytesAsType<Next>(*block);

      if (next.next().value % size() != 0)
        throw ConsistencyError("Invalid header in block of free list");

      return next.next();
    }();

    free_ = next;
    return { { block, size() }, { { hdrOffset(), free_.value } } };

  } else {
    Block block;
    std::vector<AllocWrite> writes;
    std::tie(block, writes) = parent_alloc_.allocBlock();

    // this should be guaranteed by the parent-allocator
    Expects(block.size == size() * 2);
    Expects(block.addr.value % size() == 0);

    // half of the parent block is unused
    free_.value = block.addr.value + size();
    next_cache_.emplace(free_, Addr(0));

    writes.reserve(writes.size() + 2);
    writes.push_back({ hdrOffset(), free_.value });
    writes.push_back({ free_, Next(Addr(0)).data() });
    return { { block.addr, size() }, writes };
  }
}

template <class P, BlockType T>
std::vector<AllocWrite> TierAllocator<P, T>::freeBlock(Addr block) {
  auto next = free_;
  free_ = block;
  next_cache_[block] = next;
  return { { hdrOffset(), free_.value }, { block, Next(next).data() } };
}

template class TierAllocator<PageAllocator, BlockType::t1>;
template class TierAllocator<T1Allocator, BlockType::t2>;
template class TierAllocator<T2Allocator, BlockType::t3>;
template class TierAllocator<T3Allocator, BlockType::t4>;

} // namespace cheesebase

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

template <>
BlockType T1Allocator::type() {
  return BlockType::t1;
}

template <>
BlockType T2Allocator::type() {
  return BlockType::t2;
}

template <>
BlockType T3Allocator::type() {
  return BlockType::t3;
}

template <>
BlockType T4Allocator::type() {
  return BlockType::t4;
}

////////////////////////////////////////////////////////////////////////////////
// PageAllocator

std::pair<Block, AllocWrites> PageAllocator::allocBlock() {
  if (free_.value != 0) {
    auto page = free_;

    Addr next = [&] {
      auto lookup = next_cache_.find(page);
      if (lookup != next_cache_.end()) {
        return Addr(lookup->second);
      }
      auto hdr = gsl::as_span<const DskBlockHdr>(
          store_.loadPage(free_.pageNr())->subspan(free_.pageOffset()))[0];
      if (hdr.type() != type() || hdr.next().pageOffset() != 0)
        throw ConsistencyError("Invalid header in block of free list");
      return hdr.next();
    }();

    free_ = next;
    return { { page, size() },
             { { hdrOffset(), free_.value },
               { page, DskBlockHdr(type(), Addr(0)).data() } } };
  } else {
    auto page = eof_;
    eof_.value += k_page_size;
    return { { page, size() },
             { { Addr(offsetof(DskDatabaseHdr, end_of_file)), eof_.value },
               { page, DskBlockHdr(type(), Addr(0)).data() } } };
  }
}

AllocWrites PageAllocator::freeBlock(Addr page) {
  auto next = free_;
  free_ = page;
  auto hdr = DskBlockHdr(type(), next);
  next_cache_[page] = hdr.next().value;
  return { { hdrOffset(), free_.value }, { page, hdr.data() } };
}

////////////////////////////////////////////////////////////////////////////////
// TierAllocator

template <class ParentAlloc>
std::pair<Block, AllocWrites> TierAllocator<ParentAlloc>::allocBlock() {
  if (free_.value != 0) {
    auto block = free_;

    Addr next = [&] {
      auto lookup = next_cache_.find(block);
      if (lookup != next_cache_.end()) {
        return Addr(lookup->second);
      }
      auto page = store_.loadPage(free_.pageNr());
      auto hdr =
          gsl::as_span<const DskBlockHdr>(page->subspan(free_.pageOffset()))[0];
      if (hdr.type() != type() || hdr.next().value % size() != 0)
        throw ConsistencyError("Invalid header in block of free list");
      return hdr.next();
    }();

    free_ = next;

    return { { block, size() },
             { { hdrOffset(), free_.value },
               { block, DskBlockHdr(type(), Addr(0)).data() } } };
  } else {
    std::pair<Block, AllocWrites> alloc = parent_alloc_.allocBlock();
    auto& writes = alloc.second;
    auto& block = alloc.first;

    // this should be guaranteed by the parent-allocator
    Expects(block.size == size() * 2);
    Expects(block.addr.value % size() == 0);

    // half of the parent block is unused
    free_.value = block.addr.value + size();
    next_cache_.emplace(free_, 0);

    writes.reserve(writes.size() + 3);
    writes.push_back({ hdrOffset(), free_.value });
    writes.push_back({ free_, DskBlockHdr(type(), Addr(0)).data() });
    writes.push_back({ block.addr, DskBlockHdr(type(), Addr(0)).data() });

    return { { block.addr, size() }, std::move(writes) };
  }
}

template <class ParentAlloc>
AllocWrites TierAllocator<ParentAlloc>::freeBlock(Addr block) {
  auto next = free_;
  free_ = block;
  auto hdr = DskBlockHdr(type(), next);
  next_cache_[block] = hdr.next().value;
  return { { hdrOffset(), free_.value }, { block, hdr.data() } };
}

template class TierAllocator<PageAllocator>;
template class TierAllocator<T1Allocator>;
template class TierAllocator<T2Allocator>;
template class TierAllocator<T3Allocator>;

} // namespace cheesebase

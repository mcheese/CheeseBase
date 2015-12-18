// Licensed under the Apache License 2.0 (see LICENSE file).

#include "block_alloc.h"
#include "storage/storage.h"

namespace cheesebase {

template<>
Addr T1Allocator::hdrOffset() {
  return offsetof(DskDatabaseHdr, free_alloc_t1);
}

template<>
Addr T2Allocator::hdrOffset() {
  return offsetof(DskDatabaseHdr, free_alloc_t2);
}

template<>
Addr T3Allocator::hdrOffset() {
  return offsetof(DskDatabaseHdr, free_alloc_t3);
}

template<>
Addr T4Allocator::hdrOffset() {
  return offsetof(DskDatabaseHdr, free_alloc_t4);
}

template<>
BlockType T1Allocator::type() { return BlockType::t1; }

template<>
BlockType T2Allocator::type() { return BlockType::t2; }

template<>
BlockType T3Allocator::type() { return BlockType::t3; }

template<>
BlockType T4Allocator::type() { return BlockType::t4; }

////////////////////////////////////////////////////////////////////////////////
// PageAllocator

std::pair<Block, AllocWrites> PageAllocator::allocBlock() {
  if (m_free != 0) {
    auto page = m_free;

    Addr next;
    auto lookup = m_next_cache.find(page);
    if (lookup != m_next_cache.end()) {
      next = lookup->second;
    } else {
      auto hdr = gsl::as_span<const DskBlockHdr>(
          m_store.loadPage(toPageNr(m_free))->subspan(toPageOffset(m_free)))[0];
      next = hdr.next();
      if (hdr.type() != type() || toPageOffset(next) != 0)
        throw ConsistencyError("Invalid header in block of free list");
    }

    m_free = next;
    return { { page, size() },
             { { hdrOffset(), m_free },
               { page, DskBlockHdr(type(), 0).data } } };
  } else {
    auto page = m_eof;
    m_eof += k_page_size;
    return { { page, size() },
             { { offsetof(DskDatabaseHdr, end_of_file), m_eof },
               { page, DskBlockHdr(type(), 0).data } } };
  }
}

AllocWrites PageAllocator::freeBlock(Addr page) {
  auto next = m_free;
  m_free = page;
  auto hdr = DskBlockHdr(type(), next);
  m_next_cache[page] = hdr.next();
  return { { hdrOffset(), m_free }, { page, hdr.data } };
}

////////////////////////////////////////////////////////////////////////////////
// TierAllocator

template <class ParentAlloc>
std::pair<Block, AllocWrites> TierAllocator<ParentAlloc>::allocBlock() {
  if (m_free != 0) {
    auto block = m_free;

    Addr next;
    auto lookup = m_next_cache.find(block);
    if (lookup != m_next_cache.end()) {
      next = lookup->second;
    } else {
      auto hdr = gsl::as_span<const DskBlockHdr>(
          m_store.loadPage(toPageNr(m_free))->subspan(toPageOffset(m_free)))[0];
      next = hdr.next();
      if (hdr.type() != type() || next % size() != 0)
        throw ConsistencyError("Invalid header in block of free list");
    }

    m_free = next;

    return { { block, size() },
             { { hdrOffset(), m_free },
               { block, DskBlockHdr(type(), 0).data } } };
  } else {
    std::pair<Block, AllocWrites> alloc = m_parent_alloc.allocBlock();
    auto& writes = alloc.second;
    auto& block = alloc.first;

    // this should be guaranteed by the parent-allocator
    Expects(block.size == size() * 2);
    Expects(block.addr % size() == 0);

    // half of the parent block is unused
    m_free = block.addr + size();
    m_next_cache.emplace(m_free, 0);

    writes.reserve(writes.size() + 3);
    writes.push_back({ hdrOffset(), m_free });
    writes.push_back({ m_free, DskBlockHdr(type(), 0).data });
    writes.push_back({ block.addr, DskBlockHdr(type(), 0).data });

    return { { block.addr, size() }, std::move(writes) };
  }
}

template <class ParentAlloc>
AllocWrites TierAllocator<ParentAlloc>::freeBlock(Addr block) {
  auto next = m_free;
  m_free = block;
  auto hdr = DskBlockHdr(type(), next);
  m_next_cache[block] = hdr.next();
  return { { hdrOffset(), m_free }, { block, hdr.data } };
}

template class TierAllocator<PageAllocator>;
template class TierAllocator<T1Allocator>;
template class TierAllocator<T2Allocator>;
template class TierAllocator<T3Allocator>;

} // namespace cheesebase

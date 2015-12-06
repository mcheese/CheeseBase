// Licensed under the Apache License 2.0 (see LICENSE file).

#include "block_alloc.h"
#include "storage/storage.h"

namespace cheesebase {

// explicit instantiation
template T1Allocator;
template T2Allocator;
template T3Allocator;
template T4Allocator;

constexpr Addr PageAllocator::free_offset() {
  return offsetof(DskDatabaseHdr, free_alloc_page);
}

constexpr Addr T1Allocator::free_offset() {
  return offsetof(DskDatabaseHdr, free_alloc_t1);
}

constexpr Addr T2Allocator::free_offset() {
  return offsetof(DskDatabaseHdr, free_alloc_t2);
}

constexpr Addr T3Allocator::free_offset() {
  return offsetof(DskDatabaseHdr, free_alloc_t3);
}

constexpr Addr T4Allocator::free_offset() {
  return offsetof(DskDatabaseHdr, free_alloc_t4);
}

constexpr BlockType PageAllocator::type() { return BlockType::page; }

constexpr BlockType T1Allocator::type() { return BlockType::t1; }

constexpr BlockType T2Allocator::type() { return BlockType::t2; }

constexpr BlockType T3Allocator::type() { return BlockType::t3; }

constexpr BlockType T4Allocator::type() { return BlockType::t4; }

BlockAllocator::BlockAllocator(Storage& store, Addr free)
    : m_store(store), m_free(free) {}

void BlockAllocator::set_free(Addr free) { m_free = free; }

Addr BlockAllocator::get_free() const { return m_free; }

void BlockAllocator::clear_cache() { m_next_cache.clear(); }

////////////////////////////////////////////////////////////////////////////////
// PageAllocator

PageAllocator::PageAllocator(Storage& store, Addr free, Addr eof)
    : BlockAllocator(store, free), m_eof(eof) {}

constexpr size_t PageAllocator::size() { return k_page_size; }

std::pair<Block, AllocWrites> PageAllocator::alloc() {
  if (m_free != 0) {
    auto page = m_free;

    Addr next;
    auto lookup = m_next_cache.find(page);
    if (lookup != m_next_cache.end()) {
      next = lookup->second;
    } else {
      auto hdr = gsl::as_span<const DskBlockHdr>(
          m_store.load(page_nr(m_free))->subspan(page_offset(m_free)))[0];
      next = hdr.next();
      if (hdr.type() != this->type() || page_offset(next) != 0)
        throw ConsistencyError();
    }

    m_free = next;
    return { { page, this->size() },
             { { this->free_offset(), m_free },
               { page, DskBlockHdr(this->type(), 0).data } } };
  } else {
    auto page = m_eof;
    m_eof += k_page_size;
    return { { page, this->size() },
             { { offsetof(DskDatabaseHdr, end_of_file), m_eof },
               { page, DskBlockHdr(this->type(), 0).data } } };
  }
}

AllocWrites PageAllocator::free(Addr page) {
  auto next = m_free;
  m_free = page;
  auto hdr = DskBlockHdr(this->type(), next);
  m_next_cache[page] = hdr.next();
  return { { free_offset(), m_free }, { page, hdr.data } };
}

////////////////////////////////////////////////////////////////////////////////
// TierAllocator

template <class ParentAlloc>
TierAllocator<ParentAlloc>::TierAllocator(Storage& store, Addr free,
                                          ParentAlloc& parent)
    : BlockAllocator(store, free), m_parent_alloc(parent) {}

template <class ParentAlloc>
constexpr size_t TierAllocator<ParentAlloc>::size() {
  return ParentAlloc::size() / 2;
}

template<class ParentAlloc>
std::pair<Block, AllocWrites> TierAllocator<ParentAlloc>::alloc() {
  if (m_free != 0) {
    auto block = m_free;

    Addr next;
    auto lookup = m_next_cache.find(block);
    if (lookup != m_next_cache.end()) {
      next = lookup->second;
    } else {
      auto hdr = gsl::as_span<const DskBlockHdr>(
          m_store.load(page_nr(m_free))->subspan(page_offset(m_free)))[0];
      next = hdr.next();
      if (hdr.type() != this->type() || next % this->size() != 0)
        throw ConsistencyError();
    }

    m_free = next;

    return { { block, this->size() },
             { { this->free_offset(), m_free },
               { block, DskBlockHdr(this->type(), 0).data } } };
  } else {
    std::pair<Block, AllocWrites> alloc = m_parent_alloc.alloc();
    auto& writes = alloc.second;
    auto& block = alloc.first;

    // this should be guaranteed by the parent-allocator
    Expects(block.size == this->size() * 2);
    Expects(block.addr % this->size() == 0);

    // half of the parent block is unused
    m_free = block.addr + this->size();
    m_next_cache.emplace(m_free, 0);

    writes.reserve(writes.size() + 3);
    writes.push_back({ this->free_offset(), m_free });
    writes.push_back({ m_free, DskBlockHdr(this->type(), 0).data });
    writes.push_back({ block.addr, DskBlockHdr(this->type(), 0).data });

    return { { block.addr, this->size() }, std::move(writes) };
  }
}

template<class ParentAlloc>
AllocWrites TierAllocator<ParentAlloc>::free(Addr block) {
  auto next = m_free;
  m_free = block;
  auto hdr = DskBlockHdr(this->type(), next);
  m_next_cache[block] = hdr.next();
  return { { this->free_offset(), m_free }, { block, hdr.data } };
}

} // namespace cheesebase

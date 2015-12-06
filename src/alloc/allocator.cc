// Licensed under the Apache License 2.0 (see LICENSE file).

#include "allocator.h"
#include "storage/storage.h"

namespace cheesebase {

Allocator::Allocator(const DskDatabaseHdr& h, Storage& store)
    : m_store(store)
    , m_pg_alloc(store, h.free_alloc_page, h.end_of_file)
    , m_t1_alloc(store, h.free_alloc_t1, m_pg_alloc)
    , m_t2_alloc(store, h.free_alloc_t2, m_t1_alloc)
    , m_t3_alloc(store, h.free_alloc_t3, m_t2_alloc)
    , m_t4_alloc(store, h.free_alloc_t4, m_t3_alloc) {}

AllocTransaction Allocator::transaction() {
  return AllocTransaction(*this, ExLock<Mutex>{ m_mutex });
}

std::pair<Block, AllocWrites> AllocTransaction::block_alloc(size_t size) {
  constexpr auto hdr = sizeof(DskBlockHdr);

  if (size <= m_alloc.m_t4_alloc.size() - hdr)
    return m_alloc.m_t4_alloc.alloc();
  else if (size <= m_alloc.m_t3_alloc.size() - hdr)
    return m_alloc.m_t3_alloc.alloc();
  else if (size <= m_alloc.m_t2_alloc.size() - hdr)
    return m_alloc.m_t2_alloc.alloc();
  else if (size <= m_alloc.m_t1_alloc.size() - hdr)
    return m_alloc.m_t1_alloc.alloc();
  else if (size <= m_alloc.m_pg_alloc.size() - hdr)
    return m_alloc.m_pg_alloc.alloc();
  else
    throw alloc_too_big();
}

AllocWrites AllocTransaction::block_free(Addr block) {
  DskBlockHdr hdr;
  if (m_writes.count(block) > 0) {
    hdr.data = m_writes.at(block);
  } else {
    hdr = gsl::as_span<DskBlockHdr>(
        m_alloc.m_store.load(page_nr(block))->subspan(page_offset(block)))[0];
  }
  switch (hdr.type()) {
  case BlockType::page:
    return m_alloc.m_pg_alloc.free(block);
  case BlockType::t1:
    return m_alloc.m_t1_alloc.free(block);
  case BlockType::t2:
    return m_alloc.m_t2_alloc.free(block);
  case BlockType::t3:
    return m_alloc.m_t3_alloc.free(block);
  case BlockType::t4:
    return m_alloc.m_t4_alloc.free(block);
  case BlockType::multi:
    throw ConsistencyError();
  default:
    throw ConsistencyError();
  }
}

void Allocator::clear_cache() {
  m_pg_alloc.clear_cache();
  m_t1_alloc.clear_cache();
  m_t2_alloc.clear_cache();
  m_t3_alloc.clear_cache();
  m_t4_alloc.clear_cache();
}

AllocTransaction::AllocTransaction(Allocator& alloc, ExLock<Mutex> lock)
    : m_alloc(alloc), m_lock(std::move(lock)){};

AllocTransaction::~AllocTransaction() { m_alloc.clear_cache(); }

Block AllocTransaction::alloc(size_t size) {
  auto alloc = block_alloc(size);
  auto& block = alloc.first;
  auto& writes = alloc.second;

  for (auto& w : writes) { m_writes[w.first] = w.second; }
  return block;
}

void AllocTransaction::free(Addr block) {
  for (auto& w : block_free(block)) {
    m_writes[w.first] = w.second;
  }
}

std::vector<Write> AllocTransaction::commit() {
  std::vector<Write> writes;
  writes.reserve(m_writes.size());

  for (auto& w : m_writes) {
    writes.push_back({ w.first, gsl::as_bytes(gsl::span<Addr>(w.second)) });
  }

  return writes;
}

void AllocTransaction::abort() {
  m_writes.clear();
  m_alloc.clear_cache();
}

} // namespace cheesebase

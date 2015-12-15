// Licensed under the Apache License 2.0 (see LICENSE file).

#include "allocator.h"
#include "storage/storage.h"

namespace cheesebase {

Allocator::Allocator(const DskDatabaseHdr& h, Storage& store)
    : m_store(store)
    , m_pg_alloc(store, h.free_alloc_pg, h.end_of_file)
    , m_t1_alloc(store, h.free_alloc_t1, m_pg_alloc)
    , m_t2_alloc(store, h.free_alloc_t2, m_t1_alloc)
    , m_t3_alloc(store, h.free_alloc_t3, m_t2_alloc)
    , m_t4_alloc(store, h.free_alloc_t4, m_t3_alloc) {}

AllocTransaction Allocator::startTransaction() {
  return AllocTransaction(this, ExLock<Mutex>{ m_mutex });
}

std::pair<Block, AllocWrites> AllocTransaction::allocBlock(size_t size) {
  constexpr auto hdr = sizeof(DskBlockHdr);

  if (size <= m_alloc->m_t4_alloc.size() - hdr)
    return m_alloc->m_t4_alloc.allocBlock();
  else if (size <= m_alloc->m_t3_alloc.size() - hdr)
    return m_alloc->m_t3_alloc.allocBlock();
  else if (size <= m_alloc->m_t2_alloc.size() - hdr)
    return m_alloc->m_t2_alloc.allocBlock();
  else if (size <= m_alloc->m_t1_alloc.size() - hdr)
    return m_alloc->m_t1_alloc.allocBlock();
  else if (size <= m_alloc->m_pg_alloc.size() - hdr)
    return m_alloc->m_pg_alloc.allocBlock();
  else
    throw AllocError("requested size to big");
}

AllocWrites AllocTransaction::freeBlock(Addr block) {
  DskBlockHdr hdr;
  if (m_writes.count(block) > 0) {
    hdr.data = m_writes.at(block);
  } else {
    hdr = gsl::as_span<DskBlockHdr>(m_alloc->m_store.loadPage(toPageNr(block))
                                        ->subspan(toPageOffset(block)))[0];
  }
  AllocWrites ret;

  switch (hdr.type()) {
  case BlockType::pg:
    ret = m_alloc->m_pg_alloc.freeBlock(block);
    break;
  case BlockType::t1:
    ret = m_alloc->m_t1_alloc.freeBlock(block);
    break;
  case BlockType::t2:
    ret = m_alloc->m_t2_alloc.freeBlock(block);
    break;
  case BlockType::t3:
    ret = m_alloc->m_t3_alloc.freeBlock(block);
    break;
  case BlockType::t4:
    ret = m_alloc->m_t4_alloc.freeBlock(block);
    break;
  default:
    throw ConsistencyError();
  }

  if (hdr.next() != 0) {
    auto next = freeBlock(hdr.next());
    std::move(next.begin(), next.end(), std::back_inserter(ret));
  }

  return ret;
}

void Allocator::clearCache() {
  m_pg_alloc.clearCache();
  m_t1_alloc.clearCache();
  m_t2_alloc.clearCache();
  m_t3_alloc.clearCache();
  m_t4_alloc.clearCache();
}

AllocTransaction::AllocTransaction(gsl::not_null<Allocator*> alloc,
                                   ExLock<Mutex> lock)
    : m_alloc(alloc), m_lock(std::move(lock)){};

AllocTransaction::~AllocTransaction() { end(); }

Block AllocTransaction::alloc(size_t size) {
  Expects(m_lock.owns_lock());
  auto alloc = allocBlock(size);
  auto& block = alloc.first;
  auto& writes = alloc.second;

  for (auto& w : writes) { m_writes[w.first] = w.second; }
  return block;
}

void AllocTransaction::free(Addr block) {
  Expects(m_lock.owns_lock());
  for (auto& w : freeBlock(block)) { m_writes[w.first] = w.second; }
}

Block AllocTransaction::allocExtension(Addr block, size_t size) {
  Expects(m_lock.owns_lock());

  DskBlockHdr hdr;
  if (m_writes.count(block) > 0) {
    hdr.data = m_writes.at(block);
  } else {
    hdr = gsl::as_span<DskBlockHdr>(m_alloc->m_store.loadPage(toPageNr(block))
                                        ->subspan(toPageOffset(block)))[0];
  }

  if (hdr.next() != 0)
    throw AllocError("block to extend is not the last block");

  auto alloc = allocBlock(size);
  auto& new_block = alloc.first;
  auto& writes = alloc.second;

  m_writes[block] = DskBlockHdr(hdr.type(), new_block.addr).data;

  for (auto& w : writes) { m_writes[w.first] = w.second; }

  return new_block;
}

std::vector<Write> AllocTransaction::commit() {
  Expects(m_lock.owns_lock());
  std::vector<Write> writes;
  writes.reserve(m_writes.size());

  for (auto& w : m_writes) {
    writes.push_back({ w.first, gsl::as_bytes(gsl::span<Addr>(w.second)) });
  }

  return writes;
}

void AllocTransaction::end() {
  m_writes.clear();
  if (m_lock.owns_lock()) {
    m_alloc->clearCache();
    m_lock.unlock();
  }
}

} // namespace cheesebase

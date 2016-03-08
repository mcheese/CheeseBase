// Licensed under the Apache License 2.0 (see LICENSE file).

#include "allocator.h"
#include "storage.h"

namespace cheesebase {

Allocator::Allocator(const DskDatabaseHdr& h, Storage& store)
    : store_(store)
    , pg_alloc_(store, h.free_alloc_pg, h.end_of_file)
    , t1_alloc_(store, h.free_alloc_t1, pg_alloc_)
    , t2_alloc_(store, h.free_alloc_t2, t1_alloc_)
    , t3_alloc_(store, h.free_alloc_t3, t2_alloc_)
    , t4_alloc_(store, h.free_alloc_t4, t3_alloc_) {}

AllocTransaction Allocator::startTransaction() {
  return AllocTransaction(this, ExLock<Mutex>{ mutex_ });
}

std::pair<Block, AllocWrites> AllocTransaction::allocBlock(size_t size) {
  constexpr auto hdr = sizeof(DskBlockHdr);

  if (size <= alloc_->t4_alloc_.size() - hdr)
    return alloc_->t4_alloc_.allocBlock();
  else if (size <= alloc_->t3_alloc_.size() - hdr)
    return alloc_->t3_alloc_.allocBlock();
  else if (size <= alloc_->t2_alloc_.size() - hdr)
    return alloc_->t2_alloc_.allocBlock();
  else if (size <= alloc_->t1_alloc_.size() - hdr)
    return alloc_->t1_alloc_.allocBlock();
  else if (size <= alloc_->pg_alloc_.size() - hdr)
    return alloc_->pg_alloc_.allocBlock();
  else
    throw AllocError("requested size to big");
}

AllocWrites AllocTransaction::freeBlock(Addr block) {
  DskBlockHdr hdr;
  if (writes_.count(block) > 0) {
    hdr.data_ = writes_.at(block);
  } else {
    hdr = gsl::as_span<DskBlockHdr>(alloc_->store_.loadPage(toPageNr(block))
                                        ->subspan(toPageOffset(block)))[0];
  }
  AllocWrites ret;

  switch (hdr.type()) {
  case BlockType::pg:
    ret = alloc_->pg_alloc_.freeBlock(block);
    break;
  case BlockType::t1:
    ret = alloc_->t1_alloc_.freeBlock(block);
    break;
  case BlockType::t2:
    ret = alloc_->t2_alloc_.freeBlock(block);
    break;
  case BlockType::t3:
    ret = alloc_->t3_alloc_.freeBlock(block);
    break;
  case BlockType::t4:
    ret = alloc_->t4_alloc_.freeBlock(block);
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
  pg_alloc_.clearCache();
  t1_alloc_.clearCache();
  t2_alloc_.clearCache();
  t3_alloc_.clearCache();
  t4_alloc_.clearCache();
}

AllocTransaction::AllocTransaction(gsl::not_null<Allocator*> alloc,
                                   ExLock<Mutex> lock)
    : alloc_(alloc), lock_(std::move(lock)){};

AllocTransaction::~AllocTransaction() { end(); }

Block AllocTransaction::alloc(size_t size) {
  Expects(lock_.owns_lock());
  auto alloc = allocBlock(size);
  auto& block = alloc.first;
  auto& writes = alloc.second;

  for (auto& w : writes) { writes_[w.first] = w.second; }
  return block;
}

void AllocTransaction::free(Addr block) {
  Expects(lock_.owns_lock());
  for (auto& w : freeBlock(block)) { writes_[w.first] = w.second; }
}

Block AllocTransaction::allocExtension(Addr block, size_t size) {
  Expects(lock_.owns_lock());

  DskBlockHdr hdr;
  if (writes_.count(block) > 0) {
    hdr.data_ = writes_.at(block);
  } else {
    hdr = gsl::as_span<DskBlockHdr>(alloc_->store_.loadPage(toPageNr(block))
                                        ->subspan(toPageOffset(block)))[0];
  }

  if (hdr.next() != 0)
    throw AllocError("block to extend is not the last block");

  auto alloc = allocBlock(size);
  auto& new_block = alloc.first;
  auto& writes = alloc.second;

  writes_[block] = DskBlockHdr(hdr.type(), new_block.addr).data();

  for (auto& w : writes) { writes_[w.first] = w.second; }

  return new_block;
}

std::vector<Write> AllocTransaction::commit() {
  Expects(lock_.owns_lock());
  std::vector<Write> writes;
  writes.reserve(writes_.size());

  for (auto& w : writes_) {
    writes.push_back(
        { w.first, gsl::as_bytes<Addr>(gsl::span<Addr>(w.second)) });
  }

  return writes;
}

void AllocTransaction::end() {
  writes_.clear();
  if (lock_.owns_lock()) {
    alloc_->clearCache();
    lock_.unlock();
  }
}

} // namespace cheesebase

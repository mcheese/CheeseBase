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
  return AllocTransaction(*this, ExLock<Mutex>{ mutex_ });
}

std::pair<Block, std::vector<AllocWrite>>
AllocTransaction::allocBlock(size_t size) {
  if (size <= alloc_->t4_alloc_.size())
    return alloc_->t4_alloc_.allocBlock();
  else if (size <= alloc_->t3_alloc_.size())
    return alloc_->t3_alloc_.allocBlock();
  else if (size <= alloc_->t2_alloc_.size())
    return alloc_->t2_alloc_.allocBlock();
  else if (size <= alloc_->t1_alloc_.size())
    return alloc_->t1_alloc_.allocBlock();
  else if (size <= alloc_->pg_alloc_.size())
    return alloc_->pg_alloc_.allocBlock();
  else
    throw AllocError("requested size to big");
}

std::vector<AllocWrite> AllocTransaction::freeBlock(Addr addr, size_t size) {
  if (size <= alloc_->t4_alloc_.size())
    return alloc_->t4_alloc_.freeBlock(addr);
  else if (size <= alloc_->t3_alloc_.size())
    return alloc_->t3_alloc_.freeBlock(addr);
  else if (size <= alloc_->t2_alloc_.size())
    return alloc_->t2_alloc_.freeBlock(addr);
  else if (size <= alloc_->t1_alloc_.size())
    return alloc_->t1_alloc_.freeBlock(addr);
  else if (size <= alloc_->pg_alloc_.size())
    return alloc_->pg_alloc_.freeBlock(addr);
  else
    throw AllocError("requested size to big");
}

void Allocator::clearCache() {
  pg_alloc_.clearCache();
  t1_alloc_.clearCache();
  t2_alloc_.clearCache();
  t3_alloc_.clearCache();
  t4_alloc_.clearCache();
}

AllocTransaction::AllocTransaction(Allocator& alloc, ExLock<Mutex> lock)
    : alloc_(&alloc), lock_(std::move(lock)){};

AllocTransaction::~AllocTransaction() { end(); }

Block AllocTransaction::alloc(size_t size) {
  Expects(lock_.owns_lock());
  auto alloc = allocBlock(size);
  auto& block = alloc.first;
  auto& writes = alloc.second;

  // in case it was freed in the same TA and has a Next written in
  writes_.erase(block.addr);
  for (auto& w : writes) {
    writes_[w.first] = w.second;
  }
  return block;
}

void AllocTransaction::free(Block block) { free(block.addr, block.size); }

void AllocTransaction::free(Addr addr, size_t size) {
  Expects(lock_.owns_lock());
  for (auto& w : freeBlock(addr, size)) {
    writes_[w.first] = w.second;
  }
}

std::vector<Write> AllocTransaction::commit() {
  Expects(lock_.owns_lock());
  std::vector<Write> writes;
  writes.reserve(writes_.size());

  for (auto& w : writes_) {
    writes.push_back({ w.first, w.second });
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

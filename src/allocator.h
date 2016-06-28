// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "block_alloc.h"
#include <boost/container/flat_map.hpp>

namespace cheesebase {

class Allocator;

class AllocTransaction {
  friend class Allocator;

public:
  MOVE_ONLY(AllocTransaction)
  ~AllocTransaction();

  // Allocates one block of at least size bytes.
  Block alloc(size_t size);

  // Free block.
  void free(Addr addr, size_t size);
  void free(Block block);

  // Get writes of all done changes to pass to the storage.
  std::vector<Write> commit();

  // End the transaction, clearing the object and allowing it to be reused.
  // note: it is not required to end() before destructing the object.
  void end();

private:
  AllocTransaction(Allocator& alloc, ExLock<Mutex> lock);

  std::pair<Block, std::vector<AllocWrite>> allocBlock(size_t size);
  std::vector<AllocWrite> freeBlock(Addr addr, size_t size);

  Allocator* alloc_;
  ExLock<Mutex> lock_;
  boost::container::flat_map<Addr, uint64_t> writes_;
};

class Allocator {
  friend class AllocTransaction;

public:
  Allocator(const DskDatabaseHdr& header, Storage& store);

  AllocTransaction startTransaction();

private:
  void clearCache();

  Mutex mutex_;
  PageAllocator pg_alloc_;
  T1Allocator t1_alloc_;
  T2Allocator t2_alloc_;
  T3Allocator t3_alloc_;
  T4Allocator t4_alloc_;
};

} // namespace cheesebase

// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "block_alloc.h"

namespace cheesebase {

class Allocator;

class AllocTransaction {
  friend class Allocator;

public:
  MOVE_ONLY(AllocTransaction)
  ~AllocTransaction();

  // Allocates one block of at least size bytes. The first 8 byte of the block
  // are contains the header, DO NOT CHANGE.
  Block alloc(size_t size);

  // Append one block of at least size bytes to the provided one.
  Block allocExtension(Addr block, size_t size);

  // Free block and every block that it links to.
  void free(Addr block);

  // Get writes of all done changes to pass to the storage.
  std::vector<Write> commit();

  // End the transaction, clearing the object and allowing it to be reused.
  // note: it is not required to end() before destructing the object.
  void end();

private:
  AllocTransaction(gsl::not_null<Allocator*> alloc, ExLock<Mutex> lock);
  std::pair<Block, AllocWrites> allocBlock(size_t size);
  AllocWrites freeBlock(Addr block);

  Allocator* alloc_;
  ExLock<Mutex> lock_;
  std::map<Addr, Addr> writes_;
};

class Allocator {
  friend class AllocTransaction;

public:
  Allocator(const DskDatabaseHdr& header, Storage& store);

  AllocTransaction startTransaction();

private:
  void clearCache();

  Storage& store_;
  Mutex mutex_;
  PageAllocator pg_alloc_;
  T1Allocator t1_alloc_;
  T2Allocator t2_alloc_;
  T3Allocator t3_alloc_;
  T4Allocator t4_alloc_;
};

} // namespace cheesebase

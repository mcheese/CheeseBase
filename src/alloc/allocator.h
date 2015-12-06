// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "block_alloc.h"

namespace cheesebase {

DEF_EXCEPTION(alloc_too_big);

class AllocTransaction {
  friend class Allocator;

public:
  ~AllocTransaction();
  MOVE_ONLY(AllocTransaction);

  Block alloc(size_t size);
  void free(Addr block);
  std::vector<Write> commit();
  void abort();

private:
  AllocTransaction(Allocator& alloc, ExLock<Mutex> lock);
  std::pair<Block, AllocWrites> allocBlock(size_t size);
  AllocWrites freeBlock(Addr block);

  Allocator& m_alloc;
  ExLock<Mutex> m_lock;
  std::map<Addr, Addr> m_writes;
};

class Allocator {
  friend class AllocTransaction;

public:
  Allocator(const DskDatabaseHdr& header, Storage& store);

  AllocTransaction startTransaction();

private:
  void clearCache();

  Storage& m_store;
  Mutex m_mutex;
  PageAllocator m_pg_alloc;
  T1Allocator m_t1_alloc;
  T2Allocator m_t2_alloc;
  T3Allocator m_t3_alloc;
  T4Allocator m_t4_alloc;
};

} // namespace cheesebase

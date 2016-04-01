// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common.h"

#include "allocator.h"
#include "keycache.h"
#include "storage.h"
#include "block_locks.h"

#include <memory>
#include <string>

namespace cheesebase {

class Database;

class Transaction {
  friend class Database;

public:
  ReadRef load(PageNr p);
  Block alloc(size_t s);
  Block allocExtension(Addr block, size_t s);
  void free(Addr a);
  Key key(const std::string& s);
  BlockLockW getLockW(Addr);
  BlockLockR getLockR(Addr);
  Database& db() const noexcept { return db_; }

  void commit(Writes w);

private:
  Transaction(Database& db);

  Database& db_;
  Storage& storage_;
  AllocTransaction alloc_;
  KeyTransaction kcache_;
};

const Addr k_root{ k_page_size };

class Database {
  friend class Transaction;

public:
  Database(const std::string& name);
  Transaction startTransaction();
  std::string resolveKey(Key k) const;
  boost::optional<Key> getKey(const std::string& k) const;
  ReadRef loadPage(PageNr p);
  BlockLockW getLockW(Addr);
  BlockLockR getLockR(Addr);

private:
  // for test cases
  Database() = default;
  std::unique_ptr<Storage> store_;
  std::unique_ptr<Allocator> alloc_;
  std::unique_ptr<KeyCache> keycache_;
  std::unique_ptr<BlockLockPool> lock_pool_;
};

} // namespace cheesebase

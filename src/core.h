// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common.h"

#include "allocator.h"
#include "block_locks.h"
#include "keycache.h"
#include "storage.h"

#include <memory>
#include <string>

namespace cheesebase {

constexpr Addr kRoot{ k_page_size };

class Database;

class Transaction {
  friend class Database;

public:
  ReadRef<k_page_size> load(PageNr p);

  template <std::ptrdiff_t S>
  ReadRef<S> loadBlock(Addr addr) {
    return storage_.loadBlock<S>(addr);
  }

  Block alloc(size_t s);
  void free(Addr a, size_t s);
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

class Database {
  friend class Transaction;

public:
  Database(const std::string& name);
  Transaction startTransaction();
  std::string resolveKey(Key k) const;
  boost::optional<Key> getKey(const std::string& k) const;
  ReadRef<k_page_size> loadPage(PageNr p);

  template <std::ptrdiff_t S>
  auto loadBlock(Addr addr) {
    return store_->loadBlock<S>(addr);
  }

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

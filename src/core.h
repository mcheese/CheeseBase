// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common/common.h"

#include "blockman/allocator.h"
#include "keycache/keycache.h"
#include "storage/storage.h"

#include <memory>
#include <string>

namespace cheesebase {

DEF_EXCEPTION(DatabaseError);

class Database;

class Transaction {
  friend class Database;

public:
  ReadRef load(PageNr p);
  Block alloc(size_t s);
  void free(Addr a);
  Key key(const std::string& s);

  void commit(Writes w);
private:
  Transaction(Database& db);

  Storage& m_storage;
  AllocTransaction m_alloc;
  KeyTransaction m_kcache;
};

class Database {
  friend class Transaction;

public:
  Database(const std::string& name);
  Transaction startTransaction();
  std::string resolveKey(Key k) const;
  ReadRef loadPage(PageNr p);

private:
  // for test cases
  Database() = default;
  std::unique_ptr<Storage> m_store;
  std::unique_ptr<Allocator> m_alloc;
  std::unique_ptr<KeyCache> m_keycache;
};

} // namespace cheesebase

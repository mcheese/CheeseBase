// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common.h"

#include "allocator.h"
#include "keycache.h"
#include "storage.h"

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
  const Database& db;

private:
  Transaction(Database& db);

  Storage& m_storage;
  AllocTransaction m_alloc;
  KeyTransaction m_kcache;
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

private:
  // for test cases
  Database() = default;
  std::unique_ptr<Storage> m_store;
  std::unique_ptr<Allocator> m_alloc;
  std::unique_ptr<KeyCache> m_keycache;
};

} // namespace cheesebase

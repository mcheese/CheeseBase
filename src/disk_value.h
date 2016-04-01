// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common.h"
#include "block_locks.h"
#include "core.h"

namespace cheesebase {

// Forward declarations
class Transaction;
class Database;

namespace model {
class Value;
}

namespace disk {

// Base class representing a write only value on disk
class ValueW {
public:
  virtual ~ValueW() = default;

  Addr addr() const { return addr_; }

  virtual Writes getWrites() const = 0;
  virtual void destroy() = 0;

protected:
  ValueW(Transaction& ta) : addr_{ 0 }, ta_{ ta } {}
  ValueW(Transaction& ta, Addr addr)
      : addr_{ addr }, ta_{ ta }, lck_{ ta.getLockW(addr) } {
    Expects(addr != 0);
  }
  Addr addr_;
  Transaction& ta_;
  BlockLockW lck_;
};

// Base class representing a read only value on disk
class ValueR {
public:
  virtual ~ValueR() = default;

  Addr addr() const { return addr_; }

  virtual std::unique_ptr<model::Value> getValue() = 0;

protected:
  ValueR(Database& db, Addr addr)
      : addr_{ addr }, db_{ db }, lck_{ db.getLockR(addr) } {
    Expects(addr != 0);
  }
  Addr addr_;
  Database& db_;
  BlockLockR lck_;
};

} // namespace disk

} // namespace cheesebase

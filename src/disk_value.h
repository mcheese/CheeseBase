// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common.h"

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
  Addr addr() const { return addr_; }

  virtual Writes getWrites() const = 0;
  virtual void destroy() = 0;

protected:
  ValueW(Transaction& ta, Addr addr) : ta_{ ta }, addr_{ addr } {}
  Addr addr_;
  Transaction& ta_;
};

// Base class representing a read only value on disk
class ValueR {
public:
  Addr addr() const { return addr_; }

  virtual std::unique_ptr<model::Value> getValue() = 0;

protected:
  ValueR(Database& db, Addr addr) : db_{ db }, addr_{ addr } {}
  Addr addr_;
  Database& db_;
};

} // namespace disk

} // namespace cheesebase

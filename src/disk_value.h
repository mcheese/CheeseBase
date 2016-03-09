// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common.h"
#include "core.h"
#include "model.h"

namespace cheesebase {

class Transaction;

namespace disk {

// Base class representing a value on disk
class Value {
public:
  Addr addr() const { return addr_; }

protected:
  Value(Addr a) : addr_{ a } {}
  Addr addr_;
};

// Base class representing a write only value on disk
class ValueW : public Value {
public:
  virtual Writes getWrites() const = 0;
  virtual void destroy() = 0;

protected:
  ValueW(Transaction& ta, Addr addr) : ta_{ ta }, Value(addr) {}
  Transaction& ta_;
};

// Base class representing a read only value on disk
class ValueR : public Value {
public:
  virtual model::PValue getValue() = 0;

protected:
  ValueR(Database& db, Addr addr) : db_{ db }, Value(addr) {}
  Database& db_;
};

} // namespace disk

} // namespace cheesebase

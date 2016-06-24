// Licensed under the Apache License 2.0 (see LICENSE file).
#pragma once

#include "common.h"

namespace cheesebase {
class Database;
class Transaction;
namespace model {
class Value;
}
namespace disk {
class ValueW;
class ValueR;
namespace btree {
namespace NodeR {

template <class C>
void getAll(Database& db, Addr addr, C& obj);
std::unique_ptr<model::Value> getChildValue(Database& db, Addr addr, Key key);
std::unique_ptr<ValueW> getChildCollectionW(Transaction& ta, Addr addr, Key);
std::unique_ptr<ValueR> getChildCollectionR(Database& db, Addr addr, Key key);

} // namespace NodeR
} // namespace btree
} // namespace disk
} // namespace cheesebase

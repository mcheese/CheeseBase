// Licensed under the Apache License 2.0 (see LICENSE file).
#pragma once

#include "../../common.h"
#include "../../model/model.h"

namespace cheesebase {

class Transaction;
class Database;

using ArrayMap = std::map<uint64_t, model::Value>;

namespace disk {

class ValueW;
class ValueR;

// argument for insert queries
enum class Overwrite { Insert, Update, Upsert };

namespace btree {

class NodeW;

class BtreeWritable {
  friend class RootLeafW;
  friend class RootInternalW;

public:
  // create new tree
  BtreeWritable(Transaction& ta);
  // open existing tree
  BtreeWritable(Transaction& ta, Addr root);

  ~BtreeWritable();

  Addr addr() const;
  bool insert(Key key, const model::Value& val, Overwrite);
  bool remove(Key key);
  Key append(const model::Value& val);
  void destroy();
  Writes getWrites() const;

private:
  std::unique_ptr<btree::NodeW> root_;
};

class BtreeReadOnly {
public:
  BtreeReadOnly(Database& db, Addr root);

  model::Tuple getObject();
  ArrayMap getArray();
  model::Value getChildValue(Key key);
  std::unique_ptr<ValueW> getChildCollectionW(Transaction&, Key key);
  std::unique_ptr<ValueR> getChildCollectionR(Key key);
  std::unique_ptr<ValueW> getChild(Key key);

private:
  Database& db_;
  Addr root_;
};

} // namespace btree
} // namespace disk
} // namespace cheesebase

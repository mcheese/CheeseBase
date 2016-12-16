// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "../core.h"
#include "../exceptions.h"
#include "btree/btree.h"
#include "value.h"

namespace cheesebase {
namespace disk {

class ObjectW : public ValueW {
public:
  ObjectW(Transaction& ta) : ValueW(ta), tree_{ ta } { addr_ = tree_.addr(); }

  ObjectW(Transaction& ta, Addr addr) : ValueW(ta, addr), tree_{ ta, addr } {}

  Writes getWrites() const override { return tree_.getWrites(); }

  void destroy() override { return tree_.destroy(); }

  bool insert(Key key, const model::Value& val, Overwrite ow) {
    return tree_.insert(key, val, ow);
  }

  bool insert(const std::string& key, const model::Value& val, Overwrite ow) {
    return tree_.insert(ta_.key(key), val, ow);
  }

  bool remove(Key key) { return tree_.remove(key); }

  bool remove(const std::string& key) {
    auto k = ta_.db().getKey(key);
    if (!k) return false;
    return tree_.remove(*k);
  }

private:
  btree::BtreeWritable tree_;
};

class ObjectR : public ValueR {
public:
  ObjectR(Database& db, Addr addr) : ValueR(db, addr), tree_{ db, addr } {}

  model::Value getValue() override;
  model::Value getChildValue(const std::string& key);
  std::unique_ptr<disk::ValueW> getChildCollectionW(Transaction& ta,
                                                    const std::string& key);
  std::unique_ptr<disk::ValueR> getChildCollectionR(const std::string& key);
  model::Tuple getObject();

private:
  btree::BtreeReadOnly tree_;
};

} // namespace disk
} // namespace cheesebase

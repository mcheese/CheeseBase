// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "disk_value.h"
#include "disk_btree.h"
#include "model.h"

namespace cheesebase {
namespace disk {

class ArrayW : public ValueW {
public:
  ArrayW(Transaction& ta) : ValueW(ta, 0), tree_{ ta } { addr_ = tree_.addr(); }

  ArrayW(Transaction& ta, Addr addr) : ValueW(ta, addr), tree_{ ta, addr } {}

  Writes getWrites() const override { return tree_.getWrites(); }

  void destroy() override { return tree_.destroy(); }

  bool insert(Key index, const model::Value& val, Overwrite ow) {
    Expects(DskKey(index).key() == index);
    return tree_.insert(index, val, ow);
  }

  Key append(const model::Value& val) { return tree_.append(val); }

  bool remove(Key key) { return tree_.remove(key); }

private:
  btree::BtreeWritable tree_;
};

class ArrayR : public ValueR {
public:
  ArrayR(Database& db, Addr addr) : ValueR(db, addr), tree_{ db, addr } {}

  model::PValue getValue() override {
    return std::make_unique<model::Array>(tree_.getArray());
  };

  model::PValue getChildValue(const std::string& key) {
    auto k = db_.getKey(key);
    if (!k) return nullptr;
    return tree_.getChildValue(*k);
  }

  model::Array getArray() { return tree_.getArray(); }

private:
  btree::BtreeReadOnly tree_;
};

} // namespace disk
} // namespace cheesebase

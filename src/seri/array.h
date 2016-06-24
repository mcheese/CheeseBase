// Licensed under the Apache License 2.0 (see LICENSE file).
#pragma once

#include "value.h"
#include "btree/btree.h"
#include "../model.h"
#include "../exceptions.h"

namespace cheesebase {
namespace disk {

class ArrayW : public ValueW {
public:
  ArrayW(Transaction& ta) : ValueW(ta), tree_{ ta } { addr_ = tree_.addr(); }

  ArrayW(Transaction& ta, Addr addr) : ValueW(ta, addr), tree_{ ta, addr } {}

  Writes getWrites() const override { return tree_.getWrites(); }

  void destroy() override { return tree_.destroy(); }

  bool insert(Key index, const model::Value& val, Overwrite ow) {
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
  }

  model::PValue getChildValue(model::Index index) {
    if (index > Key::sMaxKey) throw IndexOutOfRangeError();
    return tree_.getChildValue(Key(index));
  }

  std::unique_ptr<ValueW> getChildCollectionW(Transaction& ta,
                                              model::Index index) {
    if (index > Key::sMaxKey) throw IndexOutOfRangeError();
    return tree_.getChildCollectionW(ta, Key(index));
  }

  std::unique_ptr<ValueR> getChildCollectionR(model::Index index) {
    if (index > Key::sMaxKey) throw IndexOutOfRangeError();
    return tree_.getChildCollectionR(Key(index));
  }

  model::Array getArray() { return tree_.getArray(); }

private:
  btree::BtreeReadOnly tree_;
};

} // namespace disk
} // namespace cheesebase

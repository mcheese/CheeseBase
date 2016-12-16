// Licensed under the Apache License 2.0 (see LICENSE file).
#pragma once

#include "../exceptions.h"
#include "btree/btree.h"
#include "value.h"

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
  model::Value getValue() override;
  model::Value getChildValue(uint64_t index);
  std::unique_ptr<ValueW> getChildCollectionW(Transaction& ta, uint64_t index);
  std::unique_ptr<ValueR> getChildCollectionR(uint64_t index);
  model::Collection getArray();

private:
  btree::BtreeReadOnly tree_;
};

} // namespace disk
} // namespace cheesebase

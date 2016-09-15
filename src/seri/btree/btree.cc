// Licensed under the Apache License 2.0 (see LICENSE file).
#include "btree.h"

#include "../array.h"
#include "../object.h"
#include "common.h"
#include "leaf.h"
#include "read.h"

namespace cheesebase {
namespace disk {
namespace btree {

////////////////////////////////////////////////////////////////////////////////
// BtreeWriteable

BtreeWritable::BtreeWritable(Transaction& ta, Addr root) {
  auto page = ta.load(root.pageNr());
  root_ = openRootW(ta, root, *this);
}

BtreeWritable::BtreeWritable(Transaction& ta) {
  root_ = std::make_unique<RootLeafW>(ta, *this);
}

BtreeWritable::~BtreeWritable() {}

Addr BtreeWritable::addr() const { return root_->addr(); }

bool BtreeWritable::insert(Key key, const model::Value& val, Overwrite o) {
  return root_->insert(key, val, o, nullptr);
}

Key BtreeWritable::append(const model::Value& val) {
  return root_->append(val, nullptr);
}

bool BtreeWritable::remove(Key key) { return root_->remove(key, nullptr); }

void BtreeWritable::destroy() { root_->destroy(); }

Writes BtreeWritable::getWrites() const { return root_->getWrites(); }

////////////////////////////////////////////////////////////////////////////////
// BtreeReadOnly

BtreeReadOnly::BtreeReadOnly(Database& db, Addr root) : db_(db), root_(root) {}

model::Tuple BtreeReadOnly::getObject() {
  model::Tuple obj;
  NodeR::getAll(db_, root_, obj);
  return obj;
}

ArrayMap BtreeReadOnly::getArray() {
  ArrayMap arr;
  NodeR::getAll(db_, root_, arr);
  return arr;
}

model::Value BtreeReadOnly::getChildValue(Key key) {
  return NodeR::getChildValue(db_, root_, key);
}

std::unique_ptr<ValueW> BtreeReadOnly::getChildCollectionW(Transaction& ta,
                                                           Key key) {
  return NodeR::getChildCollectionW(ta, root_, key);
}

std::unique_ptr<ValueR> BtreeReadOnly::getChildCollectionR(Key key) {
  return NodeR::getChildCollectionR(db_, root_, key);
}

} // namespace btree
} // namespace disk
} // namespace cheesebase

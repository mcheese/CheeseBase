// Licensed under the Apache License 2.0 (see LICENSE file).

#include "disk_btree.h"
#include "disk_btree_impl.h"
#include "disk_object.h"
#include "disk_array.h"

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
  root_ = std::make_unique<RootLeafW>(AllocateNew(), ta, Addr(0), *this);
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

model::Object BtreeReadOnly::getObject() {
  model::Object obj;
  openNodeR(db_, root_)->getAll(obj);
  return obj;
}

model::Array BtreeReadOnly::getArray() {
  model::Array obj;
  openNodeR(db_, root_)->getAll(obj);
  return obj;
}

std::unique_ptr<model::Value> BtreeReadOnly::getChildValue(Key key) {
  return openNodeR(db_, root_)->getChildValue(key);
}

std::unique_ptr<ValueW> BtreeReadOnly::getChildCollectionW(Transaction& ta,
                                                           Key key) {
  return openNodeR(db_, root_)->getChildCollectionW(ta, key);
}

std::unique_ptr<ValueR> BtreeReadOnly::getChildCollectionR(Key key) {
  return openNodeR(db_, root_)->getChildCollectionR(key);
}

} // namespace btree
} // namespace disk
} // namespace cheesebase

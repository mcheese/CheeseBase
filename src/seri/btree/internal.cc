// Licensed under the Apache License 2.0 (see LICENSE file).
#include "internal.h"

#include "../../core.h"
#include "../array.h"
#include "../model.h"
#include "../object.h"
#include "../string.h"
#include "../value.h"
#include "leaf.h"
#include <algorithm>

namespace cheesebase {
namespace disk {
namespace btree {

namespace {

// Returns (illegal) max value \c Key.
Key infiniteKey() {
  Key k{ 0 };
  k.value = static_cast<uint64_t>(-1);
  return k;
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
// InternalEntriesW

InternalEntriesW::InternalEntriesW(Transaction& ta, Addr addr)
    : ta_{ ta }, addr_{ addr } {}

InternalEntriesW::InternalEntriesW(Transaction& ta, Addr first,
                                   DskInternalNode::iterator begin,
                                   DskInternalNode::iterator end)
    : ta_{ ta }
    , addr_{ ta.alloc(kBlockSize).addr }
    , node_{ std::make_unique<DskInternalNode>() } {
  auto amount = std::distance(begin, end);
  Expects(amount <= kMaxInternalEntries);
  Expects(amount > 0);

  node_->hdr.fromSize(amount);
  node_->first = first;
  std::copy(begin, end, node_->begin());
  for (auto it = node_->pairs.begin() + amount; it < node_->pairs.end(); ++it) {
    it->zero();
  }
}

InternalEntriesW::InternalEntriesW(Transaction& ta, Addr addr, Addr left,
                                   Key sep, Addr right)
    : ta_{ ta }, addr_{ addr }, node_{ std::make_unique<DskInternalNode>() } {
  node_->hdr.fromSize(1);
  node_->first = left;
  node_->begin()->entry.fromKey(sep);
  node_->begin()->addr = right;
  for (auto it = std::next(node_->pairs.begin()); it < node_->pairs.end();
       ++it) {
    it->zero();
  }
}

Addr DskInternalNode::searchAddr(Key key) const {
  auto it = std::upper_bound(begin(), end(), key);
  return (it == begin() ? first : std::prev(it)->addr);
}

namespace {
//! Return \c Addr of sibling. Prefers left sibling if possible.
auto searchSiblingAddrHelper(const DskInternalNode& node, Key key) {
  auto it = std::upper_bound(node.begin(), node.end(), key);

  // search result is one to high (begin() means node.first)
  if (it == node.begin()) return it->addr; // right sibling
  if (it == std::next(node.begin())) return node.first;
  return std::prev(it, 2)->addr;
}

} // anonymous namespace

Addr InternalEntriesW::searchChildAddr(Key key) {
  if (!node_) {
    auto ref = ta_.loadBlock<kBlockSize>(addr_);
    return getFromSpan<DskInternalNode>(*ref).searchAddr(key);
  }
  return node_->searchAddr(key);
}

Addr InternalEntriesW::searchSiblingAddr(Key key) {
  if (!node_) {
    auto ref = ta_.loadBlock<kBlockSize>(addr_);
    return searchSiblingAddrHelper(getFromSpan<DskInternalNode>(*ref), key);
  }
  return searchSiblingAddrHelper(*node_, key);
}

void InternalEntriesW::init() {
  if (!node_) {
    node_ = std::make_unique<DskInternalNode>();
    auto ref = ta_.loadBlock<kBlockSize>(addr_);
    auto target = gsl::as_writeable_bytes(Span<DskInternalNode>(*node_));
    std::copy(ref->begin(), ref->end(), target.begin());
    node_->hdr.check();
  }
}

size_t InternalEntriesW::size() {
  init();
  return node_->hdr.size();
}

bool InternalEntriesW::isFull() { return size() >= kMaxInternalEntries; }

void InternalEntriesW::insert(Key key, Addr addr) {
  init();
  Expects(!isFull());
  Expects(size() > 0);

  auto end = node_->end();
  auto it = std::upper_bound(node_->begin(), end, key);
  if (it != end) std::move_backward(it, end, std::next(end));

  it->addr = addr;
  it->entry.fromKey(key);

  ++(node_->hdr);
}

DskInternalNode::iterator InternalEntriesW::search(Key key) {
  init();
  Expects(size() >= 1);
  auto it = std::upper_bound(node_->begin(), node_->end(), key);
  Expects(it > node_->begin());
  return std::prev(it);
}

void InternalEntriesW::remove(DskInternalNode::iterator e) {
  init();
  Expects(e >= node_->begin() && e < node_->end());
  std::copy(std::next(e), node_->end(), e);
  std::prev(node_->end())->zero();
  --(node_->hdr);
}

Key InternalEntriesW::remove(Key key) {
  auto it = search(key);
  auto removed_key = it->entry.key.key();
  remove(it);
  return removed_key;
}

void InternalEntriesW::removeTail(DskInternalNode::iterator from) {
  init();
  auto end = node_->end();
  auto begin = node_->begin();
  Expects(from < end);
  Expects(from > begin);

  for (auto it = from; it < end; ++it) it->zero();
  node_->hdr.fromSize(std::distance(begin, from));
}

void InternalEntriesW::removeHead(DskInternalNode::iterator to) {
  init();
  auto end = node_->end();
  auto begin = node_->begin();
  auto amount = std::distance(std::next(to), end);
  Expects(to < end);
  Expects(to > begin);

  node_->first = to->addr;
  std::copy(std::next(to), end, begin);
  for (auto it = begin + amount; it < end; ++it) it->zero();
  node_->hdr.fromSize(amount);
}

void InternalEntriesW::prepend(DskInternalNode::iterator from,
                               DskInternalNode::iterator to, Key sep) {
  init();
  auto amount = std::distance(from, to);
  if (amount == 0) return;
  Expects(amount + size() <= kMaxInternalEntries);
  Expects(sep > std::prev(to)->entry.key.key());

  std::copy_backward(begin(), end(), end() + amount);
  std::copy(std::next(from), to, begin());
  (begin() + amount - 1)->addr = node_->first;
  (begin() + amount - 1)->entry.fromKey(sep);
  node_->first = from->addr;
  node_->hdr.fromSize(size() + amount);
}

void InternalEntriesW::append(DskInternalNode::iterator from,
                              DskInternalNode::iterator to) {
  init();
  auto amount = std::distance(from, to);
  if (amount == 0) return;
  Expects(amount + size() <= kMaxInternalEntries);
  Expects(from->entry.key.key() > std::prev(end())->entry.key.key());

  std::copy(from, to, end());
  node_->hdr.fromSize(size() + amount);
}

void InternalEntriesW::makeRoot(Addr left, Key sep, Addr right) {
  init();
  node_->first = left;
  node_->begin()->entry.fromKey(sep);
  node_->begin()->addr = right;
  for (auto it = std::next(node_->begin()); it < node_->end(); ++it) {
    it->zero();
  }
  node_->hdr.fromSize(1);
}

void InternalEntriesW::takeNodeFrom(InternalEntriesW& other) {
  init();
  node_ = std::move(other.node_);
}

Addr InternalEntriesW::first() {
  init();
  return node_->first;
}

Key InternalEntriesW::update(Key key, Key new_key) {
  init();
  Expects(size() > 0);

  auto end = node_->end();
  auto it = std::upper_bound(node_->begin(), end, key);
  Expects(it != node_->begin());

  auto updated_key = std::prev(it)->entry.key.key();
  std::prev(it)->entry.fromKey(new_key);
  return updated_key;
}

void InternalEntriesW::addWrite(Writes& writes) const noexcept {
  if (node_) {
    node_->hdr.check();
    writes.push_back({ addr_, gsl::as_bytes(Span<DskInternalNode>(*node_)) });
  }
}

void InternalEntriesW::destroy() {
  if (!node_) {
    auto ref = ta_.loadBlock<kBlockSize>(addr_);
    auto& node = getFromSpan<DskInternalNode>(*ref);

    openNodeW(ta_, node.first)->destroy();
    for (auto& e : node) {
      openNodeW(ta_, e.addr)->destroy();
    }

  } else {
    openNodeW(ta_, node_->first)->destroy();
    for (auto& e : *node_) {
      openNodeW(ta_, e.addr)->destroy();
    }
  }

  ta_.free(addr_, kBlockSize);
}

DskInternalNode::iterator InternalEntriesW::begin() {
  init();
  return node_->begin();
}

DskInternalNode::iterator InternalEntriesW::mid() {
  init();
  auto size = node_->hdr.size();
  Expects(size >= 3);
  return node_->begin() + size / 2;
}

DskInternalNode::iterator InternalEntriesW::end() {
  init();
  return node_->end();
}

////////////////////////////////////////////////////////////////////////////////
// AbsInternalW

AbsInternalW::AbsInternalW(Transaction& ta, Addr addr)
    : NodeW(addr), entries_{ ta, addr } {}

AbsInternalW::AbsInternalW(AllocateNew, Transaction& ta, Addr first,
                           DskInternalNode::iterator begin,
                           DskInternalNode::iterator end)
    : NodeW(Addr(0)), entries_{ ta, first, begin, end } {
  addr_ = entries_.addr_;
}

AbsInternalW::AbsInternalW(Transaction& ta, Addr addr, Addr left, Key sep,
                           Addr right)
    : NodeW(addr), entries_{ ta, addr, left, sep, right } {}

bool AbsInternalW::insert(Key key, const model::Value& val, Overwrite ow,
                          AbsInternalW* parent) {
  parent_ = parent;
  return searchChild(key).insert(key, val, ow, this);
}

void AbsInternalW::insert(Key key, std::unique_ptr<NodeW> c) {
  if (!entries_.isFull()) {
    entries_.insert(key, c->addr());
    childs_.emplace(c->addr(), std::move(c));
  } else {
    split(key, std::move(c));
  }
}

Key AbsInternalW::append(const model::Value& val, AbsInternalW* parent) {
  parent_ = parent;
  return searchChild(infiniteKey()).append(val, this);
}

bool AbsInternalW::remove(Key key, AbsInternalW* parent) {
  parent_ = parent;
  return searchChild(key).remove(key, this);
}

Writes AbsInternalW::getWrites() const {
  Writes w;
  w.reserve(1 + childs_.size()); // may be more, but a good guess

  entries_.addWrite(w);

  for (auto& c : childs_) {
    auto cw = c.second->getWrites();
    w.insert(w.end(), cw.begin(), cw.end());
  }

  return w;
}

NodeW& AbsInternalW::searchChild(Key k) {
  Addr addr = entries_.searchChildAddr(k);

  auto lookup = childs_.find(addr);
  if (lookup != childs_.end()) {
    return *lookup->second;
  } else {
    auto emplace =
        childs_.emplace_hint(lookup, addr, openNodeW(entries_.ta_, addr));
    return *emplace->second;
  }
}

void AbsInternalW::destroy() {
  entries_.destroy();
  childs_.clear();
}

void AbsInternalW::appendChild(std::pair<Addr, std::unique_ptr<NodeW>>&& c) {
  childs_.insert(std::move(c));
}

NodeW& AbsInternalW::getSibling(Key key) {
  Addr sibl = entries_.searchSiblingAddr(key);

  auto lookup = childs_.find(sibl);
  if (lookup != childs_.end()) {
    return *lookup->second;
  } else {
    return *childs_.emplace_hint(lookup, sibl, openNodeW(entries_.ta_, sibl))
                ->second;
  }
}

DskInternalNode::iterator AbsInternalW::searchEntry(Key key) {
  return entries_.search(key);
}

void AbsInternalW::removeMerged(DskInternalNode::iterator it) {
  auto lookup = childs_.find(it->addr);
  if (lookup == childs_.end()) {
    throw ConsistencyError("removeMerged with unknown Address");
  }
  childs_.erase(lookup);

  entries_.remove(it);
  if (entries_.size() < kMinInternalEntries) balance();
}

Key AbsInternalW::updateMerged(Key key, Key new_key) {
  return entries_.update(key, new_key);
}

////////////////////////////////////////////////////////////////////////////////
// InternalW

void InternalW::split(Key key, std::unique_ptr<NodeW> c) {
  Expects(parent_ != nullptr);
  Expects(entries_.isFull());

  auto mid = entries_.mid();
  auto end = entries_.end();
  auto mid_key = mid->entry.key.key();

  auto sibling = std::make_unique<InternalW>(AllocateNew(), entries_.ta_,
                                             mid->addr, std::next(mid), end);

  for (auto it = mid; it < end; ++it) {
    tryTransfer(childs_, sibling->childs_, it->addr);
  }
  entries_.removeTail(mid);

  if (mid_key > key) {
    insert(key, std::move(c));
  } else {
    sibling->insert(key, std::move(c));
  }

  Ensures(entries_.size() >= kMinInternalEntries);
  Ensures(sibling->entries_.size() >= kMinInternalEntries);

  parent_->insert(mid_key, std::move(sibling));
}

void InternalW::balance() {
  Expects(parent_ != nullptr);
  Expects(entries_.size() < kMinInternalEntries);

  auto first_key = entries_.begin()->entry.key.key();
  auto& sibl = static_cast<InternalW&>(parent_->getSibling(first_key));
  sibl.entries_.init();
  sibl.parent_ = parent_;
  auto sibl_key = sibl.entries_.begin()->entry.key.key();

  if (sibl.entries_.size() <= kMinInternalEntries) {
    // merge
    if (first_key > sibl_key) {
      sibl.merge(*this);
    } else {
      merge(sibl);
    }
  } else {
    // pull stuff
    auto to_pull = (sibl.entries_.size() - entries_.size()) / 2;
    Ensures(to_pull > 0 && to_pull < kMaxInternalEntries);

    if (first_key > sibl_key) {
      // pull biggest from left
      auto from = sibl.entries_.end() - to_pull;
      auto to = sibl.entries_.end();
      Ensures(from < to);
      auto sep_key = parent_->updateMerged(first_key, from->entry.key.key());
      entries_.prepend(from, to, sep_key);

      for (auto it = from; it < to; ++it) {
        tryTransfer(sibl.childs_, childs_, it->addr);
      }

      sibl.entries_.removeTail(from);

    } else {
      // pull smallest from right
      auto first = sibl.entries_.first();
      auto from = sibl.entries_.begin();
      auto to = sibl.entries_.begin() + to_pull;
      auto sep_key = parent_->updateMerged(sibl_key, to->entry.key.key());
      entries_.insert(sep_key, first);

      tryTransfer(sibl.childs_, childs_, first);
      for (auto it = from; it < to; ++it) {
        tryTransfer(sibl.childs_, childs_, it->addr);
      }

      entries_.append(from, to);
      sibl.entries_.removeHead(to);
    }
  }
}

void InternalW::merge(InternalW& right) {
  Expects(&right != this);
  Expects(entries_.size() + right.entries_.size() + 1 <= kMaxInternalEntries);

  auto from = right.entries_.begin();
  auto to = right.entries_.end();

  Expects(entries_.begin()->entry.key.key() < from->entry.key.key());
  auto right_entry = parent_->searchEntry(from->entry.key.key());
  entries_.insert(right_entry->entry.key.key(), right.entries_.first());
  entries_.append(from, to);

  for (auto& c : right.childs_) {
    childs_.emplace(c.first, std::move(c.second));
  }

  parent_->removeMerged(right_entry);
}

////////////////////////////////////////////////////////////////////////////////
// RootInternalW

RootInternalW::RootInternalW(Transaction& ta, Addr addr, BtreeWritable& parent)
    : AbsInternalW(ta, addr), parent_(parent) {}

RootInternalW::RootInternalW(Transaction& ta, Addr addr,
                             std::unique_ptr<LeafW> left_leaf, Key sep,
                             std::unique_ptr<LeafW> right_leaf,
                             BtreeWritable& parent)
    : AbsInternalW(ta, addr, left_leaf->addr(), sep, right_leaf->addr())
    , parent_{ parent } {
  auto left_addr = left_leaf->addr();
  auto right_addr = right_leaf->addr();
  childs_.emplace(left_addr, std::move(left_leaf));
  childs_.emplace(right_addr, std::move(right_leaf));
}

void RootInternalW::split(Key key, std::unique_ptr<NodeW> child) {
  Expects(entries_.isFull());

  auto beg = entries_.begin();
  auto mid = entries_.mid();
  auto end = entries_.end();
  auto mid_key = mid->entry.key.key();

  auto left = std::make_unique<InternalW>(AllocateNew(), entries_.ta_,
                                          entries_.first(), beg, mid);

  auto right = std::make_unique<InternalW>(AllocateNew(), entries_.ta_,
                                           mid->addr, std::next(mid), end);

  tryTransfer(childs_, left->childs_, entries_.first());
  for (auto it = beg; it < mid; ++it) {
    tryTransfer(childs_, left->childs_, it->addr);
  }

  for (auto it = mid; it < end; ++it) {
    tryTransfer(childs_, right->childs_, it->addr);
  }

  Ensures(childs_.empty());

  if (mid_key > key) {
    left->insert(key, std::move(child));
  } else {
    right->insert(key, std::move(child));
  }

  entries_.removeTail(std::next(beg));
  entries_.makeRoot(left->addr(), mid_key, right->addr());

  Ensures(left->entries_.size() >= kMinInternalEntries);
  Ensures(right->entries_.size() >= kMinInternalEntries);
  auto right_addr = right->addr();
  auto left_addr = left->addr();
  childs_.emplace(right_addr, std::move(right));
  childs_.emplace(left_addr, std::move(left));
}

void RootInternalW::balance() {
  // this node may be smaller than min size
  if (entries_.size() > 0) return;

  Expects(childs_.size() == 1);

  auto childp = std::move(childs_.begin()->second);
  childs_.clear();

  auto child_internal = dynamic_cast<InternalW*>(childp.get());
  if (child_internal != nullptr) {
    // the child is internal node
    // pull content into this node

    entries_.takeNodeFrom(child_internal->entries_);
    childs_ = std::move(child_internal->childs_);
    entries_.ta_.free(child_internal->addr(), kBlockSize);

    return;
  }

  auto child_leaf = dynamic_cast<LeafW*>(childp.get());
  if (child_leaf != nullptr) {
    // the child is leaf node
    // tree becomes single RootLeafW

    auto new_me = std::unique_ptr<RootLeafW>(
        new RootLeafW(std::move(*child_leaf), addr_, parent_));
    parent_.root_ = std::move(new_me);

    return;
  }

  // there should be now way this can happen.
  throw ConsistencyError("Invalid merge below root node");
}

} // namespace btree
} // namespace disk
} // namespace cheesebase

// Licensed under the Apache License 2.0 (see LICENSE file).
#include "leaf.h"

#include "../../core.h"
#include "../array.h"
#include "../model.h"
#include "../object.h"
#include "../string.h"
#include "../value.h"
#include "internal.h"
#include <algorithm>

namespace cheesebase {
namespace disk {
namespace btree {

////////////////////////////////////////////////////////////////////////////////
// AbsLeafW

AbsLeafW::AbsLeafW(AllocateNew, AbsLeafW&& o, Addr next)
    : NodeW(o.ta_.alloc(kBlockSize).addr)
    , linked_(std::move(o.linked_))
    , ta_{ o.ta_ }
    , node_{ std::move(o.node_) }
    , size_{ o.size_ } {
  node_->hdr.fromAddr(next);
}

AbsLeafW::AbsLeafW(AllocateNew, Transaction& ta, Addr next)
    : NodeW(ta.alloc(kBlockSize).addr)
    , ta_{ ta }
    , node_{ std::make_unique<DskLeafNode>() }
    , size_{ 0 } {
  node_->hdr.fromAddr(next);
}

AbsLeafW::AbsLeafW(Transaction& ta, Addr addr) : NodeW(addr), ta_{ ta } {}

Writes AbsLeafW::getWrites() const {
  Writes w;
  w.reserve(1 + linked_.size()); // may be more, but a good guess

  if (node_) w.push_back({ addr_, gsl::as_bytes(Span<DskLeafNode>(*node_)) });

  for (auto& c : linked_) {
    auto cw = c.second->getWrites();
    std::move(cw.begin(), cw.end(), std::back_inserter(w));
  }

  return w;
}

void AbsLeafW::destroy() {
  auto dstr = [this](const DskLeafNode& node) {
    auto it = node.words.begin();
    while (it < node.words.end() && *it != 0) it += destroyValue(it);
  };

  if (node_) {
    dstr(*node_);
  } else {
    auto ref = ta_.loadBlock<kBlockSize>(addr_);
    dstr(getFromSpan<DskLeafNode>(*ref));
  }

  ta_.free(addr_, kBlockSize);
}

template <typename ConstIt>
size_t AbsLeafW::destroyValue(ConstIt it) {
  auto entry = DskLeafEntry(*it);

  if (entry.value.type == ValueType::object ||
      entry.value.type == ValueType::string ||
      entry.value.type == ValueType::array) {

    auto lookup = linked_.find(entry.key.key());
    if (lookup != linked_.end()) {
      lookup->second->destroy();
      linked_.erase(lookup);
    } else {
      auto addr = Addr(*std::next(it));
      switch (entry.value.type) {
      case ValueType::object:
        ObjectW(ta_, addr).destroy();
        break;
      case ValueType::string:
        StringW(ta_, addr).destroy();
        break;
      case ValueType::array:
        ArrayW(ta_, addr).destroy();
        break;
      }
    }
  }

  return entry.extraWords() + 1;
}

Key AbsLeafW::append(const model::Value& val, AbsInternalW* parent) {
  parent_ = parent;
  init();
  Expects(node_->hdr.next() == Addr(0));

  Key key{ 0 };
  auto it = node_->words.begin();
  while (it < node_->words.end() && *it != 0) {
    auto e = DskLeafEntry(*it);
    key = Key(e.key.key().value + 1);
    it += 1 + e.extraWords();
  }


  auto ins = insert(key, val, Overwrite::Insert, parent);
  Ensures(ins == true);
  return key;
}

bool AbsLeafW::insert(Key key, const model::Value& val, Overwrite ow,
                      AbsInternalW* parent) {
  parent_ = parent;

  auto type = valueType(val);
  if (type == ValueType::missing) return true;

  int extra_words = gsl::narrow_cast<int>(nrExtraWords(type));

  init();

  // find position to insert
  auto it = node_->search(key);
  Ensures(it <= node_->end());
  auto top = node_->begin() + size_;
  bool update = it < top && keyFromWord(*it) == key;
  if ((ow == Overwrite::Update && !update) ||
      (ow == Overwrite::Insert && update)) {
    return false;
  }

  // enough space to insert?
  if (size_ + 1 + extra_words -
          (update ? (DskLeafEntry(*it).extraWords() + 1) : 0) <=
      kMaxLeafWords) {

    // make space
    if (update) {
      auto old_size = destroyValue(it);
      int diff = 1 + extra_words - old_size;
      node_->shift(it + old_size, diff);
      size_ += diff;
    } else {
      node_->shift(it, 1 + extra_words);
      size_ += 1 + extra_words;
    }

    // put the first word
    *it = DskLeafEntry{ key, type }.word();

    // put extra words
    // recurse into inserting remotely stored elements if needed
    it++;
    if (type == ValueType::object) {
      auto& obj = boost::get<model::STuple>(val);
      auto el = std::make_unique<ObjectW>(ta_);
      for (auto& c : *obj) {
        el->insert(ta_.key(c.first), c.second, Overwrite::Insert);
      }
      *it = el->addr().value;
      auto emp = linked_.emplace(key, std::move(el));
      Expects(emp.second);

    } else if (type == ValueType::array) {
      auto& arr = boost::get<model::SCollection>(val);
      auto el = std::make_unique<ArrayW>(ta_);

      Key idx{ 0 };
      for (auto& c : *arr) {
        if (c.get().type() != typeid(model::Missing)) {
          el->insert(idx, c, Overwrite::Insert);
        }
        idx.value++;
      }

      *it = el->addr().value;
      auto emp = linked_.emplace(key, std::move(el));
      Expects(emp.second);

    } else if (type == ValueType::string) {
      auto& str = boost::get<model::String>(val);
      auto el = std::make_unique<StringW>(ta_, str);
      *it = el->addr().value;
      auto emp = linked_.emplace(key, std::move(el));
      Expects(emp.second);

    } else {
      auto extras = extraWords(val);
      std::copy(extras.begin(), extras.end(), it);
    }

  } else {
    split(key, val);
  }

  return true;
}

template <typename It>
void AbsLeafW::appendWords(It from, It to) {
  init();
  Expects(from <= to);
  Expects(size_ + std::distance(from, to) <= kMaxLeafWords);

  std::copy(from, to, node_->begin() + size_);
  size_ += std::distance(from, to);
}

template <typename It>
void AbsLeafW::prependWords(It from, It to) {
  init();
  Expects(from <= to);
  Expects(size_ + std::distance(from, to) <= kMaxLeafWords);

  node_->shift(node_->begin(), std::distance(from, to));
  std::copy(from, to, node_->begin());
  size_ += std::distance(from, to);
}

bool AbsLeafW::remove(Key key, AbsInternalW* parent) {
  parent_ = parent;
  init();

  // find position
  auto it = node_->search(key);

  // return false if not found
  if (it == node_->end() || keyFromWord(*it) != key) return false;

  auto val_size = destroyValue(it);
  node_->shift(it + val_size, -gsl::narrow_cast<int>(val_size));
  size_ -= val_size;

  if (size_ < kMinLeafWords) balance();

  return true;
}

void AbsLeafW::init() {
  if (!node_) {
    node_ = std::make_unique<DskLeafNode>();
    auto block = ta_.loadBlock<kBlockSize>(addr_);
    std::copy(block->begin(), block->end(),
              gsl::as_writeable_bytes(Span<DskLeafNode, 1>(*node_)).begin());
    size_ = node_->findSize();
  }
}

size_t AbsLeafW::size() const { return size_; }

std::unique_ptr<LeafW> AbsLeafW::splitHelper(Key key, const model::Value& val) {
  init();
  Expects(size_ > kMinLeafWords && size_ <= kMaxLeafWords);

  auto right_leaf =
      std::make_unique<LeafW>(AllocateNew(), ta_, node_->hdr.next());

  auto new_val_len = nrExtraWords(valueType(val)) + 1;
  auto top = node_->begin() + size_;
  auto it = node_->begin();
  auto half = it + (size_ + new_val_len) / 2;
  Ensures(half < node_->end());

  bool new_here = false;
  while (it < half) {
    if (!new_here && key < keyFromWord(*it)) {
      new_here = true;
      half -= new_val_len;
    } else {
      it += entrySize(*it);
    }
  }

  node_->hdr.fromAddr(right_leaf->addr());
  right_leaf->appendWords(it, top);
  {
    auto linked_from = linked_.lower_bound(keyFromWord(*it));
    for (auto it = linked_from; it < linked_.end(); ++it) {
      right_leaf->linked_.emplace(std::move(*it));
    }
    linked_.erase(linked_from, linked_.end());
  }
  std::fill(it, top, 0);
  size_ = std::distance(node_->begin(), it);

  if (new_here)
    insert(key, val, Overwrite::Upsert, parent_);
  else
    right_leaf->insert(key, val, Overwrite::Upsert, parent_);

  Ensures(size_ >= kMinLeafWords && size_ <= kMaxLeafWords);
  Ensures(right_leaf->size() >= kMinLeafWords &&
          right_leaf->size() <= kMaxLeafWords);

  return right_leaf;
}

////////////////////////////////////////////////////////////////////////////////
// LeafW

void LeafW::split(Key key, const model::Value& val) {
  auto right = splitHelper(key, val);
  auto sep_key = keyFromWord(*right->node_->begin());
  parent_->insert(sep_key, std::move(right));
}

void LeafW::merge(LeafW& right) {
  appendWords(right.node_->begin(), right.node_->begin() + right.size());

  for (auto& l : right.linked_) {
    linked_.emplace(std::move(l));
  }
  right.linked_.clear();
  node_->hdr.fromAddr(right.node_->hdr.next());
  ta_.free(right.addr(), kBlockSize);
  parent_->removeMerged(
      parent_->searchEntry(keyFromWord(*right.node_->begin())));
}

void LeafW::balance() {
  Expects(node_);
  Expects(size_ < kMinLeafWords);
  Expects(size_ > 1); // even if merging, the node should not be empty

  auto first_key = keyFromWord(*node_->begin());
  auto& sibl = dynamic_cast<LeafW&>(parent_->getSibling(first_key));
  sibl.init();
  sibl.parent_ = parent_;
  Expects(sibl.size() <= kMaxLeafWords && sibl.size() >= kMinLeafWords);

  auto sibl_key = keyFromWord(*sibl.node_->begin());

  if (sibl.size() + size_ <= kMaxLeafWords) {
    // actually merge them

    if (sibl_key > first_key) {
      merge(sibl);
    } else {
      sibl.merge(*this);
    }

  } else {
    // too big, just steal some values
    auto medium = (size_ + sibl.size()) / 2;
    Ensures(medium >= kMinLeafWords);

    if (sibl_key > first_key) {
      // pull lowest
      auto it = sibl.node_->begin();
      auto sibl_beg = sibl.node_->begin();
      while (size_ + std::distance(sibl_beg, it) < medium) {
        auto entry = DskLeafEntry(*it);
        tryTransfer(sibl.linked_, linked_, entry.key.key());
        it += entry.extraWords() + 1;
      }
      appendWords(sibl_beg, it);
      sibl.node_->shift(it, -std::distance(sibl_beg, it));
      sibl.size_ -= std::distance(sibl_beg, it);

      Ensures(sibl_key < keyFromWord(*sibl_beg));
      parent_->updateMerged(sibl_key, keyFromWord(*sibl_beg));

    } else {
      // pull biggest
      auto it = sibl.node_->begin();
      auto sibl_top = sibl.node_->begin() + sibl.size();
      while (size_ + std::distance(it, sibl_top) > medium) {
        auto entry = DskLeafEntry(*it);
        tryTransfer(sibl.linked_, linked_, entry.key.key());
        it += entry.extraWords() + 1;
      }
      prependWords(it, sibl_top);
      sibl.node_->shift(sibl_top, -std::distance(it, sibl_top));
      sibl.size_ -= std::distance(it, sibl_top);

      Ensures(sibl_key < keyFromWord(*node_->begin()));
      parent_->updateMerged(first_key, keyFromWord(*node_->begin()));
    }

    Ensures(size_ >= kMinLeafWords);
    Ensures(sibl.size() >= kMinLeafWords);
  }
}

////////////////////////////////////////////////////////////////////////////////
// RootLeafW

RootLeafW::~RootLeafW() {}

RootLeafW::RootLeafW(Transaction& ta, BtreeWritable& parent)
    : AbsLeafW(AllocateNew(), ta), tree_(parent) {}

RootLeafW::RootLeafW(Transaction& ta, Addr addr, BtreeWritable& parent)
    : AbsLeafW(ta, addr), tree_(parent) {}

RootLeafW::RootLeafW(LeafW&& o, Addr a, BtreeWritable& parent)
    : AbsLeafW(o.ta_, a), tree_(parent) {
  node_ = std::move(o.node_);
  linked_ = std::move(o.linked_);
  size_ = o.size();
  ta_.free(o.addr_, kBlockSize);
}

void RootLeafW::split(Key key, const model::Value& val) {
  auto right = splitHelper(key, val);
  auto left =
      std::make_unique<LeafW>(AllocateNew(), std::move(*this), right->addr());

  auto sep_key = keyFromWord(*right->node_->begin());
  auto new_me = std::unique_ptr<RootInternalW>(new RootInternalW(
      ta_, addr_, std::move(left), sep_key, std::move(right), tree_));
  tree_.root_ = std::move(new_me);
}

void RootLeafW::balance() {
  // root leaf merging is not existent
  // this node type may be empty
  return; // NOP
}

} // namespace btree
} // namespace disk
} // namespace cheesebase

// Licensed under the Apache License 2.0 (see LICENSE file).

#include "disk_btree_impl.h"

#include "core.h"
#include "model.h"
#include "disk_model.h"
#include "disk_value.h"
#include "disk_string.h"
#include "disk_object.h"
#include "disk_array.h"
#include <algorithm>

namespace cheesebase {
namespace disk {
namespace btree {

namespace {

constexpr std::ptrdiff_t kNodeSize = kBlockSize - sizeof(DskBlockHdr);
constexpr size_t kLeafNodeMaxWords = kNodeSize / sizeof(uint64_t);
constexpr size_t kLeafNodeMinWords = kLeafNodeMaxWords / 2 - kLeafEntryMaxWords;
constexpr size_t kMaxLeafWords = (kNodeSize - 8) / 8;
constexpr size_t kMinLeafWords = kMaxLeafWords / 2 - kLeafEntryMaxWords;
constexpr size_t kMaxInternalEntries = (kNodeSize - 16) / 16;
constexpr size_t kMinInternalEntries = (kMaxInternalEntries / 2) - 1;

////////////////////////////////////////////////////////////////////////////////
// on disk structures

//! Returns (illegal) max value \c Key.
Key infiniteKey() {
  Key k{ 0 };
  k.value = static_cast<uint64_t>(-1);
  return k;
};

//! 6 byte key struct to write on disk.
CB_PACKED(struct DskKey {
  DskKey() = default;
  explicit DskKey(Key key)
      : big_{ static_cast<uint32_t>(key.value) }
      , small_{ static_cast<uint16_t>(key.value >> 32) } {
    Expects(key.value < (static_cast<uint64_t>(1) << 48));
  }

  Key key() const noexcept {
    return Key(static_cast<uint64_t>(big_) +
               (static_cast<uint64_t>(small_) << 32));
  }

private:
  uint32_t big_;
  uint16_t small_;
});
static_assert(sizeof(DskKey) == 6, "Invalid disk key size");

CB_PACKED(struct DskValueHdr {
  uint8_t magic_byte;
  uint8_t type;
});
static_assert(sizeof(DskValueHdr) == 2, "Invalid disk value header size");

CB_PACKED(struct DskLeafEntry {
  DskLeafEntry(uint64_t w) {
    *reinterpret_cast<uint64_t*>(this) = w;
    if (value.magic_byte != '!')
      throw ConsistencyError("No magic byte in value");
  }
  DskLeafEntry(Key k, ValueType t) : value{ '!', t }, key{ k } {}

  size_t extraWords() const { return nrExtraWords(value.type); }
  uint64_t word() { return *reinterpret_cast<uint64_t*>(this); }

  DskValueHdr value;
  DskKey key;
});
static_assert(sizeof(DskLeafEntry) == 8, "Invalid DskLeafEntry size");

CB_PACKED(struct DskInternalEntry {
  DskInternalEntry() = default;
  DskInternalEntry(uint64_t w) {
    *reinterpret_cast<uint64_t*>(this) = w;
    if (magic[0] != '-' || magic[1] != '>')
      throw ConsistencyError("No magic byte in key");
  }

  uint64_t fromKey(Key k) noexcept {
    key = DskKey(k);
    magic[0] = '-';
    magic[1] = '>';
    return *reinterpret_cast<uint64_t*>(this);
  }

  uint64_t word() const noexcept {
    return *reinterpret_cast<const uint64_t*>(this);
  }

  void zero() noexcept {
    *reinterpret_cast<uint64_t*>(this) = 0;
  }

  char magic[2];
  DskKey key;
});
static_assert(sizeof(DskInternalEntry) == 8, "Invalid DskInternalEntry size");

Key keyFromWord(uint64_t w) { return DskLeafEntry(w).key.key(); }

size_t entrySize(uint64_t e) { return DskLeafEntry(e).extraWords() + 1; }

CB_PACKED(struct DskLeafHdr {
  DskLeafHdr() = default;

  DskLeafHdr& fromAddr(Addr d) {
    Expects((d.value & (static_cast<uint64_t>(0xff) << 56)) == 0);
    constexpr uint64_t magic = static_cast<uint64_t>('L') << 56;
    data = d.value + magic;
    return *this;
  }

  DskLeafHdr& fromDsk(uint64_t w) {
    data = w;
    if (!hasMagic()) throw ConsistencyError("No magic byte in leaf node");
    return *this;
  }

  bool hasMagic() const { return (data >> 56) == 'L'; }
  Addr next() const { return Addr(data & lowerBitmask(56)); }

  uint64_t data;
});
static_assert(sizeof(DskLeafHdr) == 8, "Invalid DskLeafHdr size");

CB_PACKED(struct DskInternalHdr {
  DskInternalHdr() = default;
  DskInternalHdr& fromSize(uint64_t d) {
    Expects((d & (static_cast<uint64_t>(0xff) << 56)) == 0);
    constexpr uint64_t magic = static_cast<uint64_t>('I') << 56;
    data = d + magic;
    return *this;
  }
  DskInternalHdr& fromRaw(uint64_t w) {
    data = w;
    if (!hasMagic()) throw ConsistencyError("No magic byte in internal node");
    return *this;
  }

  bool hasMagic() const noexcept { return (data >> 56) == 'I'; }
  void check() const {
    if (!hasMagic()) throw ConsistencyError("Expected internal node header");
  }

  size_t size() const {
    size_t s = gsl::narrow_cast<size_t>(data & lowerBitmask(56));
    if (s > kMaxInternalEntries)
      throw ConsistencyError("Internal node entry count to big");
    return s;
  }

  void operator--() {
    Expects(data & lowerBitmask(56));
    data--;
  }

  void operator++() {
    data++;
  }

  uint64_t data;
});
static_assert(sizeof(DskInternalHdr) == 8, "Invalid DskInternalHdr size");

////////////////////////////////////////////////////////////////////////////////
// free functions

bool isNodeLeaf(const ReadRef<kBlockSize>& block) {
  // first byte of Addr is always 0
  // next-ptr of leafs put a flag in the first byte, marking the node as leaf
  return gsl::as_span<const DskLeafHdr>(block->subspan(
      ssizeof<DskBlockHdr>(), sizeof(DskLeafHdr)))[0].hasMagic();
}

std::unique_ptr<NodeW> openNodeW(Transaction& ta, Addr addr) {
  auto block = ta.loadBlock<kBlockSize>(addr);

  if (isNodeLeaf(block))
    return std::make_unique<LeafW>(ta, addr);
  else
    return std::make_unique<InternalW>(ta, addr);
}

template <class C, typename K>
void tryTransfer(C& from, C& to, const K& key) {
  auto lookup = from.find(key);
  if (lookup != from.end()) {
    to.insert(std::move(*lookup));
    from.erase(lookup);
  }
}

} // anonymous namespace

std::unique_ptr<NodeW> openRootW(Transaction& ta, Addr addr, BtreeWritable& tree) {
  auto block = ta.loadBlock<kBlockSize>(addr);

  if (isNodeLeaf(block))
    return std::make_unique<RootLeafW>(ta, addr, tree);
  else
    return std::make_unique<RootInternalW>(ta, addr, tree);
}

////////////////////////////////////////////////////////////////////////////////
// Node

Node::Node(Addr addr) : addr_(addr) {}

Addr Node::addr() const { return addr_; }

////////////////////////////////////////////////////////////////////////////////
// NodeW

NodeW::NodeW(Addr addr) : Node(addr) {}

////////////////////////////////////////////////////////////////////////////////
// AbsLeafW

CB_PACKED(struct DskLeafNode {
  DskLeafNode() = default;

  DskLeafHdr hdr;
  std::array<uint64_t, (kNodeSize - ssizeof<DskLeafHdr>()) / 8> words;

  using iterator = decltype(words)::iterator;
  using const_iterator = decltype(words)::const_iterator;

  size_t findSize() const {
    auto it = words.begin();
    while (it < words.end() && *it != 0) {
      it += entrySize(*it);
    }
    return std::distance(words.begin(), it);
  }

  auto begin() noexcept { return words.begin(); }
  auto end() noexcept { return words.end(); }

  auto search(Key key) {
    auto it = words.begin();
    while ((it < words.end()) && (*it != 0) && (keyFromWord(*it) < key)) {
      it += entrySize(*it);
    }
    return it;
  }

  void shift(iterator it, std::ptrdiff_t amount) {
    if (amount >= 0) {
      std::copy_backward(it, end() - amount, end());
    } else {
      std::copy(it, end(), it + amount);
      std::fill(end() + amount, end(), 0);
    }
  }
});
static_assert(ssizeof<DskLeafNode>() == kNodeSize, "Invalid DskLeafNode size");

AbsLeafW::AbsLeafW(AllocateNew, AbsLeafW&& o, Addr next)
    : NodeW(o.ta_.alloc(kNodeSize).addr)
    , ta_{ o.ta_ }
    , node_{ std::move(o.node_) }
    , size_{ o.size_ }
    , linked_(std::move(o.linked_)) {
  node_->hdr.fromAddr(next);
}

AbsLeafW::AbsLeafW(AllocateNew, Transaction& ta, Addr next)
    : NodeW(ta.alloc(kNodeSize).addr)
    , ta_{ ta }
    , node_{ std::make_unique<DskLeafNode>() }
    , size_{ 0 } {
  node_->hdr.fromAddr(next);
}

AbsLeafW::AbsLeafW(Transaction& ta, Addr addr) : NodeW(addr), ta_{ ta } {}

Writes AbsLeafW::getWrites() const {
  Writes w;
  w.reserve(1 + linked_.size()); // may be more, but a good guess

  if (node_)
    w.push_back({ Addr(addr_.value + sizeof(DskBlockHdr)),
                  gsl::as_bytes(Span<DskLeafNode>(*node_)) });

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
    auto block = ta_.loadBlock<kBlockSize>(addr_);
    auto& node = gsl::as_span<DskLeafNode>(
        block->subspan(ssizeof<DskBlockHdr>(), kNodeSize))[0];
    dstr(node);
  }

  ta_.free(addr_);
}

template <typename ConstIt>
size_t AbsLeafW::destroyValue(ConstIt it) {
  auto entry = DskLeafEntry(*it);

  switch (entry.value.type) {
  case ValueType::object:
    ObjectW(ta_, Addr(*std::next(it))).destroy();
    break;
  case ValueType::string:
    StringW(ta_, Addr(*std::next(it))).destroy();
    break;
  case ValueType::array:
    ArrayW(ta_, Addr(*std::next(it))).destroy();
    break;
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
  init();

  int extra_words = gsl::narrow_cast<int>(nrExtraWords(val));

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
  if (size_ + 1 + extra_words - (update ?  (DskLeafEntry(*it).extraWords() + 1) : 0) <=
      kMaxLeafWords) {

    // make space
    if (update) {
      auto old_entry = DskLeafEntry(*it);
      auto old_extra = gsl::narrow_cast<int>(old_entry.extraWords());
      switch (old_entry.value.type) {
      case ValueType::object:
        ObjectW(ta_, Addr(*std::next(it))).destroy();
        linked_.erase(old_entry.key.key());
        break;
      case ValueType::string:
        StringW(ta_, Addr(*std::next(it))).destroy();
        linked_.erase(old_entry.key.key());
        break;
      case ValueType::array:
        ArrayW(ta_, Addr(*std::next(it))).destroy();
        linked_.erase(old_entry.key.key());
        break;
      }
      node_->shift(it + old_extra + 1, extra_words - old_extra);
      size_ += extra_words - old_extra;
    } else {
      node_->shift(it, 1 + extra_words);
      size_ += 1 + extra_words;
    }

    // put the first word
    auto t = valueType(val);
    *it = DskLeafEntry{ key, t }.word();

    // put extra words
    // recurse into inserting remotely stored elements if needed
    it++;
    if (t == ValueType::object) {
      auto& obj = dynamic_cast<const model::Object&>(val);
      auto el = std::make_unique<ObjectW>(ta_);
      for (auto& c : obj) {
        el->insert(ta_.key(c.first), *c.second, Overwrite::Insert);
      }
      *it = el->addr().value;
      auto emp = linked_.emplace(key, std::move(el));
      Expects(emp.second);

    } else if (t == ValueType::array) {
      auto& arr = dynamic_cast<const model::Array&>(val);
      auto el = std::make_unique<ArrayW>(ta_);
      for (auto& c : arr) {
        el->insert(Key(c.first), *c.second, Overwrite::Insert);
      }
      *it = el->addr().value;
      auto emp = linked_.emplace(key, std::move(el));
      Expects(emp.second);

    } else if (t == ValueType::string) {
      auto& str = dynamic_cast<const model::Scalar&>(val);
      auto el =
          std::make_unique<StringW>(ta_, boost::get<model::String>(str.data()));
      *it = el->addr().value;
      auto emp = linked_.emplace(key, std::move(el));
      Expects(emp.second);

    } else {
      auto extras = extraWords(dynamic_cast<const model::Scalar&>(val));
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
    auto span = block->subspan(ssizeof<DskBlockHdr>(), kNodeSize);
    std::copy(span.begin(), span.end(),
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
    for (auto it = linked_from; it < linked_.end(); ++it){
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
  Ensures(right_leaf->size() >= kMinLeafWords && right_leaf->size() <= kMaxLeafWords);

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
  ta_.free(right.addr());
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
    : AbsLeafW(AllocateNew(), ta)
    , tree_(parent) {}

RootLeafW::RootLeafW(Transaction& ta, Addr addr, BtreeWritable& parent)
    : AbsLeafW(ta, addr), tree_(parent) {}

RootLeafW::RootLeafW(LeafW&& o, Addr a, BtreeWritable& parent)
    : AbsLeafW(o.ta_, a), tree_(parent) {
  node_ = std::move(o.node_);
  linked_ = std::move(o.linked_);
  size_ = o.size();
  ta_.free(o.addr_);
}

void RootLeafW::split(Key key, const model::Value& val) {
  auto right = splitHelper(key, val);
  auto left =
      std::make_unique<LeafW>(AllocateNew(), std::move(*this), right->addr());

  auto sep_key = keyFromWord(*right->node_->begin());
  auto new_me = std::unique_ptr<RootInternalW>(new RootInternalW(
      ta_, addr_, std::move(left), sep_key,
      std::move(right), tree_));
  tree_.root_ = std::move(new_me);
}

void RootLeafW::balance() {
  // root leaf merging is not existent
  // this node type may be empty
  return; // NOP
}

////////////////////////////////////////////////////////////////////////////////
// InternalEntriesW

CB_PACKED(struct DskInternalPair {
  DskInternalPair() = default;

  DskInternalEntry entry;
  Addr addr;

  void zero() {
    addr.value = 0;
    entry.zero();
  }
});
static_assert(sizeof(DskInternalPair) == 16, "Invalid DskInternalPair size");

bool operator<(Key k, const DskInternalPair& p) {
  return k < p.entry.key.key();
}

CB_PACKED(struct DskInternalNode {
  DskInternalHdr hdr;
  Addr first;
  std::array<DskInternalPair, kMaxInternalEntries> pairs;

  char padding[kNodeSize % 16];

  DskInternalPair* begin() noexcept { return pairs.data(); }
  DskInternalPair* end() { return pairs.data() + hdr.size(); }
  const DskInternalPair* begin() const noexcept { return pairs.data(); }
  const DskInternalPair* end() const { return pairs.data() + hdr.size(); }
});
static_assert(sizeof(DskInternalNode) == kNodeSize,
              "Invalid DskInternalNode size");

InternalEntriesW::InternalEntriesW(Transaction& ta, Addr addr)
    : ta_{ ta }, addr_{ addr } {}

InternalEntriesW::InternalEntriesW(Transaction& ta, Addr first,
                                   DskInternalPair* begin, DskInternalPair* end)
    : ta_{ ta }
    , addr_{ ta.alloc(kNodeSize).addr }
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

namespace {

//! Return reference to \c Addr of child where key belongs.
template <typename N>
auto& searchInternalAddrHelper(N& node, Key key) {
  static_assert(
      std::is_same<DskInternalNode, typename std::remove_cv<N>::type>(),
      "Expected DskInternalNode");

  auto it = std::upper_bound(node.begin(), node.end(), key);
  return (it == node.begin() ? node.first : std::prev(it)->addr);
}

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
    return searchInternalAddrHelper(gsl::as_span<const DskInternalNode>(
                                  ref->subspan(ssizeof<DskBlockHdr>()))[0],
                              key);
  }
  return searchInternalAddrHelper(*node_, key);
}

Addr InternalEntriesW::searchSiblingAddr(Key key) {
  if (!node_) {
    auto ref = ta_.loadBlock<kBlockSize>(addr_);
    return searchSiblingAddrHelper(gsl::as_span<DskInternalNode>(
                                 ref->subspan(ssizeof<DskBlockHdr>()))[0],
                             key);
  }
  return searchSiblingAddrHelper(*node_, key);
}

void InternalEntriesW::init() {
  if (!node_) {
    node_ = std::make_unique<DskInternalNode>();
    auto ref = ta_.loadBlock<kBlockSize>(addr_);
    auto source = ref->subspan(ssizeof<DskBlockHdr>(), kNodeSize);
    auto target = gsl::as_writeable_bytes(Span<DskInternalNode>(*node_));
    std::copy(source.begin(), source.end(), target.begin());
    node_->hdr.check();
  }
}

size_t InternalEntriesW::size() {
  init();
  return node_->hdr.size();
}

bool InternalEntriesW::isFull() {
  return size() >= kMaxInternalEntries;
}

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

DskInternalPair* InternalEntriesW::search(Key key) {
  init();
  Expects(size() >= 1);
  auto it = std::upper_bound(node_->begin(), node_->end(), key);
  Expects(it > node_->begin());
  return std::prev(it);
}

void InternalEntriesW::remove(DskInternalPair* e) {
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

void InternalEntriesW::removeTail(DskInternalPair* from) {
  init();
  auto end = node_->end();
  auto begin = node_->begin();
  Expects(from < end);
  Expects(from > begin);

  for (auto it = from; it < end; ++it) it->zero();
  node_->hdr.fromSize(std::distance(begin, from));
}

void InternalEntriesW::removeHead(DskInternalPair* to) {
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

void InternalEntriesW::prepend(DskInternalPair* from, DskInternalPair* to,
                               Key sep) {
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

void InternalEntriesW::append(DskInternalPair* from, DskInternalPair* to) {
  init();
  auto amount = std::distance(from, to);
  if (amount == 0) return;
  Expects(amount + size() <= kMaxInternalEntries);
  Expects(from->entry.key.key() > std::prev(end())->entry.key.key());

  std::copy(from, to, end());
  node_->hdr.fromSize(size() + amount);
}

void InternalEntriesW::makeRoot(Addr left, Key sep, Addr right){
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
  if (node_){
    node_->hdr.check();
    writes.push_back({ Addr(addr_.value + ssizeof<DskBlockHdr>()),
                       gsl::as_bytes(Span<DskInternalNode>(*node_)) });
  }
}

void InternalEntriesW::destroy() {
  if (!node_) {
    auto ref = ta_.loadBlock<kBlockSize>(addr_);
    auto& node =
        gsl::as_span<DskInternalNode>(ref->subspan(ssizeof<DskBlockHdr>()))[0];

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

  ta_.free(addr_);
}

DskInternalPair* InternalEntriesW::begin() {
  init();
  return node_->begin();
}

DskInternalPair* InternalEntriesW::mid() {
  init();
  auto size = node_->hdr.size();
  Expects(size >= 3);
  return node_->begin() + size / 2;
}

DskInternalPair* InternalEntriesW::end() {
  init();
  return node_->end();
}

////////////////////////////////////////////////////////////////////////////////
// AbsInternalW

AbsInternalW::AbsInternalW(Transaction& ta, Addr addr)
    : NodeW(addr), entries_{ ta, addr} {}

AbsInternalW::AbsInternalW(AllocateNew, Transaction& ta, Addr first,
    DskInternalPair* begin, DskInternalPair* end)
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

DskInternalPair* AbsInternalW::searchEntry(Key key) {
  return entries_.search(key);
}

void AbsInternalW::removeMerged(DskInternalPair* it) {
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

  auto sibling = std::make_unique<InternalW>(
      AllocateNew(), entries_.ta_, mid->addr, std::next(mid), end);

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
    : AbsInternalW(ta, addr, left_leaf->addr(), sep,
                                    right_leaf->addr())
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

  auto left = std::make_unique<InternalW>(
      AllocateNew(), entries_.ta_, entries_.first(), beg, mid);

  auto right = std::make_unique<InternalW>(
      AllocateNew(), entries_.ta_, mid->addr, std::next(mid), end);

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
    entries_.ta_.free(child_internal->addr());

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

////////////////////////////////////////////////////////////////////////////////
// NodeR

namespace NodeR {
namespace {

template <typename ConstIt>
std::pair<Key, model::PValue> readValue(Database& db, ConstIt& it) {
  auto entry = DskLeafEntry(*it++);
  std::pair<Key, model::PValue> ret;
  ret.first = entry.key.key();

  if (entry.value.type & 0b10000000) {
    // short string
    size_t size = (entry.value.type & 0b00111111);
    std::string str;
    str.reserve(size);
    uint64_t word = 0;
    for (size_t i = 0; i < size; ++i) {
      if (i % 8 == 0) word = *it++;
      str.push_back(static_cast<char>(word));
      word >>= 8;
    }
    ret.second = std::make_unique<model::Scalar>(std::move(str));
  } else {
    switch (entry.value.type) {
    case ValueType::object:
      ret.second = ObjectR(db, Addr(*it++)).getValue();
      break;
    case ValueType::array:
      ret.second = ArrayR(db, Addr(*it++)).getValue();
      break;
    case ValueType::number:
      union {
        uint64_t word;
        model::Number number;
      } num;
      num.word = *it++;
      ret.second = std::make_unique<model::Scalar>(num.number);
      break;
    case ValueType::string:
      ret.second = StringR(db, Addr(*it++)).getValue();
      break;
    case ValueType::boolean_true:
      ret.second = std::make_unique<model::Scalar>(true);
      break;
    case ValueType::boolean_false:
      ret.second = std::make_unique<model::Scalar>(false);
      break;
    case ValueType::null:
      ret.second = std::make_unique<model::Scalar>(model::Null());
      break;
    default:
      throw ConsistencyError("Unknown value type");
    }
  }

  return ret;
}

const DskLeafNode& leafView(ReadRef<kBlockSize>& block) {
  return gsl::as_span<const DskLeafNode>(
      block->subspan(ssizeof<DskBlockHdr>(), kNodeSize))[0];
}

Addr getAllInLeaf(Database& db, ReadRef<kBlockSize>& block, model::Object& obj) {
  auto node = leafView(block);
  auto it = node.begin();
  auto next = node.hdr.next();

  while (it != node.end() && *it != 0) {
    auto pair = readValue(db, it);
    obj.append(db.resolveKey(pair.first), std::move(pair.second));
  }

  return next;
}

Addr getAllInLeaf(Database& db, ReadRef<kBlockSize>& block, model::Array& arr) {
  auto node = leafView(block);
  auto it = node.begin();
  auto next = node.hdr.next();

  while (it < node.end() && *it != 0) {
    auto pair = readValue(db, it);
    arr.append(pair.first.value, std::move(pair.second));
  }

  return next;
}

const DskInternalNode& internalView(ReadRef<kBlockSize>& block) {
  auto& n = gsl::as_span<DskInternalNode>(
      block->subspan(ssizeof<DskBlockHdr>(), kNodeSize))[0];
  n.hdr.check();
  return n;
}


template <class Val, class Obj, class Arr, class Ta>
std::unique_ptr<Val> getChildCollection(Ta& ta, Addr addr, Key key) {
  auto block = ta.template loadBlock<kBlockSize>(addr);

  if (isNodeLeaf(block)) {
    auto node = leafView(block);
    auto it = node.search(key);

    if (std::next(it) >= node.end() || *it == 0) return nullptr;
    auto entry = DskLeafEntry(*it);
    if (entry.key.key() != key) return nullptr;
    Addr child_addr{ *std::next(it) };

    block.free();

    auto t = entry.value.type;
    if (t == ValueType::object) {
      return std::make_unique<Obj>(ta, child_addr);
    } else if (t == ValueType::array) {
      return std::make_unique<Arr>(ta, child_addr);
    } else {
      return nullptr;
    }

  } else {
    auto node = internalView(block);
    auto child_addr = searchInternalAddrHelper(node, key);
    block.free();
    return getChildCollection<Val, Obj, Arr>(ta, child_addr, key);
  }
}

} // anonymous namespace

template <class C>
void getAll(Database& db, Addr addr, C& obj) {
  auto block = db.loadBlock<kBlockSize>(addr);

  if (isNodeLeaf(block)) {
    auto next = getAllInLeaf(db, block, obj);
    block.free();
    while (!next.isNull()) {
      auto next_block = db.loadBlock<kBlockSize>(next);
      next = getAllInLeaf(db, next_block, obj);
    }

  } else {
    auto leftmost = internalView(block).first;
    block.free();
    getAll(db, leftmost, obj);
  }
}

// explicit instantiation
template void getAll<model::Object>(Database& db, Addr addr,
                                    model::Object& obj);
template void getAll<model::Array>(Database& db, Addr addr, model::Array& obj);

std::unique_ptr<model::Value> getChildValue(Database& db, Addr addr, Key key) {
  auto block = db.loadBlock<kBlockSize>(addr);

  if (isNodeLeaf(block)) {
    auto node = leafView(block);
    auto it = node.search(key);

    if (it >= node.end() || *it == 0) return nullptr;
    if (DskLeafEntry(*it).key.key() != key) return nullptr;

    // TODO: should free block here, but readValue needs it and may recurse
    return readValue(db, it).second;

  } else {
    auto node = internalView(block);
    auto child_addr = searchInternalAddrHelper(node, key);
    block.free();
    return getChildValue(db, child_addr, key);
  }
}

std::unique_ptr<ValueW> getChildCollectionW(Transaction& ta, Addr addr, Key key) {
  return getChildCollection<ValueW, ObjectW, ArrayW>(ta, addr, key);
}

std::unique_ptr<ValueR> getChildCollectionR(Database& db, Addr addr, Key key) {
  return getChildCollection<ValueR, ObjectR, ArrayR>(db, addr, key);
}

} // namespace NodeR
} // namespace btree
} // namespace disk
} // namespace cheesebase

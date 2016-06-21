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

CB_PACKED(struct DskEntry {
  DskEntry(uint64_t w) {
    *reinterpret_cast<uint64_t*>(this) = w;
    if (value.magic_byte != '!')
      throw ConsistencyError("No magic byte in value");
  }
  DskEntry(Key k, ValueType t) : value{ '!', t }, key{ k } {}

  size_t extraWords() const { return nrExtraWords(value.type); }
  uint64_t word() { return *reinterpret_cast<uint64_t*>(this); }

  DskValueHdr value;
  DskKey key;
});
static_assert(sizeof(DskEntry) == 8, "Invalid DskEntry size");

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

Key keyFromWord(uint64_t w) { return DskEntry(w).key.key(); }

size_t entrySize(uint64_t e) { return DskEntry(e).extraWords() + 1; }

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

constexpr size_t kMaxInternalEntries = (kNodeSize - 16) / 16;
constexpr size_t kMinInternalEntries = (kMaxInternalEntries / 2) - 2;

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

bool isNodeLeaf(const ReadRef& page, Addr addr) {
  // first byte of Addr is always 0
  // next-ptr of leafs put a flag in the first byte, marking the node as leaf
  return gsl::as_span<const DskLeafHdr>(page->subspan(
      addr.pageOffset() + ssizeof<DskBlockHdr>(), sizeof(DskLeafHdr)))[0]
      .hasMagic();
}

size_t searchLeafPosition(Key key, Span<const uint64_t> span) {
  for (size_t i = 1; i < static_cast<size_t>(span.size());
       i += entrySize(span[i])) {
    if (span[i] == 0 || keyFromWord(span[i]) >= key) return i;
  }
  return static_cast<size_t>(span.size());
}

size_t searchInternalPosition(Key key, Span<const uint64_t> span) {
  // 4, 6, 8... words: <hdr><addr>(<key><addr>)+
  Expects((span.size() >= 4 && span.size() % 2 == 0));

  // <hdr> <adr> | (<key> <adr>) (<key> <adr>) (<key> <adr>)
  //               ^                           ^
  //             first                        last
  size_t first = 0;
  size_t last = gsl::narrow_cast<size_t>((span.size() - 2) / 2);

  while (first != last) {
    Expects(last >= first);
    auto mid = first + (last - first) / 2;
    if (key < DskInternalEntry(span[2 + mid * 2]).key.key()) {
      Expects(last != mid);
      last = mid;
    } else {
      if (first == mid)
        return last * 2 + 1;
      else
        first = mid;
    }
  }
  return first * 2 + 1;
}

std::unique_ptr<NodeW> openNodeW(Transaction& ta, Addr addr) {
  auto page = ta.load(addr.pageNr());

  if (isNodeLeaf(page, addr))
    return std::make_unique<LeafW>(ta, addr);
  else
    return std::make_unique<InternalW>(ta, addr);
}

} // anonymous namespace

std::unique_ptr<NodeW> openRootW(Transaction& ta, Addr addr, BtreeWritable& tree) {
  auto page = ta.load(addr.pageNr());

  if (isNodeLeaf(page, addr))
    return std::make_unique<RootLeafW>(ta, addr, tree);
  else
    return std::make_unique<RootInternalW>(ta, addr, tree);
}

std::unique_ptr<NodeR> openNodeR(Database& db, Addr addr) {
  auto page = db.loadPage(addr.pageNr());

  if (isNodeLeaf(page, addr))
    return std::make_unique<LeafR>(db, addr, std::move(page));
  else
    return std::make_unique<InternalR>(db, addr, std::move(page));
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

AbsLeafW::AbsLeafW(AllocateNew, Transaction& ta, Addr next)
    : NodeW(ta.alloc(k_node_max_bytes).addr), ta_{ta} {
  buf_ = std::make_unique<std::array<uint64_t, k_node_max_words>>();
  std::fill(buf_->begin(), buf_->end(), 0);
  (*buf_)[0] = DskLeafHdr().fromAddr(next).data;
  top_ = 1;
}

AbsLeafW::AbsLeafW(Transaction& ta, Addr addr) : NodeW(addr), ta_{ ta } {}

Writes AbsLeafW::getWrites() const {
  Writes w;
    w.reserve(1 + linked_.size()); // may be more, but a good guess

    if (buf_)
      w.push_back({ Addr(addr_.value + sizeof(DskBlockHdr)),
                    gsl::as_bytes(Span<uint64_t>(*buf_)) });

    for (auto& c : linked_) {
      auto cw = c.second->getWrites();
      std::move(cw.begin(), cw.end(), std::back_inserter(w));
    }

    return w;
  }

  void AbsLeafW::destroy() {
    auto bufv = getDataView();
    auto& buf = bufv.first;

    auto it = buf.begin() + 1;
    while (it < buf.end() && *it != 0) {
      it += destroyValue(it);
    }
    ta_.free(addr_);
  }

  size_t AbsLeafW::findSize() {
    size_t i = 1; // first element always is a Addr, skip to value
    while (i < buf_->size() && buf_->at(i) != 0) {
      i += entrySize(buf_->at(i));
    }
    return i;
  }

  template <typename ConstIt>
  size_t AbsLeafW::destroyValue(ConstIt it) {
    auto entry = DskEntry(*it);

    switch (entry.value.type) {
    case ValueType::object:
      ObjectW(ta_, Addr(*(it + 1))).destroy();
      break;
    case ValueType::string:
      StringW(ta_, Addr(*(it + 1))).destroy();
      break;
    case ValueType::array:
      ArrayW(ta_, Addr(*(it + 1))).destroy();
      break;
    }
    return entry.extraWords() + 1;
  }

  Key AbsLeafW::append(const model::Value& val, AbsInternalW* parent) {
    parent_ = parent;
    if (!buf_) initFromDisk();

    Key key{ 0 };
    for (size_t i = 1; i < buf_->size() && buf_->at(i) != 0; i++) {
      auto e = DskEntry(buf_->at(i));
      key = Key(e.key.key().value + 1);
      i += e.extraWords();
    }

    auto ins = insert(key, val, Overwrite::Insert, parent);
    Ensures(ins == true);
    return key;
  }

  bool AbsLeafW::insert(Key key, const model::Value& val, Overwrite ow,
                        AbsInternalW* parent) {
    parent_ = parent;
    if (!buf_) initFromDisk();

    int extra_words = gsl::narrow_cast<int>(nrExtraWords(val));

    // find position to insert
    auto pos = searchLeafPosition(key, *buf_);
    Ensures(pos <= top_);
    bool update = pos < top_ && keyFromWord(buf_->at(pos)) == key;
    if ((ow == Overwrite::Update && !update) ||
        (ow == Overwrite::Insert && update)) {
      return false;
    }

    // enough space to insert?
    if (top_ + extra_words - (update ? DskEntry(buf_->at(pos)).extraWords() : 0) <
        k_node_max_words) {

      // make space
      if (update) {
        auto old_entry = DskEntry(buf_->at(pos));
        auto old_extra = gsl::narrow_cast<int>(old_entry.extraWords());
        switch (old_entry.value.type) {
        case ValueType::object:
          ObjectW(ta_, Addr(buf_->at(pos + 1))).destroy();
          linked_.erase(old_entry.key.key());
          break;
        case ValueType::string:
          StringW(ta_, Addr(buf_->at(pos + 1))).destroy();
          linked_.erase(old_entry.key.key());
          break;
        case ValueType::array:
          ArrayW(ta_, Addr(buf_->at(pos + 1))).destroy();
          linked_.erase(old_entry.key.key());
          break;
        }
        shiftBuffer(pos + old_extra, extra_words - old_extra);
      } else {
        shiftBuffer(pos, 1 + extra_words);
      }

      // put the first word
      auto t = valueType(val);
      buf_->at(pos) = DskEntry{ key, t }.word();

      // put extra words
      // recurse into inserting remotely stored elements if needed
      if (t == ValueType::object) {
        auto& obj = dynamic_cast<const model::Object&>(val);
        auto el = std::make_unique<ObjectW>(ta_);
        for (auto& c : obj) {
          el->insert(ta_.key(c.first), *c.second, Overwrite::Insert);
        }
        buf_->at(pos + 1) = el->addr().value;
        auto emp = linked_.emplace(key, std::move(el));
        Expects(emp.second);

      } else if (t == ValueType::array) {
        auto& arr = dynamic_cast<const model::Array&>(val);
        auto el = std::make_unique<ArrayW>(ta_);
        for (auto& c : arr) {
          el->insert(Key(c.first), *c.second, Overwrite::Insert);
        }
        buf_->at(pos + 1) = el->addr().value;
        auto emp = linked_.emplace(key, std::move(el));
        Expects(emp.second);

      } else if (t == ValueType::string) {
        auto& str = dynamic_cast<const model::Scalar&>(val);
        auto el =
            std::make_unique<StringW>(ta_, boost::get<model::String>(str.data()));
        buf_->at(pos + 1) = el->addr().value;
        auto emp = linked_.emplace(key, std::move(el));
        Expects(emp.second);

      } else {
        auto extras = extraWords(dynamic_cast<const model::Scalar&>(val));
        for (auto i = 0; i < extra_words; ++i) {
          buf_->at(pos + 1 + i) = extras[i];
        }
      }

    } else {
      split(key, val);
    }

    return true;
  }

  void AbsLeafW::insert(Span<const uint64_t> raw) {
    Expects(buf_);
    Expects(k_node_max_words >= top_ + raw.size());

    for (auto word : raw) {
      buf_->at(top_++) = word;
    }
  }

  bool AbsLeafW::remove(Key key, AbsInternalW* parent) {
    parent_ = parent;
    if (!buf_) initFromDisk();
    Expects(top_ <= k_node_max_words);

    // find position
    auto pos = searchLeafPosition(key, *buf_);

    // return false if not found
    if (pos >= top_ || keyFromWord(buf_->at(pos)) != key) return false;

    auto size = destroyValue(buf_->begin() + pos);
    shiftBuffer(pos + size, -gsl::narrow_cast<int>(size));

    if (top_ < k_leaf_min_words) balance();

    return true;
  }

  void AbsLeafW::shiftBuffer(size_t pos, int amount) {
    Expects(buf_);
    Expects(amount + pos > 0 && amount + pos <= buf_->size());
    Expects(pos <= top_);
    if (amount == 0) return;

    if (pos < top_) {
      if (amount > 0) {
        for (size_t i = top_; i > pos; --i) {
          buf_->at(i + amount - 1) = buf_->at(i - 1);
        }
      } else {
        for (size_t i = pos; i < top_; ++i) {
          buf_->at(i + amount) = buf_->at(i);
        }
        for (size_t i = top_; i < top_ - amount; i++) {
          buf_->at(i + amount) = 0;
        }
      }
    } else {
      Expects(pos == top_);
      if (amount > 0)
        std::fill(buf_->begin() + pos, buf_->begin() + pos + amount, 0);
      else
        std::fill(buf_->begin() + pos + amount, buf_->begin() + pos, 0);
    }
    top_ += amount;
  }

  void AbsLeafW::initFromDisk() {
    if (!buf_) {
      buf_ = std::make_unique<std::array<uint64_t, k_node_max_words>>();
      copySpan(
          ta_.load(addr_.pageNr())
              ->subspan(addr_.pageOffset() + ssizeof<DskBlockHdr>(),
                        k_node_max_bytes),
          gsl::as_writeable_bytes(Span<uint64_t, k_node_max_words>(*buf_)));
      top_ = findSize();
    }
  }

  std::pair<Span<const uint64_t>, std::unique_ptr<ReadRef>>
  AbsLeafW::getDataView() const {
    if (buf_) return { Span<const uint64_t>(*buf_), nullptr };

    auto p = std::make_unique<ReadRef>(ta_.load(addr_.pageNr()));
    return { gsl::as_span<const uint64_t>((*p)->subspan(
                addr_.pageOffset() + ssizeof<DskBlockHdr>(), k_node_max_bytes)),
            std::move(p) };
  }

  size_t AbsLeafW::size() const { return top_; }

  ////////////////////////////////////////////////////////////////////////////////
  // LeafW

  void LeafW::split(Key key, const model::Value& val) {
    Expects(buf_);
    Expects(top_ <= buf_->size());
    Expects(parent_ != nullptr);

    auto right_leaf = std::make_unique<LeafW>(
        AllocateNew(), ta_, DskLeafHdr().fromDsk(buf_->at(0)).next());

    auto new_val_len = nrExtraWords(valueType(val)) + 1;
    bool new_here = false;
    size_t mid = 1;
    for (auto new_mid = mid; new_mid < top_; ++new_mid) {
      if ((new_here && new_mid + new_val_len > top_ / 2 + 1) ||
          (!new_here && new_mid > top_ / 2 + new_val_len + 1)) {
        break;
      } else {
        mid = new_mid;
      }
      auto entry = DskEntry(buf_->at(new_mid));
      if (entry.key.key() >= key) {
        new_here = true;
      }
      new_mid += entry.extraWords();
    }

    auto split_key = DskEntry(buf_->at(mid)).key.key();
    new_here = key < split_key;

    Ensures(mid < top_);
    Ensures(mid + (new_here ? new_val_len : 0) > k_leaf_min_words);

    right_leaf->insert(
        Span<uint64_t>(&buf_->at(mid), gsl::narrow_cast<int>(top_ - mid)));
    std::fill(buf_->begin() + mid, buf_->begin() + top_, 0);
    top_ = mid;
    buf_->at(0) = DskLeafHdr().fromAddr(right_leaf->addr()).data;

    // the check for existence happened before calling split so just use "Upsert"
    if (new_here)
      insert(key, val, Overwrite::Upsert, parent_);
    else
      right_leaf->insert(key, val, Overwrite::Upsert, parent_);

    Ensures(top_ >= k_leaf_min_words);
    Ensures(right_leaf->size() >= k_leaf_min_words);

    parent_->insert(split_key, std::move(right_leaf));
  }

  void LeafW::balance() {
    Expects(buf_);
    Expects(top_ < k_leaf_min_words);
    Expects(top_ > 1); // even if merging, the node should not be empty

    auto first_key = DskEntry(buf_->at(1)).key.key();
    auto& sibl = dynamic_cast<LeafW&>(parent_->getSibling(first_key));
    sibl.initFromDisk();

    // required since removeMerged on root may move the buf_ unique_ptr
    auto& buf = *buf_;
    auto& sibl_buf = *sibl.buf_;

    auto sibl_key = DskEntry(sibl_buf[1]).key.key();

    if (sibl.size() + top_ - 1 < k_node_max_words) {
      // actually merge them

      if (sibl_key > first_key) {
        // is right sibl, merge here

        insert(Span<uint64_t>(sibl_buf).subspan(1, sibl.size() - 1));
        buf[0] = sibl_buf[0]; // copy next ptr
        for (auto& c : sibl.linked_) {
          linked_.insert(std::move(c));
        }
        sibl.linked_.clear();
        ta_.free(sibl.addr());

        // do this last because it destroys sibling
        parent_->removeMerged(sibl_key, sibl.addr());

      } else {
        // is left sibl, merge there

        sibl.insert(Span<uint64_t>(buf).subspan(1, top_ - 1));
        sibl_buf[0] = buf[0]; // copy next ptr
        for (auto& c : linked_) {
          sibl.linked_.insert(std::move(c));
        }
        linked_.clear();
        ta_.free(addr_);

        // do this last because it destroys this
        parent_->removeMerged(first_key, addr_);
      }

    } else {
      // too big, just steal some values

      if (sibl_key > first_key) {
        // pull lowest
        size_t till = 1;
        while (till < sibl.top_) {
          auto next = till + entrySize(sibl_buf[till]);
          if (top_ + next - 1 > sibl.top_ - next) break;
          till = next;
        }

        Ensures(till < sibl.top_);
        Ensures(top_ + till - 1 >= k_leaf_min_words);
        Ensures(top_ + till - 1 <= k_node_max_words);
        Ensures(sibl.top_ - till + 1 >= k_leaf_min_words);

        auto to_pull = Span<uint64_t>(sibl_buf).subspan(1, till - 1);
        insert(to_pull);

        for (auto it = to_pull.begin(); it < to_pull.end(); ++it) {
          auto entry = DskEntry(*it);

          auto lookup = sibl.linked_.find(entry.key.key());
          if (lookup != sibl.linked_.end()) {
            linked_.insert(std::move(*lookup));
            sibl.linked_.erase(lookup);
          }

          it += entry.extraWords();
        }
        sibl.shiftBuffer(till, 1 - gsl::narrow_cast<int>(till));

        auto first_sibl_key = DskEntry(sibl_buf[1]).key.key();
        parent_->updateMerged(first_sibl_key, first_sibl_key);
      } else {
        // pull biggest

        size_t last = 1;
        while (last < sibl.top_) {
          auto next = last + entrySize(sibl_buf[last]);
          if (top_ + sibl.top_ - next < next) break;
          last = next;
        }

        Ensures(last < sibl.top_);
        Ensures(top_ + sibl.top_ - last >= k_leaf_min_words);
        Ensures(top_ + sibl.top_ - last <= k_node_max_words);
        Ensures(last >= k_leaf_min_words);

        auto to_pull =
            Span<const uint64_t>(sibl_buf).subspan(last, sibl.top_ - last);
        for (auto it = to_pull.begin(); it < to_pull.end(); ++it) {
          auto entry = DskEntry(*it);

          auto lookup = sibl.linked_.find(entry.key.key());
          if (lookup != sibl.linked_.end()) {
            linked_.insert(std::move(*lookup));
            sibl.linked_.erase(lookup);
          }

          it += entry.extraWords();
        }

        shiftBuffer(1, gsl::narrow_cast<int>(to_pull.size()));
        copySpan(to_pull, Span<uint64_t>(buf).subspan(1, to_pull.size()));
        std::fill(sibl_buf.begin() + last, sibl_buf.begin() + sibl.top_, 0);
        sibl.top_ = last;
        parent_->updateMerged(first_key, DskEntry(buf[1]).key.key());
      }
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // RootLeafW

  RootLeafW::RootLeafW(AllocateNew, Transaction& ta, Addr next,
                      BtreeWritable& parent)
      : AbsLeafW(AllocateNew(), ta, next), tree_(parent) {}

  RootLeafW::RootLeafW(Transaction& ta, Addr addr, BtreeWritable& parent)
      : AbsLeafW(ta, addr), tree_(parent) {}

  RootLeafW::RootLeafW(LeafW&& o, Addr a, BtreeWritable& parent)
      : AbsLeafW(o.ta_, a), tree_(parent) {
    buf_ = std::move(o.buf_);
    linked_ = std::move(o.linked_);
    top_ = o.top_;
    ta_.free(o.addr_);
  }

  void RootLeafW::split(Key key, const model::Value& val) {
    Expects(buf_);
    auto& buf = *buf_;

    auto right_leaf = std::make_unique<LeafW>(AllocateNew(), ta_, Addr(0));
    auto left_leaf =
        std::make_unique<LeafW>(AllocateNew(), ta_, right_leaf->addr());

    auto ins = left_leaf.get();

    left_leaf->linked_ = std::move(linked_);

    Key mid{ 0 };
    size_t new_val_size = 1 + nrExtraWords(val);
    for (size_t i = 1; i < top_; ++i) {
      auto entry = DskEntry(buf[i]);
      auto extra = entry.extraWords();

      // check if new key needs to be put now
      if (entry.key.key() > key) {
        // check if new key even is the middle key
        if (ins == left_leaf.get() &&
            left_leaf->size() >= top_ - i + new_val_size) {
          mid = key;
          ins = right_leaf.get();
        }
        // existence check happened before calling split, so use Upsert
        ins->insert(key, val, Overwrite::Upsert, nullptr);
        new_val_size = 0;
      }

      if (ins == left_leaf.get() &&
          left_leaf->size() >= top_ - i + new_val_size) {
        mid = entry.key.key();
        ins = right_leaf.get();
      }
      Expects(buf.size() >= i + extra + 1)
          ins->insert({ &buf[i], gsl::narrow_cast<int>(extra + 1) });
      i += extra;
    }

    if (new_val_size > 0) {
      // new key is the very last

      // this should not really happen unless we have leafs with 2 elements
      if (mid.isNull()) {
        mid = key;
      }
      right_leaf->insert(key, val, Overwrite::Upsert, nullptr);
    }
    Ensures(left_leaf->size() >= k_leaf_min_words);
    Ensures(right_leaf->size() >= k_leaf_min_words);

    auto new_me = std::unique_ptr<RootInternalW>(new RootInternalW(
        ta_, addr_, std::move(left_leaf), mid, std::move(right_leaf), tree_));
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
    return searchSiblingAddrHelper(gsl::as_span<const DskInternalNode>(
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

Key InternalEntriesW::remove(Key key) {
  init();
  Expects(size() >= 1);

  auto end = node_->end();
  auto it = std::upper_bound(node_->begin(), end, key);
  Expects(it != node_->begin());

  auto removed_key = std::prev(it)->entry.key.key();
  std::move(it, end, std::prev(it));
  std::prev(end)->zero();
  --(node_->hdr);
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
  Expects(amount <= kMaxInternalEntries - size());
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
  Expects(amount <= kMaxInternalEntries - size());
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
    auto& node = gsl::as_span<const DskInternalNode>(
        ref->subspan(ssizeof<DskBlockHdr>()))[0];
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

std::pair<Key, std::unique_ptr<NodeW>> AbsInternalW::removeMerged(Key key,
                                                                  Addr addr) {
  std::pair<Key, std::unique_ptr<NodeW>> ret;
  ret.first = entries_.remove(key);
  auto lookup = childs_.find(addr);
  if (lookup == childs_.end()) {
    throw ConsistencyError("removeMerged with unknown Address");
  }
  ret.second = std::move(lookup->second);
  childs_.erase(lookup);
  if (entries_.size() < kMinInternalEntries) balance();
  return ret;
}

Key AbsInternalW::updateMerged(Key key, Key new_key) {
  return entries_.update(key, new_key);
}

////////////////////////////////////////////////////////////////////////////////
// InternalW

namespace {

template <class C, typename K>
void tryTransfer(C& from, C& to, const K& key) {
  auto lookup = from.find(key);
  if (lookup != from.end()) {
    to.insert(std::move(*lookup));
    from.erase(lookup);
  }
}

} // anonymous namespace
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
  Expects(entries_.size() <= kMinInternalEntries);

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
  Expects(entries_.size() + right.entries_.size() + 1 <= kMaxInternalEntries);

  auto from = right.entries_.begin();
  auto to = right.entries_.end();

  Expects(entries_.begin()->entry.key.key() < from->entry.key.key());

  auto right_node = parent_->removeMerged(from->entry.key.key(), right.addr());
  entries_.insert(right_node.first, right.entries_.first());
  entries_.append(from, to);

  for (auto& c : right.childs_) {
    childs_.emplace(c.first, std::move(c.second));
  }
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
  childs_.emplace(left_leaf->addr(), std::move(left_leaf));
  childs_.emplace(right_leaf->addr(), std::move(right_leaf));
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

  childs_.emplace(right->addr(), std::move(right));
  childs_.emplace(left->addr(), std::move(left));
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

NodeR::NodeR(Database& db, Addr addr, ReadRef page)
    : Node(addr), db_(db), page_(std::move(page)) {}

Span<const uint64_t> NodeR::getData() const {
  return gsl::as_span<const uint64_t>(page_->subspan(
      addr_.pageOffset() + ssizeof<DskBlockHdr>(), k_node_max_bytes));
}

////////////////////////////////////////////////////////////////////////////////
// LeafR

LeafR::LeafR(Database& db, Addr addr, ReadRef page)
    : NodeR(db, addr, std::move(page)) {}

LeafR::LeafR(Database& db, Addr addr)
    : NodeR(db, addr, db.loadPage(addr.pageNr())) {}

void LeafR::getAll(model::Object& obj) {
  auto next = getAllInLeaf(obj);
  while (!next.isNull()) {
    next = LeafR(db_, next).getAllInLeaf(obj);
  }
}

Addr LeafR::getAllInLeaf(model::Object& obj) {
  auto data = getData();
  auto it = data.begin();
  auto end = data.end();
  auto next = DskLeafHdr().fromDsk(*it++).next();

  while (it != end && *it != 0) {
    auto pair = readValue(it);
    obj.append(db_.resolveKey(pair.first), std::move(pair.second));
  }

  return next;
}

void LeafR::getAll(model::Array& obj) {
  auto next = getAllInLeaf(obj);
  while (!next.isNull()) {
    next = LeafR(db_, next).getAllInLeaf(obj);
  }
}

Addr LeafR::getAllInLeaf(model::Array& obj) {
  auto data = getData();
  auto it = data.begin();
  auto end = data.end();
  auto next = DskLeafHdr().fromDsk(*it++).next();

  while (it != end && *it != 0) {
    auto pair = readValue(it);
    obj.append(pair.first.value, std::move(pair.second));
  }

  return next;
}

std::unique_ptr<model::Value> LeafR::getChildValue(Key key) {
  auto view = getData();
  auto pos = searchLeafPosition(key, view);
  if (pos >= static_cast<size_t>(view.size()) || view[pos] == 0) return nullptr;

  if (DskEntry(view[pos]).key.key() != key) return nullptr;

  auto it = view.begin() + pos;
  return readValue(it).second;
}

std::unique_ptr<ValueW> LeafR::getChildCollectionW(Transaction& ta, Key key) {
  auto view = getData();
  auto pos = searchLeafPosition(key, view);
  if (pos + 1 >= static_cast<size_t>(view.size()) || view[pos] == 0)
    return nullptr;

  auto entry = DskEntry(view[pos]);
  if (entry.key.key() != key) return nullptr;

  auto t = entry.value.type;
  if (t == ValueType::object) {
    return std::make_unique<ObjectW>(ta, Addr(view[pos + 1]));
  } else if (t == ValueType::array) {
    return std::make_unique<ArrayW>(ta, Addr(view[pos + 1]));
  } else {
    return nullptr;
  }
}

std::unique_ptr<ValueR> LeafR::getChildCollectionR(Key key) {
  auto view = getData();
  auto pos = searchLeafPosition(key, view);
  if (pos + 1 >= static_cast<size_t>(view.size()) || view[pos] == 0)
    return nullptr;

  auto entry = DskEntry(view[pos]);
  if (entry.key.key() != key) return nullptr;

  auto t = entry.value.type;
  if (t == ValueType::object) {
    return std::make_unique<ObjectR>(db_, Addr(view[pos + 1]));
  } else if (t == ValueType::array) {
    return std::make_unique<ArrayR>(db_, Addr(view[pos + 1]));
  } else {
    return nullptr;
  }
}

std::pair<Key, model::PValue>
LeafR::readValue(Span<const uint64_t>::const_iterator& it) {
  auto entry = DskEntry(*it++);
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
      ret.second = ObjectR(db_, Addr(*it++)).getValue();
      break;
    case ValueType::array:
      ret.second = ArrayR(db_, Addr(*it++)).getValue();
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
      ret.second = StringR(db_, Addr(*it++)).getValue();
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

////////////////////////////////////////////////////////////////////////////////
// InternalR

InternalR::InternalR(Database& db, Addr addr, ReadRef page)
    : NodeR(db, addr, std::move(page)) {
  auto view = getData();
  data_ = view.subspan(0, DskInternalHdr().fromRaw(view[0]).size()*2 + 2);
}

void InternalR::getAll(model::Object& obj) {
  // just follow the left most path and let leafs go through
  searchChildNode(Key(0))->getAll(obj);
}

void InternalR::getAll(model::Array& arr) {
  // just follow the left most path and let leafs go through
  searchChildNode(Key(0))->getAll(arr);
}

std::unique_ptr<model::Value> InternalR::getChildValue(Key key) {
  return searchChildNode(key)->getChildValue(key);
}

std::unique_ptr<ValueW> InternalR::getChildCollectionW(Transaction& ta,
                                                       Key key) {
  return searchChildNode(key)->getChildCollectionW(ta, key);
}

std::unique_ptr<ValueR> InternalR::getChildCollectionR(Key key) {
  return searchChildNode(key)->getChildCollectionR(key);
}

std::unique_ptr<NodeR> InternalR::searchChildNode(Key k) {
  auto pos = searchInternalPosition(k, data_);
  return openNodeR(db_, Addr(data_[pos]));
}

} // namespace btree
} // namespace disk
} // namespace cheesebase

// Licensed under the Apache License 2.0 (see LICENSE file).
#pragma once

#include "btree.h"
#include "common.h"
#include "../model.h"
#include "../value.h"
#include "../../common.h"
#include "../../cache.h"
#include <boost/container/flat_map.hpp>

namespace cheesebase {
namespace disk {
namespace btree {

constexpr size_t kLeafEntryMaxWords = 4; // hdr + 24 byte inline string
constexpr size_t kMaxLeafWords = (kNodeSize - 8) / 8;
constexpr size_t kMinLeafWords = kMaxLeafWords / 2 - kLeafEntryMaxWords;

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

inline Key keyFromWord(uint64_t w) { return DskLeafEntry(w).key.key(); }
inline size_t entrySize(uint64_t e) { return DskLeafEntry(e).extraWords() + 1; }

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

class LeafW;

class AbsLeafW : public NodeW {
public:
  AbsLeafW(AllocateNew, AbsLeafW&& o, Addr next);
  AbsLeafW(AllocateNew, Transaction& ta, Addr next = Addr(0));
  AbsLeafW(Transaction& ta, Addr addr);

  // serialize and insert value, may trigger split
  bool insert(Key key, const model::Value&, Overwrite,
              AbsInternalW* parent) override;

  // find maximum key and insert value as key+1
  Key append(const model::Value&, AbsInternalW* parent) override;

  template <typename It>
  void appendWords(It from, It to);
  template <typename It>
  void prependWords(It from, It to);

  bool remove(Key key, AbsInternalW* parent) override;

  Writes getWrites() const override;

  void destroy() override;

  boost::container::flat_map<Key, std::unique_ptr<ValueW>> linked_;

  size_t size() const;

protected:
  void init();

  Transaction& ta_;
  std::unique_ptr<DskLeafNode> node_;
  size_t size_{ 0 };
  std::unique_ptr<LeafW> splitHelper(Key, const model::Value&);
  virtual void split(Key, const model::Value&) = 0;
  virtual void balance() = 0;
  // Destroy value at pos (if remote). Return size of the entry.
  template <typename ConstIt>
  size_t destroyValue(ConstIt it);
  AbsInternalW* parent_;
};

class LeafW : public AbsLeafW {
  friend class RootLeafW;

public:
  using AbsLeafW::AbsLeafW;

private:
  void split(Key, const model::Value&) override;
  void merge(LeafW& right);
  void balance() override;
};

// tree just a single leaf
class RootLeafW : public AbsLeafW {
  friend class RootInternalW;

public:
  RootLeafW(Transaction& ta, BtreeWritable& tree);
  RootLeafW(Transaction& ta, Addr addr, BtreeWritable& tree);
  virtual ~RootLeafW();

private:
  RootLeafW(LeafW&&, Addr addr, BtreeWritable& parent);
  BtreeWritable& tree_;
  void split(Key, const model::Value&) override;
  void balance() override;
};

} // namespace btree
} // namespace disk
} // namespace cheesebase

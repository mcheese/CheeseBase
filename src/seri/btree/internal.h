// Licensed under the Apache License 2.0 (see LICENSE file).
#pragma once

#include "btree.h"
#include "common.h"
#include "../../common.h"
#include "../../cache.h"
#include <boost/container/flat_map.hpp>

namespace cheesebase {
namespace disk {
namespace btree {

constexpr size_t kMaxInternalEntries = (kNodeSize - 16) / 16;
constexpr size_t kMinInternalEntries = (kMaxInternalEntries / 2) - 1;

// Magic byte + number of elements
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

  void operator++() { data++; }

  uint64_t data;
});
static_assert(sizeof(DskInternalHdr) == 8, "Invalid DskInternalHdr size");

// Magic bytes + key
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

  void zero() noexcept { *reinterpret_cast<uint64_t*>(this) = 0; }

  char magic[2];
  DskKey key;
});
static_assert(sizeof(DskInternalEntry) == 8, "Invalid DskInternalEntry size");

CB_PACKED(struct DskInternalPair {
  DskInternalPair() = default;

  void zero() {
    addr.value = 0;
    entry.zero();
  }

  DskInternalEntry entry;
  Addr addr;
});
static_assert(sizeof(DskInternalPair) == 16, "Invalid DskInternalPair size");

inline bool operator<(Key k, const DskInternalPair& p) {
  return k < p.entry.key.key();
}

CB_PACKED(struct DskInternalNode {
  DskInternalHdr hdr;
  Addr first;
  std::array<DskInternalPair, kMaxInternalEntries> pairs;
  char padding[kNodeSize % 16];

  using iterator = decltype(pairs.begin());
  Addr searchAddr(Key key) const;
  auto begin() noexcept { return pairs.begin(); }
  auto end() noexcept { return pairs.begin() + hdr.size(); }
  auto begin() const noexcept { return pairs.begin(); }
  auto end() const noexcept { return pairs.begin() + hdr.size(); }
});
static_assert(sizeof(DskInternalNode) == kNodeSize,
              "Invalid DskInternalNode size");

class LeafW;
class AbsInternalW;

class InternalEntriesW {
  friend class AbsInternalW;

public:
  InternalEntriesW(Transaction& ta, Addr first, DskInternalNode::iterator begin,
                   DskInternalNode::iterator end);
  InternalEntriesW(Transaction& ta, Addr addr);
  InternalEntriesW(Transaction& ta, Addr addr, Addr left, Key sep, Addr right);

  Addr searchChildAddr(Key key);
  Addr searchSiblingAddr(Key key);
  void insert(Key key, Addr addr);

  //! Get iterator to entry including \param key.
  DskInternalNode::iterator search(Key key);

  void remove(DskInternalNode::iterator entry);

  //! Remove entry that includes key, returns lowest key of removed entry.
  Key remove(Key key);

  //! Update entry that includes key to new_key, returns overwritten key.
  Key update(Key key, Key new_key);

  //! Destroy all children and free memory.
  void destroy();

  //! True if no more space.
  bool isFull();

  //! Number of entries == # \c Key == # \c Addr - 1.
  size_t size();

  //! Add \c Write of this node to the container.
  void addWrite(Writes&) const noexcept;

  //! Iterator to first entry.
  DskInternalNode::iterator begin();

  //! Iterator to middle entry. Rounds down on uneven entries.
  DskInternalNode::iterator mid();

  //! Iterator to past the last entry.
  DskInternalNode::iterator end();

  //! Get leftmost \c Addr
  Addr first();

  //! Remove all entries starting at \param from.
  void removeTail(DskInternalNode::iterator from);

  //! Remove all entries until \param to.
  void removeHead(DskInternalNode::iterator to);

  //! Prepend range of entries. \param sep has to be a seperator between the
  //! last entry and the first of the existing node (\class Key used in parent).
  void prepend(DskInternalNode::iterator from, DskInternalNode::iterator to,
               Key sep);

  //! Append range of entries.
  void append(DskInternalNode::iterator from, DskInternalNode::iterator to);

  //! Transform to root, 2 childs and a seperator, dismiss old entries.
  void makeRoot(Addr left, Key sep, Addr right);

  //! Take over \c DskInternalNode from \param other.
  void takeNodeFrom(InternalEntriesW& other);

  Transaction& ta_;

  void init();
  Addr addr_;
  std::unique_ptr<DskInternalNode> node_;
};

class AbsInternalW : public NodeW {
  friend class RootInternalW;

public:
  AbsInternalW(Transaction& ta, Addr addr);
  AbsInternalW(AllocateNew, Transaction& ta, Addr first,
               DskInternalNode::iterator begin, DskInternalNode::iterator end);

  // used when extending single root leaf to internal root
  AbsInternalW(Transaction& ta, Addr addr, Addr left, Key sep, Addr right);

  bool insert(Key key, const model::Value&, Overwrite,
              AbsInternalW* parent) override;

  // find maximum key and insert value as key+1
  Key append(const model::Value&, AbsInternalW* parent) override;

  void insert(Key key, std::unique_ptr<NodeW> c);
  bool remove(Key key, AbsInternalW* parent) override;
  Writes getWrites() const override;
  NodeW& searchChild(Key k);
  void destroy() override;
  void appendChild(std::pair<Addr, std::unique_ptr<NodeW>>&&);
  NodeW& getSibling(Key key);

  //! Return Key-Addr-Pair iterator refered to by \param key.
  DskInternalNode::iterator searchEntry(Key key);

  //! Remove Key-Addr-Pair referenced by \param entry.
  void removeMerged(DskInternalNode::iterator entry);

  //! Replace \c Key which includes key with new_key, returns replaced \c Key.
  Key updateMerged(Key key, Key new_key);

protected:
  InternalEntriesW entries_;
  AbsInternalW* parent_;
  boost::container::flat_map<Addr, std::unique_ptr<NodeW>> childs_;

private:
  virtual void split(Key, std::unique_ptr<NodeW>) = 0;
  virtual void balance() = 0;
};

class InternalW : public AbsInternalW {
public:
  using AbsInternalW::AbsInternalW;

private:
  void split(Key, std::unique_ptr<NodeW>) override;
  void balance() override;
  void merge(InternalW& right);
};

class RootInternalW : public AbsInternalW {
  friend class RootLeafW;

public:
  RootInternalW(Transaction& ta, Addr addr, BtreeWritable& parent);

private:
  // used to construct while splitting RootLeafW
  RootInternalW(Transaction& ta, Addr addr, std::unique_ptr<LeafW> left_leaf,
                Key sep, std::unique_ptr<LeafW> right_leaf,
                BtreeWritable& parent);

  void split(Key, std::unique_ptr<NodeW>) override;
  void balance() override;
  BtreeWritable& parent_;
};

} // namespace btree
} // namespace disk
} // namespace cheesebase

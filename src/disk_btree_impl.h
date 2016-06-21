// Licensed under the Apache License 2.0 (see LICENSE file).
#pragma once

#include "common.h"
#include "structs.h"
#include "disk_btree.h"
#include "cache.h"
#include <boost/container/flat_map.hpp>

namespace cheesebase {
namespace disk {
namespace btree {

////////////////////////////////////////////////////////////////////////////////
// constants

constexpr size_t k_entry_min_words = 1; // 6B header + 1B magic + 1B type
constexpr size_t k_entry_max_words = 4; // min + 24 byte inline string
constexpr size_t k_node_max_bytes = 256 - sizeof(DskBlockHdr);
constexpr std::ptrdiff_t kNodeSize = k_node_max_bytes;
constexpr std::ptrdiff_t kBlockSize = 256;
constexpr size_t k_node_max_words = k_node_max_bytes / sizeof(uint64_t);
constexpr size_t k_leaf_min_words =
    k_node_max_words / 2 - (k_entry_max_words - k_entry_min_words);
constexpr size_t k_internal_min_words = k_node_max_words / 2 - 1;


////////////////////////////////////////////////////////////////////////////////
// Writable

// Dummy type used as argument
enum class AllocateNew {};
enum class DontRead {};

using NodeRef = PageRef<Span<const Byte, kNodeSize>>;

class AbsInternalW;
class AbsLeafW;

class Node {
public:
  virtual ~Node() = default;
  Addr addr() const;

protected:
  Node(Addr addr);
  Addr addr_;
};

class NodeW : public Node {
  friend class RootInternalW;

public:
  NodeW(Addr addr);
  virtual ~NodeW() = default;

  virtual Writes getWrites() const = 0;

  // inserts value, returns true on success
  virtual bool insert(Key key, const model::Value&, Overwrite,
                      AbsInternalW* parent) = 0;

  // inserts value at maximum existing key + 1 and returns this key
  virtual Key append(const model::Value& val, AbsInternalW* parent) = 0;

  // deallocate node and all its children
  virtual void destroy() = 0;

  // delete value, returns true if found and removed
  virtual bool remove(Key key, AbsInternalW* parent) = 0;
};

class AbsLeafW : public NodeW {
public:
  AbsLeafW(AllocateNew, Transaction& ta, Addr next);
  AbsLeafW(Transaction& ta, Addr addr);

  // serialize and insert value, may trigger split
  bool insert(Key key, const model::Value&, Overwrite,
              AbsInternalW* parent) override;

  // find maximum key and insert value as key+1
  Key append(const model::Value&, AbsInternalW* parent) override;

  // append raw words without further checking
  void insert(Span<const uint64_t> raw);

  bool remove(Key key, AbsInternalW* parent) override;

  Writes getWrites() const override;

  void destroy() override;

  boost::container::flat_map<Key, std::unique_ptr<ValueW>> linked_;

  size_t size() const;

protected:
  void shiftBuffer(size_t pos, int amount);
  void initFromDisk();

  // get a view over internal data, independent of buf_ being initialized
  // second part of pair is needed to keep ReadRef locked if buf_ is not used
  std::pair<Span<const uint64_t>, std::unique_ptr<ReadRef>> getDataView() const;

  Transaction& ta_;
  std::unique_ptr<std::array<uint64_t, k_node_max_words>> buf_;
  size_t top_{ 0 }; // size in uint64_ts
  size_t findSize();
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
  void balance() override;
};

// tree just a single leaf
class RootLeafW : public AbsLeafW {
  friend class RootInternalW;

public:
  RootLeafW(AllocateNew, Transaction& ta, Addr next, BtreeWritable& tree);
  RootLeafW(Transaction& ta, Addr addr, BtreeWritable& tree);

private:
  RootLeafW(LeafW&&, Addr addr, BtreeWritable& parent);
  BtreeWritable& tree_;
  void split(Key, const model::Value&) override;
  void balance() override;
};

struct DskInternalNode;
struct DskInternalPair;

class InternalEntriesW {
  friend class AbsInternalW;

public:
  InternalEntriesW(Transaction& ta, Addr first, DskInternalPair* begin,
                   DskInternalPair* end);
  InternalEntriesW(Transaction& ta, Addr addr);
  InternalEntriesW(Transaction& ta, Addr addr, Addr left, Key sep, Addr right);

  Addr searchChildAddr(Key key);
  Addr searchSiblingAddr(Key key);
  void insert(Key key, Addr addr);

  //! Get iterator to entry including \param key.
  DskInternalPair* search(Key key);

  void remove(DskInternalPair* entry);

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
  DskInternalPair* begin();

  //! Iterator to middle entry. Rounds down on uneven entries.
  DskInternalPair* mid();

  //! Iterator to past the last entry.
  DskInternalPair* end();

  //! Get leftmost \c Addr
  Addr first();

  //! Remove all entries starting at \param from.
  void removeTail(DskInternalPair* from);

  //! Remove all entries until \param to.
  void removeHead(DskInternalPair* to);

  //! Prepend range of entries. \param sep has to be a seperator between the
  //! last entry and the first of the existing node (\class Key used in parent).
  void prepend(DskInternalPair* from, DskInternalPair* to, Key sep);

  //! Append range of entries.
  void append(DskInternalPair* from, DskInternalPair* to);

  //! Transform to root, 2 childs and a seperator, dismiss old entries.
  void makeRoot(Addr left, Key sep, Addr right);

  //! Take over \c DskInternalNode from \param other.
  void takeNodeFrom(InternalEntriesW& other);

  Transaction& ta_;

private:
  void init();
  Addr addr_;
  std::unique_ptr<DskInternalNode> node_;
};

class AbsInternalW : public NodeW {
  friend class RootInternalW;

public:
  AbsInternalW(Transaction& ta, Addr addr);
  AbsInternalW(AllocateNew, Transaction& ta, Addr first, DskInternalPair* begin,
               DskInternalPair* end);

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
  DskInternalPair* searchEntry(Key key);

  //! Remove Key-Addr-Pair referenced by \param entry.
  void removeMerged(DskInternalPair* entry);

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

  // append words without further checking, increases top position
  void appendRaw(Span<const uint64_t> raw);

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
      Key sep, std::unique_ptr<LeafW> right_leaf, BtreeWritable& parent);

  void split(Key, std::unique_ptr<NodeW>) override;
  void balance() override;
  BtreeWritable& parent_;
};

std::unique_ptr<NodeW> openRootW(Transaction& ta, Addr addr,
                                 BtreeWritable& parent);

////////////////////////////////////////////////////////////////////////////////
// ReadOnly

class NodeR : public Node {
public:
  virtual void getAll(model::Object& obj) = 0;
  virtual void getAll(model::Array& obj) = 0;
  virtual std::unique_ptr<model::Value> getChildValue(Key) = 0;
  virtual std::unique_ptr<ValueW> getChildCollectionW(Transaction&, Key) = 0;
  virtual std::unique_ptr<ValueR> getChildCollectionR(Key) = 0;

protected:
  NodeR(Database& db, Addr addr, ReadRef page);

  Span<const uint64_t> getData() const;

  Database& db_;
  ReadRef page_;
};

class LeafR : public NodeR {
public:
  LeafR(Database& db, Addr addr, ReadRef page);
  LeafR(Database& db, Addr addr);

  // fill obj with values in this and all following leafs
  void getAll(model::Object& obj) override;
  void getAll(model::Array& obj) override;

  // fill obj with values in this leaf, returns next leaf address
  Addr getAllInLeaf(model::Object& obj);
  Addr getAllInLeaf(model::Array& obj);

  std::unique_ptr<model::Value> getChildValue(Key) override;
  std::unique_ptr<ValueW> getChildCollectionW(Transaction&, Key) override;
  std::unique_ptr<ValueR> getChildCollectionR(Key) override;

private:
  std::pair<Key, std::unique_ptr<model::Value>>
  readValue(Span<const uint64_t>::const_iterator& it);
};

class InternalR : public NodeR {
public:
  InternalR(Database& db, Addr addr, ReadRef page);

  void getAll(model::Object& obj) override;
  void getAll(model::Array& obj) override;

  std::unique_ptr<model::Value> getChildValue(Key) override;
  std::unique_ptr<ValueW> getChildCollectionW(Transaction&, Key) override;
  std::unique_ptr<ValueR> getChildCollectionR(Key) override;

private:
  std::unique_ptr<NodeR> searchChildNode(Key k);

  Span<const uint64_t> data_;
};

std::unique_ptr<NodeR> openNodeR(Database& db, Addr addr);

} // namespace btree
} // namespace disk
} // namespace cheesebase
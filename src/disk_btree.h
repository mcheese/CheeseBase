// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "disk_value.h"
#include "common.h"
#include "structs.h"
#include "model.h"
#include "storage.h"
#include <boost/container/flat_map.hpp>
#include <memory>

namespace cheesebase {

class Transaction;
class Database;

namespace disk {

////////////////////////////////////////////////////////////////////////////////
// constants

constexpr size_t k_entry_min_words = 1; // 6B header + 1B magic + 1B type
constexpr size_t k_entry_max_words = 4; // min + 24 byte inline string
constexpr size_t k_node_max_bytes = 256 - sizeof(DskBlockHdr);
constexpr BlockType k_block_type = BlockType::t4;
static_assert(toBlockSize(k_block_type) ==
                  k_node_max_bytes + sizeof(DskBlockHdr),
              "Node should occupy a whole block");
constexpr size_t k_node_max_words = k_node_max_bytes / sizeof(uint64_t);
constexpr size_t k_leaf_min_words =
    k_node_max_words / 2 - (k_entry_max_words - k_entry_min_words);
constexpr size_t k_internal_min_words = k_node_max_words / 2 - 1;

////////////////////////////////////////////////////////////////////////////////
// Classes

// argument for insert queries
enum class Overwrite { Insert, Update, Upsert };

namespace btree {

class NodeW;

class BtreeWritable {
  friend class RootLeafW;
  friend class RootInternalW;

public:
  // create new tree
  BtreeWritable(Transaction& ta);
  // open existing tree
  BtreeWritable(Transaction& ta, Addr root);

  Addr addr() const;
  bool insert(Key key, const model::Value& val, Overwrite);
  bool remove(Key key);
  Key append(const model::Value& val);
  void destroy();
  Writes getWrites() const;

private:
  std::unique_ptr<btree::NodeW> root_;
};

class BtreeReadOnly {
public:
  BtreeReadOnly(Database& db, Addr root);

  model::Object getObject();
  model::Array getArray();
  model::PValue getChildValue(Key key);
  std::unique_ptr<ValueW> getChildCollectionW(Transaction&, Key key);
  std::unique_ptr<ValueR> getChildCollectionR(Key key);
  std::unique_ptr<ValueW> getChild(Key key);

private:
  Database& db_;
  Addr root_;
};

////////////////////////////////////////////////////////////////////////////////
// Writable

// Dummy type used as argument
enum class AllocateNew {};
enum class DontRead {};

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
  NodeW(Transaction& ta, Addr addr);
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

  // filled size in words
  size_t size() const;

protected:
  void shiftBuffer(size_t pos, int amount);
  void initFromDisk();
  virtual size_t findSize() = 0;

  // get a view over internal data, independent of buf_ being initialized
  // second part of pair is needed to keep ReadRef locked if buf_ is not used
  std::pair<Span<const uint64_t>, std::unique_ptr<ReadRef>> getDataView() const;

  Transaction& ta_;
  std::unique_ptr<std::array<uint64_t, k_node_max_words>> buf_;
  size_t top_{ 0 }; // size in uint64_ts
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

  std::map<Key, std::unique_ptr<ValueW>> linked_;

protected:
  size_t findSize() override;
  virtual void split(Key, const model::Value&) = 0;
  virtual void merge() = 0;
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
  void merge() override;
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
  void merge() override;
};

class AbsInternalW : public NodeW {
  friend class RootInternalW;

public:
  AbsInternalW(Transaction& ta, Addr addr);
  AbsInternalW(AllocateNew, Transaction& ta);

  // used when extending single root leaf to internal root
  AbsInternalW(Transaction& ta, Addr addr, size_t top,
               std::unique_ptr<std::array<uint64_t, k_node_max_words>> buf);

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
  NodeW& getSilbling(Key key, Addr addr);

  // Remove Key+Addr after *a*. *k* search should find *a*. Returns removed Key.
  Key removeMerged(Key k, Addr a);

  // Replace Key after *a* with *k*. *k* search should find *a*. Returns
  // replaced Key.
  Key updateMerged(Key k, Addr a);

protected:
  size_t findSize() override;
  AbsInternalW* parent_;
  boost::container::flat_map<Addr, std::unique_ptr<NodeW>> childs_;

private:
  virtual void split(Key, std::unique_ptr<NodeW>) = 0;
  virtual void merge() = 0;
};

class InternalW : public AbsInternalW {
public:
  using AbsInternalW::AbsInternalW;

  // append words without further checking, increases top position
  void appendRaw(Span<const uint64_t> raw);

private:
  void split(Key, std::unique_ptr<NodeW>) override;
  void merge() override;
};

class RootInternalW : public AbsInternalW {
  friend class RootLeafW;

public:
  RootInternalW(Transaction& ta, Addr addr, BtreeWritable& parent);

private:
  // used to construct while splitting RootLeafW
  RootInternalW(Transaction& ta, Addr addr, size_t top,
                std::unique_ptr<std::array<uint64_t, k_node_max_words>> buf,
                BtreeWritable& parent);
  void split(Key, std::unique_ptr<NodeW>) override;
  void merge() override;
  BtreeWritable& parent_;
};

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
  std::pair<Key, model::PValue>
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

} // namespace btree
} // namespace disk
} // namespace cheesebase

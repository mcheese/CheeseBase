// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common/common.h"
#include "common/structs.h"
#include "model.h"
#include "storage/storage.h"
#include <boost/container/flat_map.hpp>
#include <memory>

namespace cheesebase {

class Transaction;
class Database;

namespace btree {

////////////////////////////////////////////////////////////////////////////////
// constants

using Word = uint64_t;
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
// classes

// Dummy type used as argument
enum class AllocateNew {};
enum class DontRead {};

// argument for insert queries
enum class Overwrite { Insert, Update, Upsert };

class Node {
public:
  Addr addr() const;

protected:
  Node(Addr addr);
  Addr m_addr;
};

////////////////////////////////////////////////////////////////////////////////
// Writable

class NodeW;
class AbsInternalW;
class AbsLeafW;

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
  bool insert(const std::string& key, const model::Value& val, Overwrite);
  bool remove(Key key);
  bool remove(const std::string& key);
  void destroy();
  Writes getWrites() const;

private:
  Transaction& m_ta;
  std::unique_ptr<NodeW> m_root;
};

class NodeW : public Node {
  friend class RootInternalW;

public:
  NodeW(Transaction& ta, Addr addr);

  virtual Writes getWrites() const = 0;

  // inserts value, returns true on success
  virtual bool insert(Key key, const model::Value&, Overwrite,
                      AbsInternalW* parent) = 0;

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

  // get a view over internal data, independent of m_buf being initialized
  // second part of pair is needed to keep ReadRef locked if m_buf is not used
  std::pair<gsl::span<const uint64_t>, std::unique_ptr<ReadRef>>
  getDataView() const;

  Transaction& m_ta;
  std::unique_ptr<std::array<Word, k_node_max_words>> m_buf;
  size_t m_top{ 0 }; // size in Words
};

class AbsLeafW : public NodeW {
public:
  AbsLeafW(AllocateNew, Transaction& ta, Addr next);
  AbsLeafW(Transaction& ta, Addr addr);

  // serialize and insert value, may trigger split
  bool insert(Key key, const model::Value&, Overwrite,
              AbsInternalW* parent) override;

  // append raw words without further checking
  void insert(gsl::span<const uint64_t> raw);

  bool remove(Key key, AbsInternalW* parent) override;

  Writes getWrites() const override;

  void destroy() override;

  boost::container::flat_map<Key, std::unique_ptr<BtreeWritable>> m_linked;

protected:
  size_t findSize() override;
  virtual void split(Key, const model::Value&, size_t insert_pos) = 0;
  virtual void merge() = 0;
  // Destroy value at pos (if remote). Return size of the entry.
  template <typename ConstIt>
  size_t destroyValue(ConstIt it);
  AbsInternalW* m_parent;
};

class LeafW : public AbsLeafW {
  friend class RootLeafW;

public:
  using AbsLeafW::AbsLeafW;

private:
  void split(Key, const model::Value&, size_t insert_pos) override;
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
  BtreeWritable& m_tree;
  void split(Key, const model::Value&, size_t insert_pos) override;
  void merge() override;
};

class AbsInternalW : public NodeW {
  friend class RootInternalW;

public:
  AbsInternalW(Transaction& ta, Addr addr);
  AbsInternalW(AllocateNew, Transaction& ta);

  // used when extending single root leaf to internal root
  AbsInternalW(Transaction& ta, Addr addr, size_t top,
               std::unique_ptr<std::array<Word, k_node_max_words>> buf);

  bool insert(Key key, const model::Value&, Overwrite,
              AbsInternalW* parent) override;
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
  AbsInternalW* m_parent;
  boost::container::flat_map<Addr, std::unique_ptr<NodeW>> m_childs;

private:
  virtual void split(Key, std::unique_ptr<NodeW>) = 0;
  virtual void merge() = 0;
};

class InternalW : public AbsInternalW {
public:
  using AbsInternalW::AbsInternalW;

  // append words without further checking, increases top position
  void append(gsl::span<const uint64_t> raw);

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
  BtreeWritable& m_parent;
};

////////////////////////////////////////////////////////////////////////////////
// ReadOnly

class BtreeReadOnly {
public:
  BtreeReadOnly(Database& db, Addr root);

  model::Object getObject();
  std::unique_ptr<model::Value> getValue(const std::string& key);

private:
  Database& m_db;
  Addr m_root;
};

class NodeR : public Node {
public:
  virtual void getAll(model::Object& obj) = 0;
  virtual std::unique_ptr<model::Value> getValue(Key key) = 0;

protected:
  NodeR(Database& db, Addr addr, ReadRef page);

  gsl::span<const uint64_t> getData() const;

  Database& m_db;
  ReadRef m_page;
};

class LeafR : public NodeR {
public:
  LeafR(Database& db, Addr addr, ReadRef page);
  LeafR(Database& db, Addr addr);

  // fill obj with values in this and all following leafs
  void getAll(model::Object& obj) override;

  // fill obj with values in this leaf, returns next leaf address
  Addr getAllInLeaf(model::Object& obj);

  std::unique_ptr<model::Value> getValue(Key key) override;

private:
  std::pair<model::Key, model::PValue>
  readValue(gsl::span<const uint64_t>::const_iterator& it);
};

class InternalR : public NodeR {
public:
  InternalR(Database& db, Addr addr, ReadRef page);

  void getAll(model::Object& obj) override;

  std::unique_ptr<model::Value> getValue(Key key) override;

private:
  std::unique_ptr<NodeR> searchChild(Key k);

  gsl::span<const uint64_t> m_data;
};

} // namespace btree
} // namespace cheesebase

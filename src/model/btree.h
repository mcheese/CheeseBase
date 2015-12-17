// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common/common.h"
#include "common/structs.h"
#include <boost/container/flat_map.hpp>
#include <memory>

namespace cheesebase {

class Transaction;
class Database;

namespace model {
class Value;
}

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
constexpr size_t k_node_min_words =
    k_node_max_words / 2 - (k_entry_max_words - k_entry_min_words);

////////////////////////////////////////////////////////////////////////////////
// classes

// Dummy type used as argument
enum class AllocateNew {};

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

class BtreeWritable {
public:
  // create new tree
  BtreeWritable(Transaction& ta);
  // open existing tree
  BtreeWritable(Transaction& ta, Addr root);

  Addr addr() const;
  bool insert(Key key, const model::Value& val);
  Writes getWrites() const;

private:
  std::unique_ptr<NodeW> m_root;
};

class NodeW : public Node {
public:
  NodeW(Transaction& ta, Addr addr);

  virtual Writes getWrites() const = 0;

  // inserts value, returns true if it overwrote something
  virtual bool insert(Key key, const model::Value&) = 0;

protected:
  void shiftBuffer(size_t pos, int amount);
  void initFromDisk();
  virtual size_t findSize() = 0;

  Transaction& m_ta;
  std::unique_ptr<std::array<Word, k_node_max_words>> m_buf;
  size_t m_top{ 0 }; // size in Words
};

class LeafW : public NodeW {
public:
  LeafW(AllocateNew, Transaction& ta, Addr next);
  LeafW(Transaction& ta, Addr addr);

  bool insert(Key key, const model::Value&) override;

private:
  Writes getWrites() const override;
  size_t findSize() override;

  std::vector<std::unique_ptr<BtreeWritable>> m_linked;
};

class InternalW : public NodeW {
public:
  InternalW(Transaction& ta, Addr addr);

  bool insert(Key key, const model::Value&) override;

private:
  Writes getWrites() const override;
  size_t findSize() override;

  boost::container::flat_map<Addr, std::unique_ptr<NodeW>> m_childs;
};

////////////////////////////////////////////////////////////////////////////////
// ReadOnly

class BtreeReadOnly {
  BtreeReadOnly(Database& db, Addr root);
};

class NodeR {

};

class LeafR {

};

class InternalR {

};

} // namespace btree
} // namespace cheesebase

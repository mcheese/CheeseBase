// Licensed under the Apache License 2.0 (see LICENSE file).
#include "read.h"

#include "../array.h"
#include "../object.h"
#include "../string.h"
#include "internal.h"
#include "leaf.h"

namespace cheesebase {
namespace disk {
namespace btree {
namespace NodeR {
namespace {

template <typename ConstIt>
std::pair<Key, model::Value> readValue(Database& db, ConstIt& it) {
  gCountReads++;

  auto entry = DskLeafEntry(*it++);
  std::pair<Key, model::Value> ret;
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
    ret.second = std::move(str);
  } else {
    switch (entry.value.type) {
    case ValueType::object:
      ret.second = model::Tuple(std::make_unique<ObjectR>(db, Addr(*it++)));
      break;
    case ValueType::array:
      ret.second = model::Collection(std::make_unique<ArrayR>(db, Addr(*it++)));
      break;
    case ValueType::number:
      union {
        uint64_t word;
        model::Number number;
      } num;
      num.word = *it++;
      ret.second = num.number;
      break;
    case ValueType::string:
      ret.second = StringR(db, Addr(*it++)).getValue();
      break;
    case ValueType::boolean_true:
      ret.second = model::Bool{ true };
      break;
    case ValueType::boolean_false:
      ret.second = model::Bool{ false };
      break;
    case ValueType::null:
      ret.second = model::Null{};
      break;
    default:
      throw ConsistencyError("Unknown value type");
    }
  }

  return ret;
}

const DskLeafNode& leafView(ReadRef<kBlockSize>& block) {
  return bytesAsType<DskLeafNode>(*block);
}

Addr getAllInLeaf(Database& db, ReadRef<kBlockSize>& block, model::Tuple& obj) {
  auto node = leafView(block);
  auto it = node.begin();
  auto next = node.hdr.next();

  while (it != node.end() && *it != 0) {
    auto pair = readValue(db, it);
    obj.emplace(db.resolveKey(pair.first), std::move(pair.second));
  }

  return next;
}

Addr getAllInLeaf(Database& db, ReadRef<kBlockSize>& block, ArrayMap& arr) {
  auto node = leafView(block);
  auto it = node.begin();
  auto next = node.hdr.next();

  while (it < node.end() && *it != 0) {
    auto pair = readValue(db, it);
    arr.emplace(pair.first.value, std::move(pair.second));
  }

  return next;
}

const DskInternalNode& internalView(ReadRef<kBlockSize>& block) {
  auto& n = bytesAsType<DskInternalNode>(*block);
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
    auto child_addr = node.searchAddr(key);
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
template void getAll<model::Tuple>(Database& db, Addr addr, model::Tuple& obj);
template void getAll<ArrayMap>(Database& db, Addr addr, ArrayMap& obj);

model::Value getChildValue(Database& db, Addr addr, Key key) {
  auto block = db.loadBlock<kBlockSize>(addr);

  if (isNodeLeaf(block)) {
    auto node = leafView(block);
    auto it = node.search(key);

    if (it >= node.end() || *it == 0) return model::Missing{};
    if (DskLeafEntry(*it).key.key() != key) return model::Missing{};

    // TODO: should free block here, but readValue needs it and may recurse
    return readValue(db, it).second;

  } else {
    auto node = internalView(block);
    auto child_addr = node.searchAddr(key);
    block.free();
    return getChildValue(db, child_addr, key);
  }
}

std::unique_ptr<ValueW> getChildCollectionW(Transaction& ta, Addr addr,
                                            Key key) {
  return getChildCollection<ValueW, ObjectW, ArrayW>(ta, addr, key);
}

std::unique_ptr<ValueR> getChildCollectionR(Database& db, Addr addr, Key key) {
  return getChildCollection<ValueR, ObjectR, ArrayR>(db, addr, key);
}

} // namespace NodeR
} // namespace btree
} // namespace disk
} // namespace cheesebase

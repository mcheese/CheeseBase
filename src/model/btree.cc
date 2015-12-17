// Licensed under the Apache License 2.0 (see LICENSE file).

#include "btree.h"
#include "core.h"
#include "model.h"

namespace cheesebase {
namespace btree {

////////////////////////////////////////////////////////////////////////////////
// on disk structures

namespace {

CB_PACKED(struct DskEntry {
  DskEntry() = default;
  DskEntry(uint64_t w) {
    *reinterpret_cast<uint64_t*>(this) = w;
    if (value.magic_byte != '!')
      throw ConsistencyError("No magic byte in value");
  }
  DskEntry(Key k, model::ValueType t) : key{ k }, value{ '!', t } {}

  size_t extraWords() const { return model::valueExtraWords(value.type); }
  uint64_t word() { return *reinterpret_cast<uint64_t*>(this); }

  DskValueHdr value;
  DskKey key;
});
static_assert(sizeof(DskEntry) == 8, "Invalid DskEntry size");

Key keyFromWord(uint64_t w) { return DskEntry(w).key.key(); }

size_t entrySize(uint64_t e) { return DskEntry(e).extraWords() + 1; }

CB_PACKED(struct DskNextPtr {
  DskNextPtr() = default;
  DskNextPtr(uint64_t d) {
    Expects((d & (static_cast<uint64_t>(0xff) << 56)) == 0);
    constexpr uint64_t magic = static_cast<uint64_t>('L') << 56;
    data = d + magic;
  }

  bool hasMagic() const { return (data >> 56) == 'L'; }
  Addr next() const { return data & (((uint64_t)1 << 56) - 1); }

  uint64_t data;
});
static_assert(sizeof(DskNextPtr) == 8, "Invalid NextPtr size");

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
// free functions

namespace {

std::unique_ptr<NodeW> openNodeW(Transaction& ta, Addr addr) {
  auto page = ta.load(toPageNr(addr));
  auto hdr = gsl::as_span<DskBlockHdr>(
      page->subspan(toPageOffset(addr), sizeof(DskBlockHdr)))[0];

  if (hdr.type() != k_block_type)
    throw ConsistencyError("Unexpected block size");

  // first byte of Addr is always 0
  // next-ptr of leafs put a flag in the first byte, marking the node as leaf
  auto leaf = gsl::as_span<const DskNextPtr>(page->subspan(
      toPageOffset(addr) + sizeof(DskBlockHdr), sizeof(DskNextPtr)))[0]
                  .hasMagic();

  if (leaf)
    return std::make_unique<LeafW>(ta, addr);
  else
    return std::make_unique<InternalW>(ta, addr);
}

size_t searchLeafPosition(Key key, gsl::span<const uint64_t> span) {
  for (size_t i = 1; i < static_cast<size_t>(span.size());
       i += entrySize(span[i])) {
    if (span[i] == 0 || keyFromWord(span[i]) >= key) return i;
  }
  return static_cast<size_t>(span.size());
}

size_t searchInternalPosition(Key key, gsl::span<const uint64_t> span) {
  for (int i = 1; i < static_cast<size_t>(span.size()); i += 2) {
    if (span[i] == 0 || keyFromWord(span[i]) >= key) return i;
  }
  return static_cast<size_t>(span.size());
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
// Btree

BtreeWritable::BtreeWritable(Transaction& ta, Addr root) {
  m_root = openNodeW(ta, root);
}

BtreeWritable::BtreeWritable(Transaction& ta) {
  m_root = std::make_unique<LeafW>(AllocateNew(), ta, 0);
}

Addr BtreeWritable::addr() const {
  Expects(m_root);
  return m_root->addr();
}

bool BtreeWritable::insert(Key key, const model::Value& val) {
  Expects(m_root);
  return m_root->insert(key, val);
}

Writes BtreeWritable::getWrites() const {
  Expects(m_root);
  return m_root->getWrites();
}

////////////////////////////////////////////////////////////////////////////////
// Node

Node::Node(Addr addr) : m_addr(addr) {}

Addr Node::addr() const { return m_addr; }

////////////////////////////////////////////////////////////////////////////////
// NodeW

NodeW::NodeW(Transaction& ta, Addr addr) : m_ta(ta), Node(addr) {}

void NodeW::shiftBuffer(size_t pos, int amount) {
  Expects(m_buf);
  Expects(pos <= m_top);
  Expects(amount + pos > 0 && amount + pos < m_buf->size());

  if (amount > 0) {
    for (size_t i = m_top; i > pos; --i) {
      m_buf->at(i + amount - 1) = m_buf->at(i - 1);
    }
  } else if (amount < 0) {
    for (size_t i = pos; i < m_top; ++i) {
      m_buf->at(i + amount) = m_buf->at(i);
    }
    for (size_t i = m_top; i < m_top - amount; i++) {
      m_buf->at(i + amount) = 0;
    }
  }
  m_top += amount;
}

void NodeW::initFromDisk() {
  Expects(!m_buf);

  m_buf = std::make_unique<std::array<uint64_t, k_node_max_words>>();
  copySpan(m_ta.load(toPageNr(m_addr))
               ->subspan(toPageOffset(m_addr) + sizeof(DskBlockHdr),
                         k_node_max_bytes),
           gsl::as_writeable_bytes(gsl::span<uint64_t>(*m_buf)));
  m_top = findSize();
}

////////////////////////////////////////////////////////////////////////////////
// LeafW

LeafW::LeafW(AllocateNew, Transaction& ta, Addr next)
    : NodeW(ta, ta.alloc(k_node_max_bytes).addr) {
  m_buf = std::make_unique<std::array<uint64_t, k_node_max_words>>();
  std::fill(m_buf->begin(), m_buf->end(), 0);
  (*m_buf)[0] = DskNextPtr(next).data;
  m_top = 1;
}

LeafW::LeafW(Transaction& ta, Addr addr) : NodeW(ta, addr) {}

Writes LeafW::getWrites() const {
  Writes w;
  w.reserve(1 + m_linked.size()); // may be more, but a good guess

  if (m_buf)
    w.push_back({ m_addr + sizeof(DskBlockHdr),
                  gsl::as_bytes(gsl::span<uint64_t>(*m_buf)) });

  for (auto& c : m_linked) {
    auto cw = c->getWrites();
    std::move(cw.begin(), cw.end(), std::back_inserter(w));
  }

  return w;
}

size_t LeafW::findSize() {
  size_t i = 1; // first element always is a Addr, skip to value
  while (i < m_buf->size() && m_buf->at(i) != 0) {
    i += entrySize(m_buf->at(i));
  }
  return i;
}

bool LeafW::insert(Key key, const model::Value& val) {
  if (!m_buf) initFromDisk();

  auto extras = val.extraWords();
  int extra_words = gsl::narrow_cast<int>(extras.size());
  bool overwrite;

  // enough space to insert?
  if (m_top < k_node_max_words + extra_words) {

    // find position to insert
    auto pos = searchLeafPosition(key, *m_buf);
    Ensures(pos < k_node_max_words + extra_words);

    // make space
    if (pos < m_top && keyFromWord(m_buf->at(pos)) == key) {
      auto extra = gsl::narrow_cast<int>(DskEntry(m_buf->at(pos)).extraWords());
      shiftBuffer(pos, extra_words - extra);
      overwrite = true;
    } else {
      shiftBuffer(pos, 1 + extra_words);
      overwrite = false;
    }

    // put the first word
    auto t = val.type();
    m_buf->at(pos) = DskEntry{ key, t }.word();

    // put extra words
    // recurse into inserting remotely stored elements if needed
    if (t == model::ValueType::object) {
      auto& obj = dynamic_cast<const model::Object&>(val);
      m_linked.push_back(std::make_unique<BtreeWritable>(m_ta));
      for (auto& c : obj) {
        m_linked.back()->insert(m_ta.key(c.first), *c.second);
      }
      m_buf->at(pos + 1) = m_linked.back()->addr();
    } else if (t == model::ValueType::list) {
      throw std::runtime_error("NIY");
    } else if (t == model::ValueType::string) {
      throw std::runtime_error("NIY");
    } else {
      for (auto i = 0; i < extra_words; ++i) {
        m_buf->at(pos + 1 + i) = extras[i];
      }
    }

  } else {

    // TODO: implement splitting etc..
    throw std::runtime_error("NIY");
  }

  return overwrite;
}

////////////////////////////////////////////////////////////////////////////////
// InternalW

InternalW::InternalW(Transaction& ta, Addr addr) : NodeW(ta, addr) {}

bool InternalW::insert(Key key, const model::Value&) {
  throw std::runtime_error("NIY");
}

Writes InternalW::getWrites() const {
  Writes w;
  w.reserve(1 + m_childs.size()); // may be more, but a good guess

  if (m_buf)
    w.push_back({ m_addr + sizeof(DskBlockHdr),
                  gsl::as_bytes(gsl::span<uint64_t>(*m_buf)) });

  for (auto& c : m_childs) {
    auto cw = c.second->getWrites();
    std::move(cw.begin(), cw.end(), std::back_inserter(w));
  }

  return w;
}

size_t InternalW::findSize() {
  size_t i = 1; // first element always is a Addr, skip to value
  while (i < m_buf->size() && m_buf->at(i) != 0) { i += 2; }
  return i;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace btree
} // namespace cheesebase

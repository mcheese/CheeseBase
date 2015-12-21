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

CB_PACKED(struct DskInternalEntry {
  DskInternalEntry() = default;
  DskInternalEntry(uint64_t w) {
    *reinterpret_cast<uint64_t*>(this) = w;
    if (magic[0] != '-' || magic[1] != '>')
      throw ConsistencyError("No magic byte in key");
  }

  uint64_t fromKey(Key k) {
    key = k;
    magic[0] = '-';
    magic[1] = '>';
    return *reinterpret_cast<uint64_t*>(this);
  }

  uint64_t word() { return *reinterpret_cast<uint64_t*>(this); }

  char magic[2];
  DskKey key;
});
static_assert(sizeof(DskInternalEntry) == 8, "Invalid DskInternalEntry size");

Key keyFromWord(uint64_t w) { return DskEntry(w).key.key(); }

size_t entrySize(uint64_t e) { return DskEntry(e).extraWords() + 1; }

CB_PACKED(struct DskLeafHdr {
  DskLeafHdr() = default;

  DskLeafHdr& fromAddr(Addr d) {
    Expects((d & (static_cast<uint64_t>(0xff) << 56)) == 0);
    constexpr uint64_t magic = static_cast<uint64_t>('L') << 56;
    data = d + magic;
    return *this;
  }

  DskLeafHdr& fromDsk(uint64_t w) {
    data = w;
    if (!hasMagic()) throw ConsistencyError("No magic byte in leaf node");
    return *this;
  }

  bool hasMagic() const { return (data >> 56) == 'L'; }
  Addr next() const { return data & (((uint64_t)1 << 56) - 1); }

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

  bool hasMagic() const { return (data >> 56) == 'I'; }
  size_t size() const {
    return gsl::narrow_cast<size_t>(data & (((uint64_t)1 << 56) - 1));
  }

  uint64_t data;
});
static_assert(sizeof(DskInternalHdr) == 8, "Invalid DskInternalHdr size");

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
// free functions

namespace {

bool isNodeLeaf(const ReadRef& page, Addr addr) {
  auto hdr = gsl::as_span<DskBlockHdr>(
      page->subspan(toPageOffset(addr), sizeof(DskBlockHdr)))[0];

  if (hdr.type() != k_block_type)
    throw ConsistencyError("Unexpected block size");

  // first byte of Addr is always 0
  // next-ptr of leafs put a flag in the first byte, marking the node as leaf
  return gsl::as_span<const DskLeafHdr>(page->subspan(
      toPageOffset(addr) + sizeof(DskBlockHdr), sizeof(DskLeafHdr)))[0]
      .hasMagic();
}

std::unique_ptr<NodeW> openNodeW(Transaction& ta, Addr addr, AbsInternalW& p) {
  auto page = ta.load(toPageNr(addr));

  if (isNodeLeaf(page, addr))
    return std::make_unique<LeafW>(ta, addr);
  else
    return std::make_unique<InternalW>(ta, addr);
}

std::unique_ptr<NodeR> openNodeR(Database& db, Addr addr) {
  auto page = db.loadPage(toPageNr(addr));

  if (isNodeLeaf(page, addr))
    return std::make_unique<LeafR>(db, addr, std::move(page));
  else
    return std::make_unique<InternalR>(db, addr, std::move(page));
}

size_t searchLeafPosition(Key key, gsl::span<const uint64_t> span) {
  for (size_t i = 1; i < static_cast<size_t>(span.size());
       i += entrySize(span[i])) {
    if (span[i] == 0 || keyFromWord(span[i]) >= key) return i;
  }
  return static_cast<size_t>(span.size());
}

size_t searchInternalPosition(Key key, gsl::span<const uint64_t> span) {
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
        return last * 2 + 2;
      else
        first = mid;
    }
  }
  return first * 2 + 2;
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
// Node

Node::Node(Addr addr) : m_addr(addr) {}

Addr Node::addr() const { return m_addr; }

////////////////////////////////////////////////////////////////////////////////
// BtreeWriteable

BtreeWritable::BtreeWritable(Transaction& ta, Addr root) {
  auto page = ta.load(toPageNr(root));
  if (isNodeLeaf(page, root))
    m_root = std::make_unique<RootLeafW>(ta, root, *this);
  else
    m_root = std::make_unique<RootInternalW>(ta, root, *this);
}

BtreeWritable::BtreeWritable(Transaction& ta) {
  m_root = std::make_unique<RootLeafW>(AllocateNew(), ta, 0, *this);
}

Addr BtreeWritable::addr() const {
  Expects(m_root);
  return m_root->addr();
}

bool BtreeWritable::insert(Key key, const model::Value& val, Overwrite o) {
  Expects(m_root);
  return m_root->insert(key, val, o, nullptr);
}

Writes BtreeWritable::getWrites() const {
  Expects(m_root);
  return m_root->getWrites();
}

////////////////////////////////////////////////////////////////////////////////
// NodeW

NodeW::NodeW(Transaction& ta, Addr addr) : m_ta(ta), Node(addr) {}

size_t NodeW::size() const { return m_top; }

void NodeW::shiftBuffer(size_t pos, int amount) {
  Expects(m_buf);
  Expects(amount + pos > 0 && amount + pos <= m_buf->size());
  Expects(pos <= m_top);

  if (pos < m_top && amount != 0) {
    if (amount > 0) {
      for (size_t i = m_top; i > pos; --i) {
        m_buf->at(i + amount - 1) = m_buf->at(i - 1);
      }
    } else {
      for (size_t i = pos; i < m_top; ++i) {
        m_buf->at(i + amount) = m_buf->at(i);
      }
      for (size_t i = m_top; i < m_top - amount; i++) {
        m_buf->at(i + amount) = 0;
      }
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
// AbsLeafW

AbsLeafW::AbsLeafW(AllocateNew, Transaction& ta, Addr next)
    : NodeW(ta, ta.alloc(k_node_max_bytes).addr) {
  m_buf = std::make_unique<std::array<uint64_t, k_node_max_words>>();
  std::fill(m_buf->begin(), m_buf->end(), 0);
  (*m_buf)[0] = DskLeafHdr().fromAddr(next).data;
  m_top = 1;
}

AbsLeafW::AbsLeafW(Transaction& ta, Addr addr) : NodeW(ta, addr) {}

Writes AbsLeafW::getWrites() const {
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

size_t AbsLeafW::findSize() {
  size_t i = 1; // first element always is a Addr, skip to value
  while (i < m_buf->size() && m_buf->at(i) != 0) {
    i += entrySize(m_buf->at(i));
  }
  return i;
}

bool AbsLeafW::insert(Key key, const model::Value& val, Overwrite ow,
                      AbsInternalW* parent) {
  m_parent = parent;
  if (!m_buf) initFromDisk();

  auto extras = val.extraWords();
  int extra_words = gsl::narrow_cast<int>(extras.size());

  // find position to insert
  auto pos = searchLeafPosition(key, *m_buf);
  Ensures(pos < k_node_max_words + extra_words);
  bool update = pos < m_top && keyFromWord(m_buf->at(pos)) == key;
  if ((ow == Overwrite::Update && !update) ||
      (ow == Overwrite::Insert && update)) {
    return false;
  }

  // enough space to insert?
  if (m_top + extra_words < k_node_max_words) {

    // make space
    if (update) {
      auto extra = gsl::narrow_cast<int>(DskEntry(m_buf->at(pos)).extraWords());
      shiftBuffer(pos, extra_words - extra);
    } else {
      shiftBuffer(pos, 1 + extra_words);
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
        auto r = m_linked.back()->insert(m_ta.key(c.first), *c.second,
                                         Overwrite::Insert);
        Expects(r == true);
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
    split(key, val, pos);
  }

  return true;
}

////////////////////////////////////////////////////////////////////////////////
// LeafW

void LeafW::split(Key key, const model::Value& val, size_t pos) {
  Expects(m_buf);
  Expects(m_top <= m_buf->size());
  Expects(m_parent != nullptr);

  auto right_leaf = std::make_unique<LeafW>(
      AllocateNew(), m_ta, DskLeafHdr().fromDsk(m_buf->at(0)).next());

  auto new_val_len = model::valueExtraWords(val.type()) + 1;
  bool new_here = false;
  size_t i = 1;
  for (; i < m_top; ++i) {
    if ((new_here && i >= m_top - i + new_val_len + 1) ||
        (!new_here && i + new_val_len >= m_top - i + 1)) {

      break;
    }
    auto entry = DskEntry(m_buf->at(i));
    if (entry.key.key() >= key) { new_here = true; }
    i += entry.extraWords();
  }

  auto split_key = DskEntry(m_buf->at(i)).key.key();

  Ensures(i < m_top);
  Ensures(i + (new_here ? new_val_len : 0) > k_node_min_words);

  right_leaf->insert(
      gsl::span<uint64_t>(&m_buf->at(i), gsl::narrow_cast<int>(m_top - i)));
  std::fill(m_buf->begin() + i, m_buf->begin() + m_top, 0);
  m_top = i;
  m_buf->at(0) = DskLeafHdr().fromAddr(right_leaf->addr()).data;

  // the check for existence happened before calling split so just use "Upsert"
  if (new_here)
    insert(key, val, Overwrite::Upsert, m_parent);
  else
    right_leaf->insert(key, val, Overwrite::Upsert, m_parent);

  m_parent->insert(split_key, std::move(right_leaf));
}

void AbsLeafW::insert(gsl::span<const uint64_t> raw) {
  Expects(m_buf);
  Expects(k_node_max_words >= m_top + raw.size());

  for (auto word : raw) { m_buf->at(m_top++) = word; }
}

////////////////////////////////////////////////////////////////////////////////
// RootLeafW

RootLeafW::RootLeafW(AllocateNew, Transaction& ta, Addr next,
                     BtreeWritable& parent)
    : AbsLeafW(AllocateNew(), ta, next), m_tree(parent) {}

RootLeafW::RootLeafW(Transaction& ta, Addr addr, BtreeWritable& parent)
    : AbsLeafW(ta, addr), m_tree(parent) {}

void RootLeafW::split(Key key, const model::Value& val, size_t pos) {
  bool overwrite{ false };
  Expects(m_buf);
  auto& buf = *m_buf;
  auto new_me = std::unique_ptr<RootInternalW>(
      new RootInternalW(m_ta, m_addr, 4, std::move(m_buf), m_tree));

  auto right_leaf = std::make_unique<LeafW>(AllocateNew(), m_ta, 0);
  auto left_leaf =
      std::make_unique<LeafW>(AllocateNew(), m_ta, right_leaf->addr());
  auto ins = left_leaf.get();

  left_leaf->m_linked = std::move(m_linked);

  Key mid{ 0 };
  size_t new_val_size = 1 + valueExtraWords(val.type());
  for (size_t i = 1; i < m_top; ++i) {
    auto entry = DskEntry(buf[i]);
    auto extra = entry.extraWords();

    // check if new key needs to be put now
    if (new_val_size > 0 && i >= pos /*&& entry.key.key() > key*/) {
      // check if new key even is the middle key
      if (ins == left_leaf.get() &&
          left_leaf->size() >= m_top - i + new_val_size) {
        mid = key;
        ins = right_leaf.get();
      }
      // existence check happened before calling split, so use Upsert
      ins->insert(key, val, Overwrite::Upsert, new_me.get());
      new_val_size = 0;
    }

    if (ins == left_leaf.get() &&
        left_leaf->size() >= m_top - i + new_val_size) {
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
    if (mid == 0) { mid = key; }
    right_leaf->insert(key, val, Overwrite::Upsert, new_me.get());
  }

  buf[0] = DskInternalHdr().fromSize(4).data;
  buf[1] = left_leaf->addr();
  buf[2] = DskInternalEntry().fromKey(mid);
  buf[3] = right_leaf->addr();
  std::fill(buf.begin() + 4, buf.end(), 0);

  new_me->m_childs.emplace(right_leaf->addr(), std::move(right_leaf));
  new_me->m_childs.emplace(left_leaf->addr(), std::move(left_leaf));
  m_tree.m_root = std::move(new_me);
}

////////////////////////////////////////////////////////////////////////////////
// AbsInternalW

AbsInternalW::AbsInternalW(Transaction& ta, Addr addr) : NodeW(ta, addr) {
  auto hdr = gsl::as_span<DskInternalHdr>(
      (ta.load(toPageNr(addr)))
          ->subspan(toPageOffset(addr) + sizeof(DskBlockHdr),
                    sizeof(DskInternalHdr)))[0];
  if (!hdr.hasMagic()) throw ConsistencyError("Expected internal node");
  if (hdr.size() > k_node_max_words)
    throw ConsistencyError("Invalid fill size in internal node");
  m_top = hdr.size();
}

AbsInternalW::AbsInternalW(AllocateNew, Transaction& ta)
    : NodeW(ta, ta.alloc(k_node_max_bytes).addr) {
  m_buf = std::make_unique<std::array<uint64_t, k_node_max_words>>();
  std::fill(m_buf->begin(), m_buf->end(), 0);
  m_top = 1;
}

AbsInternalW::AbsInternalW(
    Transaction& ta, Addr addr, size_t top,
    std::unique_ptr<std::array<Word, k_node_max_words>> buf)
    : NodeW(ta, addr) {
  m_buf = std::move(buf);
  m_top = top;
}

bool AbsInternalW::insert(Key key, const model::Value& val, Overwrite ow,
                          AbsInternalW* parent) {
  m_parent = parent;
  return searchChild(key).insert(key, val, ow, this);
}

void AbsInternalW::insert(Key key, std::unique_ptr<NodeW> c) {
  if (!m_buf) initFromDisk();

  if (k_node_max_words >= m_top + 2) {
    // can insert
    auto pos = searchInternalPosition(
        key, gsl::span<uint64_t>(*m_buf).subspan(0, m_top));
    auto addr = c->addr();
    shiftBuffer(pos, 2);
    m_buf->at(pos) = DskInternalEntry().fromKey(key);
    m_buf->at(pos + 1) = addr;
    m_childs.emplace(addr, std::move(c));
  } else {
    // no space

    split(key, std::move(c));
  }
}

Writes AbsInternalW::getWrites() const {
  Writes w;
  w.reserve(1 + m_childs.size()); // may be more, but a good guess

  if (m_buf) {
    m_buf->at(0) = DskInternalHdr().fromSize(m_top).data;
    w.push_back({ m_addr + sizeof(DskBlockHdr),
                  gsl::as_bytes(gsl::span<uint64_t>(*m_buf)) });
  }

  for (auto& c : m_childs) {
    auto cw = c.second->getWrites();
    std::move(cw.begin(), cw.end(), std::back_inserter(w));
  }

  return w;
}

NodeW& AbsInternalW::searchChild(Key k) {
  Expects(m_top <= k_node_max_words);

  auto buf =
      (m_buf ? gsl::span<const uint64_t>(*m_buf)
             : gsl::as_span<const uint64_t>(
                   m_ta.load(toPageNr(m_addr))
                       ->subspan(toPageOffset(m_addr) + sizeof(DskBlockHdr),
                                 k_node_max_bytes)))
          .subspan(0, m_top);
  auto pos = searchInternalPosition(k, buf);

  Addr addr = ((pos >= m_top || DskInternalEntry(buf[pos]).key.key() > k)
                   ? buf[pos - 1]
                   : buf[pos + 1]);

  auto lookup = m_childs.find(addr);
  if (lookup != m_childs.end()) {
    return *lookup->second;
  } else {
    auto emplace =
        m_childs.emplace_hint(lookup, addr, openNodeW(m_ta, addr, *this));
    return *emplace->second;
  }
}

void AbsInternalW::appendChild(std::pair<Addr, std::unique_ptr<NodeW>>&& c) {
  m_childs.insert(std::move(c));
}

size_t AbsInternalW::findSize() {
  return DskInternalHdr().fromRaw(m_buf->at(0)).size();
}

////////////////////////////////////////////////////////////////////////////////
// InternalW

void InternalW::append(gsl::span<const uint64_t> raw) {
  Expects(raw.size() + m_top <= k_node_max_words);
  for (auto& c : raw) { m_buf->at(m_top++) = c; }
}

void InternalW::split(Key key, std::unique_ptr<NodeW> c) {
  Expects(m_parent != nullptr);
  Expects(m_buf);
  Expects(m_top <= k_node_max_words);
  Expects(m_top + 2 > k_node_max_words);
  Expects(m_top % 2 == 0);

  auto right = std::make_unique<InternalW>(AllocateNew(), m_ta);

  size_t mid_pos = ((m_top - 2) / 2);
  bool new_here = true;
  if (mid_pos % 2 != 0) {
    if (DskInternalEntry(m_buf->at(mid_pos + 1)).key.key() < key) {
      new_here = false;
      ++mid_pos;
    } else {
      --mid_pos;
    }
  }
  auto mid_key = DskInternalEntry(m_buf->at(mid_pos)).key.key();

  for (size_t i = 1; i < mid_pos; i += 2) {
    auto lookup = m_childs.find(m_buf->at(i));
    if (lookup != m_childs.end()) {
      right->appendChild(std::move(*lookup));
      m_childs.erase(lookup);
    }
  }

  right->append(
      { &m_buf->at(mid_pos + 1), gsl::narrow_cast<int>(m_top - mid_pos - 1) });
  std::fill(m_buf->begin() + mid_pos, m_buf->end(), 0);
  m_top = mid_pos;

  if (new_here)
    this->insert(key, std::move(c));
  else
    right->insert(key, std::move(c));

  m_parent->insert(mid_key, std::move(right));
}

////////////////////////////////////////////////////////////////////////////////
// RootInternalW

RootInternalW::RootInternalW(Transaction& ta, Addr addr, BtreeWritable& parent)
    : AbsInternalW(ta, addr), m_parent(parent) {}

RootInternalW::RootInternalW(
    Transaction& ta, Addr addr, size_t top,
    std::unique_ptr<std::array<uint64_t, k_node_max_words>> buf,
    BtreeWritable& parent)
    : AbsInternalW(ta, addr, top, std::move(buf)), m_parent(parent) {}

void RootInternalW::split(Key key, std::unique_ptr<NodeW> c) {
  Expects(m_buf);
  Expects(m_top <= k_node_max_words);
  Expects(m_top + 2 > k_node_max_words);
  Expects(m_top % 2 == 0);

  auto left = std::make_unique<InternalW>(AllocateNew(), m_ta);
  auto right = std::make_unique<InternalW>(AllocateNew(), m_ta);

  size_t mid_pos = ((m_top - 2) / 2);
  bool new_left = true;
  if (mid_pos % 2 != 0) {
    if (DskInternalEntry(m_buf->at(mid_pos + 1)).key.key() < key) {
      new_left = false;
      ++mid_pos;
    } else {
      --mid_pos;
    }
  }

  auto mid_key = DskInternalEntry(m_buf->at(mid_pos)).key.key();

  left->append({ &m_buf->at(1), gsl::narrow_cast<int>(mid_pos - 1) });
  right->append(
      { &m_buf->at(mid_pos + 1), gsl::narrow_cast<int>(m_top - mid_pos - 1) });

  if (new_left)
    left->insert(key, std::move(c));
  else
    right->insert(key, std::move(c));

  for (auto& c : m_childs) {
    bool found = false;
    for (size_t i = 1; i < mid_pos; ++i) {
      if (c.first == m_buf->at(i)) {
        left->appendChild(std::move(c));
        found = true;
        break;
      }
    }
    if (!found) right->appendChild(std::move(c));
  }
  m_childs.clear();

  m_buf->at(1) = left->addr();
  m_buf->at(2) = DskInternalEntry().fromKey(mid_key);
  m_buf->at(3) = right->addr();
  std::fill(m_buf->begin() + 4, m_buf->end(), 0);
  m_top = 4;

  m_childs.emplace(right->addr(), std::move(right));
  m_childs.emplace(left->addr(), std::move(left));
}

////////////////////////////////////////////////////////////////////////////////
// BtreeReadOnly

BtreeReadOnly::BtreeReadOnly(Database& db, Addr root)
    : m_db(db), m_root(root) {}

model::Object BtreeReadOnly::getObject() {
  model::Object obj;
  openNodeR(m_db, m_root)->getAll(obj);
  return obj;
}

////////////////////////////////////////////////////////////////////////////////
// NodeR

NodeR::NodeR(Database& db, Addr addr, ReadRef page)
    : Node(addr), m_db(db), m_page(std::move(page)) {}

gsl::span<const uint64_t> NodeR::getData() const {
  return gsl::as_span<const uint64_t>(m_page->subspan(
      toPageOffset(m_addr) + sizeof(DskBlockHdr), k_node_max_bytes));
}

////////////////////////////////////////////////////////////////////////////////
// LeafR

LeafR::LeafR(Database& db, Addr addr, ReadRef page)
    : NodeR(db, addr, std::move(page)) {}

void LeafR::getAll(model::Object& obj) {
  auto data = getData();
  auto it = data.begin();
  auto end = data.end();
  auto next = DskLeafHdr().fromDsk(*it++).next();

  while (it != end && *it != 0) { obj.append(readValue(it)); }
  if (next != 0) { openNodeR(m_db, next)->getAll(obj); }
}

std::pair<model::Key, model::PValue>
LeafR::readValue(gsl::span<const uint64_t>::const_iterator& it) {
  auto entry = DskEntry(*it++);
  std::pair<model::Key, model::PValue> ret;
  ret.first = m_db.resolveKey(entry.key.key());

  if (entry.value.type & 0b10000000) {
    // short string
    auto size = (entry.value.type & 0b00111111);
    std::string str;
    str.reserve(size);
    uint64_t word;
    for (size_t i = 0; i < size; ++i) {
      if (i % 8 == 0) word = *it++;
      str.push_back(static_cast<char>(word));
      word >>= 8;
    }
    ret.second = std::make_unique<model::Scalar>(std::move(str));
  } else {
    switch (entry.value.type) {
    case model::ValueType::object:
      ret.second = std::make_unique<model::Object>(
          BtreeReadOnly(m_db, *it++).getObject());
      break;
    case model::ValueType::list:
      throw std::runtime_error("arrays NYI");
      break;
    case model::ValueType::number:
      ret.second = std::make_unique<model::Scalar>(
          *reinterpret_cast<const model::Number*>(&(*it++)));
      break;
    case model::ValueType::string:
      throw std::runtime_error("long string NYI");
      break;
    case model::ValueType::boolean_true:
      ret.second = std::make_unique<model::Scalar>(true);
      break;
    case model::ValueType::boolean_false:
      ret.second = std::make_unique<model::Scalar>(false);
      break;
    case model::ValueType::null:
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
  m_data = view.subspan(0, DskInternalHdr().fromRaw(view[0]).size());
}

void InternalR::getAll(model::Object& obj) {
  // just follow the left most path and let leafs go through
  searchChild(0)->getAll(obj);
}

std::unique_ptr<NodeR> InternalR::searchChild(Key k) {
  auto pos = searchInternalPosition(k, m_data);
  auto addr = (DskInternalEntry(m_data[pos]).key.key() > k ? m_data[pos - 1]
                                                           : m_data[pos + 1]);
  return openNodeR(m_db, addr);
}

////////////////////////////////////////////////////////////////////////////////
} // namespace btree
} // namespace cheesebase

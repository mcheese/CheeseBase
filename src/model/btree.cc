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

std::unique_ptr<NodeW> openNodeW(Transaction& ta, Addr addr) {
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
        return last * 2 + 1;
      else
        first = mid;
    }
  }
  return first * 2 + 1;
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
// Node

Node::Node(Addr addr) : m_addr(addr) {}

Addr Node::addr() const { return m_addr; }

////////////////////////////////////////////////////////////////////////////////
// BtreeWriteable

BtreeWritable::BtreeWritable(Transaction& ta, Addr root) : m_ta(ta) {
  auto page = ta.load(toPageNr(root));
  if (isNodeLeaf(page, root))
    m_root = std::make_unique<RootLeafW>(ta, root, *this);
  else
    m_root = std::make_unique<RootInternalW>(ta, root, *this);
}

BtreeWritable::BtreeWritable(Transaction& ta) : m_ta(ta) {
  m_root = std::make_unique<RootLeafW>(AllocateNew(), ta, 0, *this);
}

Addr BtreeWritable::addr() const { return m_root->addr(); }

bool BtreeWritable::insert(Key key, const model::Value& val, Overwrite o) {
  return m_root->insert(key, val, o, nullptr);
}

bool BtreeWritable::insert(const std::string& key, const model::Value& val,
                           Overwrite ow) {
  return insert(m_ta.key(key), val, ow);
}

bool BtreeWritable::remove(Key key) { return m_root->remove(key, nullptr); }

bool BtreeWritable::remove(const std::string& key) {
  auto k = m_ta.db.getKey(key);
  if (!k) return false;
  return remove(*k);
}

void BtreeWritable::destroy() { m_root->destroy(); }

Writes BtreeWritable::getWrites() const { return m_root->getWrites(); }

////////////////////////////////////////////////////////////////////////////////
// NodeW

NodeW::NodeW(Transaction& ta, Addr addr) : m_ta(ta), Node(addr) {}

size_t NodeW::size() const { return m_top; }

void NodeW::shiftBuffer(size_t pos, int amount) {
  Expects(m_buf);
  Expects(amount + pos > 0 && amount + pos <= m_buf->size());
  Expects(pos <= m_top);
  if (amount == 0) return;

  if (pos < m_top) {
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
  } else {
    Expects(pos == m_top);
    if (amount > 0)
      std::fill(m_buf->begin() + pos, m_buf->begin() + pos + amount, 0);
    else
      std::fill(m_buf->begin() + pos + amount, m_buf->begin() + pos, 0);
  }
  m_top += amount;
}

void NodeW::initFromDisk() {
  if (!m_buf) {
    m_buf = std::make_unique<std::array<uint64_t, k_node_max_words>>();
    copySpan(m_ta.load(toPageNr(m_addr))
                 ->subspan(toPageOffset(m_addr) + sizeof(DskBlockHdr),
                           k_node_max_bytes),
             gsl::as_writeable_bytes(gsl::span<uint64_t>(*m_buf)));
    m_top = findSize();
  }
}

std::pair<gsl::span<const uint64_t>, std::unique_ptr<ReadRef>>
NodeW::getDataView() const {
  if (m_buf) return { gsl::span<const uint64_t>(*m_buf), nullptr };

  auto p = std::make_unique<ReadRef>(m_ta.load(toPageNr(m_addr)));
  return { gsl::as_span<const uint64_t>((*p)->subspan(
               toPageOffset(m_addr) + sizeof(DskBlockHdr), k_node_max_bytes)),
           std::move(p) };
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
    auto cw = c.second->getWrites();
    std::move(cw.begin(), cw.end(), std::back_inserter(w));
  }

  return w;
}

void AbsLeafW::destroy() {
  auto bufv = getDataView();
  auto& buf = bufv.first;

  auto it = buf.begin() + 1;
  while (it < buf.end() && *it != 0) { it += destroyValue(it); }
  m_ta.free(m_addr);
}

size_t AbsLeafW::findSize() {
  size_t i = 1; // first element always is a Addr, skip to value
  while (i < m_buf->size() && m_buf->at(i) != 0) {
    i += entrySize(m_buf->at(i));
  }
  return i;
}

template <typename ConstIt>
size_t AbsLeafW::destroyValue(ConstIt it) {
  auto entry = DskEntry(*it);

  switch (entry.value.type) {
  case model::ValueType::object:
    BtreeWritable(m_ta, *(it + 1)).destroy();
    break;
  case model::ValueType::list:
  case model::ValueType::string:
    throw std::runtime_error("overwriting list or string NIY");
    break;
  }
  return entry.extraWords() + 1;
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
      auto old_entry = DskEntry(m_buf->at(pos));

      auto extra = gsl::narrow_cast<int>(old_entry.extraWords());
      shiftBuffer(pos + extra, extra_words - extra);
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
      auto emp = m_linked.emplace(key, std::make_unique<BtreeWritable>(m_ta));
      for (auto& c : obj) {
        auto r = emp.first->second->insert(m_ta.key(c.first), *c.second,
                                           Overwrite::Insert);
        Expects(r == true);
      }
      m_buf->at(pos + 1) = emp.first->second->addr();
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

void AbsLeafW::insert(gsl::span<const uint64_t> raw) {
  Expects(m_buf);
  Expects(k_node_max_words >= m_top + raw.size());

  for (auto word : raw) { m_buf->at(m_top++) = word; }
}

bool AbsLeafW::remove(Key key, AbsInternalW* parent) {
  m_parent = parent;
  if (!m_buf) initFromDisk();
  Expects(m_top <= k_node_max_words);

  // find position
  auto pos = searchLeafPosition(key, *m_buf);

  // return false if not found
  if (pos >= m_top || keyFromWord(m_buf->at(pos)) != key) return false;

  auto size = destroyValue(m_buf->begin() + pos);
  shiftBuffer(pos + size, -gsl::narrow_cast<int>(size));

  if (m_top < k_leaf_min_words) merge();

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
  size_t mid = 1;
  for (auto new_mid = mid; new_mid < m_top; ++new_mid) {
    if ((new_here && new_mid + new_val_len > m_top / 2 + 1) ||
        (!new_here && new_mid > m_top / 2 + new_val_len + 1)) {
      break;
    } else {
      mid = new_mid;
    }
    auto entry = DskEntry(m_buf->at(new_mid));
    if (entry.key.key() >= key) { new_here = true; }
    new_mid += entry.extraWords();
  }

  auto split_key = DskEntry(m_buf->at(mid)).key.key();
  new_here = key < split_key;

  Ensures(mid < m_top);
  Ensures(mid + (new_here ? new_val_len : 0) > k_leaf_min_words);

  right_leaf->insert(
      gsl::span<uint64_t>(&m_buf->at(mid), gsl::narrow_cast<int>(m_top - mid)));
  std::fill(m_buf->begin() + mid, m_buf->begin() + m_top, 0);
  m_top = mid;
  m_buf->at(0) = DskLeafHdr().fromAddr(right_leaf->addr()).data;

  // the check for existence happened before calling split so just use "Upsert"
  if (new_here)
    insert(key, val, Overwrite::Upsert, m_parent);
  else
    right_leaf->insert(key, val, Overwrite::Upsert, m_parent);

  Ensures(m_top >= k_leaf_min_words);
  Ensures(right_leaf->size() >= k_leaf_min_words);

  m_parent->insert(split_key, std::move(right_leaf));
}

void LeafW::merge() {
  Expects(m_buf);
  Expects(m_top < k_leaf_min_words);
  Expects(m_top > 1); // even if merging, the node should not be empty

  auto first_key = DskEntry(m_buf->at(1)).key.key();
  auto& sibl = dynamic_cast<LeafW&>(m_parent->getSilbling(first_key, m_addr));
  sibl.initFromDisk();

  // required since removeMerged on root may move the m_buf unique_ptr
  auto& buf = *m_buf;
  auto& sibl_buf = *sibl.m_buf;

  auto sibl_key = DskEntry(sibl_buf[1]).key.key();

  if (sibl.size() + m_top - 1 < k_node_max_words) {
    // actually merge them

    if (sibl_key > first_key) {
      // is right sibl, merge here

      insert(gsl::span<uint64_t>(sibl_buf).subspan(1, sibl.size() - 1));
      buf[0] = sibl_buf[0]; // copy next ptr
      for (auto& c : sibl.m_linked) { m_linked.insert(std::move(c)); }
      sibl.m_linked.clear();
      m_ta.free(sibl.addr());

      // do this last because it destroys sibling
      m_parent->removeMerged(sibl_key, sibl.addr());

    } else {
      // is left sibl, merge there

      sibl.insert(gsl::span<uint64_t>(buf).subspan(1, m_top - 1));
      sibl_buf[0] = buf[0]; // copy next ptr
      for (auto& c : m_linked) { sibl.m_linked.insert(std::move(c)); }
      m_linked.clear();
      m_ta.free(m_addr);

      // do this last because it destroys this
      m_parent->removeMerged(first_key, m_addr);
    }

  } else {
    // too big, just steal some values

    if (sibl_key > first_key) {
      // pull lowest
      size_t till = 1;
      while (till < sibl.m_top) {
        auto next = till + entrySize(sibl_buf[till]);
        if (m_top + next - 1 > sibl.m_top - next) break;
        till = next;
      }

      Ensures(till < sibl.m_top);
      Ensures(m_top + till - 1 >= k_leaf_min_words);
      Ensures(m_top + till - 1 <= k_node_max_words);
      Ensures(sibl.m_top - till + 1 >= k_leaf_min_words);

      auto to_pull = gsl::span<uint64_t>(sibl_buf).subspan(1, till - 1);
      insert(to_pull);

      for (auto it = to_pull.begin(); it < to_pull.end(); ++it) {
        auto entry = DskEntry(*it);

        auto lookup = sibl.m_linked.find(entry.key.key());
        if (lookup != sibl.m_linked.end()) {
          m_linked.insert(std::move(*lookup));
          sibl.m_linked.erase(lookup);
        }

        it += entry.extraWords();
      }
      sibl.shiftBuffer(till, 1 - gsl::narrow_cast<int>(till));

      m_parent->updateMerged(DskEntry(sibl_buf[1]).key.key(), sibl.addr());
    } else {
      // pull biggest

      size_t last = 1;
      while (last < sibl.m_top) {
        auto next = last + entrySize(sibl_buf[last]);
        if (m_top + sibl.m_top - next < next) break;
        last = next;
      }

      Ensures(last < sibl.m_top);
      Ensures(m_top + sibl.m_top - last >= k_leaf_min_words);
      Ensures(m_top + sibl.m_top - last <= k_node_max_words);
      Ensures(last >= k_leaf_min_words);

      auto to_pull =
          gsl::span<const uint64_t>(sibl_buf).subspan(last, sibl.m_top - last);
      for (auto it = to_pull.begin(); it < to_pull.end(); ++it) {
        auto entry = DskEntry(*it);

        auto lookup = sibl.m_linked.find(entry.key.key());
        if (lookup != sibl.m_linked.end()) {
          m_linked.insert(std::move(*lookup));
          sibl.m_linked.erase(lookup);
        }

        it += entry.extraWords();
      }

      shiftBuffer(1, gsl::narrow_cast<int>(to_pull.size()));
      copySpan(to_pull, gsl::span<uint64_t>(buf).subspan(1, to_pull.size()));
      std::fill(sibl_buf.begin() + last, sibl_buf.begin() + sibl.m_top, 0);
      sibl.m_top = last;
      m_parent->updateMerged(DskEntry(buf[1]).key.key(), m_addr);
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
// RootLeafW

RootLeafW::RootLeafW(AllocateNew, Transaction& ta, Addr next,
                     BtreeWritable& parent)
    : AbsLeafW(AllocateNew(), ta, next), m_tree(parent) {}

RootLeafW::RootLeafW(Transaction& ta, Addr addr, BtreeWritable& parent)
    : AbsLeafW(ta, addr), m_tree(parent) {}

RootLeafW::RootLeafW(LeafW&& o, Addr a, BtreeWritable& parent)
    : AbsLeafW(o.m_ta, a), m_tree(parent) {
  m_buf = std::move(o.m_buf);
  m_linked = std::move(o.m_linked);
  m_top = o.m_top;
  m_ta.free(o.m_addr);
}

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
  Ensures(left_leaf->size() >= k_leaf_min_words);
  Ensures(right_leaf->size() >= k_leaf_min_words);

  buf[0] = DskInternalHdr().fromSize(4).data;
  buf[1] = left_leaf->addr();
  buf[2] = DskInternalEntry().fromKey(mid);
  buf[3] = right_leaf->addr();
  std::fill(buf.begin() + 4, buf.end(), 0);

  new_me->m_childs.emplace(right_leaf->addr(), std::move(right_leaf));
  new_me->m_childs.emplace(left_leaf->addr(), std::move(left_leaf));
  m_tree.m_root = std::move(new_me);
}

void RootLeafW::merge() {
  // root leaf merging is not existent
  // this node type may be empty
  return; // NOP
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
    shiftBuffer(pos + 1, 2);
    m_buf->at(pos + 1) = DskInternalEntry().fromKey(key);
    m_buf->at(pos + 2) = addr;
    m_childs.emplace(addr, std::move(c));
  } else {
    // no space

    split(key, std::move(c));
  }
}

bool AbsInternalW::remove(Key key, AbsInternalW* parent) {
  m_parent = parent;
  return searchChild(key).remove(key, this);
}

bool remove(Key key) { return false; }

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

  auto bufview = getDataView();
  auto buf = bufview.first.subspan(0, m_top);
  auto pos = searchInternalPosition(k, buf);

  Addr addr = buf[pos];

  auto lookup = m_childs.find(addr);
  if (lookup != m_childs.end()) {
    return *lookup->second;
  } else {
    auto emplace = m_childs.emplace_hint(lookup, addr, openNodeW(m_ta, addr));
    return *emplace->second;
  }
}

void AbsInternalW::destroy() {
  auto bufr = getDataView();
  auto buf = bufr.first.subspan(0, m_top);

  for (auto it = buf.begin() + 1; it < buf.end(); it += 2) {
    openNodeW(m_ta, *it)->destroy();
  }

  m_ta.free(m_addr);
}

void AbsInternalW::appendChild(std::pair<Addr, std::unique_ptr<NodeW>>&& c) {
  m_childs.insert(std::move(c));
}

NodeW& AbsInternalW::getSilbling(Key key, Addr addr) {
  Expects(m_top >= 3); // not empty
  Expects(m_top <= k_node_max_words);
  auto bufp = getDataView();
  auto& buf = bufp.first;

  auto pos = searchInternalPosition(key, buf.subspan(0, m_top));
  Ensures(pos > 0 && pos < m_top);
  Ensures(buf[pos] == addr);
  Addr sibl = buf[(pos == 1 ? pos + 2 : pos - 2)];

  auto lookup = m_childs.find(sibl);
  if (lookup != m_childs.end()) {
    return *lookup->second;
  } else {
    return *m_childs.emplace_hint(lookup, sibl, openNodeW(m_ta, sibl))->second;
  }
}

Key AbsInternalW::removeMerged(Key key, Addr addr) {
  if (!m_buf) initFromDisk();

  auto pos = searchInternalPosition(
      key, gsl::span<uint64_t>(*m_buf).subspan(0, m_top));

  if (m_buf->at(pos) == addr && pos > 1) {
    Key removed = DskInternalEntry(m_buf->at(pos - 1)).key.key();
    shiftBuffer(pos + 1, -2);

    m_childs.erase(addr);

    if (m_top < k_internal_min_words) merge();
    return removed;
  }
  throw ConsistencyError(
      "Try to remove invalid key from internal node (merge)");
}

Key AbsInternalW::updateMerged(Key key, Addr addr) {
  if (!m_buf) initFromDisk();

  auto pos = searchInternalPosition(
      key, gsl::span<uint64_t>(*m_buf).subspan(0, m_top));

  if (pos > 1 && m_buf->at(pos) == addr) {
    Key old = DskInternalEntry(m_buf->at(pos - 1)).key.key();
    m_buf->at(pos - 1) = DskInternalEntry().fromKey(key);
    return old;
  } else if (pos + 2 < m_top && m_buf->at(pos + 2) == addr) {
    Key old = DskInternalEntry(m_buf->at(pos + 1)).key.key();
    m_buf->at(pos + 1) = DskInternalEntry().fromKey(key);
    return old;
  }

  throw ConsistencyError("Try to update invalid key in internal node (merge)");
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

  size_t mid_pos = ((m_top + 1) / 2);
  if (mid_pos % 2 != 0) {
    if (DskInternalEntry(m_buf->at(mid_pos + 1)).key.key() < key) {
      ++mid_pos;
    } else {
      --mid_pos;
    }
  }

  auto mid_key = DskInternalEntry(m_buf->at(mid_pos)).key.key();
  bool new_here = mid_key > key;

  for (size_t i = mid_pos + 1; i < m_top; i += 2) {
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

  Ensures(m_top >= k_internal_min_words);
  Ensures(right->size() >= k_internal_min_words);

  m_parent->insert(mid_key, std::move(right));
}

void InternalW::merge() {
  Expects(m_buf);
  Expects(m_top < k_internal_min_words);
  Expects(m_top > 3);

  auto first_key = DskInternalEntry(m_buf->at(2)).key.key();
  auto& sibl =
      dynamic_cast<InternalW&>(m_parent->getSilbling(first_key, m_addr));
  sibl.initFromDisk();
  Expects(sibl.size() > 3);

  // this is required since removeMerged() on the root node may trigger a move
  // of the m_buf unique_ptr.
  auto& buf = *m_buf;
  auto& sibl_buf = *sibl.m_buf;

  auto sibl_key = DskInternalEntry(sibl_buf[2]).key.key();

  if (m_top + sibl.size() <= k_node_max_words) {
    // merge

    if (first_key < sibl_key) {
      // is right sibling, merge into here

      // if we pull parent key now sibl would be deleted
      auto parent_key_insert_pos = m_top++;

      append(gsl::span<uint64_t>(sibl_buf).subspan(1, sibl.size() - 1));
      for (auto& c : sibl.m_childs) appendChild(std::move(c));

      m_ta.free(sibl.addr());
      buf[parent_key_insert_pos] = DskInternalEntry().fromKey(
          m_parent->removeMerged(sibl_key, sibl.addr()));

    } else {
      // is left sibling, merge into it

      auto parent_key_insert_pos = sibl.m_top++;

      sibl.append(gsl::span<uint64_t>(buf).subspan(1, size() - 1));
      for (auto& c : m_childs) sibl.appendChild(std::move(c));

      m_ta.free(addr());
      sibl_buf[parent_key_insert_pos] =
          DskInternalEntry().fromKey(m_parent->removeMerged(first_key, addr()));
    }

  } else {
    // pull entries

    auto avg = (size() + sibl.size()) / 2;
    Expects(size() < avg);
    auto to_pull = avg - size();
    if (to_pull % 2 == 1) to_pull--;

    if (first_key > sibl_key) {
      // is left sibling, pull biggest

      //
      //           [ - .X. - ]
      //              /   \
      //             /     \
      // [h.#.#.#.A.B.]   [h.C.        ]
      //          ^^^^
      //
      //           [ - .A. - ]
      //              /   \
      //             /     \
      // [h.#.#.#.    ]   [h.B.X.C.    ]
      //

      auto beg = sibl_buf.begin() + (sibl.size() - to_pull);
      auto end = beg + to_pull;
      auto it = beg;

      // make space
      shiftBuffer(1, gsl::narrow_cast<int>(to_pull));

      // first to pull goes into header
      auto split_key = DskInternalEntry(*it++).key.key();
      buf[to_pull] =
          DskInternalEntry().fromKey(m_parent->updateMerged(split_key, addr()));

      auto insert = buf.begin() + 1;
      while (it < end) {
        auto lookup = sibl.m_childs.find(*it);
        if (lookup != sibl.m_childs.end()) {
          m_childs.insert(std::move(*lookup));
          sibl.m_childs.erase(lookup);
        }
        *insert++ = *it++;               // Addr
        if (it < end) *insert++ = *it++; // Key
      }

      std::fill(beg, end, 0);
      sibl.m_top -= to_pull;

    } else {
      // is right sibling, pull smallest

      auto first = sibl_buf.begin() + 1;
      auto last = first + to_pull - 1;
      auto it = first;

      auto split_key = DskInternalEntry(*last--).key.key();
      buf[m_top++] = DskInternalEntry().fromKey(
          m_parent->updateMerged(split_key, sibl.addr()));

      while (it <= last) {
        auto lookup = sibl.m_childs.find(*it);
        if (lookup != sibl.m_childs.end()) {
          m_childs.insert(std::move(*lookup));
          sibl.m_childs.erase(lookup);
        }
        buf[m_top++] = *it++;                 // Addr
        if (it <= last) buf[m_top++] = *it++; // Key
      }

      sibl.shiftBuffer(1 + to_pull, -gsl::narrow_cast<int>(to_pull));
    }
  }
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
  } else {
    if (DskInternalEntry(m_buf->at(mid_pos)).key.key() < key) {
      new_left = false;
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

  Ensures(left->size() >= k_internal_min_words);
  Ensures(right->size() >= k_internal_min_words);

  for (auto& c : m_childs) {
    bool found = false;
    for (size_t i = 1; i < mid_pos; i += 2) {
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

void RootInternalW::merge() {
  // this node may be smaller than min size
  if (m_top >= 4) return;

  // At 1 child left it transforms this child into a RootLeafW and replaces the
  // tree root with it.

  Expects(m_top == 2);
  Expects(m_childs.size() == 1);

  auto childp = std::move(m_childs.begin()->second);
  m_childs.clear();

  auto child_internal = dynamic_cast<InternalW*>(childp.get());
  if (child_internal != nullptr) {
    // pull content into this node

    m_buf = std::move(child_internal->m_buf);
    m_top = child_internal->m_top;
    m_childs = std::move(child_internal->m_childs);
    m_ta.free(child_internal->addr());

    return;
  }

  auto child_leaf = dynamic_cast<LeafW*>(childp.get());
  if (child_leaf != nullptr) {
    // tree becomes single RootLeafW

    auto new_me = std::unique_ptr<RootLeafW>(
        new RootLeafW(std::move(*child_leaf), m_addr, m_parent));
    m_parent.m_root = std::move(new_me);

    return;
  }

  // There should be now way this can happen.
  throw ConsistencyError("Invalid merge below root node");
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

std::unique_ptr<model::Value> BtreeReadOnly::getValue(const std::string& key) {
  auto k = m_db.getKey(key);
  if (!k) return nullptr;
  return openNodeR(m_db, m_root)->getValue(*k);
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

LeafR::LeafR(Database& db, Addr addr)
    : NodeR(db, addr, db.loadPage(toPageNr(addr))) {}

void LeafR::getAll(model::Object& obj) {
  auto next = getAllInLeaf(obj);
  while (next != 0) { next = LeafR(m_db, next).getAllInLeaf(obj); }
}

Addr LeafR::getAllInLeaf(model::Object& obj) {
  auto data = getData();
  auto it = data.begin();
  auto end = data.end();
  auto next = DskLeafHdr().fromDsk(*it++).next();

  while (it != end && *it != 0) { obj.append(readValue(it)); }

  return next;
}

std::unique_ptr<model::Value> LeafR::getValue(Key key) {
  auto view = getData();
  auto pos = searchLeafPosition(key, view);
  if (pos >= static_cast<size_t>(view.size()) || view[pos] == 0) return nullptr;
  if (DskEntry(view[pos]).key.key() != key) return nullptr;
  auto it = view.begin() + pos;
  return readValue(it).second;
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

std::unique_ptr<model::Value> InternalR::getValue(Key key) {
  return searchChild(key)->getValue(key);
}

std::unique_ptr<NodeR> InternalR::searchChild(Key k) {
  auto pos = searchInternalPosition(k, m_data);
  auto addr = m_data[pos];
  return openNodeR(m_db, addr);
}

////////////////////////////////////////////////////////////////////////////////
} // namespace btree
} // namespace cheesebase

// Licensed under the Apache License 2.0 (see LICENSE file).

#include "keycache.h"
#include "murmurhash3.h"

namespace cheesebase {

DskKey KeyTransaction::getKey(const std::string& str) {
  if (str.size() > 256) throw KeyCacheError("key too long");
  Expects(m_cache != nullptr);
  auto h = hashString(str);

  // check local cache
  if (m_local.count(h) > 0) {
    for (auto& p : m_local[h]) {
      if (p.second.first == str) return DskKey(h, p.first);
    }
  }

  ShLock<UgMutex> lck;
  if (!m_ug_lck.owns_lock()) lck = ShLock<UgMutex>(m_cache->m_mtx);
  // check global cache
  auto lookup = m_cache->m_cache.find(h);
  size_t i = 0;
  if (lookup != m_cache->m_cache.end()) {
    for (; i < lookup->second.size(); ++i) {
      if (lookup->second[i] == str) {
        return DskKey(h, gsl::narrow<KeyIndex>(i));
      }
    }
  }

  // needs to be inserted
  if (!m_ug_lck.owns_lock()) {
    lck.unlock();
    m_ug_lck = UgLock<UgMutex>(m_cache->m_mtx);
  }
  Ensures(m_ug_lck.owns_lock());
  // nobody else can write now, need to check if str is there now
  lookup = m_cache->m_cache.find(h);
  if (lookup != m_cache->m_cache.end()) {
    for (; i < lookup->second.size(); ++i) {
      if (lookup->second[i] == str) return DskKey(h, gsl::narrow<KeyIndex>(i));
    }
  }

  if (i > std::numeric_limits<KeyIndex>::max())
    throw KeyCacheError("can not store key name");
  auto idx = gsl::narrow_cast<KeyIndex>(i);

  m_local[h].emplace(idx, std::pair<std::string, DskKeyCacheSize>{
                              str, gsl::narrow<DskKeyCacheSize>(str.size()) });

  return DskKey(h, idx);
}

void KeyTransaction::upgrade() {
  Expects(m_cache != nullptr);
  if (!m_ug_lck.owns_lock()) { m_ug_lck = UgLock<UgMutex>(m_cache->m_mtx); }
}

Writes KeyTransaction::commit() {
  Writes writes;
  Expects(m_cache != nullptr);
  if (m_local.empty()) return writes;
  Expects(m_ug_lck.owns_lock());

  auto block = m_cache->m_cur_block;
  auto off = m_cache->m_offset;

  m_ex_lck = std::move(m_ug_lck);

  for (const auto& v : m_local) {
    auto& main_vec = m_cache->m_cache[v.first];

    for (const auto& s : v.second) {
      auto idx = s.first;
      auto& str = s.second.first;
      auto& len = s.second.second;
      Expects(str.size() == len);
      Expects(idx == main_vec.size());
      main_vec.push_back(str);

      if (block.size - off < sizeof(DskKeyCacheSize) + len) {
        block = m_alloc->allocExtension(block.addr,
                                        k_page_size - sizeof(DskBlockHdr));
        off = sizeof(DskBlockHdr);
      }
      Ensures(block.size - off >= sizeof(DskKeyCacheSize) + len);

      writes.push_back(
          { block.addr + off, gsl::as_bytes(gsl::span<const DskKeyCacheSize>(len)) });
      off += sizeof(DskKeyCacheSize);
      writes.push_back({ block.addr + off, gsl::as_bytes(gsl::span<const char>(
                                               str.data(), len)) });
      off += len;
    }
  }

  return writes;
}

void KeyTransaction::end() {
  m_local.clear();
  m_cache = nullptr;
  m_alloc = nullptr;
  if (m_ug_lck.owns_lock()) m_ug_lck.unlock();
  if (m_ex_lck.owns_lock()) m_ex_lck.unlock();
}

std::string KeyCache::getString(const DskKey& key) {
  ShLock<UgMutex> lck{ m_mtx };
  auto lookup = m_cache.find(key.hash);
  if (lookup == m_cache.end() || lookup->second.size() <= key.index)
    throw KeyCacheError("key not known");
  return lookup->second[key.index];
}

boost::optional<DskKey> KeyCache::getKey(const std::string & str) {
  ShLock<UgMutex> lck{ m_mtx };

  auto h = hashString(str);
  auto lookup = m_cache.find(h);
  size_t i = 0;
  if (lookup != m_cache.end()) {
    for (; i < lookup->second.size(); ++i) {
      if (lookup->second[i] == str) {
        Expects(i <= std::numeric_limits<KeyIndex>::max());
        return DskKey(h, gsl::narrow_cast<KeyIndex>(i));
      }
    }
  }

  return boost::none;
}

KeyTransaction KeyCache::startTransaction(AllocTransaction& alloc) {
  return KeyTransaction(this, &alloc, ShLock<UgMutex>(m_mtx));
}
}

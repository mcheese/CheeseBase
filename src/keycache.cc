// Licensed under the Apache License 2.0 (see LICENSE file).

#include "keycache.h"
#include "murmurhash3.h"

namespace cheesebase {

Key KeyTransaction::getKey(const std::string& str) {
  if (str.size() > 256) throw KeyCacheError("key too long");
  Expects(cache_ != nullptr);
  auto hash = hashString(str);

  // check local cache
  if (local_.count(hash) > 0) {
    for (auto& p : local_[hash]) {
      if (p.second.first == str) return StringKey(hash, p.first + 1).key();
    }
  }

  ShLock<UgMutex> lck;
  if (!ug_lck_.owns_lock()) lck = ShLock<UgMutex>(cache_->mtx_);
  // check global cache
  auto lookup = cache_->cache_.find(hash);
  size_t i = 0;
  if (lookup != cache_->cache_.end()) {
    for (; i < lookup->second.size(); ++i) {
      if (lookup->second[i] == str) {
        return StringKey(hash, gsl::narrow<StringKey::Index>(i + 1)).key();
      }
    }
  }

  // needs to be inserted
  if (!ug_lck_.owns_lock()) {
    lck.unlock();
    ug_lck_ = UgLock<UgMutex>(cache_->mtx_);
  }
  Ensures(ug_lck_.owns_lock());
  // nobody else can write now, need to check if str is there now
  lookup = cache_->cache_.find(hash);
  if (lookup != cache_->cache_.end()) {
    for (; i < lookup->second.size(); ++i) {
      if (lookup->second[i] == str)
        return StringKey(hash, gsl::narrow<StringKey::Index>(i) + 1).key();
    }
  }

  if (i > std::numeric_limits<StringKey::Index>::max())
    throw KeyCacheError("can not store key name");
  auto idx = gsl::narrow_cast<StringKey::Index>(i);

  local_[hash].emplace(idx, std::pair<std::string, DskKeyCacheSize>{
                             str, gsl::narrow<DskKeyCacheSize>(str.size()) });

  return StringKey(hash, idx + 1).key();
}

void KeyTransaction::upgrade() {
  Expects(cache_ != nullptr);
  if (!ug_lck_.owns_lock()) {
    ug_lck_ = UgLock<UgMutex>(cache_->mtx_);
  }
}

Writes KeyTransaction::commit() {
  Writes writes;
  Expects(cache_ != nullptr);
  if (local_.empty()) return writes;
  Expects(ug_lck_.owns_lock());

  auto block = cache_->cur_block_;
  auto off = cache_->offset_;

  ex_lck_ = std::move(ug_lck_);

  for (const auto& v : local_) {
    auto& main_vec = cache_->cache_[v.first];

    for (const auto& s : v.second) {
      auto idx = s.first;
      auto& str = s.second.first;
      auto& len = s.second.second;
      Expects(str.size() == len);
      Expects(idx == main_vec.size());
      main_vec.push_back(str);

      if (block.size < off + sizeof(DskKeyCacheSize) + len) {
        if (block.size >= off + sizeof(DskKeyCacheSize)) {
          writes.push_back(
              { Addr(block.addr.value + off),
                gsl::as_bytes<const DskKeyCacheSize>({ s_terminator }) });
        }

        block = alloc_->allocExtension(block.addr,
                                       k_page_size - sizeof(DskBlockHdr));
        off = sizeof(DskBlockHdr);
      }
      Ensures(block.size >= off + sizeof(DskKeyCacheSize) + len);

      writes.push_back({ Addr(block.addr.value + off),
                         gsl::as_bytes<const DskKeyCacheSize>({ len }) });
      off += sizeof(DskKeyCacheSize);
      writes.push_back({ Addr(block.addr.value + off),
                         gsl::as_bytes<const char>({ str.data(), len }) });
      off += len;
    }
  }

  if (block.size >= off + sizeof(DskKeyCacheSize)) {
    writes.push_back(
        { Addr(block.addr.value + off),
          gsl::as_bytes<const DskKeyCacheSize>({ s_terminator }) });
  }

  cache_->cur_block_ = block;
  cache_->offset_ = off;
  return writes;
}

void KeyTransaction::end() {
  local_.clear();
  cache_ = nullptr;
  alloc_ = nullptr;
  if (ug_lck_.owns_lock()) ug_lck_.unlock();
  if (ex_lck_.owns_lock()) ex_lck_.unlock();
}

KeyCache::KeyCache(Block first_block, Storage& store)
    : store_(store), cur_block_(first_block) {
  // empty string is always known
  cache_[hashString("")].emplace_back("");

  // go through all linked blocks adding every string
  auto next = first_block.addr;
  while (!next.isNull()) {
    cur_block_.addr = next;
    auto page = store_.loadPage(cur_block_.addr.pageNr());
    auto block = page->subspan(cur_block_.addr.pageOffset());
    auto hdr = gsl::as_span<DskBlockHdr>(block)[0];
    cur_block_.size = toBlockSize(hdr.type());
    block = block.subspan(0, cur_block_.size);
    next = hdr.next();
    offset_ = sizeof(DskBlockHdr);

    while (offset_ + sizeof(DskKeyCacheSize) <= cur_block_.size) {
      auto size = gsl::as_span<DskKeyCacheSize>(
          block.subspan(offset_, sizeof(DskKeyCacheSize)))[0];
      if (size == 0) break;
      offset_ += sizeof(DskKeyCacheSize);
      auto str_span = gsl::as_span<const char>(block.subspan(offset_, size));
      std::string str{ str_span.begin(), str_span.end() };
      offset_ += size;
      cache_[hashString(str)].emplace_back(std::move(str));
    }
  }
}

std::string KeyCache::getString(Key k) {
  ShLock<UgMutex> lck{ mtx_ };
  StringKey key{ k };
  Expects(key.index > 0);
  auto lookup = cache_.find(key.hash);
  if (lookup == cache_.end() || lookup->second.size() < key.index)
    throw KeyCacheError("key not known");
  return lookup->second[key.index - 1];
}

boost::optional<Key> KeyCache::getKey(const std::string& str) {
  ShLock<UgMutex> lck{ mtx_ };

  auto h = hashString(str);
  auto lookup = cache_.find(h);
  size_t i = 0;
  if (lookup != cache_.end()) {
    for (; i < lookup->second.size(); ++i) {
      if (lookup->second[i] == str) {
        Expects(i <= std::numeric_limits<StringKey::Index>::max());
        return StringKey(h, gsl::narrow_cast<StringKey::Index>(i + 1)).key();
      }
    }
  }

  return boost::none;
}

KeyTransaction KeyCache::startTransaction(AllocTransaction& alloc) {
  return KeyTransaction(this, &alloc);
}

} // namespace cheesebase

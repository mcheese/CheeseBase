// Licensed under the Apache License 2.0 (see LICENSE file).

// Convert between key strings and representation. Store cached keys on disk.

#pragma once

#include "common.h"
#include "sync.h"
#include "storage.h"
#include "allocator.h"
#include <unordered_map>
#include <boost/container/flat_map.hpp>
#include <boost/optional.hpp>

namespace cheesebase {

using DskKeyCacheSize = uint16_t;

class KeyCache;

struct StringKey {
  using Hash = uint32_t;
  using Index = uint16_t;

  StringKey() = default;
  constexpr StringKey(Hash hsh, Index idx) : index{ idx }, hash{ hsh } {}
  StringKey(Key key)
      : index{ static_cast<Index>(key.value >> 32) }
      , hash{ static_cast<Hash>(key.value) } {}

  Key key() const noexcept {
    return Key(static_cast<uint64_t>(hash) +
               (static_cast<uint64_t>(index) << 32));
  }

  bool operator==(const StringKey& o) const noexcept {
    return index == o.index && hash == o.hash;
  }

  bool operator!=(const StringKey& o) const noexcept {
    return index != o.index && hash != o.hash;
  }

  bool operator<(const StringKey& o) const noexcept {
    return index < o.index || (index == o.index && hash < o.hash);
  }

  bool operator>(const StringKey& o) const noexcept {
    return index > o.index || (index == o.index && hash > o.hash);
  }

  Index index;
  Hash hash;
};

static const DskKeyCacheSize s_terminator{ 0 };

class KeyTransaction {
  friend KeyCache;

public:
  KeyTransaction() : cache_(nullptr), alloc_(nullptr) {}
  MOVE_ONLY(KeyTransaction)

  Key getKey(const std::string& str);
  void upgrade();

  Writes commit();
  void end();

private:
  KeyTransaction(gsl::not_null<KeyCache*> cache,
                 gsl::not_null<AllocTransaction*> alloc)
      : cache_(cache), alloc_(alloc) {}

  KeyCache* cache_;
  AllocTransaction* alloc_;
  UgLock<UgMutex> ug_lck_;
  ExLock<UgMutex> ex_lck_;
  boost::container::flat_map<
      StringKey::Hash,
      boost::container::flat_map<
          StringKey::Index, std::pair<std::string, DskKeyCacheSize>>> local_;
};

class KeyCache {
  friend KeyTransaction;

public:
  KeyCache(Block first_block, Storage& store);

  // Get string version of internal key representation. Throws on failure since
  // every key in the database should be successfully found here.
  std::string getString(Key key);

  // Try to get key representation of string. Returns boost::none if the string
  // is not known. Use your transaction if you may just have inserted it.
  boost::optional<Key> getKey(const std::string& str);

  // Start KeyTransaction, allowing you to insert new strings. It is required
  // to call commit() and add the returned Writes to the disk transaction.
  // If the transaction goes out of scope without calling commit() all changes
  // are discarded and added strings invalid.
  KeyTransaction startTransaction(AllocTransaction& alloc);

private:
  Storage& store_;
  std::unordered_map<StringKey::Hash, std::vector<std::string>> cache_;
  UgMutex mtx_;
  Block cur_block_;
  uint64_t offset_{ sizeof(DskBlockHdr) };
};

} // namespace cheesebase

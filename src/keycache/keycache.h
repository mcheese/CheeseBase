// Licensed under the Apache License 2.0 (see LICENSE file).

// Convert between key strings and representation. Store cached keys on disk.

#pragma once

#include "common/common.h"
#include "common/structs.h"
#include "common/sync.h"
#include "storage/storage.h"
#include "blockman/allocator.h"
#include <unordered_map>
#include <boost/container/flat_map.hpp>
#include <boost/optional.hpp>

namespace cheesebase {

DEF_EXCEPTION(KeyCacheError);

class KeyCache;
using KeyHash = decltype(DskKey::hash);
using KeyIndex = decltype(DskKey::index);

static const DskKeyCacheSize s_terminator{0};

class KeyTransaction {
  friend KeyCache;
public:
  KeyTransaction() : m_cache(nullptr), m_alloc(nullptr) {}
  MOVE_ONLY(KeyTransaction);

  Key getKey(const std::string& str);
  void upgrade();

  Writes commit();
  void end();

private:
  KeyTransaction(gsl::not_null<KeyCache*> cache,
                 gsl::not_null<AllocTransaction*> alloc, ShLock<UgMutex> lck)
      : m_cache(cache), m_alloc(alloc) {}

  KeyCache* m_cache;
  AllocTransaction* m_alloc;
  UgLock<UgMutex> m_ug_lck;
  ExLock<UgMutex> m_ex_lck;
  boost::container::flat_map < KeyHash,
      boost::container::flat_map<
          KeyIndex, std::pair<std::string, DskKeyCacheSize>>> m_local;
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
  Storage& m_store;
  std::unordered_map<KeyHash, std::vector<std::string>> m_cache;
  UgMutex m_mtx;
  Block m_cur_block;
  Offset m_offset{ sizeof(DskBlockHdr) };
};

} // namespace cheesebase

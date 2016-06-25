// Licensed under the Apache License 2.0 (see LICENSE file).

#include "core.h"

#include "allocator.h"
#include "keycache.h"
#include "storage.h"
#include "seri/object.h"

#include <boost/filesystem.hpp>

namespace cheesebase {

Database::Database(const std::string& file)
    : lock_pool_{ std::make_unique<BlockLockPool>() } {
  DskDatabaseHdr hdr;

  if (boost::filesystem::exists(file)) {
    store_ = std::make_unique<Storage>(file, OpenMode::open_existing);
    auto page = store_->loadPage(PageNr(0));
    hdr = gsl::as_span<const DskDatabaseHdr>(
        page->subspan(0, ssizeof<DskDatabaseHdr>()))[0];

    auto kc_blk =
        gsl::as_span<KeyNext>(page->subspan(ssizeof<DskDatabaseHdr>()))[0];
    kc_blk.check();
    if (hdr.magic != k_magic || hdr.free_alloc_pg.value % k_page_size != 0 ||
        hdr.free_alloc_t1.value % (k_page_size / 2) != 0 ||
        hdr.free_alloc_t2.value % (k_page_size / 4) != 0 ||
        hdr.free_alloc_t3.value % (k_page_size / 8) != 0 ||
        hdr.free_alloc_t4.value % (k_page_size / 16) != 0 ||
        hdr.end_of_file.value % k_page_size != 0 ||
        hdr.end_of_file.value < k_page_size)
      throw DatabaseError("Invalid database header");

    alloc_ = std::make_unique<Allocator>(hdr, *store_);
    keycache_ = std::make_unique<KeyCache>(
        Block{ Addr(sizeof(DskDatabaseHdr)),
               k_page_size - sizeof(DskDatabaseHdr) },
        *store_);

  } else {
    store_ = std::make_unique<Storage>(file, OpenMode::create_new);
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic = k_magic;
    hdr.end_of_file = Addr(k_page_size);

    // manually create first block of KeyCache
    DskKeyCacheSize cache_term = 0;
    store_->storeWrite(
        Writes{ { Addr(0), gsl::as_bytes<DskDatabaseHdr>({ hdr }) },
                { Addr(sizeof(DskDatabaseHdr)), KeyNext(Addr(0)).data() },
                { Addr(sizeof(DskDatabaseHdr) + sizeof(KeyNext)), 0 } });
    alloc_ = std::make_unique<Allocator>(hdr, *store_);
    keycache_ = std::make_unique<KeyCache>(
        Block{ Addr(sizeof(DskDatabaseHdr)),
               k_page_size - sizeof(DskDatabaseHdr) },
        *store_);

    auto ta = startTransaction();
    disk::ObjectW tree(ta);
    Expects(tree.addr() == kRoot);
    ta.commit(tree.getWrites());
  }

  auto ta = startTransaction();
  disk::ObjectW(ta, kRoot);
}

Transaction Database::startTransaction() { return Transaction(*this); };

ReadRef<k_page_size> Database::loadPage(PageNr p) {
  return store_->loadPage(p);
}

std::string Database::resolveKey(Key k) const {
  return keycache_->getString(k);
}

boost::optional<Key> Database::getKey(const std::string& k) const {
  return keycache_->getKey(k);
}

BlockLockW Database::getLockW(Addr addr) { return lock_pool_->getLockW(addr); }
BlockLockR Database::getLockR(Addr addr) { return lock_pool_->getLockR(addr); }
BlockLockW Transaction::getLockW(Addr addr) { return db_.getLockW(addr); }
BlockLockR Transaction::getLockR(Addr addr) { return db_.getLockR(addr); }

ReadRef<k_page_size> Transaction::load(PageNr p) {
  return storage_.loadPage(p);
};

Block Transaction::alloc(size_t s) { return alloc_.alloc(s); };

void Transaction::free(Addr a, size_t s) { return alloc_.free(a, s); }

Key Transaction::key(const std::string& s) { return kcache_.getKey(s); }

Transaction::Transaction(Database& db)
    : db_(db)
    , storage_(*db.store_)
    , alloc_(db.alloc_->startTransaction())
    , kcache_(db.keycache_->startTransaction(alloc_)) {}

void Transaction::commit(Writes w) {
  // kcache commit does allocation, so be sure to commit it before allocator
  auto w1 = kcache_.commit();
  auto w2 = alloc_.commit();
  w.reserve(w.size() + w1.size() + w2.size());
  std::move(w1.begin(), w1.end(), std::back_inserter(w));
  std::move(w2.begin(), w2.end(), std::back_inserter(w));
  storage_.storeWrite(std::move(w));
}

} // namespace cheesebase

// Licensed under the Apache License 2.0 (see LICENSE file).

#include "core.h"

#include "allocator.h"
#include "keycache.h"
#include "storage.h"
#include "disk_object.h"

#include <boost/filesystem.hpp>

namespace cheesebase {

Database::Database(const std::string& file) {
  DskDatabaseHdr hdr;

  if (boost::filesystem::exists(file)) {
    store_ = std::make_unique<Storage>(file, OpenMode::open_existing);
    auto page = store_->loadPage(0);
    hdr = gsl::as_span<const DskDatabaseHdr>(
        page->subspan(0, sizeof(DskDatabaseHdr)))[0];

    auto kc_blk = gsl::as_span<DskBlockHdr>(page->subspan(k_page_size / 2))[0];
    if (hdr.magic != k_magic || hdr.free_alloc_pg % k_page_size != 0 ||
        hdr.free_alloc_t1 % (k_page_size / 2) != 0 ||
        hdr.free_alloc_t2 % (k_page_size / 4) != 0 ||
        hdr.free_alloc_t3 % (k_page_size / 8) != 0 ||
        hdr.free_alloc_t4 % (k_page_size / 16) != 0 ||
        hdr.end_of_file % k_page_size != 0 || hdr.end_of_file < k_page_size ||
        kc_blk.type() != BlockType::t1)
      throw DatabaseError("Invalid database header");

    alloc_ = std::make_unique<Allocator>(hdr, *store_);
    keycache_ = std::make_unique<KeyCache>(
        Block{ k_page_size / 2, k_page_size / 2 }, *store_);

  } else {
    store_ = std::make_unique<Storage>(file, OpenMode::create_new);
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic = k_magic;
    hdr.end_of_file = k_page_size;

    // manually create first block of KeyCache
    DskBlockHdr cache_hdr{ BlockType::t1, 0 };
    DskKeyCacheSize cache_term = 0;
    store_->storeWrite(Writes{
        Write{ 0, gsl::as_bytes<DskDatabaseHdr>({ hdr }) },
        Write{ k_page_size / 2, gsl::as_bytes<DskBlockHdr>({ cache_hdr }) },
        Write{ k_page_size / 2 + sizeof(DskBlockHdr),
               gsl::as_bytes<DskKeyCacheSize>({ cache_term }) } });
    alloc_ = std::make_unique<Allocator>(hdr, *store_);
    keycache_ = std::make_unique<KeyCache>(
        Block{ k_page_size / 2, k_page_size / 2 }, *store_);

    auto ta = startTransaction();
    disk::ObjectW tree(ta);
    Expects(tree.addr() == k_root);
    ta.commit(tree.getWrites());
  }

  auto ta = startTransaction();
  disk::ObjectW(ta, k_root);
}

Transaction Database::startTransaction() { return Transaction(*this); };

ReadRef Database::loadPage(PageNr p) { return store_->loadPage(p); }

std::string Database::resolveKey(Key k) const {
  return keycache_->getString(k);
}

boost::optional<Key> Database::getKey(const std::string& k) const {
  return keycache_->getKey(k);
}

ReadRef Transaction::load(PageNr p) { return storage_.loadPage(p); };

Block Transaction::alloc(size_t s) { return alloc_.alloc(s); };

Block Transaction::allocExtension(Addr block, size_t s) {
  return alloc_.allocExtension(block, s);
}

void Transaction::free(Addr a) { return alloc_.free(a); }

Key Transaction::key(const std::string& s) { return kcache_.getKey(s); }

Transaction::Transaction(Database& db)
    : db(db)
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
